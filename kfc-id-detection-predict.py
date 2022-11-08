# Databricks notebook source
# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

""" config """
try:
  cc = dbutils.widgets.get('country')
except:
  cc = 'PSE'
process = 'predict'
temp_table = []

print(e,s)
print(now_ts)

spark.sql('set spark.sql.files.ignoreMissingFiles = true')

# COMMAND ----------

cond = f''' pday between {s} and {e}
            and country_code = '{cc}'
            and ip > ''
            and ip != 'unknown'
            and ifa > ''
            and ifa not in ('B602D594AFD2B0B327E07A06F36CA6A7E42546D0', '00000000-0000-0000-0000-000000000000')
            and environment_type != 0
'''

sdf = spark.read.table('bids_core').where(cond).selectExpr(*cols)

# segmenting IPs by the amount of IFA seen in each
ips = sdf.groupby('ip').agg(countDistinct('ifa').alias('ifa'))\
         .withColumn('ip_size',when(col('ifa')<= 100,'size_1')\
                              .when(col('ifa')<= 1000,'size_2')\
                              .otherwise('size_3'))\
         .withColumnRenamed('ip','ip_address')\
         .drop('ifa')
ips = save_temp_table(ips,'ip_count_ifa')

#iptype unknown
sdf = sdf.withColumn('ip_type',when(col('ip_type')>"",col('ip_type')).otherwise('UNKNOWN'))

#split to ip types
sdf = sdf.withColumn('ip_end',when((col('ip').like('%.0') | col('ip').like('%::')),lit('endzero')).otherwise('endnonzero'))

#convert to unix ts (ptime)
sdf = sdf.withColumn('ptime',to_unix(col('ptime')))

#create new source column 
sdf = sdf.withColumn('source', when( (col('source') == 3) | (col('source') == 4) ,'kfc')\
                              .when( (col('source') == 2) | (col('source') == 1) ,'ws'))

#remove existing ids at the disctionary (left exclude)
kfc_id_dictionary = spark.read.table('kfc_id_dictionary').withColumnRenamed('ifa','dict_ifa')
sdf = sdf.join(kfc_id_dictionary,[(sdf.ifa == kfc_id_dictionary.kfc_id) | (sdf.ifa == kfc_id_dictionary.dict_ifa)],'left')\
         .drop('kfc_id','dict_ifa').where('first_seen is null').drop('kfc_id','dict_ifa','first_seen')

sdf = save_temp_table(sdf,'raw_dataset')

# COMMAND ----------

#aggregate the dataframe
agg_sdf = aggregate_dataframe(sdf,ips, agg_fields = ['ifa','source'])

# save temp table
agg_sdf = save_temp_table(agg_sdf,'agg_sdf')

# COMMAND ----------

from pyspark.ml.feature import HashingTF, IDF, Normalizer
from pyspark.ml import Pipeline

pipesdf = agg_sdf
preProcStages = []

for f in features_cols:

  hashingTF = HashingTF(inputCol=f, outputCol=f + '_hash')
  idf = IDF(inputCol=f + '_hash', outputCol=f + '_idf')
  preProcStages += [hashingTF, idf]

pipe = Pipeline(stages = preProcStages)
  
fit_pipe = pipe.fit(pipesdf)

pipesdf = fit_pipe.transform(pipesdf)


for f in features_cols:
  pipesdf = pipesdf.drop(f,f + '_hash' ).withColumnRenamed(f + '_idf',f)

# COMMAND ----------

# save temp table
agg_sdf = save_temp_table(pipesdf,'tf_idf')

# COMMAND ----------

kfc_ws_join = sdf.where('ip_end = "endnonzero"').select('country_code','ip','device_model','ptime','source','ifa').drop_duplicates()

kfc_ws_join = kfc_ws_join.join(ips,[kfc_ws_join.ip == ips.ip_address]).drop('ip_address','ip_size')

kfc_join = add_suffix(kfc_ws_join.where('source = "kfc" and ifa REGEXP "^[0-9]+$"') ,'_kfc')
ws_join = add_suffix(kfc_ws_join.where('source = "ws" and ifa like "________-____-____-____-____________"') ,'_ws')

kfc_ws_join = kfc_join.join(ws_join, [
  kfc_join.country_code_kfc == ws_join.country_code_ws,
  kfc_join.ip_kfc == ws_join.ip_ws,
  kfc_join.device_model_kfc == ws_join.device_model_ws,
  kfc_join.ptime_kfc == ws_join.ptime_ws
])

kfc_ws_join = kfc_ws_join.select('ifa_kfc','ifa_ws').drop_duplicates()

# COMMAND ----------

kfc = add_suffix(agg_sdf.where('source = "kfc" and ifa REGEXP "^[0-9]+$"') ,'_kfc')
ws = add_suffix(agg_sdf.where('source = "ws" and ifa like "________-____-____-____-____________"') ,'_ws')

data = kfc_ws_join.join(kfc,'ifa_kfc').join(ws,'ifa_ws')

data = data.select(sorted(data.columns))

# save temp table
data = save_temp_table(data,'joined')

# COMMAND ----------

data = similarly(data, features_cols,training = False)

# save temp table
data = save_temp_table(data,'similarity')

# COMMAND ----------

post, key, cols = post_processing(data, features_cols,training = False)
post = post.select(*model_cols)
post,model_features_schema,key = change_col_type(post)

# COMMAND ----------

path = f's3://bs-ml-models/kfc-id-detection/{env}/{cc}/'
model = PipelineModel.load(path)

# COMMAND ----------

df = model.transform(post)

# COMMAND ----------

df = df.withColumn("match_prob", extract_prob_udf(col("probability"))).drop('features','rawPrediction','probability','pre_features')

# COMMAND ----------

df = df.where('match_prob > 0.9')

windowSpec  = Window.partitionBy("ifa_kfc", "prediction").orderBy(desc("match_prob"),desc('ifa_ws'))

df = df.withColumn("rank",rank().over(windowSpec)).where('rank = 1')

windowSpec  = Window.partitionBy("ifa_kfc", "prediction")

df = df.withColumn("multi",sum(col('rank')).over(windowSpec)).where('not (prediction = 1 and multi > 1)').drop('rank','multi')

windowSpec  = Window.partitionBy("ifa_ws", "prediction").orderBy(desc("match_prob"))

df = df.withColumn("rank",rank().over(windowSpec)).where('rank = 1')

windowSpec  = Window.partitionBy("ifa_ws", "prediction")

df = df.withColumn("multi",sum(col('rank')).over(windowSpec)).where('not (prediction = 1 and multi > 1)').drop('rank','multi')

# COMMAND ----------

t = db + f'kfc_id_detection_predict_raw_{cc}'
print(t)
df.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(t)

# COMMAND ----------

clean_temp_talbes()

# COMMAND ----------

dbutils.notebook.exit(t)
