# Databricks notebook source
# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

""" config """
try:
  cc = dbutils.widgets.get('country')
except:
  cc = 'PSE'
process = 'preprocessing'
temp_table = []

print(e,s)
print(now_ts)

t = db + f'kfc_id_detection_raw_{cc}'

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

ips = sdf.groupby('ip').agg(countDistinct('ifa').alias('ifa'))\
         .withColumn('ip_size',when(col('ifa')<= 100,'size_1')\
                              .when(col('ifa')<= 1000,'size_2')\
                              .otherwise('size_3'))\
         .drop('ifa').withColumnRenamed('ip','ip_address')
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

#fliter only detected ifas/id_kfc from dict
kfc_id_dictionary = spark.read.table('kfc_id_dictionary').withColumn('key', concat('kfc_id',lit(','),'ifa') ).drop('first_seen').withColumnRenamed('ifa','dict_ifa')

kfc_events = sdf.where('source = "kfc"')
kfc_events = kfc_events.join(kfc_id_dictionary,[(kfc_events.ifa == kfc_id_dictionary.kfc_id) | (kfc_events.ifa == kfc_id_dictionary.dict_ifa)] ).drop('kfc_id','dict_ifa')

ws_events = sdf.where('source = "ws"')
ws_events = ws_events.join(kfc_id_dictionary,ws_events.ifa == kfc_id_dictionary.dict_ifa).drop('kfc_id','dict_ifa')

sdf = kfc_events.union(ws_events)

sdf = save_temp_table(sdf,'raw_dataset')

# COMMAND ----------

# aggregate the dataframe
agg_sdf = aggregate_dataframe(sdf,ips,agg_fields = ['key','source'])

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

#sampling the data
ifas = sdf.where('source = "kfc"').selectExpr('key as key_kfc').drop_duplicates()
sample_size = min((math.ceil(3*10**4/ifas.count()*10**3)/10**3)**0.5,1.0)
ifas = ifas.sample(sample_size)

#split into sources
kfc = add_suffix(agg_sdf.where('source = "kfc"') ,'_kfc').join(ifas,'key_kfc')
ws = add_suffix(agg_sdf.where('source = "ws"') ,'_ws')

# COMMAND ----------

kfc_ws_join = sdf.where('ip_end = "endnonzero"').select('country_code','ip','device_model','ptime','source','key').drop_duplicates()

kfc_ws_join = kfc_ws_join.join(ips,[kfc_ws_join.ip == ips.ip_address]).drop('ip_address','ip_size')

# COMMAND ----------

kfc_join = add_suffix(kfc_ws_join.where('source = "kfc"') ,'_kfc').drop('source')
ws_join = add_suffix(kfc_ws_join.where('source = "ws"') ,'_ws').drop('source')

kfc_ws_join = kfc_join.join(ws_join, [
  kfc_join.country_code_kfc == ws_join.country_code_ws,
  kfc_join.ip_kfc == ws_join.ip_ws,
  kfc_join.device_model_kfc == ws_join.device_model_ws,
  kfc_join.ptime_kfc == ws_join.ptime_ws,
  kfc_join.key_kfc != ws_join.key_ws 
])

kfc_ws_join = kfc_ws_join.select('key_kfc','key_ws').drop_duplicates()

# COMMAND ----------

false_label = kfc_ws_join.join(kfc,'key_kfc').join(ws,'key_ws')

false_label = false_label.select(sorted(false_label.columns)).withColumn('label',lit(False))

# COMMAND ----------

true_label = kfc.join(ws, [
                            kfc.country_code_kfc == ws.country_code_ws,
                            kfc.key_kfc == ws.key_ws,
                            kfc.device_model_kfc == ws.device_model_ws
                          ]
                     )

true_label = true_label.select(sorted(true_label.columns)).withColumn('label',lit(True))

# COMMAND ----------

bids_label = false_label.union(true_label).drop('ifa_kfc','ifa_ws')
bids_label = bids_label.withColumnRenamed('key_kfc','ifa_kfc').withColumnRenamed('key_ws','ifa_ws')

# COMMAND ----------

# save temp table
bids_label = save_temp_table(bids_label,'joined')

# COMMAND ----------

bids_label = similarly(bids_label, features_cols)

# COMMAND ----------

post, key, cols = post_processing(bids_label, features_cols)

# COMMAND ----------

post.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(t)

spark.catalog.clearCache()

spark.sql(f"optimize {t}")
spark.sql(f"vacuum {t} retain 0 hours")
print(t)

# COMMAND ----------

print(f'sample size: {sample_size}')

# COMMAND ----------

clean_temp_talbes()

# COMMAND ----------

dbutils.notebook.exit([t,str(sample_size)])
