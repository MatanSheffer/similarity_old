# Databricks notebook source
# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

""" config """
cc = 'MYS'
target_field = 'label'

""" timeframe config """
now_ts = int(datetime.datetime.now().timestamp())
days_back = 30
e = to_pday(now_ts)
s = to_pday(now_ts,-days_back)

print(e,s)
print(now_ts)

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

ip_ifa_count = int(sdf.groupby('ip').agg(countDistinct('ifa').alias('ifa')).agg(expr(f'percentile(ifa, {1-PercentagePopulation})').alias('precentile')).collect()[0][0])

ips = sdf.groupby('ip').agg(countDistinct('ifa').alias('ifa')).where(f'ifa <= {ip_ifa_count}').withColumn('ip_size',when(col('ifa')<= 100,1)\
                                                                                                         .when(col('ifa')<= 1000,2)\
                                                                                                         .otherwise(3)
                                                                         ).drop('ifa').withColumnRenamed('ip','ip_address')

for i in ip_type:
  sdf = sdf.withColumn('ip' + i +'_endzero',when(( col('ip_type') == i ) & (col('ip').like('%.0') | col('ip').like('%::')),col('ip')))
  sdf = sdf.withColumn('ip' + i +'_endnonzero',when(( col('ip_type') == i ) & ~(col('ip').like('%.0') | col('ip').like('%::')),col('ip')))

sdf = sdf.withColumnRenamed('ip','ip_join')
sdf = sdf.withColumn('ptime',to_unix(col('ptime')))

#create new source column 
sdf = sdf.withColumn('source', when( (col('source') == 3) | (col('source') == 4) ,'kfc')\
                              .when( (col('source') == 2) | (col('source') == 1) ,'ws'))

kfc_id_dictionary = spark.read.table('kfc_id_dictionary').withColumnRenamed('ifa','dict_ifa')
sdf = sdf.join(kfc_id_dictionary,
               [ (sdf.ifa == kfc_id_dictionary.dict_ifa) | (sdf.ifa == kfc_id_dictionary.dict_ifa)]
               ,'left').where('first_seen is null').drop('kfc_id','dict_ifa','first_seen')

# COMMAND ----------

kfc_ws_join = sdf.where('ipBROADBAND_endnonzero = ip_join or ipCELLULAR_endnonzero = ip_join')\
                          .select('country_code','ifa','ip_join','device_model','ptime','source').drop_duplicates()

kfc_ws_join = kfc_ws_join.join(ips,[kfc_ws_join.ip_join == ips.ip_address]).drop('ip_address')

kfc_join = add_suffix(kfc_ws_join.where('source = "kfc" and ifa not like "%-%"') ,'_kfc')
ws_join = add_suffix(kfc_ws_join.where('source = "ws" and ifa like "%-%"') ,'_ws')

kfc_ws_join = kfc_join.join(ws_join, [
  kfc_join.country_code_kfc == ws_join.country_code_ws,
  kfc_join.ip_join_kfc == ws_join.ip_join_ws,
  kfc_join.device_model_kfc == ws_join.device_model_ws,
  kfc_join.ptime_kfc == ws_join.ptime_ws 
])

kfc_ws_join = kfc_ws_join.select('ifa_kfc','ifa_ws').drop_duplicates()

data, features_cols_new = aggregate_dataframe(sdf,ips, features_cols, ['ifa','source'])

kfc = add_suffix(data.where('source = "kfc" and ifa not like "%-%"') ,'_kfc')
ws = add_suffix(data.where('source = "ws" and ifa like "%-%"') ,'_ws')

data = kfc_ws_join.join(kfc,'ifa_kfc').join(ws,'ifa_ws')

data = data.select(sorted(data.columns))

# COMMAND ----------

data = similarly(data, features_cols_new,training = False)

# COMMAND ----------

post, key, cols = post_processing(data, features_cols_new,training = False)
post = post.select(*model_cols)

# COMMAND ----------

path = f's3://bs-ml-models/kfc-id-detection/gbt/{cc}/'
model = CrossValidatorModel.load(path)

# COMMAND ----------

post,model_features_schema,key = change_col_type(post)
post = build_sclaed_features(post,model_features_schema)

# COMMAND ----------

w = Window.partitionBy('ifa_kfc')
post = post.withColumn('numberOfMatchesPerId', count(lit(1)).over(w)).where(f'numberOfMatchesPerId < {numberOfMatchesPerId}').drop('numberOfMatchesPerId')

# COMMAND ----------

df = model.transform(post)
df = df.withColumn("match_prob", extract_prob_udf(col("probability"))).drop('features','rawPrediction','probability')

# COMMAND ----------

t = f'{db}kfc_id_detection_test_res_{cc}_{s}_{e}'
print(t)
df.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(t)
