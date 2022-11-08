# Databricks notebook source
# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

""" config """
has_faild = False
isr = pytz.timezone('Israel')
predict_config = []

spark.sql('set spark.sql.files.ignoreMissingFiles = true')

# COMMAND ----------

print("Predicting Result")
print("-"*50)

for c in countries:
  start = datetime.datetime.now(tz=isr)
  print(f'Iteration {c} start at: {start}')
  try:
    t = dbutils.notebook.run('./kfc-id-detection-predict',0, {'country': c, 'env': env} )
    predict_config.append([c,t])
    print(f"Iteration {c} finished after")
  except Exception as e:
    
    global has_faild
    has_faild = True
    
    print(e)
    stage = 'Model Predict'
    sendEmail(c,e,start,env,stage)
    print(f"Iteration {c} failed after after")   

  end = datetime.datetime.now(tz=isr)
  print(end-start)
  print("*"*50)

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

df = reduce(DataFrame.unionAll, [spark.read.table(table[1]) for table in predict_config])

df = df.where('prediction = 1')

windowSpec  = Window.partitionBy("ifa_kfc", "prediction").orderBy(desc("match_prob"),desc('ifa_ws'))

df = df.withColumn("rank",rank().over(windowSpec)).where('rank = 1')

windowSpec  = Window.partitionBy("ifa_kfc", "prediction")

df = df.withColumn("multi",sum(col('rank')).over(windowSpec)).where('not (prediction = 1 and multi > 1)').drop('rank','multi')

windowSpec  = Window.partitionBy("ifa_ws", "prediction").orderBy(desc("match_prob"))

df = df.withColumn("rank",rank().over(windowSpec)).where('rank = 1')

windowSpec  = Window.partitionBy("ifa_ws", "prediction")

df = df.withColumn("multi",sum(col('rank')).over(windowSpec)).where('not (prediction = 1 and multi > 1)').drop('rank','multi')

# COMMAND ----------

df = df.withColumn("execution_time", lit(to_ptime(now_ts)).cast(LongType()))

# COMMAND ----------

t = db_default + f'kfc_id_detection_predict_output'
print(t)
df.write.format('delta').option("overwriteSchema", "true").mode('append').saveAsTable(t)

# COMMAND ----------

dict_table = db_default + 'kfc_id_dictionary'

dict_output = spark.read.table(t)\
                    .where(f'execution_time = {to_ptime(now_ts)}')\
                    .selectExpr('ifa_kfc as kfc_id', 'ifa_ws as ifa','execution_time as first_seen','1 as match_source')\
                    .drop_duplicates()

kfc_ifa_dict = spark.read.table(dict_table).selectExpr('kfc_id','True as ind').drop_duplicates()

dict_output = dict_output.join(kfc_ifa_dict,'kfc_id','left').where('ind is null').drop('ind')

dict_output.write.format('delta').option("overwriteSchema", "true").mode('append').saveAsTable(dict_table)

# COMMAND ----------

if env == "prod":
  redis_sdf = spark.sql(f"select kfc_id as id, ifa from {dict_table} where first_seen = {to_ptime(now_ts)} and match_source = 1")
  
  print("starts write to redis")

  redis_sdf.write.format("org.apache.spark.sql.redis").option("table", "kid").option("key.column", "id").option("host", "kfc-ifa-dictionary-0001-001.jrzbeh.0001.euc1.cache.amazonaws.com").save(mode='append')
  
  print("write to redis succeeded")

# COMMAND ----------

if has_faild:
  raise Exception("There is a problem, please see the notebook for more information")
