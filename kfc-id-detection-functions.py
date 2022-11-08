# Databricks notebook source
# DBTITLE 1,Imports
import datetime, math, random, pytz, ast
import pygeohash as pgh
from pyspark.sql.functions import col, lit, pandas_udf, count, rand, when, concat, array, max,collect_set, desc, dense_rank, countDistinct, struct, greatest, split, rand, rank, expr, broadcast, collect_list,round,sum, array_except,array_intersect,array_union, size
from pyspark.sql.window import Window
from pyspark.sql.types import *
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import numpy as np
from pyspark.ml.feature import VectorAssembler,StandardScaler
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier,GBTClassifier, DecisionTreeClassifier, GBTClassificationModel, DecisionTreeRegressionModel, LogisticRegressionModel, LogisticRegression
from pyspark.ml.tuning import CrossValidator, CrossValidatorModel
from pyspark.ml import PipelineModel
from pyspark.sql.window import Window
import boto3

# COMMAND ----------

# DBTITLE 1,Functions
def aggregate_dataframe(sdf, ips ,agg_fields = ['ifa'],lists = False):
  
  dims_sdf = dimension_aggregation(sdf,agg_fields)
  dims_sdf = save_temp_table(dims_sdf,'agg_dims_sdf')
   
  ipstype = sdf.withColumn('time',when(col('ip_type').like('%CELLULAR%'),col('ptime')).otherwise(col('pday')))\
               .withColumn('ip_type',concat(lit('ip'),col('ip_type')))\
               .select(*agg_fields,'ip','ip_type','time','ip_end')\
               .withColumn('agg',concat(col('time'), lit('_'),col('ip')) ) \
               .withColumn('pivot',concat('ip_type',lit('_'),'ip_end') )\
               .groupBy(*agg_fields).pivot('pivot').agg(collect_list('agg'))
  ipstype = save_temp_table(ipstype,'agg_ipstype')
  
  bundle = sdf.select(*agg_fields,'bundle_id','ptime')\
              .withColumn('agg',concat(col('bundle_id'), lit('_'),col('ptime')))\
              .groupBy(*agg_fields).agg(collect_list('agg').alias('bundle_id'))
  bundle = save_temp_table(bundle,'agg_bundle')

  
  ips = sdf.join(ips.withColumnRenamed('ip_address','ip'),'ip')\
           .groupBy(*agg_fields)\
           .pivot('ip_size').agg(collect_set('ip').alias('ips'))
  ips = save_temp_table(ips,'agg_ips')

  apps = sdf.groupBy(*agg_fields).agg(collect_set('bundle_id').alias('apps'))
  apps = save_temp_table(apps,'agg_apps')

  stg_1 = dims_sdf.join(ipstype,agg_fields)\
  
  stg_2 = bundle.join(ips,agg_fields)\
                .join(apps,agg_fields)
  
  res = stg_1.join(stg_2,agg_fields)
  
  for c in res.columns:
    if 'endzero' in c:
      res = res.drop(c)

  return res

##################################################################################################################################

def dimension_aggregation(sdf,agg_fields):
  
  sdf = sdf.select(*agg_fields,*dimintion_cols)

  windowSpec  = Window.partitionBy(*agg_fields).orderBy(desc('count'))
  
  sdf = sdf.groupBy(sdf.columns).count()

  sdf = sdf.withColumn("rank",rank().over(windowSpec)).where('rank = 1')

  windowSpec_2  = Window.partitionBy(*agg_fields)

  sdf = sdf.withColumn("multi",sum(col('rank')).over(windowSpec_2)).where('multi = 1').drop('rank','multi','count')
  
  return sdf

##################################################################################################################################

@udf
def cos_sim(v1,v2):
    try:
        p = 2
        return float(v1.dot(v2))/float(v1.norm(p)*v2.norm(p))
    except:
        return 0.0
      
##################################################################################################################################

def to_pday(t,days=0):
  return int(datetime.datetime.fromtimestamp(t+days*60*60*24).strftime('%Y%m%d'))

##################################################################################################################################

def to_ptime(t,days=0):
  return int(datetime.datetime.fromtimestamp(t+days*60*60*24).strftime("%Y%m%d%H"))

##################################################################################################################################

@udf
def to_unix(t):
  return int(datetime.datetime.strptime(str(t), '%Y%m%d%H').timestamp())

##################################################################################################################################

#new ver
#udf_location = udf(lambda x,y,z: pgh.encode(x,y,precision=6) if (x != 0 and y != 0 and ~z) else 'NoLocation' )

udf_location = udf(lambda x,y,z: f'{x},{y}' if (x != 0 and y != 0 and ~z) else 'NoLocation' )

##################################################################################################################################

def add_suffix(df, string):
  for c in df.columns:
    df = df.withColumnRenamed(c,c+string)
  return df

##################################################################################################################################

def uniqe_ips(kfc,ws):
  
  kfc_uniqe = [ s[0].split("_")[1] for s in kfc]
  ws_uniqe = [ s[0].split("_")[1] for s in ws]

  uniqe_set = set(kfc_uniqe)&set(ws_uniqe)
  dif = set(kfc_uniqe + ws_uniqe) - uniqe_set
  
  ip_1 = 0
  ip_2 = 0
  ip_3 = 0
  
  dif_1 = 0
  dif_2 = 0
  dif_3 = 0    

  for i in uniqe_set:
    if i.split('/')[1] == '3':
      ip_3 += 1

    elif i.split('/')[1] == '2':
      ip_2 += 1

    elif i.split('/')[1] == '1':
      ip_1 += 1
  
  for i in dif:
    if i.split('/')[1] == '3':
      dif_3 += 1

    elif i.split('/')[1] == '2':
      dif_2 += 1

    elif i.split('/')[1] == '1':
      dif_1 += 1
  
  return ip_1,ip_2,ip_3,dif_1,dif_2,dif_3

##################################################################################################################################

def uniqe_apps(kfc,ws):
  
  if len(ws) == 0 or len(kfc) == 0:
    return 0,len(ws)+len(kfc)
  
  kfc_uniqe = [ s[0].split("_")[1] for s in kfc]
  ws_uniqe = [ s[0].split("_")[1] for s in ws]

  uniqe_set = set(kfc_uniqe)&set(ws_uniqe)
  dif = set(kfc_uniqe + ws_uniqe) - uniqe_set
  
  return len(uniqe_set), len(dif)

##################################################################################################################################

def cs_vector(kfc,ws):
      if len(ws) == 0 or len(kfc) == 0:
        return 0,0 
      
      keys = set([])

      for i in ws:
        keys.add(i[0])

      for i in kfc:
        keys.add(i[0])
      
      kfc = sorted(kfc, key=lambda x: x[0])
      ws = sorted(ws, key=lambda x: x[0])
      keys = sorted(keys)

      ws_new = []
      i = 0
      for k in keys:
        if(len(ws)>= i+1):
          if(k == ws[i][0]):
            ws_new.append(float(ws[i][1]))
            i = i+ 1
          else:
            ws_new.append(0)
        else:
          ws_new.append(0)


      kfc_new = []
      i = 0
      for k in keys:
        if(len(kfc)>= i+1):
          if(k == kfc[i][0]):
            kfc_new.append(float(kfc[i][1]))
            i = i+ 1
          else:
            kfc_new.append(0)
        else:
          kfc_new.append(0)

        
#       if  len(ws_new) > 1:
#         ws_new = [i - np.mean(ws_new) for i in ws_new]
#       if len(kfc_new) > 1:
#         kfc_new = [i - np.mean(kfc_new) for i in kfc_new]

      cs = cosine_similarity([kfc_new], [ws_new])[0][0]


      return cs, len(keys)
    

##################################################################################################################################

def cs_pd(pdf):

  for c in features_cols:
    pdf[c+'_cs'],pdf[c+'_len'] = zip(*pdf.apply(lambda row: cs_vector(row[c+'_kfc'],row[c+'_ws']), axis=1))
  
  pdf['apps_uniqe'],pdf['apps_dif'] = zip(*pdf.apply(lambda row: uniqe_apps(row['bundle_id_kfc'],row['bundle_id_ws']), axis=1))    
  pdf['1_uniqe'],pdf['2_uniqe'],pdf['3_uniqe'],pdf['1_dif'],pdf['2_dif'],pdf['3_dif']=  zip(*pdf.apply(lambda row: uniqe_ips(row['arr_concat_ip_kfc'],row['arr_concat_ip_ws']), axis=1) )

  return pdf

##################################################################################################################################
  
def similarly(sdf, features_cols_new, training = True,key = ['ifa_kfc','ifa_ws'], agg = ['ifa_kfc']):
  
  for i in range(1,4):
    if f'size_{i}_kfc' not in sdf.columns:
      sdf = sdf.withColumn(f'size_{i}_kfc',array())
    if f'size_{i}_ws' not in sdf.columns:
      sdf = sdf.withColumn(f'size_{i}_ws',array())

  cs = sdf.select(*key,*[f+'_kfc' for f in features_cols_new],*[f+'_ws' for f in features_cols_new])
  
  # calculating cosine similarity for each feature
  for f in features_cols:
    # apply the UDF to the column
    cs = cs.withColumn(f+"_cs", cos_sim(col(f+'_kfc') , col(f+"_ws") )).drop(f+'_kfc',f+"_ws")
    
  uniqe = sdf.drop(*[f+'_kfc' for f in features_cols_new],*[f+'_ws' for f in features_cols_new])
  
  uniqe = uniqe.withColumn('apps_uniqe', size(array_intersect('apps_ws', 'apps_kfc')))
  uniqe = uniqe.withColumn('apps_dif', size(array_union(array_except('apps_ws', 'apps_kfc'),array_except('apps_kfc','apps_ws'))))

  
  for i in range(1,4):
    uniqe = uniqe.withColumn(f'{i}_uniqe',size(array_intersect(f'size_{i}_ws', f'size_{i}_kfc')))
    uniqe = uniqe.withColumn(f'{i}_dif',size(array_union(array_except(f'size_{i}_ws', f'size_{i}_kfc'),array_except(f'size_{i}_ws', f'size_{i}_kfc'))))
    
  for c in sdf.columns:
    if 'array' in dict(sdf.dtypes)[c]:
      sdf = sdf.drop(c)
      continue
    
    for d in dimintion_cols:
      if d in dict(sdf.dtypes)[c]:
        sdf = sdf.drop(c)
        continue
  
  return uniqe.join(cs,key)

##################################################################################################################################

def post_processing(sdf, features_cols_new, training = True, key = ['ifa_kfc','ifa_ws']):
  import pyspark.sql.functions as F

  post_cols = []
  for fc in features_cols_new:
    post_cols.append(fc + '_cs')
    
  post_cols = post_cols + [f'{i}_uniqe' for i in range(1,4)] + [f'{i}_dif' for i in range(1,4)] + ['apps_uniqe', 'apps_dif']
  
  agg_expr = [F.sum(col).alias(col) for col in post_cols] 

  if training:
    post = sdf.select(*key,*post_cols,'label')
    post = post.groupBy(*key).agg(
      *agg_expr,
      F.max('label').alias('label')
    )
  else:
    post = sdf.select(*key,*post_cols)
    post = post.groupBy(*key).agg(
      *agg_expr
    )
    
  
  return post, key, post_cols

##################################################################################################################################

def change_col_type(sdf):
  key = []
  for c in sdf.dtypes:
    if c[1] == 'string':
      key.append(c[0])
    else:
      sdf = sdf.withColumn(c[0],col(c[0]).cast("double"))
      sdf = sdf.withColumn(c[0],when(col(c[0]) > 500,500).otherwise(col(c[0])))

  return sdf , sdf.drop(*key,target_field).columns, key

##################################################################################################################################

def build_sclaed_features(sdf,model_features_schema):
  # specify the input and output columns of the vector assembler
  assembler = VectorAssembler(inputCols=model_features_schema,
                             outputCol='pre_features', handleInvalid = 'keep')
  # transform the data
  sdf = assembler.transform(sdf)

  scaler = StandardScaler(inputCol="pre_features", outputCol="features",
                          withStd=True, withMean=False)

  # Compute summary statistics by fitting the StandardScaler
  scalerModel = scaler.fit(sdf)

  # Normalize each feature to have unit standard deviation.
  sdf = scalerModel.transform(sdf)
  
  return sdf.drop('pre_features')

##################################################################################################################################

def extract_prob(v):
    try:
        return float(np.max(v))  # Your VectorUDT is of length 2
    except ValueError:
        return None

extract_prob_udf = udf(extract_prob, DoubleType())

##################################################################################################################################

def runJob(country, table):
  
  import os
  import requests

  # databricks config
  token = os.getenv('databricksToken')
  URL = 'https://bsi-prod.cloud.databricks.com/api/2.0/'
  headers = {'Authorization': 'Bearer %s' % token, 'Content-Type': 'application/json'}
  jobs = [696958695393236]

  for JOB_ID in jobs:
    runNowData = { 'job_id': JOB_ID,
                   'notebook_param': {
                                        "country": country,
                                        "table": table
                                     }
                 }
    runJobAction = 'jobs/run-now'
    runNowRequest = requests.post(URL + runJobAction, json=runNowData, headers=headers)

    print('Request to start a new run sent:', runNowRequest.text)

    if runNowRequest.status_code != 200:
        res = f'Failed to trigger the job {JOB_ID}'
    else:
        res = f'Job {JOB_ID} was successfully triggered'
        
  return res

##################################################################################################################################

def sendEmail(cc,e,start,env,stage):
  end = datetime.datetime.now(tz=isr)
  region_name='eu-central-1'
  client = boto3.client('sns',region_name= region_name)
  email = 'matans@bsightful.co'
  article_arn = 'arn:aws:sns:eu-central-1:679864361151:matan-email'
  
  response = client.publish(
  TopicArn=article_arn,
  Message=f"""
      An iteration failed after {end - start}
      Iteration Started at {start}
      Stage: {stage}
      Country: {cc}
      Environment: {env}
      Exception: {e}
  
  """,
  Subject=f'KFC ID Detection has Faild - {stage}, <{env}|{cc}>',
  MessageStructure='string',
  
  #   client.subscribe(
  #   TopicArn=article_arn,
  #   Protocol='email',
  #   Endpoint=email  # <-- email address who'll receive an email.
  #   )
  )
  
##################################################################################################################################

def save_temp_table(sdf,table_name):
  isr = pytz.timezone('Israel')
  start = datetime.datetime.now(tz=isr)
  
  sdf_temp = db + f'kfc_id_detection_temp_{process}_{cc}_{env}_{table_name}'
  sdf.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(sdf_temp)

  spark.catalog.clearCache()

  spark.sql(f"optimize {sdf_temp}")
  spark.sql(f"vacuum {sdf_temp} retain 0 hours")

  sdf = spark.read.table(sdf_temp)
  end = datetime.datetime.now(tz=isr)
  
  print(f'{sdf_temp} has completed after {end-start}.')
  
  global temp_table
  temp_table.append(sdf_temp)
  
  return(sdf)

##################################################################################################################################

def clean_temp_talbes():
  global temp_table
  for t in temp_table:
    spark.sql(f'drop table {t}')
    print(f'table {t} has been deleted.')
  print(f'{len(temp_table)} tables has been deleted.')


# COMMAND ----------

# DBTITLE 1,Config
""" config """

cols = ['ifa', 'ip', 'country_code', 'device_model', 'source', 'pday', 'ip_type', 'ptime', 'bundle_id' ]

features_cols = ['ipBROADBAND_endnonzero', 'ipCORPORATE_endnonzero','ipCELLULAR_endnonzero','ipUNKNOWN_endnonzero', 'bundle_id'] 

dimintion_cols = ['country_code', 'device_model']

ip_type = ['BROADBAND', 'CORPORATE', 'CELLULAR']

model_cols = [
                'ifa_kfc', 'ifa_ws', 'bundle_id_cs', 
                'ipBROADBAND_endnonzero_cs', 'ipCELLULAR_endnonzero_cs', 'ipCORPORATE_endnonzero_cs', 'ipUNKNOWN_endnonzero_cs',
                '1_uniqe', '2_uniqe', '3_uniqe', '1_dif', '2_dif', '3_dif', 'apps_uniqe', 'apps_dif'
             ]

""" timeframe config """
now_ts = int(datetime.datetime.now().timestamp())
days_back = 14
e = to_pday(now_ts)
s = to_pday(now_ts,-days_back)


""" vars """
target_field = 'label'
countries = ['PSE', 'LBN', 'MYS', 'IDN']

numberOfMatchesPerId = 1000 # ids with over N potential matches will not be classify
PercentagePopulation = 0.001  # % of ifas will consider as anomaly

""" get environment """
try:
  env =  dbutils.widgets.get('env')
except:
  env = 'stg'
  
db = "matan_db."
db_default = db
experiment_id = 453551187289123

if env == 'prod':
  db = "kfc_id_detection_internal_db"
  db_default = ""
  experiment_id = 4296493224492826
  
print(f'env = {env}, db = {db}, db_default = {db_default}' )


""" set configuration """
# spark.sql("SET spark.databricks.io.cache.enable = False")
spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = False")
spark.sql("SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = True")
spark.sql("SET spark.sql.execution.arrow.pyspark.enabled = True")
spark.sql("SET spark.databricks.adaptive.autoOptimizeShuffle.enabled = True")
spark.sql('set spark.sql.files.ignoreMissingFiles = true')
