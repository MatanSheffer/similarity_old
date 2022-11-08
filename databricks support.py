# Databricks notebook source
# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

import numpy as np
from pyspark.ml.linalg import *
from pyspark.sql.types import * 
from pyspark.sql.functions import *



@udf
def cos_sim(v1,v2):
    try:
        p = 2
        return float(v1.dot(v2))/float(v1.norm(p)*v2.norm(p))
    except:
        return 0.0
cc = 'PSE'
db = 'matan_db.'
t = db + f'kfc_id_detection_raw_{cc}'
# features_cols = ['ipBROADBAND_endzero','ipBROADBAND_endnonzero','ipCORPORATE_endzero', 'ipCORPORATE_endnonzero', 'ipCELLULAR_endzero','ipCELLULAR_endnonzero','ipUNKNOWN_endzero','ipUNKNOWN_endnonzero', 'bundle_id'] 
features_cols = ['bundle_id'] 

bids_label = spark.read.table(t+'stam_3').where('label = True').limit(100)

# # This command will create Cosine Similarity

for f in features_cols:
  # apply the UDF to the column
  bids_label = bids_label.withColumn(f+"_cs", cos_sim(col(f+'_idf_kfc') , col(f+"_idf_kfc") ))
  
new = bids_label.select('ifa_kfc','ifa_ws',*sorted([c for c in bids_label.columns if 'bundle_id' in c])).display()

# COMMAND ----------

from pyspark.ml.feature import HashingTF, IDF, Normalizer
from pyspark.ml import Pipeline

pipesdf = agg_sdf

features_cols = ['bundle_id']

for f in features_cols:

  hashingTF = HashingTF(inputCol=f, outputCol=f + '_hash')
  idf = IDF(inputCol=f + '_hash', outputCol=f + '_idf')
#   normalizer = Normalizer(inputCol=f + '_idf', outputCol=f + '_norm')
  pipe = Pipeline(stages = [hashingTF,idf])
  fit_pipe = pipe.fit(pipesdf)
  pipesdf = fit_pipe.transform(pipesdf).drop(f,f + '_hash' ).withColumnRenamed(f + '_idf',f)



# COMMAND ----------

def aggregate_dataframe(sdf, ips ,agg_fields = ['ifa'],lists = False):
  
  dims_sdf = dimension_aggregation(sdf,agg_fields)
   
  ipstype = sdf.withColumn('time',when(col('ip_type').like('%CELLULAR%'),col('ptime')).otherwise(col('pday')))\
               .withColumn('ip_type',concat(lit('ip'),col('ip_type')))\
               .select(*agg_fields,'ip','ip_type','time','ip_end')\
               .join(ips.withColumnRenamed('ip_address','ip'),'ip')\
               .withColumn('agg',concat(col('time'), lit('_'),col('ip'),lit('/'),col('ip_size')) ) \
               .withColumn('pivot',concat('ip_type',lit('_'),'ip_end') )\
               .groupBy(*agg_fields).pivot('pivot').agg(collect_list('agg'))
  
  bundle = sdf.select(*agg_fields,'bundle_id','ptime')\
              .withColumn('agg',concat(col('bundle_id'), lit('_'),col('ptime')))\
              .groupBy(*agg_fields).agg(collect_list('agg').alias('bundle_id'))
  
  res = dims_sdf.join(ipstype,agg_fields).join(bundle,agg_fields)\

  return res

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.kfc_id_detection_predict_output
