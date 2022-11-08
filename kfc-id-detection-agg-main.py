# Databricks notebook source
# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

""" config """
t = db + 'kfc_id_detection_agg_daily' 
post_t = db + 'kfc_id_detection_agg_daily_post_processing'

""" timeframe config """
now_ts = int(datetime.datetime.now().timestamp())

# COMMAND ----------

now_ts

# COMMAND ----------


