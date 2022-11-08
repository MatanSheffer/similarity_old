# Databricks notebook source
# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.ifa_cookie_predict_output_are

# COMMAND ----------

# MAGIC %sql
# MAGIC select first_seen,match_source, count(*)
# MAGIC from kfc_id_dictionary
# MAGIC where first_seen >= 2022040200
# MAGIC group by 1,2
# MAGIC order by 1,2

# COMMAND ----------

[
  ['3344238424458108629',	'0000B6EF-8ED6-40D5-95B2-27F2A6D42589'],
  ['4591396451611338643',	'0000D2DC-9138-48AC-928D-2604753088C1'],
  ['5629816484459870737',	'00018499-FBF5-4670-ABE4-16CBADE84AD4'],
  ['1790376560229977827',	'000428C7-1918-4BAC-B959-2B049DDEA8BA'],
  ['2562101829737089941',	'0005D9BC-5B7C-432D-86BE-3A5A43E2AC77'],
  ['2675478675219190486',	'00078F43-8440-4F50-BC57-012AE18FAE66'],
  ['1048317603794359484',	'0007F5BE-51D5-4539-AFFB-4EEA9F31E0AA'],
  ['9098317494770619545',	'0008A723-2744-48AC-BB32-50AB1D4E0B78'],
  ['7095617499071814054',	'00099D19-0A00-451D-A897-3CDB27DD1310'],
  ['1767375992184994826',	'0009B57E-6420-4541-91A2-DB00140F9911'],
  ['2428601868291060555',	'000AB022-D100-42C2-A7D9-45A242BD7E3D'],
  ['5595347349404766517',	'000B1766-1E0A-4CBC-9918-ABDA4134CB15'],
  ['6191376594547726099',	'000B4E32-107A-44CD-B335-1DA8BDBB4433'],
  ['3176497327200363463',	'000BBEB1-F99F-400C-810B-3BAFFC4DAA96'],
  ['6055311952217295190',	'000D0F60-D893-4460-8487-9B387159B0F0'],
  ['1154624010553949310',	'000E11AB-8E34-46C0-95A4-87CA20D82108'],
  ['7507120775982026799',	'000E23CE-22FE-461D-AF96-086214E777B5'],
  ['1884112026729481373',	'000FA5BC-B36B-41BD-BBB6-A92E6184B3ED'],
  ['6553247487459598611',	'000FC698-BB08-4058-888E-B4AB779F9954'],
  ['1569844851966612228',	'000FF9EA-E41B-4264-B626-D4A6E8B0E350'],
  ['8753769066797184699',	'00111D9C-7814-4B7D-B29A-9AA05D32E2E2'],
  ['5608084997726501860',	'0011B779-8909-482E-B576-0D4D16FC9DD7'],
  ['4590245451390375966',	'0011C77D-D18F-4CF7-8230-9810B4F511D6'],
  ['6681463670530213804',	'0011F90D-1FF4-46FD-8C58-CA36CB98D82A'],
  ['4281281171116797819',	'00128BEC-7C6E-4C31-B94E-0A7DA2486D7A'],
  ['8667212463649574559',	'0012F775-5011-4826-AD81-2CDEC4A61BC5'],
  ['8153347116091186190',	'0012F7BF-834B-4658-85C1-3DA69D219E73'],
  ['6471016008056548451',	'00130FE2-119A-41CE-9BDE-4ABEE3B4EA93'],
  ['1602998295891688626',	'001321F9-D5E7-4864-9972-BBF86247676A'],
  ['1530312150667058574',	'00132A3E-538B-47F8-83DC-F921AA13CF97'],
  ['9099936171972320121',	'001674AE-FA3F-4BBF-978F-5CFFFEB2B0EE'],
  ['5675340311801590326',	'0016EFCF-6DEE-42B5-B454-0D491B505195'],
  ['3693969633660879844',	'0017B343-0408-4BF9-AA91-5E26A82E2050'],
  ['9210146372570304389',	'0017DEB0-C796-4F6D-9436-8ACEC20A6F0C'],
  ['2676002578193715994',	'0017E0B8-A9A5-41E7-B053-5229AF1C4776'],
  ['975872375322002204',	'00183784-6064-4D06-B5E4-477A08EE21F7'],
  ['4914954677059724973',	'001869DF-79E1-4F47-8342-785C60804D48'],
  ['3726631920399261457',	'00188CA4-0D79-4444-BF6E-862FEC15F296'],
  ['6447697152062169709',	'001932A3-6568-435C-B6F8-F50D54933569'],
  ['6624405332935834048',	'00194197-BAB7-4483-BDDA-22DC72A5349D'],
  ['9125069903358207175',	'001950B3-BDC9-463B-8EB9-2BCBCB384D97'],
  ['7463500404796545437',	'0019CCAF-03DC-4748-B17D-C2D2742317EC'],
  ['7281145397743176472',	'001A8054-24C9-47BF-BCCB-3C8227024FB0'],
  ['175109897088633524',	'001A98B4-418A-4547-A92D-923EAC365B71'],
  ['1277656783252117702',	'001B2A8F-B5AC-43FD-A8F2-A64F117A7DAC'],
  ['2606131075504494247',	'001B5EE7-2217-44C5-94AC-83F4C2486000'],
  ['2564268726801492582',	'001BBF59-ADE7-453D-8180-6FF1D55C5C70'],
  ['2813649149889028307',	'001BCC43-6C63-4A67-8D91-D35A317C2CB6'],
  ['2212643701148416999',	'001CC7A2-601A-4457-AE57-FF9D424CBF70'],
  ['2851644183508015185',	'001D69AC-49A3-4189-8888-06DB7F41E862']
]

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.kfc_id_detection_predict_raw_IDN
# MAGIC where prediction = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CREATE TABLE matan_db.kfc_id_detection_predict_output

# COMMAND ----------

# MAGIC %sql
# MAGIC select new.prediction  pred_new, old.prediction pred_old, count(*) cnt
# MAGIC from
# MAGIC (
# MAGIC select * 
# MAGIC from matan_db.tfidf_kfc_id_detection_predict_raw_mys_v3) new
# MAGIC full outer join
# MAGIC (
# MAGIC select *
# MAGIC from matan_db.kfc_id_detection_predict_raw_mys
# MAGIC ) old
# MAGIC 
# MAGIC on old.ifa_kfc = new.ifa_kfc and 
# MAGIC old.ifa_ws = new.ifa_ws
# MAGIC where new.prediction is not null and old.prediction is not null
# MAGIC group by 1,2
# MAGIC order by 3 desc

# COMMAND ----------

# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

cc = 'IDN'

s = 20220309
e = 20220323

cond = f''' pday between {s} and {e}
            and country_code = '{cc}'
            and ip > ''
            and ip != 'unknown'
            and ifa > ''
            and ifa not in ('B602D594AFD2B0B327E07A06F36CA6A7E42546D0', '00000000-0000-0000-0000-000000000000')
            and environment_type != 0
'''

sdf = spark.read.table('bids_core').where(cond).select('ifa').drop_duplicates()

sdf = sdf.withColumn('is_ifa',when(col('ifa').like('%-%'),True).otherwise(False))

kfc_id_dictionary = spark.read.table('kfc_id_dictionary').withColumnRenamed('ifa','dict_ifa')
sdf = sdf.join(kfc_id_dictionary,
               [ (sdf.ifa == kfc_id_dictionary.dict_ifa) | (sdf.ifa == kfc_id_dictionary.dict_ifa)]
               ,'left').withColumn('dict',when(col('first_seen').isNotNull(),True).otherwise(False)).drop('kfc_id','dict_ifa','first_seen')

from pyspark.sql.functions import rank,desc,round,when,col,sum, countDistinct,count
from pyspark.sql.window import Window

windowSpec  = Window.partitionBy("ifa_kfc", "prediction").orderBy(desc("match_prob"))

df = spark.read.table('matan_db.kfc_id_detection_predict_raw_idn').where('match_prob > 0.9 and prediction = 1')
df = df.withColumn("rank",rank().over(windowSpec)).where('rank = 1')

windowSpec_2  = Window.partitionBy("ifa_kfc", "prediction")

df = df.withColumn("multi",sum(col('rank')).over(windowSpec_2)).where('not (prediction = 1 and multi > 1)').drop('rank','multi')

windowSpec3  = Window.partitionBy("ifa_ws", "prediction").orderBy(desc("match_prob"))

df = df.withColumn("rank",rank().over(windowSpec3)).where('rank = 1')

windowSpec_4  = Window.partitionBy("ifa_ws", "prediction")

algo_res = df.withColumn("multi",sum(col('rank')).over(windowSpec_4)).where('not (prediction = 1 and multi > 1)').drop('rank','multi')


sdf = sdf.join(algo_res,
               [ (sdf.ifa == algo_res.ifa_kfc) | (sdf.ifa == algo_res.ifa_ws)]
               ,'left').withColumn('algo',when(col('ifa_ws').isNotNull(),True).otherwise(False)).drop('ifa_ws','ifa_kfc')


sdf.groupby('is_ifa','dict','algo').count().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select case when bids.ifa like '________-____-____-____-____________' then 'ifa'
# MAGIC             when bids.ifa REGEXP '^[0-9]+$' then 'kfc_id'
# MAGIC             else 'else'
# MAGIC       end id_type,
# MAGIC       if(events <4,'less than 4 events','more than 4 events') user_type,
# MAGIC       case when naiv.ifa is not null then 'dictionary'
# MAGIC            when ml.ifa is not null then 'algorithm'
# MAGIC       else 'potential' end as output_type,
# MAGIC       count(distinct bids.ifa) ifas,
# MAGIC       round(mean(events)) mean_events
# MAGIC from
# MAGIC (
# MAGIC     select ifa, count(*) events
# MAGIC     from bids_core
# MAGIC     where country_code = 'IDN'
# MAGIC           and pday between 20220309 and 20220323
# MAGIC           and source in (3,4)
# MAGIC    group by 1
# MAGIC ) bids
# MAGIC 
# MAGIC left join
# MAGIC 
# MAGIC (
# MAGIC     select distinct ifa
# MAGIC     from
# MAGIC 
# MAGIC     (
# MAGIC     select ifa_kfc ifa
# MAGIC     from
# MAGIC     (
# MAGIC       select *, sum(rank_kfc) over (partition by ifa_kfc order by match_prob desc) multi
# MAGIC       from
# MAGIC       (
# MAGIC       select *, 
# MAGIC             rank() over (partition by ifa_kfc order by match_prob desc) rank_kfc,
# MAGIC             rank() over (partition by ifa_ws order by match_prob desc) rank_ws
# MAGIC 
# MAGIC       from matan_db.kfc_id_detection_predict_raw_idn
# MAGIC       where prediction = 1
# MAGIC             and match_prob > 0.9)
# MAGIC       where rank_kfc = 1 and rank_ws = 1 )
# MAGIC     where multi =1
# MAGIC 
# MAGIC     union all 
# MAGIC 
# MAGIC     select ifa_ws ifa
# MAGIC     from
# MAGIC     (
# MAGIC       select *, sum(rank_kfc) over (partition by ifa_kfc order by match_prob desc) multi
# MAGIC       from
# MAGIC       (
# MAGIC       select *, 
# MAGIC             rank() over (partition by ifa_kfc order by match_prob desc) rank_kfc,
# MAGIC             rank() over (partition by ifa_ws order by match_prob desc) rank_ws
# MAGIC 
# MAGIC       from matan_db.kfc_id_detection_predict_raw_idn
# MAGIC       where prediction = 1
# MAGIC             and match_prob > 0.9)
# MAGIC       where rank_kfc = 1 and rank_ws = 1 )
# MAGIC     where multi =1
# MAGIC 
# MAGIC     )
# MAGIC ) ml
# MAGIC on bids.ifa = ml.ifa
# MAGIC 
# MAGIC left join
# MAGIC (
# MAGIC     select distinct ifa
# MAGIC     from
# MAGIC     (
# MAGIC       select kfc_id ifa
# MAGIC       from kfc_id_dictionary
# MAGIC       where first_seen <= 2022032323
# MAGIC 
# MAGIC       union all
# MAGIC 
# MAGIC       select ifa
# MAGIC       from kfc_id_dictionary
# MAGIC       where first_seen <= 2022032323
# MAGIC     )
# MAGIC ) naiv
# MAGIC on bids.ifa = naiv.ifa
# MAGIC group by 1,2,3
# MAGIC order by 3,2,1

# COMMAND ----------

tukh%sql
select * from matan_db.kfc_id_detection_predict_agg_temp_IDN

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct ifa) ifas
# MAGIC from bids_core
# MAGIC where pday between 20220101 and 20220201
# MAGIC       and country_code = 'MYS'

# COMMAND ----------

@udf(FloatType())
def cs(col1, col2):
  if len(col1) == 0 or len(col2) == 0:
    return float(0)
  
  col1_keys = [i[0] for i in sorted(col1)]
  print(sorted(col1), "\n")
  col1_val = [i[1] for i in sorted(col1)]
  print("col1_keys: ", col1_keys, "\n", "col1_val:", col1_val)

  col2_keys = [i[0] for i in sorted(col2)]
  col2_val = [i[1] for i in sorted(col2)]
  print("col2_keys: ", col2_keys, "\n", "col2_val:", col2_val)
  
  index = sorted(set([*col1_keys, *col2_keys]))
  len_index = len(index)
  print(index, len_index)
  
  col1_index = list(index.index(i) for i in col1_keys)
  col2_index = list(index.index(i) for i in col2_keys)
  
  print(col1_index, "\n", col1_val, "\n")
  print(col2_index, "\n", col2_val)

  from pyspark.mllib.linalg import Vectors
  sparse1 = Vectors.sparse(len_index, col1_index, col1_val)
  sparse2 = Vectors.sparse(len_index, col2_index, col2_val)
  
  from sklearn.metrics.pairwise import cosine_similarity
  import math
  # cosine_similarity(sparse1, sparse2)
  def norm(vector):
      return math.sqrt(sum(x * x for x in vector))    

  def cosine_similarity(vec_a, vec_b):
    norm_a = norm(vec_a)
    norm_b = norm(vec_b)
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    return dot / (norm_a * norm_b)

  return float(cosine_similarity(sparse1,sparse2))

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.kfc_id_detection_raw_pse_20220208_20220310

# COMMAND ----------

4922376*0.001

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct ifa) ifas
# MAGIC from bids_core
# MAGIC where pday between 20220101 and 20220201
# MAGIC       and country_code = 'PSE'

# COMMAND ----------

# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

""" config """
cc = 'IDN'

""" timeframe config """
now_ts = int(datetime.datetime.now().timestamp())
days_back = 30
e = to_pday(now_ts)
s = to_pday(now_ts,-days_back)


cond = f''' pday between {s} and {e}
            and country_code = '{cc}'
            and ip > ''
            and ip != 'unknown'
            and ifa > ''
            and ifa not in ('B602D594AFD2B0B327E07A06F36CA6A7E42546D0', '00000000-0000-0000-0000-000000000000')
            and environment_type != 0
'''
df = spark.read.table('bids_core').where(cond).select('ip','ifa').drop_duplicates()
df = df.groupby('ip').agg(countDistinct('ifa').alias('ifa'))
df1 = df.agg(expr('percentile(ifa, array(0.98,0.99,0.995,0.999))').alias('precentile'))
df1.cache
df1.display()

# COMMAND ----------

""" config """
cc = 'PSE'

""" timeframe config """
now_ts = int(datetime.datetime.now().timestamp())
days_back = 30
e = to_pday(now_ts)
s = to_pday(now_ts,-days_back)


cond = f''' pday between {s} and {e}
            and country_code = '{cc}'
            and ip > ''
            and ip != 'unknown'
            and ifa > ''
            and ifa not in ('B602D594AFD2B0B327E07A06F36CA6A7E42546D0', '00000000-0000-0000-0000-000000000000')
            and environment_type != 0
'''
df = spark.read.table('bids_core').where(cond).select('ip','ifa').drop_duplicates()
df = df.groupby('ip').agg(countDistinct('ifa').alias('ifa')).agg(expr('percentile(ifa, 0.999)').alias('precentile')).collect()[0][0]
df2= df
df2.cache
df2.display()

# COMMAND ----------

c = df2.collect()[0][0]
print(c)

# COMMAND ----------

int(c)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select prediction,count(*) cnt, count(distinct ifa_kfc), count(distinct ifa_ws)
# MAGIC from matan_db.kfc_id_detection_test_res_IDN_20220121_20220220
# MAGIC group by 1

# COMMAND ----------

arr = [
  ['3344238424458108629',	'0000B6EF-8ED6-40D5-95B2-27F2A6D42589'],
['4591396451611338643',	'0000D2DC-9138-48AC-928D-2604753088C1'],
['5629816484459870737',	'00018499-FBF5-4670-ABE4-16CBADE84AD4'],
['1790376560229977827',	'000428C7-1918-4BAC-B959-2B049DDEA8BA'],
['2562101829737089941',	'0005D9BC-5B7C-432D-86BE-3A5A43E2AC77'],
['2675478675219190486',	'00078F43-8440-4F50-BC57-012AE18FAE66'],
['1048317603794359484',	'0007F5BE-51D5-4539-AFFB-4EEA9F31E0AA'],
['9098317494770619545',	'0008A723-2744-48AC-BB32-50AB1D4E0B78'],
['7095617499071814054',	'00099D19-0A00-451D-A897-3CDB27DD1310'],
['1767375992184994826',	'0009B57E-6420-4541-91A2-DB00140F9911'],
['2428601868291060555',	'000AB022-D100-42C2-A7D9-45A242BD7E3D'],
['5595347349404766517',	'000B1766-1E0A-4CBC-9918-ABDA4134CB15'],
['6191376594547726099',	'000B4E32-107A-44CD-B335-1DA8BDBB4433'],
['3176497327200363463',	'000BBEB1-F99F-400C-810B-3BAFFC4DAA96'],
['6055311952217295190',	'000D0F60-D893-4460-8487-9B387159B0F0'],
['1154624010553949310',	'000E11AB-8E34-46C0-95A4-87CA20D82108'],
['7507120775982026799',	'000E23CE-22FE-461D-AF96-086214E777B5'],
['1884112026729481373',	'000FA5BC-B36B-41BD-BBB6-A92E6184B3ED'],
['6553247487459598611',	'000FC698-BB08-4058-888E-B4AB779F9954'],
['1569844851966612228',	'000FF9EA-E41B-4264-B626-D4A6E8B0E350'],
['8753769066797184699',	'00111D9C-7814-4B7D-B29A-9AA05D32E2E2'],
['5608084997726501860',	'0011B779-8909-482E-B576-0D4D16FC9DD7'],
['4590245451390375966',	'0011C77D-D18F-4CF7-8230-9810B4F511D6'],
['6681463670530213804',	'0011F90D-1FF4-46FD-8C58-CA36CB98D82A'],
['4281281171116797819',	'00128BEC-7C6E-4C31-B94E-0A7DA2486D7A'],
['8667212463649574559',	'0012F775-5011-4826-AD81-2CDEC4A61BC5'],
['8153347116091186190',	'0012F7BF-834B-4658-85C1-3DA69D219E73'],
['6471016008056548451',	'00130FE2-119A-41CE-9BDE-4ABEE3B4EA93'],
['1602998295891688626',	'001321F9-D5E7-4864-9972-BBF86247676A'],
['1530312150667058574',	'00132A3E-538B-47F8-83DC-F921AA13CF97'],
['9099936171972320121',	'001674AE-FA3F-4BBF-978F-5CFFFEB2B0EE'],
['5675340311801590326',	'0016EFCF-6DEE-42B5-B454-0D491B505195'],
['3693969633660879844',	'0017B343-0408-4BF9-AA91-5E26A82E2050'],
['9210146372570304389',	'0017DEB0-C796-4F6D-9436-8ACEC20A6F0C'],
['2676002578193715994',	'0017E0B8-A9A5-41E7-B053-5229AF1C4776'],
['975872375322002204',	'00183784-6064-4D06-B5E4-477A08EE21F7'],
['4914954677059724973',	'001869DF-79E1-4F47-8342-785C60804D48'],
['3726631920399261457',	'00188CA4-0D79-4444-BF6E-862FEC15F296'],
['6447697152062169709',	'001932A3-6568-435C-B6F8-F50D54933569'],
['6624405332935834048',	'00194197-BAB7-4483-BDDA-22DC72A5349D'],
['9125069903358207175',	'001950B3-BDC9-463B-8EB9-2BCBCB384D97'],
['7463500404796545437',	'0019CCAF-03DC-4748-B17D-C2D2742317EC'],
['7281145397743176472',	'001A8054-24C9-47BF-BCCB-3C8227024FB0'],
['175109897088633524',	'001A98B4-418A-4547-A92D-923EAC365B71'],
['1277656783252117702',	'001B2A8F-B5AC-43FD-A8F2-A64F117A7DAC'],
['2606131075504494247',	'001B5EE7-2217-44C5-94AC-83F4C2486000'],
['2564268726801492582',	'001BBF59-ADE7-453D-8180-6FF1D55C5C70'],
['2813649149889028307',	'001BCC43-6C63-4A67-8D91-D35A317C2CB6'],
['2212643701148416999',	'001CC7A2-601A-4457-AE57-FF9D424CBF70'],
['2851644183508015185',	'001D69AC-49A3-4189-8888-06DB7F41E862']
     ]

cc = 'IDN'
s = 20220316
e = 20220330 

i = 0
query = ''
for a in arr:
  i = i+1

  query = query + f"""
  select *
  from
  (
    select {i} as index, timestamp, ptime, ifa, origin_latitude as latitude, origin_longitude as longitude,is_centroid, ip,bundle_id, connection_type, ip_type, gps_accuracy, url
    from bids_core
    where country_code = '{cc}'
    and pday between {s} and {e}
    and ifa in ('{a[0]}','{a[1]}')
    order by timestamp
    )

  union all
  """
  

sdf = spark.sql(query[:-12])

t = f'matan_db.table_for_qa_kfc_id_detection_{cc}_{s}_{e}'
print(t)
sdf.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(t)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.table_for_qa_kfc_id_detection_IDN_20220316_20220330
# MAGIC order by 1,2

# COMMAND ----------

a = [['c','2'],['d','2']]

success = [ c[0] for c in a ]

b = ['c','d']
if success == b:
  print('YESS')

# COMMAND ----------

# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

spark.catalog.clearCache()

spark.sql(f"optimize {t}")
spark.sql(f"vacuum {t} retain 0 hours")

# COMMAND ----------

cc = 'MYS'

s = 20220129
e = 20220228

cond = f''' pday between {s} and {e}
            and country_code = '{cc}'
            and ip > ''
            and ip != 'unknown'
            and ifa > ''
            and ifa not in ('B602D594AFD2B0B327E07A06F36CA6A7E42546D0', '00000000-0000-0000-0000-000000000000')
            and environment_type != 0
'''

sdf = spark.read.table('bids_core').where(cond).select('ifa').drop_duplicates()

sdf = sdf.withColumn('is_ifa',when(col('ifa').like('%-%'),True).otherwise(False))

kfc_id_dictionary = spark.read.table('kfc_id_dictionary').withColumnRenamed('ifa','dict_ifa')
sdf = sdf.join(kfc_id_dictionary,
               [ (sdf.ifa == kfc_id_dictionary.dict_ifa) | (sdf.ifa == kfc_id_dictionary.dict_ifa)]
               ,'left').withColumn('dict',when(col('first_seen').isNotNull(),True).otherwise(False)).drop('kfc_id','dict_ifa','first_seen')

from pyspark.sql.functions import rank,desc,round,when,col,sum, countDistinct,count
from pyspark.sql.window import Window

windowSpec  = Window.partitionBy("ifa_kfc", "prediction").orderBy(desc("match_prob"))

df = spark.read.table('matan_db.kfc_id_detection_test_res_MYS_20220129_20220228').where('match_prob > 0.9 and prediction = 1')
df = df.withColumn("rank",rank().over(windowSpec)).where('rank = 1')

windowSpec_2  = Window.partitionBy("ifa_kfc", "prediction")

df = df.withColumn("multi",sum(col('rank')).over(windowSpec_2)).where('not (prediction = 1 and multi > 1)').drop('rank','multi')

windowSpec3  = Window.partitionBy("ifa_ws", "prediction").orderBy(desc("match_prob"))

df = df.withColumn("rank",rank().over(windowSpec3)).where('rank = 1')

windowSpec_4  = Window.partitionBy("ifa_ws", "prediction")

algo_res = df.withColumn("multi",sum(col('rank')).over(windowSpec_4)).where('not (prediction = 1 and multi > 1)').drop('rank','multi')


sdf = sdf.join(algo_res,
               [ (sdf.ifa == algo_res.ifa_kfc) | (sdf.ifa == algo_res.ifa_ws)]
               ,'left').withColumn('algo',when(col('ifa_ws').isNotNull(),True).otherwise(False)).drop('ifa_ws','ifa_kfc')


sdf.groupby('is_ifa','dict','algo').count().display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *
# MAGIC from matan_db.kfc_id_detection_test_res_MYS_20220129_20220228

# COMMAND ----------

# MAGIC %sql
# MAGIC select if(bids.ifa > '' ,True,False) bids,
# MAGIC        if(res.ifa_kfc > '' or res.ifa_ws >'' ,True,False) res,
# MAGIC        if(dict.kfc_id > '' or dict.ifa >'' ,True,False) dict,
# MAGIC        count(distinct bids.ifa)
# MAGIC from (
# MAGIC         select ifa
# MAGIC         from bids_core
# MAGIC         where country_code = 'PSE' 
# MAGIC               and pday between 20220107 and 20220206
# MAGIC               and ip > ''
# MAGIC               and ip != 'unknown'
# MAGIC               and ifa > ''
# MAGIC               and ifa not in ('B602D594AFD2B0B327E07A06F36CA6A7E42546D0', '00000000-0000-0000-0000-000000000000')
# MAGIC               and environment_type != 0
# MAGIC               and source = 4
# MAGIC         group by 1
# MAGIC ) bids
# MAGIC left join
# MAGIC (
# MAGIC       select * from matan_db.kfc_id_detection_test_res_pse_20220107_20220206
# MAGIC       where prediction = 1
# MAGIC ) res
# MAGIC on bids.ifa = res.ifa_kfc or bids.ifa = res.ifa_ws
# MAGIC left join
# MAGIC (
# MAGIC       select * from kfc_id_dictionary
# MAGIC       where first_seen <2022020700
# MAGIC ) dict
# MAGIC on bids.ifa = dict.kfc_id or bids.ifa = dict.ifa
# MAGIC group by 1,2,3
# MAGIC order by 4 desc

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql 
# MAGIC select 'before','ws' as source,count(*)
# MAGIC from matan_db.kfc_id_detection_raw_ws_agg_temp_MYS
# MAGIC group by 2
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 'before','kfc',count(*)
# MAGIC from matan_db.kfc_id_detection_raw_kfc_agg_temp_MYS
# MAGIC group by 2
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 'after','matches',count(*)
# MAGIC from matan_db.kfc_id_detection_raw_MYS_20220207_20220309

# COMMAND ----------

# MAGIC %sql
# MAGIC select case when different_hours = 1 then '1'
# MAGIC             when different_hours <4 then '2-3'
# MAGIC             when different_hours < 8 then '4-7'
# MAGIC             when different_hours < 15 then '8-14'
# MAGIC             else 'more than 15'
# MAGIC         end as days,
# MAGIC        if(bids.ifa like '%-%','IFA','KFC-ID') id_type,
# MAGIC        count(distinct bids.ifa) ifas
# MAGIC        
# MAGIC from (
# MAGIC         select ifa,count(distinct pday) different_hours
# MAGIC         from bids_core
# MAGIC         where country_code = 'PSE' 
# MAGIC               and pday between 20220107 and 20220206
# MAGIC               and ip > ''
# MAGIC               and ip != 'unknown'
# MAGIC               and ifa > ''
# MAGIC               and ifa not in ('B602D594AFD2B0B327E07A06F36CA6A7E42546D0', '00000000-0000-0000-0000-000000000000')
# MAGIC               and environment_type != 0
# MAGIC               and source = 4
# MAGIC         group by 1
# MAGIC ) bids
# MAGIC left join
# MAGIC (
# MAGIC       select * from matan_db.kfc_id_detection_test_res_pse_20220107_20220206
# MAGIC ) res
# MAGIC on bids.ifa = res.ifa_kfc or bids.ifa = res.ifa_ws
# MAGIC left join
# MAGIC (
# MAGIC       select * from kfc_id_dictionary
# MAGIC       where first_seen <2022020700
# MAGIC ) dict
# MAGIC on bids.ifa = dict.kfc_id or bids.ifa = dict.ifa
# MAGIC where if(dict.kfc_id > '' or dict.ifa >'' ,True,False) = False
# MAGIC       and if(res.ifa_kfc > '' or res.ifa_ws >'' ,True,False) = False
# MAGIC group by 1,2
# MAGIC 
# MAGIC order by 1,2

# COMMAND ----------

# MAGIC %sql
# MAGIC select ceil(match_prob*20)/20 p,if(0_uniqe + 1_uniqe + 2_uniqe + 3_uniqe > 1,TRuE ,FalSe) ind, count(*), count(distinct ifa_kfc), count(distinct ifa_ws)
# MAGIC from matan_db.kfc_id_detection_test_res_pse_20220107_20220206
# MAGIC where prediction = 1
# MAGIC group by 1,2
# MAGIC order by 1,2

# COMMAND ----------

# MAGIC %sql
# MAGIC select ifa_kfc, count(*)
# MAGIC from matan_db.kfc_id_detection_test_res_pse_20220107_20220206
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select if (ifas > 500,500,ceil(ifas/50)*50) ifas, count(distinct ip) ips
# MAGIC from
# MAGIC (
# MAGIC   select ip,count(distinct ifa) ifas
# MAGIC   from bids_core
# MAGIC   where pday = 20220209
# MAGIC   group by 1)
# MAGIC group by 1
# MAGIC order by 1

# COMMAND ----------

import math, numpy as np
math.floor(500*np.random.power(1))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from matan_db.kfc_

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from matan_db.kfc_id_detection_test_res_PSE_20220110_20220209
# MAGIC where ifa_kfc like '1005069624204786383'
# MAGIC -- and ifa_ws = '9C318F7A-D9D5-434E-B5A2-6DF7B96AC07F'
# MAGIC order by prediction desc,match_prob desc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from bids_core
# MAGIC where ifa in (
# MAGIC '5443552405021142013',
# MAGIC '38800ECE-0184-4E57-8E25-9297C547380B'
# MAGIC )
# MAGIC and country_code = 'PSE'
# MAGIC and pday between 20220107 and 20220206
# MAGIC order by timestamp

# COMMAND ----------

from pyspark.sql.functions import rank,desc,round,when,col,sum, countDistinct,count
from pyspark.sql.window import Window

df = spark.read.table('matan_db.kfc_id_detection_test_res_MYS_20220129_20220228').where('match_prob > 0.9')
df = df.withColumn("rank",rank().over(windowSpec)).where('rank = 1')

windowSpec_2  = Window.partitionBy("ifa_kfc", "prediction")

df = df.withColumn("multi",sum(col('rank')).over(windowSpec_2)).where('not (prediction = 1 and multi > 1)').drop('rank','multi')

windowSpec3  = Window.partitionBy("ifa_ws", "prediction").orderBy(desc("match_prob"))

df = df.withColumn("rank",rank().over(windowSpec3)).where('rank = 1')

windowSpec_4  = Window.partitionBy("ifa_ws", "prediction")

df = df.withColumn("multi",sum(col('rank')).over(windowSpec_4)).where('not (prediction = 1 and multi > 1)').drop('rank','multi')


df.display()                                                                    

# COMMAND ----------

# MAGIC %sql
# MAGIC select prediction, count(*)
# MAGIC from matan_db.kfc_id_detection_test_res_MYS_20220129_20220228
# MAGIC group by 1

# COMMAND ----------

from pyspark.sql.functions import rank,desc,round,when,col,sum, countDistinct,count, lit

# COMMAND ----------

df.groupby('prediction').agg(countDistinct('ifa_kfc'),countDistinct('ifa_ws'), count(lit(1))).show()

# COMMAND ----------

from pyspark.sql.functions import rank,desc,round,when,col
from pyspark.sql.window import Window

windowSpec  = Window.partitionBy("ifa_kfc", "prediction").orderBy(desc("match_prob"),desc('ifa_ws'))

df = spark.read.table('matan_db.kfc_id_detection_test_res_MYS_20220129_20220228').where('match_prob > 0.9')
df = df.withColumn("rank",rank().over(windowSpec)).where('rank = 1')

dic = spark.read.table('kfc_id_dictionary')#.where('first_seen > 2022020923')

res = df.select('ifa_kfc','ifa_ws','prediction').join(dic,[df.ifa_kfc == dic.kfc_id])

res = res.withColumn('label',when(col('ifa_ws') == col('ifa'),1).otherwise(0))

res.groupby('prediction').pivot('label').count().display()

# COMMAND ----------

(2574 + 385)/(2574+385+24+35)

# COMMAND ----------

df.groupby(round('match_prob',2).alias('match_prob'),'prediction').count().orderBy('match_prob','prediction').display()

# COMMAND ----------

df.groupby(round('match_prob',2).alias('match_prob'),'prediction').count().orderBy('match_prob','prediction').display()

# COMMAND ----------

df.where('prediction = 1 and match_prob >0.9').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   case
# MAGIC     when rtb.ifa > '' and sdk.ifa > '' then 'has a match'
# MAGIC     when sdk.ifa > '' then 'only sdk'
# MAGIC     when rtb.ifa > '' then 'only rtb'
# MAGIC     else 'niether'
# MAGIC   end as group,
# MAGIC   count (distinct ifnull(rtb.ifa, sdk.ifa)) ifas
# MAGIC from
# MAGIC   (
# MAGIC     select *
# MAGIC     from
# MAGIC       (
# MAGIC         select *
# MAGIC         from
# MAGIC           (
# MAGIC             select *,rank() over( partition by ifa order by cnt desc,device_model) rank
# MAGIC             from
# MAGIC               (
# MAGIC                 select ifa, device_model,count(*) cnt
# MAGIC                 from
# MAGIC                   bids_core
# MAGIC                 where
# MAGIC                   country_code = 'PSE'
# MAGIC                   and pday between 20220107
# MAGIC                   and 20220206
# MAGIC                   and ip > ''
# MAGIC                   and ip != 'unknown'
# MAGIC                   and ifa > ''
# MAGIC                   and ifa not in (
# MAGIC                     'B602D594AFD2B0B327E07A06F36CA6A7E42546D0',
# MAGIC                     '00000000-0000-0000-0000-000000000000'
# MAGIC                   )
# MAGIC                   and environment_type != 0
# MAGIC                   and source in (1, 2)
# MAGIC                 group by
# MAGIC                   1,
# MAGIC                   2
# MAGIC               )
# MAGIC           )
# MAGIC         where
# MAGIC           rank = 1
# MAGIC       )
# MAGIC   ) rtb full  outer join 
# MAGIC   
# MAGIC   (
# MAGIC     select sdk_1.ifa, sdk_1.device_model
# MAGIC     from
# MAGIC       (
# MAGIC         select a.ifa, device_model , different_days
# MAGIC         from
# MAGIC           (
# MAGIC             select *, rank() over( partition by ifa order by   cnt desc,   device_model) rank
# MAGIC             from
# MAGIC               (
# MAGIC                 select ifa, device_model, count(*) cnt
# MAGIC                 from
# MAGIC                   bids_core
# MAGIC                 where
# MAGIC                   country_code = 'PSE'
# MAGIC                   and pday between 20220107
# MAGIC                   and 20220206
# MAGIC                   and ip > ''
# MAGIC                   and ip != 'unknown'
# MAGIC                   and ifa > ''
# MAGIC                   and ifa not in ('B602D594AFD2B0B327E07A06F36CA6A7E42546D0','00000000-0000-0000-0000-000000000000')
# MAGIC                   and environment_type != 0
# MAGIC                   and source = 4
# MAGIC                 group by
# MAGIC                   1,2
# MAGIC               )
# MAGIC           ) a
# MAGIC           join 
# MAGIC           (
# MAGIC             select ifa, count(distinct pday) different_days
# MAGIC             from bids_core
# MAGIC             where country_code = 'PSE'
# MAGIC                   and pday between 20220107 and 20220206
# MAGIC                   and ip > ''
# MAGIC                   and ip != 'unknown'
# MAGIC                   and ifa > ''
# MAGIC                   and ifa not in ('B602D594AFD2B0B327E07A06F36CA6A7E42546D0','00000000-0000-0000-0000-000000000000')
# MAGIC                   and environment_type != 0
# MAGIC                   and source = 4
# MAGIC             group by 1
# MAGIC           ) b 
# MAGIC           on a.ifa = b.ifa
# MAGIC         where rank = 1
# MAGIC               and different_days > 3
# MAGIC 
# MAGIC       ) sdk_1
# MAGIC       left join 
# MAGIC       (
# MAGIC         select
# MAGIC           *
# MAGIC         from matan_db.kfc_id_detection_test_res_pse_20220107_20220206
# MAGIC       ) res 
# MAGIC       on sdk_1.ifa = res.ifa_kfc or sdk_1.ifa = res.ifa_ws
# MAGIC       left join 
# MAGIC       (
# MAGIC         select * 
# MAGIC         from kfc_id_dictionary
# MAGIC         where first_seen < 2022020700
# MAGIC       ) dict 
# MAGIC       on sdk_1.ifa = dict.kfc_id or sdk_1.ifa = dict.ifa
# MAGIC     where if(dict.kfc_id > '' or dict.ifa > '',True,False) = False
# MAGIC           and if(res.ifa_kfc > '' or res.ifa_ws > '',True,False) = False
# MAGIC   ) 
# MAGIC   sdk 
# MAGIC   on rtb.device_model = sdk.device_model
# MAGIC group by  1
# MAGIC order by  2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select device_model, count(*) cnt
# MAGIC   from
# MAGIC 
# MAGIC   group by 1
# MAGIC   order by 2 desc

# COMMAND ----------

arr1 =  np.array([
  [1641574800,33.877,35.489, 1],
  [1641578400,33.833,35.833, 2],
  [1641578400,33.877,35.489, 2],
  [1641578400,33.893,35.469, 1],
  [1641589200,33.877,35.489, 1],
  [1641607200,33.877,35.489, 2],
  [1641610800,33.895,35.468, 3]
])

arr2 =  np.array([
  [1641661200,43.887,36.509, 1],
  [1641661200,43.89,36.5, 1], 
  [1641664800,43.887,36.509, 2],
  [1641664800,43.89,36.5, 1],
  [1641747600,43.887,36.509, 2],
  [1641747600,43.89,36.5, 3],
  [1641751200,43.89,36.5, 1],
  [1641834000,43.877,36.489, 1], 
  [1641834000,43.887,36.509, 1], 
  [1641834000,43.89,36.5, 1], 
  [1641920400,43.89,36.5, 2]
])

arr3 = np.array([
  [1641574800 + 60 ,33.877 + 0.01 , 35.489 - 0.1, 1],
  [1641578400 + 60 ,33.877 + 0.01 , 35.489 - 0.1, 2],
  [1641578400 + 60 ,33.893 + 0.01 , 35.469 - 0.1, 1],
  [1641607200 + 60 ,33.877 + 0.01 , 35.489 - 0.1, 2],
  [1641610800 + 60 ,33.895 + 0.01 , 35.468 - 0.1, 3]
])

arr1 = arr1
arr2 = arr2
arr3 = arr3


# COMMAND ----------

 np.set_printoptions(formatter={'float_kind':'{:f}'.format})

# COMMAND ----------

print(np.around(np.mean(arr1, axis=0),4))
print(np.around(np.mean(arr2, axis=0),4))
print(np.around(np.mean(arr3, axis=0),4))

# COMMAND ----------

from scipy.stats import ks_2samp
for i in range(4):
  print( ks_2samp(arr1[:, i],arr2[:, i]))

# COMMAND ----------

from scipy.stats import ks_2samp
for i in range(4):
  print( ks_2samp(arr1[:, i],arr3[:, i]))

# COMMAND ----------

import hoggorm as ho
import numpy as np
# Generate some random data. Note that number of rows must match across arrays
# arr1 = np.random.rand(50, 100)
# arr2 = np.random.rand(50, 20)
# arr3 = np.random.rand(50, 500)
# Center the data before computation of RV coefficients
arr1_cent = arr1 #(arr1 - np.mean(arr1, axis=0) ) / np.linalg.norm(arr1)
arr2_cent = arr2 #(arr2 - np.mean(arr2, axis=0)) / np.linalg.norm(arr2)
arr3_cent = arr3 #(arr3 - np.mean(arr3, axis=0)) / np.linalg.norm(arr3)
# Compute RV matrix correlation coefficients on mean centered data
rv_results = ho.RVcoeff([arr1_cent, arr2_cent, arr3_cent])

rv_results


# COMMAND ----------

arr1

# COMMAND ----------

# MAGIC %sql
# MAGIC select label,count(*) from matan_db.kfc_id_detection_raw group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select label,count(*) from matan_db.kfc_id_detection_raw group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE kfc_id_dictionary
# MAGIC ZORDER BY (kfc_id)

# COMMAND ----------

# MAGIC %sql select * from matan_db.kfc_id_detection_test_res_PSE_20220107_20220206

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *
# MAGIC from matan_db.kfc_id_detection_test_res_PSE_20220107_20220206  
# MAGIC where match_prob > 0.9
# MAGIC       and prediction = 1

# COMMAND ----------

# MAGIC %sql select prediction, count(*) cnt, count(distinct ifa_kfc) ids_un from matan_db.kfc_id_detection_test_res_IDN_20220121_20220220  group by 1

# COMMAND ----------

# MAGIC %sql select round(match_prob,2) match_prob,prediction, count(*) from matan_db.kfc_id_detection_test_res_MYS_20220129_20220228 group by 1,2 order by 1,2

# COMMAND ----------

# MAGIC %sql select prediction, count(*) from matan_db.kfc_id_detection_test_res_PSE_20220107_20220206 where match_prob > 0.8 group by 1

# COMMAND ----------

t = db + 'kfc_id_detection_raw'
ml_table = db + 'kfc_id_detection_ml_flow'
# ML Flow
ml = spark.read.table(t).drop('ifa_kfc','ifa_ws')
ml = ml.where('label=True').union(ml.where('label = False').limit(ml.where('label=True').count()))

ml.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(ml_table+'_balanced')

# COMMAND ----------

# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from matan_db.kfc_id_detection_test_res_PSE_20220101_20220131

# COMMAND ----------

# MAGIC %sql
# MAGIC select predictions, count(distinct kfc.ifa) total_ifa_source_4, count(distinct dict.ifa) dict_ifa_kfc, count(distinct pred.ifa_kfc) pred_ifa_kfc, 
# MAGIC        count(distinct if(dict.ifa is null ,pred.ifa_kfc,null)) pred_new_ifas
# MAGIC from
# MAGIC (
# MAGIC select ifa
# MAGIC from bids_core
# MAGIC where country_code = 'PSE'
# MAGIC       and pday between 20220101 and 20220131
# MAGIC       and source = 4
# MAGIC group by 1
# MAGIC       
# MAGIC ) kfc
# MAGIC left join
# MAGIC (
# MAGIC select kfc_id ifa
# MAGIC from kfc_id_dictionary
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select ifa
# MAGIC from kfc_id_dictionary
# MAGIC       
# MAGIC ) dict
# MAGIC on dict.ifa = kfc.ifa 
# MAGIC left join
# MAGIC (
# MAGIC  select *
# MAGIC  from matan_db.kfc_id_detection_test_res_PSE_20220101_20220131
# MAGIC ) pred
# MAGIC on pred.ifa_kfc = kfc.ifa 
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from matan_db.kfc_id_detection_test_res_PSE_20220101_20220131

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'sdk' source,count(distinct ifa) ifa
# MAGIC from
# MAGIC (
# MAGIC   select ifa
# MAGIC   from bids_core
# MAGIC   where pday between 20220101 and 20220131
# MAGIC         and country_code = 'PSE'
# MAGIC         and ip > ''
# MAGIC         and ip != 'unknown'
# MAGIC         and ifa > ''
# MAGIC         and ifa not in ('B602D594AFD2B0B327E07A06F36CA6A7E42546D0', '00000000-0000-0000-0000-000000000000')
# MAGIC         and environment_type != 0
# MAGIC         and source = 4
# MAGIC   group by 1
# MAGIC )
# MAGIC union all
# MAGIC (
# MAGIC   select 'sdk ifa', count(distinct k.ifa) ifa
# MAGIC   from kfc_id_dictionary k join (select ifa
# MAGIC   from bids_core
# MAGIC   where pday between 20220101 and 20220131
# MAGIC         and country_code = 'PSE'
# MAGIC         and ip > ''
# MAGIC         and ip != 'unknown'
# MAGIC         and ifa > ''
# MAGIC         and ifa not in ('B602D594AFD2B0B327E07A06F36CA6A7E42546D0', '00000000-0000-0000-0000-000000000000')
# MAGIC         and environment_type != 0
# MAGIC         and source = 4
# MAGIC       group by 1) b on k.ifa = b.ifa
# MAGIC )

# COMMAND ----------

ifa-source   ifas
sdk total	 552,598
enriched ifa 293,582

# COMMAND ----------

# MAGIC %sql
# MAGIC select case when bids.ifa like '________-____-____-____-____________' then 'ifa'
# MAGIC             when bids.ifa REGEXP '^[0-9]+$' then 'kfc_id'
# MAGIC             else 'else'
# MAGIC       end id_type,
# MAGIC       if(events <4,'less than 4 events','more than 4 events') user_type,
# MAGIC       case when naiv.ifa is not null then 'dictionary'
# MAGIC            when ml.ifa is not null then 'algorithm'
# MAGIC       else 'potential' end as output_type,
# MAGIC       count(distinct bids.ifa) ifas,
# MAGIC       round(mean(events)) mean_events
# MAGIC from
# MAGIC (
# MAGIC     select ifa, count(*) events
# MAGIC     from bids_core
# MAGIC     where country_code = 'IDN'
# MAGIC           and pday between 20220309 and 20220323
# MAGIC           and source in (3,4)
# MAGIC    group by 1
# MAGIC ) bids
# MAGIC 
# MAGIC left join
# MAGIC 
# MAGIC (
# MAGIC     select distinct ifa
# MAGIC     from
# MAGIC 
# MAGIC     (
# MAGIC     select ifa_kfc ifa
# MAGIC     from
# MAGIC     (
# MAGIC       select *, sum(rank_kfc) over (partition by ifa_kfc order by match_prob desc) multi
# MAGIC       from
# MAGIC       (
# MAGIC       select *, 
# MAGIC             rank() over (partition by ifa_kfc order by match_prob desc) rank_kfc,
# MAGIC             rank() over (partition by ifa_ws order by match_prob desc) rank_ws
# MAGIC 
# MAGIC       from matan_db.kfc_id_detection_predict_raw_idn
# MAGIC       where prediction = 1
# MAGIC             and match_prob > 0.9)
# MAGIC       where rank_kfc = 1 and rank_ws = 1 )
# MAGIC     where multi =1
# MAGIC 
# MAGIC     union all 
# MAGIC 
# MAGIC     select ifa_ws ifa
# MAGIC     from
# MAGIC     (
# MAGIC       select *, sum(rank_kfc) over (partition by ifa_kfc order by match_prob desc) multi
# MAGIC       from
# MAGIC       (
# MAGIC       select *, 
# MAGIC             rank() over (partition by ifa_kfc order by match_prob desc) rank_kfc,
# MAGIC             rank() over (partition by ifa_ws order by match_prob desc) rank_ws
# MAGIC 
# MAGIC       from matan_db.kfc_id_detection_predict_raw_idn
# MAGIC       where prediction = 1
# MAGIC             and match_prob > 0.9)
# MAGIC       where rank_kfc = 1 and rank_ws = 1 )
# MAGIC     where multi =1
# MAGIC 
# MAGIC     )
# MAGIC ) ml
# MAGIC on bids.ifa = ml.ifa
# MAGIC 
# MAGIC left join
# MAGIC (
# MAGIC     select distinct ifa
# MAGIC     from
# MAGIC     (
# MAGIC       select kfc_id ifa
# MAGIC       from kfc_id_dictionary
# MAGIC       where first_seen <= 2022032323
# MAGIC 
# MAGIC       union all
# MAGIC 
# MAGIC       select ifa
# MAGIC       from kfc_id_dictionary
# MAGIC       where first_seen <= 2022032323
# MAGIC     )
# MAGIC ) naiv
# MAGIC on bids.ifa = naiv.ifa
# MAGIC group by 1,2,3
# MAGIC order by 3,2,1

# COMMAND ----------

# MAGIC %sql
# MAGIC select case when bids.ifa like '________-____-____-____-____________' then 'ifa'
# MAGIC             when bids.ifa REGEXP '^[0-9]+$' then 'kfc_id'
# MAGIC             else 'else'
# MAGIC       end id_type,
# MAGIC       if(events <4,'less than 4 events','more than 4 events') user_type,
# MAGIC       case when naiv.ifa is not null then 'dictionary'
# MAGIC            when ml.ifa is not null then 'algorithm'
# MAGIC       else 'potential' end as output_type,
# MAGIC       count(distinct bids.ifa) ifas,
# MAGIC       round(mean(events)) mean_events
# MAGIC from
# MAGIC (
# MAGIC     select ifa, count(*) events
# MAGIC     from bids_core
# MAGIC     where country_code = 'MYS'
# MAGIC           and pday between 20220129 and 20220228
# MAGIC           and source in (3,4)
# MAGIC    group by 1
# MAGIC ) bids
# MAGIC 
# MAGIC left join
# MAGIC 
# MAGIC (
# MAGIC     select distinct ifa
# MAGIC     from
# MAGIC 
# MAGIC     (
# MAGIC     select ifa_kfc ifa
# MAGIC     from
# MAGIC     (
# MAGIC       select *, sum(rank_kfc) over (partition by ifa_kfc order by match_prob desc) multi
# MAGIC       from
# MAGIC       (
# MAGIC       select *, 
# MAGIC             rank() over (partition by ifa_kfc order by match_prob desc) rank_kfc,
# MAGIC             rank() over (partition by ifa_ws order by match_prob desc) rank_ws
# MAGIC 
# MAGIC       from matan_db.kfc_id_detection_test_res_MYS_20220129_20220228
# MAGIC       where prediction = 1
# MAGIC             and match_prob > 0.9)
# MAGIC       where rank_kfc = 1 and rank_ws = 1 )
# MAGIC     where multi =1
# MAGIC 
# MAGIC     union all 
# MAGIC 
# MAGIC     select ifa_ws ifa
# MAGIC     from
# MAGIC     (
# MAGIC       select *, sum(rank_kfc) over (partition by ifa_kfc order by match_prob desc) multi
# MAGIC       from
# MAGIC       (
# MAGIC       select *, 
# MAGIC             rank() over (partition by ifa_kfc order by match_prob desc) rank_kfc,
# MAGIC             rank() over (partition by ifa_ws order by match_prob desc) rank_ws
# MAGIC 
# MAGIC       from matan_db.kfc_id_detection_test_res_MYS_20220129_20220228
# MAGIC       where prediction = 1
# MAGIC             and match_prob > 0.9)
# MAGIC       where rank_kfc = 1 and rank_ws = 1 )
# MAGIC     where multi =1
# MAGIC 
# MAGIC     )
# MAGIC ) ml
# MAGIC on bids.ifa = ml.ifa
# MAGIC 
# MAGIC left join
# MAGIC (
# MAGIC     select distinct ifa
# MAGIC     from
# MAGIC     (
# MAGIC       select kfc_id ifa
# MAGIC       from kfc_id_dictionary
# MAGIC       where first_seen <= 2022022823
# MAGIC 
# MAGIC       union all
# MAGIC 
# MAGIC       select ifa
# MAGIC       from kfc_id_dictionary
# MAGIC       where first_seen <= 2022022823
# MAGIC     )
# MAGIC ) naiv
# MAGIC on bids.ifa = naiv.ifa
# MAGIC group by 1,2,3
# MAGIC order by 3,2,1

# COMMAND ----------

4/13

# COMMAND ----------


