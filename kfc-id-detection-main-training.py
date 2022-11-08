# Databricks notebook source
# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

""" config """
has_faild = False
isr = pytz.timezone('Israel')
train_config = []

# COMMAND ----------

print("Pre-proccessing build")
print("-"*50)

for c in countries:
  start = datetime.datetime.now(tz=isr)
  print(f'Iteration {c} start at: {start}')
  try:
    output = dbutils.notebook.run('./kfc-id-detection-pre-proccessing',0, {'country': c, 'env': env} )
    output = ast.literal_eval(output)
    t,sample_size = output[0],output[1]
    print(f'sample size:{sample_size}')
    train_config.append([c,t])
    print(f"Iteration {c} finished after")
  except Exception as e:
    print(e)
    sendEmail(c,e,start,env,stage = 'Data Preproccessing')
    print(f"Iteration {c} failed after after")   
    
    global has_faild
    has_faild = True

  end = datetime.datetime.now(tz=isr)
  print(end-start)
  print("*"*50)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

def modelTrainingProcess(run):
  cc = run[0]
  t = run[1]
  try:
    dbutils.notebook.run('./kfc-id-detection-model-training',0, {'country': cc, 'table': t, 'env': env} )
  except Exception as e:
    sendEmail(c,e,start,env,stage = 'Model Training')
    print(e)
    
    global has_faild
    has_faild = True
    
with ThreadPoolExecutor(max_workers=6) as executor:
  results = executor.map(modelTrainingProcess, train_config )

# COMMAND ----------

if has_faild:
  raise Exception("There is a problem, please see the notebook for more information")
