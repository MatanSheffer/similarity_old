# Databricks notebook source
# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

""" config """
target_field = 'label'
t = dbutils.widgets.get('table')
cc = dbutils.widgets.get('country')

# COMMAND ----------

post = spark.read.table(t).select(*model_cols,'label')
post.display()

# COMMAND ----------

post.groupby('label').agg(countDistinct('ifa_kfc'),countDistinct('ifa_ws'), count(lit(1))).show()

# COMMAND ----------

w = Window.partitionBy('ifa_kfc','label')

post = post.withColumn('numberOfMatchesPerId', count(lit(1)).over(w)).where(f'numberOfMatchesPerId < {numberOfMatchesPerId}' ).drop('numberOfMatchesPerId')
post.groupby('label').agg(countDistinct('ifa_kfc'),countDistinct('ifa_ws'), count(lit(1))).show()

# COMMAND ----------

sc.setJobDescription("Sampling the data")

post_true = post.where('label = True').select('ifa_kfc').drop_duplicates().count()
post_false = post.where('label = False').select('ifa_kfc').drop_duplicates().count()

samples = min(post_true,post_false)

if samples > 10**4*3:
  samples = 10**4*3

print(samples)

ifas = post.where('label = True').select('ifa_kfc').drop_duplicates().sample(samples/post_true).union(post.where('label = False').select('ifa_kfc').drop_duplicates().sample(samples/post_false)).drop_duplicates()

post = post.join(ifas,'ifa_kfc')

train_id, validation_id, test_id = post.select('ifa_kfc').drop_duplicates().randomSplit([.65,.15,.1])

post,model_features_schema, key = change_col_type(post)

# post_with_features = build_sclaed_features(scaled_post,model_features_schema)

train = post.join(train_id,'ifa_kfc')
test = post.join(test_id,'ifa_kfc')
validation = post.join(validation_id,'ifa_kfc')

train.groupby('label').agg(countDistinct('ifa_kfc'),countDistinct('ifa_ws'), count(lit(1))).show()
test.groupby('label').agg(countDistinct('ifa_kfc'),countDistinct('ifa_ws'), count(lit(1))).show()
validation.groupby('label').agg(countDistinct('ifa_kfc'),countDistinct('ifa_ws'), count(lit(1))).show()

train.persist()
test.persist()
validation.persist()

# COMMAND ----------

# As explained in Cmd 2 of this notebook, MLflow autologging for `pyspark.ml` requires MLflow version 1.17.0 or above.
# This try-except logic allows the notebook to run with older versions of MLflow.
try:
  import mlflow.pyspark.ml
  mlflow.pyspark.ml.autolog()
except:
  print(f"Your version of MLflow ({mlflow.__version__}) does not support pyspark.ml for autologging. To use autologging, upgrade your MLflow client version or use Databricks Runtime for ML 8.3 or above.")

# COMMAND ----------

def train_tree(stepSize,maxBins,maxDepth):
  '''
  This train() function:
   - takes hyperparameters as inputs (for tuning later)
   - returns the F1 score on the validation dataset
 
  Wrapping code as a function makes it easier to reuse the code later with Hyperopt.
  '''
  # Use MLflow to track training.
  # Specify "nested=True" since this single model will be logged as a child run of Hyperopt's run.
  with mlflow.start_run(nested=True,experiment_id = experiment_id):
    
    # StringIndexer: Read input column "label" (digits) and annotate them as categorical values.
    assembler = VectorAssembler(inputCols=model_features_schema, outputCol='pre_features', handleInvalid = 'keep')
    scaler = StandardScaler(inputCol="pre_features", outputCol="features", withStd=True, withMean=False)
    
    # DecisionTreeClassifier: Learn to predict column "indexedLabel" using the "features" column.
    clf = GBTClassifier(
        stepSize = stepSize,
        maxBins = maxBins,
        maxDepth = maxDepth
    )
    
    # Chain indexer and dtc together into a single ML Pipeline.
    pipeline = Pipeline(stages = [assembler,scaler,clf])
    model = pipeline.fit(train)
    
    # Define an evaluation metric and evaluate the model on the validation dataset.
    evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
    predictions = model.transform(validation)
    validation_metric = evaluator.evaluate(predictions)
    mlflow.log_metric("val_f1_score", validation_metric)
    
  return model, validation_metric


# COMMAND ----------

from pyspark.ml.classification import GBTClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline

initial_model, val_metric = train_tree(stepSize=0.5,maxBins=300,maxDepth=5)
print(f"The trained decision tree achieved an F1 score of {val_metric} on the validation data")

# COMMAND ----------

from hyperopt import fmin, tpe, hp, Trials, STATUS_OK
 
def train_with_hyperopt(params):
  """
  An example train method that calls into MLlib.
  This method is passed to hyperopt.fmin().
  
  :param params: hyperparameters as a dict. Its structure is consistent with how search space is defined. See below.
  :return: dict with fields 'loss' (scalar loss) and 'status' (success/failure status of run)
  """
  # For integer parameters, make sure to convert them to int type if Hyperopt is searching over a continuous range of values.
  stepSize = float(params['stepSize'])
  maxBins = int(params['maxBins'])
  maxDepth = int(params['maxDepth'])

  model, f1_score = train_tree(stepSize,maxBins,maxDepth)

  # Hyperopt expects you to return a loss (for which lower is better), so take the negative of the f1_score (for which higher is better).
  loss = - f1_score
  return {'loss': loss, 'status': STATUS_OK}

# COMMAND ----------

space = {
  'maxBins': hp.quniform('maxBins', 100, 800,5),
  'maxDepth': hp.quniform('maxDepth', 2, 10,1),
  'stepSize': hp.loguniform('stepSize', np.log(0.01), np.log(1.0)),
}

# COMMAND ----------

algo=tpe.suggest
 
with mlflow.start_run(experiment_id = experiment_id):
  best_params = fmin(
    fn=train_with_hyperopt,
    space=space,
    algo=algo,
    max_evals=15
  )

# COMMAND ----------

best_params

# COMMAND ----------

best_maxDepth = int(best_params['maxDepth'])
best_maxBins = int(best_params['maxBins'])
best_stepSize = best_params['stepSize']

 
Model, val_f1_score = train_tree(best_stepSize,best_maxBins,best_maxDepth)

# COMMAND ----------

evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
 
initial_model_test_metric = evaluator.evaluate(initial_model.transform(test))
final_model_test_metric = evaluator.evaluate(Model.transform(test))
 
print(f"On the test data, the initial (untuned) model achieved F1 score {initial_model_test_metric}, and the final (tuned) model achieved {final_model_test_metric}.")


# COMMAND ----------

gbt_preds = Model.transform(train)

print("GBT Pred Training areaUnderPR")
print(evaluator.evaluate(gbt_preds))

#select only prediction and label columns
preds_and_labels = gbt_preds.selectExpr('cast (prediction as float) as prediction',f'cast({target_field} as float) as {target_field}')

preds_and_labels.groupby('prediction').pivot(target_field).count().orderBy('prediction').display()

# COMMAND ----------

gbt_preds = Model.transform(test)

print("GBT Pred Test areaUnderPR")
print(evaluator.evaluate(gbt_preds))

#select only prediction and label columns
preds_and_labels = gbt_preds.selectExpr('cast (prediction as float) as prediction',f'cast({target_field} as float) as {target_field}')

preds_and_labels.groupby('prediction').pivot(target_field).count().orderBy('prediction').display()

# COMMAND ----------

save_model = True

if (save_model):
  path = f's3://bs-ml-models/kfc-id-detection/{env}/{cc}/'
  Model.write().overwrite().save(path)

# COMMAND ----------

# StringIndexer: Read input column "label" (digits) and annotate them as categorical values.
assembler = VectorAssembler(inputCols=model_features_schema, outputCol='pre_features', handleInvalid = 'keep')
scaler = StandardScaler(inputCol="pre_features", outputCol="features", withStd=True, withMean=False)
dt = DecisionTreeClassifier(featuresCol = 'features', labelCol = 'label', maxDepth = best_maxDepth)

# Chain indexer and dtc together into a single ML Pipeline.
pipeline = Pipeline(stages = [assembler,scaler,dt])
dtModel = pipeline.fit(train)

ind = 0
for i in model_features_schema:
  print(str(ind) + '.'+i)
  ind = ind+1
  
display(dtModel.stages[2])

# COMMAND ----------

cols = train.select(*model_features_schema).columns
importance = np.around(list(Model.stages[2].featureImportances),3)
pd.DataFrame(importance, 
             cols, 
             columns=['featureImportances'])\
            .sort_values(by='featureImportances', ascending=False)

# COMMAND ----------

train.unpersist()
test.unpersist()

spark.catalog.clearCache()
