# Databricks notebook source
# MAGIC %run "./kfc-id-detection-functions"

# COMMAND ----------

""" config """
target_field = 'label'
t = db + 'kfc_id_detection_raw_IDN_20220208_20220310'

# COMMAND ----------

post = spark.read.table(t)#.select(*model_cols , 'label')
post.display()

# COMMAND ----------

post.groupby('label').agg(countDistinct('ifa_kfc'),countDistinct('ifa_ws')).show()

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

train_id, test_id = post.select('ifa_kfc').drop_duplicates().randomSplit([.8,.2])

scaled_post,model_features_schema, key = change_col_type(post)

post_with_features = build_sclaed_features(scaled_post,model_features_schema)

train = post_with_features.join(train_id,'ifa_kfc')
test = post_with_features.join(test_id,'ifa_kfc')

train.groupby('label').agg(countDistinct('ifa_kfc'),countDistinct('ifa_ws'), count(lit(1))).show()
test.groupby('label').agg(countDistinct('ifa_kfc'),countDistinct('ifa_ws'), count(lit(1))).show()

train.persist()
test.persist()

# COMMAND ----------

import seaborn as sn
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

df = test.toPandas()

# COMMAND ----------

c = df.drop(['ifa_kfc','ifa_ws'], axis=1).columns

# COMMAND ----------

# sn.set_style("whitegrid");
# sn.pairplot(df, hue="label", height=3);
# plt.show()

# COMMAND ----------

train, test = train.toPandas(), test.toPandas()

# COMMAND ----------

x_train, x_test, y_train, y_test = train[model_features_schema], test[model_features_schema], train['label'], test['label']

# COMMAND ----------

x_train.describe()

# COMMAND ----------

x,y = x_train,y_train

# COMMAND ----------

# first ten features
data_dia = y
data = x
data_n_2 = (data - data.mean()) / (data.std())              # standardization
data = pd.concat([y,data_n_2.iloc[:,0:10]],axis=1)
data = pd.melt(data,id_vars="label",
                    var_name="features",
                    value_name='value')
plt.figure(figsize=(10,10))
sn.violinplot(x="features", y="value", hue="label", data=data,split=True, inner="quart")
plt.xticks(rotation=90)

# COMMAND ----------

#correlation map
f,ax = plt.subplots(figsize=(18, 18))
sn.heatmap(x.corr(), annot=True, linewidths=.5, fmt= '.1f',ax=ax)

# COMMAND ----------

from sklearn.inspection import permutation_importance
import time
from sklearn.ensemble import GradientBoostingClassifier



forest = GradientBoostingClassifier(
  max_depth=7,
  n_estimators=750,
  learning_rate=0.7
                                   )
forest.fit(x_train, y_train)

start_time = time.time()
result = permutation_importance(
    forest, x_test, y_test, n_repeats=10, random_state=42, n_jobs=2
)
elapsed_time = time.time() - start_time
print(f"Elapsed time to compute the importances: {elapsed_time:.3f} seconds")

# COMMAND ----------

fi_t = {'Feature':feature_names,'Importances':forest_importances}
fi_t = pd.DataFrame(fi_t)
fi_t.sort_values(by='Importances',ascending=False)

# COMMAND ----------

feature_names = [f"feature {x_test.columns[i]}" for i in range(x_test.shape[1])]
forest_importances = pd.Series(result.importances_mean, index=feature_names)

fig, ax = plt.subplots()
forest_importances.plot.bar(yerr=result.importances_std, ax=ax)
ax.set_title("Feature importances using permutation on full model")
ax.set_ylabel("Mean accuracy decrease")
fig.tight_layout()
plt.show()

# COMMAND ----------

from pyspark.sql.types import *
from sklearn import datasets
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, f1_score
import random
from sklearn.model_selection import train_test_split

def gridS(sdf):
  import  pyspark.sql.functions as F
  
  
  outSchema = StructType( [ 
    StructField('replication_id',IntegerType(),True),
    StructField('measure',DoubleType(),True),
    StructField('n_estimators',IntegerType(),True),
    StructField('max_depth',IntegerType(),True),
    StructField('learning_rate',DoubleType(),True)
  ]
  )
  
  def run_model(pdf):
    
      ## this is how we are picking random set of parameters.
      max_depth = random.choice(list(range(2,10)))
      n_estimators = random.choice(list(range(100,800)))
      learning_rate = random.uniform(0, 0.5)
      replication_id = pdf.replication_id.values[0]
      
      # Split X and y
      samplelist = pdf['ifa_kfc'].unique()
      training_samp, test_samp = train_test_split(samplelist, train_size=0.7, test_size=0.3, random_state=5, shuffle=True)
      
      # Test train split
      training_data = pdf[pdf['ifa_kfc'].isin(training_samp)]
      test_data = pdf[pdf['ifa_kfc'].isin(test_samp)]
      
      Xtrain, ytrain = training_data[model_features_schema],training_data['label']
      Xcv,ycv = test_data[model_features_schema],test_data['label']

      # Initilize model and fit  
      clf = GradientBoostingClassifier(n_estimators = n_estimators, max_depth = max_depth, learning_rate = learning_rate, random_state=42)
      clf.fit(Xtrain,ytrain)
      
      # Get the f1 scores 
      measure = f1_score(clf.predict(Xcv),ycv)
      # Return result as a pandas data frame               
      res=pd.DataFrame( { 
        'replication_id':replication_id,
        'measure':measure,
        'n_estimators':n_estimators,
        'max_depth':max_depth,
        'learning_rate':learning_rate},
        index=[0])
      return res
  
  
  replication_df = spark.createDataFrame(pd.DataFrame(list(range(1,1000)),columns=['replication_id']))
  replicated_train_df = sdf.crossJoin(replication_df)
  
  results = replicated_train_df.groupby("replication_id").applyInPandas(run_model,outSchema)
  return results.sort(F.desc("measure"))




# COMMAND ----------

res = gridS(post)

# COMMAND ----------

res.display()

# COMMAND ----------

model = GradientBoostingClassifier()

# COMMAND ----------

from sklearn.metrics import roc_curve,auc

# COMMAND ----------

learning_rates = [0.5,0.6,0.7,1]
train_results = []
test_results = []
for eta in learning_rates:
    model = GradientBoostingClassifier(learning_rate=eta)
    model.fit(x_train, y_train)
    train_pred = model.predict(x_train)
    false_positive_rate, true_positive_rate, thresholds = roc_curve(y_train, train_pred)
    roc_auc = auc(false_positive_rate, true_positive_rate)
    train_results.append(roc_auc)
    y_pred = model.predict(x_test)
    false_positive_rate, true_positive_rate, thresholds = roc_curve(y_test, y_pred)
    roc_auc = auc(false_positive_rate, true_positive_rate)
    test_results.append(roc_auc)
from matplotlib.legend_handler import HandlerLine2D
line1, = plt.plot(learning_rates, train_results, 'b', label="Train AUC")
line2, = plt.plot(learning_rates, test_results, 'r', label="Test AUC")
plt.legend(handler_map={line1: HandlerLine2D(numpoints=2)})
plt.ylabel('AUC score')
plt.xlabel('learning rate')
plt.show()


# COMMAND ----------

n_estimators = [600,1000,1200]
train_results = []
test_results = []
for estimator in n_estimators:
  model = GradientBoostingClassifier(n_estimators=estimator)
  model.fit(x_train, y_train)
  train_pred = model.predict(x_train)
  false_positive_rate, true_positive_rate, thresholds = roc_curve(y_train, train_pred)
  roc_auc = auc(false_positive_rate, true_positive_rate)
  train_results.append(roc_auc)
  y_pred = model.predict(x_test)
  false_positive_rate, true_positive_rate, thresholds = roc_curve(y_test, y_pred)
  roc_auc = auc(false_positive_rate, true_positive_rate)
  test_results.append(roc_auc)
from matplotlib.legend_handler import HandlerLine2D
line1, = plt.plot(n_estimators, train_results, 'b', label='Train AUC')
line2, = plt.plot(n_estimators, test_results, 'r', label='Test AUC')
plt.legend(handler_map={line1: HandlerLine2D(numpoints=2)})
plt.ylabel('AUC score')
plt.xlabel('n_estimators')
plt.show()


# COMMAND ----------

max_depths = np.linspace(1, 12, 12, endpoint=True)
train_results = []
test_results = []
for max_depth in max_depths:
  model = GradientBoostingClassifier(max_depth=max_depth)
  model.fit(x_train, y_train)
  train_pred = model.predict(x_train)
  false_positive_rate, true_positive_rate, thresholds = roc_curve(y_train, train_pred)
  roc_auc = auc(false_positive_rate, true_positive_rate)
  train_results.append(roc_auc)
  y_pred = model.predict(x_test)
  false_positive_rate, true_positive_rate, thresholds = roc_curve(y_test, y_pred)
  roc_auc = auc(false_positive_rate, true_positive_rate)
  test_results.append(roc_auc)
from matplotlib.legend_handler import HandlerLine2D
line1, = plt.plot(max_depths, train_results, 'b', label='Train AUC')
line2, = plt.plot(max_depths, test_results, 'r', label='Test AUC')
plt.legend(handler_map={line1: HandlerLine2D(numpoints=2)})
plt.ylabel('AUC score')
plt.xlabel('Tree depth')
plt.show()

# COMMAND ----------

train.persist()
test.persist()
unpersist
spark.catalog.clearCache()

# COMMAND ----------

# class RandomGridBuilder: 
#   def __init__(self, num_models, seed=None):
#     self._param_grid = {}
#     self.num_models = num_models
#     self.seed = seed
    
#   def addDistr(self, param, distr_generator):
#     '''Add distribution based on dictionary generated by function passed to addDistr.'''
    
#     if 'pyspark.ml.param.Param' in str(type(param)):
#       self._param_grid[param] = distr_generator
#     else:
#       raise TypeError('param must be an instance of Param')

#     return self
  
#   def build(self):    
#     param_map = []
#     for n in range(self.num_models):
#       if self.seed:
#         # Set seeds for both numpy and random in case either is used for the random distribution
#         np.random.seed(self.seed + n)
#         random.seed(self.seed + n)
#       param_dict = {}
#       for param, distr in self._param_grid.items():
#         param_dict[param] = distr()
#       param_map.append(param_dict)
    
#     return param_map
  
# from pyspark.ml.classification import GBTClassifier
# from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
# from pyspark.ml import Pipeline

# # Configure an ML pipeline, which consists of tree stages: assembler,scaler and model.
# evaluator = BinaryClassificationEvaluator(labelCol="label",metricName="areaUnderPR")
# assembler = VectorAssembler(inputCols=model_features_schema, outputCol='pre_features', handleInvalid = 'keep')
# scaler = StandardScaler(inputCol="pre_features", outputCol="features", withStd=True, withMean=False)
# clf = GBTClassifier()

# # Build random grid search
# NumberOfRuns = 100

# paramGrid = RandomGridBuilder(num_models=NumberOfRuns, seed=42)\
#              .addDistr(clf.stepSize, lambda: np.random.rand()) \
#              .addDistr(clf.maxBins, lambda : math.floor(100 + 800*np.random.power(1)))\
#              .addDistr(clf.maxDepth, lambda : 2 + np.random.randint(8))\
#              .build()

# # Cross validator with random search
# cv = CrossValidator(
#                     estimator = clf, 
#                     estimatorParamMaps = paramGrid, 
#                     evaluator = evaluator,   
#                     parallelism = NumberOfRuns
#                    )

# pipe = Pipeline(stages = [assembler,scaler,cv])

# Model = pipe.fit(train)

# print(f'StepSize:{Model.bestModel.getStepSize()}, MaxBins:{Model.bestModel.getMaxBins()}, MaxDepth:{Model.bestModel.getMaxDepth()}')
