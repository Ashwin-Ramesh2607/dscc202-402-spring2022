# Databricks notebook source
# MAGIC %md
# MAGIC ## Rubric for this module
# MAGIC - Implement a routine to "promote" your model at **Staging** in the registry to **Production** based on a boolean flag that you set in the code.
# MAGIC - Using wallet addresses from your **Staging** and **Production** model test data, compare the recommendations of the two models.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables.
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Code Starts Here...

# COMMAND ----------

import random
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec
from delta.tables import *
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from mlflow.tracking.client import MlflowClient
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

# COMMAND ----------

# Read the final_modelling data
model_df = spark.table('g07_db.final_modelling')
model_df = model_df.withColumnRenamed("aaddress_id", "user_id").withColumnRenamed("balance_usd", "current_usd_holding")
model_df = model_df.dropna(how="any", subset="current_usd_holding")

# COMMAND ----------

# Generate predictions
tokensDF = spark.table("g07_db.tokens_silver").cache()
addressesDF = spark.table("g07_db.wallet_addresses")
UserID = addressesDF[addressesDF['address']=='0x0a21a99a97d4b3f309860917cf1c9c8c82e32edc'].collect()[0][1]
print(UserID)

# COMMAND ----------

users_tokens = model_df.filter(model_df.user_id == UserID).join(tokensDF, tokensDF.id==model_df.token_id).select('token_id', 'name', 'symbol')                   
print('Tokens user has held:') 
users_tokens.select('name').show()

# COMMAND ----------

unused_tokens = model_df.filter(~model_df['token_id'].isin([token['token_id'] for token in users_tokens.collect()])).select('token_id').withColumn('user_id', F.lit(UserID)).distinct()

# COMMAND ----------

# Print the staging model's predictions prediction
model = mlflow.spark.load_model('models:/'+'ALS4'+'/Staging')
predicted_toks = model.transform(unused_tokens)

print('Predicted Tokens:')
top_k_predictions = predicted_toks.join(model_df, 'token_id').join(tokensDF, tokensDF.id==model_df.token_id).select('name', 'symbol').distinct().orderBy('prediction', ascending = False)
print(top_k_predictions.show(5))

# COMMAND ----------

# Print the productions model's predictions prediction
model = mlflow.spark.load_model('models:/'+'ALS4'+'/Production')
predicted_toks = model.transform(unused_tokens)
    
print('Predicted Tokens:')
top_k_predictions = predicted_toks.join(model_df, 'token_id').join(tokensDF, tokensDF.id==model_df.token_id).select('name', 'symbol').distinct().orderBy('prediction', ascending = False)
print(top_k_predictions.show(5))

# COMMAND ----------

from mlflow.tracking import MlflowClient
client = MlflowClient()
run_stage = client.get_latest_versions('ALS4', ["Staging"])[0]
run_prod = client.get_latest_versions('ALS4', ["Production"])[0]
run_staging=run_stage.version
run_production=run_prod.version

# COMMAND ----------

# Visualize the difference in RSME
rsmes = [client.get_metric_history(run_id=run_stage.run_id, key='rmse')[0].value, client.get_metric_history(run_id=run_prod.run_id, key='rmse')[0].value]
stage = ['Staging', 'Production']
 
import matplotlib.pyplot as plt
plt.bar(stage,rsmes)
plt.ylabel("RMSE")
plt.show()

# COMMAND ----------

if client.get_metric_history(run_id=run_stage.run_id, key='rmse')[0].value < client.get_metric_history(run_id=run_prod.run_id, key='rmse')[0].value:
    filter_string = "name='{}'".format('ALS')
    for mv in client.search_model_versions(filter_string):
         if dict(mv)['current_stage'] == 'Production':
             # Archive the current model in production
             client.transition_model_version_stage(
                name="ALS",
                version=dict(mv)['version'],
                stage="Archived"
            )
    version_fin=int(run_staging)
    client.transition_model_version_stage(
    name='ALS4',
    version=version_fin,
    stage='Production'
)

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
