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

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Code Starts Here...

# COMMAND ----------

# Read in final modeling data from the table stored in our database
tripletDF = spark.table('g07_db.final_modelling').cache()
tripletDF = tripletDF.withColumnRenamed("addr_id", "user_int_id").withColumnRenamed("token_id", "token_int_id").withColumnRenamed("balance_usd", "active_holding_usd")
tripletDF = tripletDF.dropna(how="any", subset="active_holding_usd")

# Predict for a single user
from pyspark.sql import functions as F
tokensDF = spark.table("g07_db.toks_silver").cache()
addressesDF = spark.table("g07_db.wallet_addrs").cache() 
# UserID = addressesDF[addressesDF['addr']==wallet_address].collect()[0][1]
# print(UserID)
UserID = 1068072 
users_tokens = tripletDF.filter(tripletDF.aaddress_id == UserID).join(tokensDF, tokensDF.id==tripletDF.token_int_id).select('token_int_id', 'name', 'symbol')                                           
# generate list of tokens held 
tokens_held_list = [] 
# for tok in users_tokens.collect():   
#     tokens_held_list.append(tok['name'])  
print('Tokens user has held:') 
users_tokens.select('name').show() 

# COMMAND ----------

tokens_not_held = tripletDF.filter(~ tripletDF['token_int_id'].isin([token['token_int_id'] for token in users_tokens.collect()])).select('token_int_id').withColumn('user_int_id', F.lit(UserID)).distinct()

# Print prediction output for staging model
model = mlflow.spark.load_model('models:/'+'ALS1'+'/Staging')
predicted_toks = model.transform(tokens_not_held)
    
print('Predicted Tokens:')
toppredictions = predicted_toks.join(tripletDF, 'token_int_id') \
                 .join(tokensDF, tokensDF.id==tripletDF.token_int_id) \
                 .select('name', 'symbol') \
                 .distinct() \
                 .orderBy('prediction', ascending = False)
print(toppredictions.show(5))

# COMMAND ----------

# Print prediction output for production model
model = mlflow.spark.load_model('models:/'+'ALS1'+'/Production')
predicted_toks = model.transform(tokens_not_held)
    
print('Predicted Tokens:')
toppredictions = predicted_toks.join(tripletDF, 'token_int_id') \
                 .join(tokensDF, tokensDF.id==tripletDF.token_int_id) \
                 .select('name', 'symbol') \
                 .distinct() \
                 .orderBy('prediction', ascending = False)
print(toppredictions.show(5))

# COMMAND ----------

# Get staging and production models
from mlflow.tracking import MlflowClient
client = MlflowClient()
run_stage = client.get_latest_versions('ALS1', ["Staging"])[0]
run_prod = client.get_latest_versions('ALS1', ["Production"])[0]
 
# Gets version of staged model
run_staging=run_stage.version
 
# Gets version of production model
run_production=run_prod.version

# COMMAND ----------

#print(client.get_metric_history(run_id=run_stage.run_id, key='rsme'))

rsmes = [client.get_metric_history(run_id=run_stage.run_id, key='rsme')[0].value, client.get_metric_history(run_id=run_prod.run_id, key='rsme')[0].value]
stage = ['Staging', 'Production']
 
import matplotlib.pyplot as plt
plt.bar(stage,rsmes)
plt.ylabel("RSME")
plt.show()

# COMMAND ----------



# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
