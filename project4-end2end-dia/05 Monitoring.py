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

import mlflow
from pprint import pprint
from mlflow.tracking import MlflowClient
import plotly.express as px
from datetime import timedelta, datetime
import numpy as np
import pandas as pd

client = MlflowClient()

#Getting walletaddress
wallet_address = str(dbutils.widgets.get('00.Wallet_Address'))

#Model names for staging and production
model_name_staging = "g07_staging_model"
model_name_production = "g07_production_model"

mlflow.set_experiment('experiment_path')

# accessing the model versions in staging
stage_version = None

for modelv in client.search_model_versions(f"name='{model_name_staging}'"):
    if dict(modelv)['current_stage'] == 'Staging':
    stage_version=dict(modelv)['version']

if stage_version is not None:
    stage_model = mlflow.pyfunc.load_model(f"models:/{model_name_staging}/Staging")
    print("Staging Model: ", stage_model)


# accessing the model versions in staging
prod_version = None

for modelv in client.search_model_versions(f"name='{model_name_production}'"):
    if dict(modelv)['current_stage'] == 'Production':
    prod_version=dict(modelv)['version']

if prod_version is not None:
    prod_model = mlflow.pyfunc.load_model(f"models:/{model_name_production}/Production")
    print("Production Model: ", prod_model)


#accessing the database of our group (07)
databaseName = "g07_db"

#selecting the data pertaining to the wallet
dataset = spark.sql(''' SELECT * FROM {0}.silver{1}_delta  
                        WHERE wallet_id = {3}'''.format(databaseName, model_type, wallet_address,)) 
#!!!!!REPLACE WITH .SILVERTABLE FROM ETL/MODEL AND WALLTER ID VARIABLE!!!!!


# Recommendation using the production and staging models


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
