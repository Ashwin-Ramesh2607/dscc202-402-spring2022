# Databricks notebook source
# MAGIC %md
# MAGIC ## Rubric for this module
# MAGIC - Using the silver delta table(s) that were setup by your ETL module train and validate your token recommendation engine. Split, Fit, Score, Save
# MAGIC - Log all experiments using mlflow
# MAGIC - capture model parameters, signature, training/test metrics and artifacts
# MAGIC - Tune hyperparameters using an appropriate scaling mechanism for spark.  [Hyperopt/Spark Trials ](https://docs.databricks.com/_static/notebooks/hyperopt-spark-ml.html)
# MAGIC - Register your best model from the training run at **Staging**.

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
# MAGIC ## Your Code starts here...

# COMMAND ----------

# Import the necessary libraries
import delta
import mlflow
import pyspark
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# COMMAND ----------

# Define a class for model training and recommendation
class EthereumTokenRecommender:
    def __init__(self, user_name, group_name, model_name, tokens_path, addresses_path, dataset_path, metric_name, val_split, test_split, seed):
        # Set member variables
        self.seed = seed
        self.model_name = model_name
        self.group_name = group_name
        self.metric_name = metric_name
        self.dataset_path = dataset_path

        # Create new or set existing experiment for user and group
        experiment_dir = f'/Users/{user_name}@ur.rochester.edu/{group_name}_Experiments'
        mlflow.set_experiment(experiment_dir)

        # Load the experiment and log details
        self.experiment = mlflow.get_experiment_by_name(experiment_dir)
        self._log_experiment_details()

        # Load the dataset
        self.train_df, self.val_df, self.test_df, self.modelling_df = self._load_data(val_split, test_split)

        # Define the model
        self.model = ALS(
            userCol='user_id',
            itemCol='token_id',
            ratingCol='current_usd_holding',
            nonnegative=True,
            seed=self.seed)

        # Define the model signature
        self.signature = mlflow.models.signature.ModelSignature(
            inputs=mlflow.types.schema.Schema([mlflow.types.schema.ColSpec('integer', 'user_id'), mlflow.types.schema.ColSpec('integer', 'token_id')]),
            outputs=mlflow.types.schema.Schema([mlflow.types.schema.ColSpec('double')]))

        # Define the evaluator
        self.evaluator = RegressionEvaluator(
            predictionCol='prediction',
            labelCol='token_id',
            metricName=self.metric_name)

        # Load relevant tables for inference and recommendations
        self.tokens_df = spark.table(tokens_path).withColumnRenamed('address', 'tokens_address')
        self.addresses_df = spark.table(addresses_path)
        
    def _log_experiment_details(self):
        print(f'Experiment_id: {self.experiment.experiment_id}')
        print(f'Artifact Location: {self.experiment.artifact_location}')
        print(f'Tags: {self.experiment.tags}')
        print(f'Lifecycle_stage: {self.experiment.lifecycle_stage}')

    def _load_data(self, val_split, test_split):
        # Load the dataset
        modelling_df = spark.table(self.dataset_path).cache()

        # Preprocessing to set the dataframe for training
        modelling_df = modelling_df.withColumnRenamed('aaddress_id', 'user_id').withColumnRenamed('balance_usd', 'current_usd_holding') # Column renames
        modelling_df = modelling_df.dropna(how='any', subset='current_usd_holding') # Drop NA values in current_usd_holding column
        modelling_df = modelling_df.withColumn('current_usd_holding', modelling_df['current_usd_holding'].cast(DoubleType())) # Type casting to double floating point

        # Split the dataset into 3 splits
        train_df, val_df, test_df = modelling_df.randomSplit([1 - (val_split + test_split), val_split, test_split], seed=self.seed)

        # Cache and return the dataset splits
        return train_df.cache(), val_df.cache(), test_df.cache(), modelling_df
    
    def _stage_model(self, run_id):
            # Create an MLFlow Client
            versions = []
            mlflow_client = mlflow.tracking.MlflowClient()

            # Archive the current staged model
            for version in mlflow_client.search_model_versions(f"run_id='{run_id}'"):
                version = dict(version)
                versions.append(version['version'])
                if version['current_stage'] == 'Staging':
                    mlflow_client.transition_model_version_stage(
                        name='ALS model',
                        version=version['version'],
                        stage='Archived'
                    )

            # Stage the specified model
            mlflow_client.transition_model_version_stage(
                name=self.model_name,
                version=versions[0],
                stage='Staging'
            )

    def train(self, ranks, regularization_params, max_iteration):

        # Helper variable for hyperopt training
        min_error = float('inf')
        errors = [[0] * len(ranks)] * len(regularization_params)
        models = [[0] * len(ranks)] * len(regularization_params)

        # Set the number of max iterations for the model
        self.model.setMaxIter(max_iteration)

        # Run a hyperparameter tuning and training run
        for i, regularization_param in enumerate(regularization_params):
            for j, rank in enumerate(ranks):
                with mlflow.start_run() as run:
                    mlflow.set_tags({
                        'group': self.group_name,
                        'class': 'DSCC202-402',
                        'seed': self.seed
                    })

                    # Set hyperparameters for current run
                    self.model.setParams(rank=rank, regParam=regularization_param)

                    # Train the model
                    trained_model = self.model.fit(self.train_df)

                    # Validate the model
                    val_predictions = trained_model.transform(self.val_df)

                    # Postprocessing to set the predictions for evaluation
                    val_predictions = val_predictions.filter(val_predictions.prediction != float('nan')) # Remove NaN values from prediction (due to SPARK-14489)
                    val_predictions = val_predictions.withColumn(
                        'prediction', pyspark.sql.functions.abs(pyspark.sql.functions.round(val_predictions['prediction'], 0)))

                    # Evaluate the predictions on the validation set
                    error = self.evaluator.evaluate(val_predictions)

                    # Store the error and model for this hyperparameter runing run
                    errors[i][j] = error
                    models[i][j] = trained_model

                    # Log the metrics and update the best model if needed
                    print(f'For rank {rank} and regularization parameter {regularization_param}, the {self.metric_name} value is {error}')
                    if error < min_error:
                        min_error = error
                        best_params = [i, j, run.info.run_id]

                    # Log the parameters, metrics and model
                    mlflow.log_params({
                        'rank': rank, 'regParam': regularization_param, 'max_iteration': max_iteration,
                        })
                    mlflow.log_metric(self.metric_name, error) 
                    mlflow.spark.log_model(
                        spark_model=trained_model, signature=self.signature,
                        artifact_path='als-model', registered_model_name=self.model_name)

        print(f'The best model was trained on a rank of {ranks[best_params[1]]} and regularization parameter of {regularization_params[best_params[0]]}')
        self._stage_model(best_params[2])
        print('The current staging model has been archived, and the best model from this experiment has been transitioned to stage')
        
    def test(self):
        # Load the model and run predictions on the test set
        test_predictions = mlflow.spark.load_model(f'Models:/{self.model_name}/Staging').stages[0].transform(self.test_df)

        # Filter out NaN values from predictions
        test_predictions = test_predictions.filter(test_predictions.prediction != float('nan'))

        # Evaluate the predictions on the validation set
        print(f'{self.metric_name} value on the test set = {self.evaluator.evaluate(test_predictions)}')

    def infer(self, user_id, top_k):
        # Generate a dataframe of tokens that a user doesn't have
        tokens_held = self.modelling_df.filter(
            self.modelling_df.user_id == user_id
        ).join(self.tokens_df, self.tokens_df.id == self.modelling_df.token_id).select('token_id', 'name', 'symbol')                                           
        tokens_held.select('name').show()
        tokens_not_held = self.modelling_df.filter(
            ~self.modelling_df['token_id'].isin([token['token_id'] for token in tokens_held.collect()])
        ).select('token_id').withColumn('user_id', pyspark.sql.functions.lit(user_id)).distinct()

        # Load the model and run predictions on the test set
        predictions = mlflow.spark.load_model(f'Models:/{self.model_name}/Staging').transform(tokens_not_held)
        
        # Show the top_k recommendations
        top_k_predictions = predictions \
            .join(self.modelling_df, 'token_id') \
            .join(self.tokens_df, self.tokens_df.id == self.modelling_df.token_id) \
            .select('name', 'symbol') \
            .distinct() \
            .orderBy('prediction', ascending=False)
        print('Recommended Tokens\n', top_k_predictions.show(top_k))
        
        # Store the top_k_predictions as a table
        top_k_predictions \
            .write \
            .format('delta') \
            .mode('overwrite') \
            .saveAsTable('g07_db.user_recommendations')
        
        # Store the predictions as a table
        predictions = predictions.join(self.addresses_df, (predictions.user_id == self.addresses_df.aaddress_id))
        predictions = predictions.join(
            self.tokens_df, predictions.token_id == self.tokens_df.id
        ).select('token_id', 'user_id', 'prediction', 'address' , 'tokens_address').withColumnRenamed('address', 'user_address')
        predictions \
            .write \
            .format('delta') \
            .mode('overwrite') \
            .saveAsTable('g07_db.user_preds')

# COMMAND ----------

# Create an instance of the EthereumTokenRecommender class
ethereum_token_recommender = EthereumTokenRecommender(
    user_name='aramesh4', # Modify for each teammate
    group_name='G07',
    model_name='ALS-5',
    tokens_path='g07_db.tokens_silver',
    addresses_path='g07_db.wallet_addresses',
    dataset_path='g07_db.final_modelling',
    metric_name='rmse',
    val_split=0.25,
    test_split=0.15,
    seed=42
)

# COMMAND ----------

# Train the EthereumTokenRecommender on the train set
ethereum_token_recommender.train(
    ranks=[2],
    regularization_params=[0.1],
    max_iteration=1
)

# COMMAND ----------

# Test the EthereumTokenRecommender on the test set
ethereum_token_recommender.test()

# COMMAND ----------

# Recommend tokens for a user
ethereum_token_recommender.infer(
    user_id=addresses_df[addresses_df['address']=='0x0a21a99a97d4b3f309860917cf1c9c8c82e32edc'].collect()[0][1],
    top_k=15
)

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({'exit_code': 'OK'}))
