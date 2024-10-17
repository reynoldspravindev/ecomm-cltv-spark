from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import *
from utils import settings


def train(features_df, feature_coL_list):
    try:
        # Define features and label
        feature_columns = feature_coL_list # ['Recency', 'Frequency', 'MonetaryValue', 'Tenure']
        assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')

        # Transform data to have features and label
        model_data = assembler.transform(features_df)
        model_data = model_data.withColumnRenamed('MonetaryValue', 'label')

        # Split into training and test sets
        train_data, test_data = model_data.randomSplit([0.8, 0.2], seed=123)

        # Define RandomForest model
        rf = RandomForestRegressor(featuresCol='features', labelCol='label')

        # Cross-validation and hyperparameter tuning
        param_grid = ParamGridBuilder().addGrid(rf.numTrees, [10, 20]).build()
        evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")

        # Define cross-validator
        cv = CrossValidator(estimator=rf, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=3)

        # Train the model
        model = cv.fit(train_data)

        # Evaluate model on test data
        predictions = model.transform(test_data)
        rmse = evaluator.evaluate(predictions)
        print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

        return predictions

    except Exception as e:
        print("Error while training data")
        raise e


def create_features(self, source):
    try:
        source_table = None

        if source == 'uci':
            source_table = settings.source_table_uci
        elif source == 'amazon':
            source_table = settings.source_table_amzn

        df_clean = self.spark.read.format('delta').load(source_table)

        # Add a column for the total value of each transaction
        df_clean = df_clean.withColumn('TotalValue', df_clean['Quantity'] * df_clean['UnitPrice'])

        # Calculate the most recent purchase date and maximum transaction date
        max_date = df_clean.select(max('InvoiceDate')).collect()[0][0]

        # Group by CustomerID to compute Recency, Frequency, and Monetary Value
        customer_df = df_clean.groupBy('CustomerID').agg(
            datediff(lit(max_date), max('InvoiceDate')).alias('Recency'),
            countDistinct('InvoiceNo').alias('Frequency'),
            sum('TotalValue').alias('MonetaryValue')
        )

        # Calculate the customer's tenure (time since first purchase)
        tenure_df = df_clean.groupBy('CustomerID').agg(
            datediff(lit(max_date), min('InvoiceDate')).alias('Tenure')
        )

        # Join with the original customer DataFrame
        features_df = customer_df.join(tenure_df, on='CustomerID')

        return features_df

    except Exception as e:
        print("Error in feature engineering")
        raise e
