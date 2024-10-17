from pyspark.sql.types import *
from pyspark.sql.functions import *
from cltv.utils import train_model
from utils import settings


class CalcCLTV:
    def __init__(self, spark, source):
        self.spark = spark
        self.source = source

    def calc(self):
        try:
            if self.source.lower() == 'uci':
                self.load_uciml()

            feature_df = train_model.create_features(self.source.lower())

            predictions_df = train_model.train(feature_df, settings.feature_list)

            # Store predictions in a Delta table
            predictions_df \
                .select("CustomerID", "prediction") \
                .withColumn("source", lit(self.source.lower())) \
                .withColumm("CreateDate", current_date()) \
                .write \
                .format("delta") \
                .mode("append") \
                .partitionBy("source", "CREATE_DT") \
                .save(settings.predictions_table)

            print(f"predictions for {self.source} retail customer saved in delta table")

        except Exception as e:
            print("Error in calculating CLTV")
            raise e

    def load_uciml(self):
        try:
            from ucimlrepo import fetch_ucirepo

            # fetch dataset
            online_retail = fetch_ucirepo(id=352)

            retail_schema = StructType([StructField("InvoiceNo", StringType(), True),
                                        StructField("StockCode", StringType(), True),
                                        StructField("Description", StringType(), True),
                                        StructField("Quantity", IntegerType(), True),
                                        StructField("InvoiceDate", StringType(), True),
                                        StructField("UnitPrice", DoubleType(), True),
                                        StructField("CustomerID", IntegerType(), True),
                                        StructField("Country", StringType(), True)])

            retail_orders = self.spark.createDataFrame(online_retail.data.original, schema=retail_schema)

            retail_orders = retail_orders \
                .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")) \
                .withColumm("CreateDate", current_date())

            # Remove rows with missing CustomerID and negative quantities
            df_clean = retail_orders.filter("CustomerID IS NOT NULL AND Quantity > 0")

            # Remove duplicate records
            df_clean = df_clean.dropDuplicates()

            # Write the cleansed data to the bronze delta table
            df_clean.write \
                .format('delta') \
                .option("mergeSchema", "true") \
                .mode("append") \
                .partitionBy("CreateDate") \
                .save(settings.source_table_uci)

            print(f"Cleansed raw data saved in the bronze table: {settings.source_table_uci}")

        except Exception as e:
            print("Error loading UCI ML data from source")
            raise e
