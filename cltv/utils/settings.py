import os


def is_running_on_databricks():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


source_table_uci = 'dbfs:/user/hive/warehouse/uci.db/retail_orders_bronze'
source_table_amzn = 'dbfs:/user/hive/warehouse/amzn.db/retail_orders_bronze'
predictions_table = 'dbfs:/user/hive/warehouse/predictions.db/final_predictions'

feature_list = ['Recency', 'Frequency', 'MonetaryValue', 'Tenure']