from utils import settings
from cltv.modules.calc_cltv import CalcCLTV
from pyspark.sql import SparkSession

source = 'uci'
spark = None


def main():
    try:
        if settings.is_running_on_databricks():
            print("Running on Databricks")
            spark = spark.getActiveSession()
        else:
            spark = SparkSession.builder \
                .master("local[*]") \
                .config("spark.driver.host", "127.0.0.1") \
                .config("spark.driver.bindaddress", "localhost") \
                .config("spark.sql.shuffle.partitions", "2") \
                .config("spark.driver.memory", "12g")

        job_module = CalcCLTV(spark, source)
        job_module.calc()

    except Exception as e:
        print("Error running main!")
        raise e


if __name__ == "__main__":
    main()
