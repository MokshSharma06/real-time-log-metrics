# src/main.py
from databricks.sdk.runtime import *

from pyspark.sql import SparkSession
from src.utils import AppConfig
from src.bronze.kafka_to_bronze import start_bronze_stream
from src.silver.bronze_to_silver import start_silver_stream
from src.gold.metrics import start_gold_stream


def main():
    cfg = AppConfig()
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Bronze streaming pipeline on Databricks")


    bronze_query = start_bronze_stream(spark)
    silver_query = start_silver_stream(spark,cfg)
    gold_query   = start_gold_stream(spark)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
