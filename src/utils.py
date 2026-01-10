
import yaml
import os
from pyspark.sql import SparkSession

# ---CONFIG LOADER ---
def load_config(config_path="conf/config.yaml"):
    """
    Standard YAML loader.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
    

from pyspark.sql import SparkSession

def get_spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
            .appName("real-time-log-metrics") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
    )
