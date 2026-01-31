
import yaml
import os
from pyspark.sql import SparkSession

# ---CONFIG LOADER ---

class AppConfig:
    def __init__(self, config_path="conf/config.yaml"):

        with open(config_path, "r") as file:
            self._config = yaml.safe_load(file)

        env = self._config["environment"]

        # env-resolved configs
        self.paths = self._config["paths"][env]
        self.checkpoints = self._config["checkpoints"][env]
        self.kafka = self._config["kafka"][env]

        # env-agnostic configs
        self.streaming = self._config["streaming"]
        self.dedupe = self._config["deduplication"]

cfg=AppConfig(config_path="/Workspace/Repos/mokhsharma@niveditasawatsh2004gmail.onmicrosoft.com/real-time-log-metrics/conf/config.yaml")



from pyspark.sql import SparkSession

def get_spark_session():
    from pyspark.sql import SparkSession

    return (
    SparkSession.builder
        .appName("real-time-log-metrics")
        .config("spark.sql.session.timeZone", "UTC")
        # Updated delta-spark to 3.1.0 to match Spark 3.5.0
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
)
