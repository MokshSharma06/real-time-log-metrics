
import yaml
import os
from pyspark.sql import SparkSession

# ---CONFIG LOADER ---

class AppConfig:
    def __init__(self, config_path="conf/config.yaml"):
        with open(config_path, 'r') as file:
            self._config = yaml.safe_load(file)
        
        self.env = self._config['environment']
        self.paths, self.checkpoints = self._build_paths()
        self.kafka = self._config['kafka']
        self.streaming = self._config['streaming']
        self.dedupe = self._config['deduplication']

    def _build_paths(self):
        if self.env == "azure":
            acc = self._config['azure']['account_name']
            data_cont = self._config['azure']['data_container']
            check_cont = self._config['azure']['checkpoint_container']
            prefix = self._config['azure']['sub_prefix']
            
            data_base = f"abfss://{data_cont}@{acc}.dfs.core.windows.net/{prefix}".strip("/")
            meta_base = f"abfss://{check_cont}@{acc}.dfs.core.windows.net/{prefix}".strip("/")
        else:
            data_base = os.path.abspath(self._config['local']['data_base'])
            meta_base = os.path.abspath(self._config['local']['checkpoint_base'])


        paths = {k: f"{data_base}/{v}" for k, v in self._config['paths'].items()}
        
        checkpoints = {
            "bronze": f"{meta_base}/bronze",
            "silver": f"{meta_base}/silver",
            "gold": f"{meta_base}/gold"
        }
        return paths, checkpoints

cfg = AppConfig()
    

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
