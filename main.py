from src.utils import get_spark_session
from src.utils import cfg
import conf.env_loader


from src.bronze.kafka_to_bronze import start_bronze_stream
from src.silver.bronze_to_silver import start_silver_stream
from src.gold.metrics import start_gold_stream


def main():
    spark = get_spark_session()

    # -------------------- START PIPELINES --------------------
    
    bronze_query = start_bronze_stream(spark)
    silver_query = start_silver_stream(spark)
    gold_query   = start_gold_stream(spark)

    # -------------------- WAIT --------------------
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
