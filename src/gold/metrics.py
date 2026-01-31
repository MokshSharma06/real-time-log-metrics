from pyspark.sql.functions import from_json, col, to_timestamp,expr
from src.utils import get_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    window,
    count,
    sum as sum_
)
from src.utils import cfg
bronze_path=cfg.paths['bronze']
silver_path=cfg.paths['silver']
clean_path=cfg.paths['silver']['clean']
bad_path=cfg.paths['silver']['bad']
late_path=cfg.paths['silver']['late']
gold_path =cfg.paths['gold']



def compute_service_error_metrics(
    silver_clean_df,
    window_duration=cfg.streaming['window']['duration'],
    watermark_duration=cfg.streaming['watermark_duration']
):

    # Flag error records
    base_df = silver_clean_df.withColumn(
        "is_error",
        when(col("status_code") >= 400, 1).otherwise(0)
    )

    # Windowed aggregation (Gold logic)
    gold_df = (
        base_df
        .withWatermark("event_time", watermark_duration)
        .groupBy(
            window(col("event_time"), window_duration),
            col("service_name")
        )
        .agg(
            count("*").alias("total_requests"),
            sum_("is_error").alias("error_count")
        )
        .withColumn(
            "error_rate",
            col("error_count") / col("total_requests")
        )
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")
    )


    return gold_df


def start_gold_stream(spark):

    silver_clean_df = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges","true")
        .option("skipChangeCommits", "true")
        .load(clean_path)
    )

    gold_df = compute_service_error_metrics(silver_clean_df)

    query = (
        gold_df.writeStream
        .format("delta")
        # .trigger(availableNow=True)
        .queryName("Gold_Stream")
        .outputMode("append")
        .option("checkpointLocation", cfg.checkpoints['gold'])
        .start(gold_path)
    )

    return query


if __name__ == "__main__":
    spark = get_spark_session()
    query = start_gold_stream(spark)
    query.awaitTermination()
