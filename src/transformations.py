from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, current_timestamp
# record status
STATUS_CLEAN = "CLEAN"
STATUS_LATE = "LATE"
STATUS_BAD = "BAD"

# rejection reasons
REASON_NONE = "NONE"
REASON_WATERMARK = "WATERMARK_EXCEEDED"
REASON_SCHEMA = "SCHEMA_INVALID"

def classify_data(df: DataFrame) -> DataFrame:
    classified_df = (
        df.withColumn(
            "record_status",
            when(col("schema_valid") == False, lit(STATUS_BAD))
            .when(col("is_late") == True, lit(STATUS_LATE))
            .otherwise(lit(STATUS_CLEAN))
        )
        .withColumn(
            "rejection_reason",
            when(col("schema_valid") == False, lit(REASON_SCHEMA))
            .when(col("is_late") == True, lit(REASON_WATERMARK))
            .otherwise(lit(REASON_NONE))
        )
        .withColumn("processed_at", current_timestamp())
    )

    return classified_df
