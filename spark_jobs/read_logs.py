import os

from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

from spark_session import create_spark_session


# ============================================================
# 1. Configuration
# ============================================================

INPUT_PATH = (
    "s3a://inunity-log-analytics-dev/"
    "raw/app.json-2025-11-12T12-52-22.123.log"
)


# ============================================================
# 2. AWS credential validation
# ============================================================

aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

if not aws_access_key or not aws_secret_key:
    raise RuntimeError(
        "AWS credentials were not provided to the Spark task."
    )


# ============================================================
# 3. Create Spark session
# ============================================================

spark = create_spark_session(
    "InUnity-Read-Logs"
)


# ============================================================
# 4. Fixed log schema
# ============================================================

log_schema = StructType([
    StructField("level", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("caller", StringType(), True),
    StructField("msg", StringType(), True),
    StructField("request_id", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("remote_addr", StringType(), True),
    StructField("method", StringType(), True),
    StructField("host", StringType(), True),
    StructField("path", StringType(), True)
])


# ============================================================
# 5. Read logs from S3
# ============================================================

print("\n========================================")
print("       READING LOGS FROM S3")
print("========================================")

df = (
    spark.read
    .schema(log_schema)
    .json(INPUT_PATH)
)


# ============================================================
# 6. Validate records
# ============================================================

validation = (
    df.agg(
        count("*").alias("total_records"),

        spark_sum(
            when(
                col("timestamp").isNull()
                | col("request_id").isNull()
                | col("method").isNull()
                | col("path").isNull(),
                1
            ).otherwise(0)
        ).alias("invalid_required")
    )
    .first()
)

total_records = validation["total_records"]
invalid_count = validation["invalid_required"] or 0


print(
    f"Total records read from S3: "
    f"{total_records}"
)

print(
    f"Records missing required fields: "
    f"{invalid_count}"
)


# ============================================================
# 7. Show schema
# ============================================================

print("\nSchema:")
df.printSchema()


# ============================================================
# 8. Show sample records
# ============================================================

print("\nSample records:")

df.show(
    5,
    truncate=False
)


# ============================================================
# 9. Final message
# ============================================================

print("\n========================================")
print("       S3 READ COMPLETE")
print("========================================")


spark.stop()