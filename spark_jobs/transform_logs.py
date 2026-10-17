import os

from pyspark import StorageLevel

from pyspark.sql.functions import (
    col,
    trim,
    lower,
    regexp_replace,
    to_timestamp,
    sha2,
    concat_ws,
    when,
    split,
    element_at,
    initcap,
    hour,
    minute,
    count,
    sum as spark_sum
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

OUTPUT_PATH = (
    "s3a://inunity-log-analytics-dev/"
    "cleaned/"
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
    "InUnity-Clean-Transform"
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
# 5. Read raw logs from S3
# ============================================================

print("\n========================================")
print("       READING LOGS FROM S3")
print("========================================")

df = (
    spark.read
    .schema(log_schema)
    .json(INPUT_PATH)
)

records_read = df.count()

print(
    f"Records read from S3: "
    f"{records_read}"
)


# ============================================================
# 6. Clean text fields
# ============================================================

text_columns = [
    "level",
    "method",
    "host",
    "path",
    "msg",
    "request_id",
    "created_by",
    "remote_addr",
    "caller"
]

for column_name in text_columns:

    df = df.withColumn(
        column_name,
        trim(col(column_name))
    )


# ============================================================
# 7. Parse timestamp
# ============================================================

df = df.withColumn(
    "event_timestamp",
    to_timestamp(
        col("timestamp"),
        "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    )
)


# ============================================================
# 8. Timestamp validation
# ============================================================

timestamp_validation = (
    df.agg(
        spark_sum(
            when(
                col("event_timestamp").isNotNull(),
                1
            ).otherwise(0)
        ).alias("valid_timestamps"),

        spark_sum(
            when(
                col("event_timestamp").isNull(),
                1
            ).otherwise(0)
        ).alias("invalid_timestamps")
    )
    .first()
)

valid_timestamp_count = (
    timestamp_validation["valid_timestamps"] or 0
)

invalid_timestamp_count = (
    timestamp_validation["invalid_timestamps"] or 0
)


print("\n========================================")
print("       TIMESTAMP VALIDATION")
print("========================================")

print(
    f"Valid timestamps:   "
    f"{valid_timestamp_count}"
)

print(
    f"Invalid timestamps: "
    f"{invalid_timestamp_count}"
)


# ============================================================
# 9. Keep only valid timestamp records
# ============================================================

valid_df = df.filter(
    col("event_timestamp").isNotNull()
)


# ============================================================
# 10. Remove duplicate events
# ============================================================

before_dedup = valid_timestamp_count

valid_df = valid_df.dropDuplicates([
    "request_id",
    "event_timestamp",
    "method",
    "path"
])

after_dedup = valid_df.count()

duplicates_removed = (
    before_dedup - after_dedup
)


print("\n========================================")
print("       DUPLICATE VALIDATION")
print("========================================")

print(
    f"Before deduplication: "
    f"{before_dedup}"
)

print(
    f"After deduplication:  "
    f"{after_dedup}"
)

print(
    f"Duplicates removed:   "
    f"{duplicates_removed}"
)


# ============================================================
# 11. Hash remote address for privacy
# ============================================================

valid_df = valid_df.withColumn(
    "client_hash",
    sha2(
        col("remote_addr"),
        256
    )
)

valid_df = valid_df.drop(
    "remote_addr"
)


# ============================================================
# 12. Normalize UUIDs in API paths
# ============================================================

uuid_pattern = (
    r"[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}"
)

valid_df = valid_df.withColumn(
    "normalized_path",
    regexp_replace(
        col("path"),
        uuid_pattern,
        "{id}"
    )
)


# ============================================================
# 13. Create time-based columns
# ============================================================

valid_df = (
    valid_df

    .withColumn(
        "event_date",
        col("event_timestamp").cast("date")
    )

    .withColumn(
        "event_hour",
        hour(col("event_timestamp"))
    )

    .withColumn(
        "event_minute",
        minute(col("event_timestamp"))
    )
)


# ============================================================
# 14. Create request type
# ============================================================

valid_df = valid_df.withColumn(
    "request_type",
    when(
        lower(col("method")).isin(
            "get",
            "post",
            "put",
            "patch",
            "delete"
        ),
        lower(col("method"))
    ).otherwise("other")
)


# ============================================================
# 15. Create API operation
# ============================================================

valid_df = valid_df.withColumn(
    "operation",
    concat_ws(
        " ",
        lower(col("method")),
        col("normalized_path")
    )
)


# ============================================================
# 16. Feature classification
# ============================================================

# Remove tenant prefix:
#
# /t/{id}/users
#       ↓
# /users
#
# Also handle:
#
# /t/null/users

feature_path = regexp_replace(
    col("normalized_path"),
    r"^/t/(?:\{id\}|null)",
    ""
)


# Remove API version:
#
# /v1/users
# /v2/users
# /v10/users
#
# becomes:
#
# /users

feature_path = regexp_replace(
    feature_path,
    r"^/v\d+",
    ""
)


valid_df = valid_df.withColumn(
    "feature",

    # --------------------------------------------------------
    # Notifications
    # --------------------------------------------------------

    when(
        feature_path.rlike(
            r"^/(notifications|fcm|circulars)"
        ),
        "Notifications"
    )

    # --------------------------------------------------------
    # Forms & Assessments
    # --------------------------------------------------------

    .when(
        feature_path.rlike(
            r"^/(questions|forms|options)"
        ),
        "Forms & Assessments"
    )

    # --------------------------------------------------------
    # User Management
    # --------------------------------------------------------

    .when(
        feature_path.rlike(
            r"^/(users|roles|faculty-details)"
        ),
        "User Management"
    )

    # --------------------------------------------------------
    # Academic Structure
    # --------------------------------------------------------

    .when(
        feature_path.rlike(
            r"^/(departments|academic-years|courses|"
            r"classrooms|semesters|attendance)"
        ),
        "Academic Structure"
    )

    # --------------------------------------------------------
    # Outcomes Management
    # --------------------------------------------------------

    .when(
        feature_path.rlike(
            r"^/(course-outcome|program-outcome)"
        ),
        "Outcomes Management"
    )

    # --------------------------------------------------------
    # Skillbuilder
    # --------------------------------------------------------

    .when(
        feature_path.rlike(
            r"^/skillbuilder"
        ),
        "Skillbuilder"
    )

    # --------------------------------------------------------
    # Tenant / Organization
    # --------------------------------------------------------

    .when(
        feature_path.rlike(
            r"^/(tenants|super-admin)"
        ),
        "Tenant / Org"
    )

    # --------------------------------------------------------
    # Empty/root path
    # --------------------------------------------------------

    .when(
        (feature_path == "")
        | (feature_path == "/"),
        "Uncategorized"
    )

    # --------------------------------------------------------
    # Fallback classification
    # --------------------------------------------------------

    .otherwise(
        initcap(
            regexp_replace(
                element_at(
                    split(feature_path, "/"),
                    2
                ),
                "-",
                " "
            )
        )
    )
)


# ============================================================
# 17. Remove original timestamp
# ============================================================

valid_df = valid_df.drop(
    "timestamp"
)


# ============================================================
# 18. Persist transformed DataFrame
# ============================================================

valid_df = valid_df.persist(
    StorageLevel.MEMORY_AND_DISK
)


# ============================================================
# 19. Materialize cache once
# ============================================================

final_count = valid_df.count()


print("\n========================================")
print("       AFTER TRANSFORMATION")
print("========================================")

print(
    f"Final records: "
    f"{final_count}"
)

print("\nFinal schema:")

valid_df.printSchema()


# ============================================================
# 20. Feature distribution
# ============================================================

print("\n========================================")
print("       FEATURE USAGE")
print("========================================")

(
    valid_df
    .groupBy("feature")
    .count()
    .orderBy(col("count").desc())
    .show(
        20,
        truncate=False
    )
)


# ============================================================
# 21. Feature classification check
# ============================================================

print("\n========================================")
print("       FEATURE CLASSIFICATION CHECK")
print("========================================")

incorrect_feature_count = (
    valid_df
    .filter(
        col("feature").isin(
            "T",
            "V1"
        )
    )
    .count()
)

print(
    "Incorrect T/V1 feature records: "
    f"{incorrect_feature_count}"
)


if incorrect_feature_count > 0:

    print("\nRemaining T/V1 records:")

    (
        valid_df
        .filter(
            col("feature").isin(
                "T",
                "V1"
            )
        )
        .select(
            "normalized_path",
            "feature"
        )
        .show(
            20,
            truncate=False
        )
    )


# ============================================================
# 22. Sample transformed records
# ============================================================

print("\n========================================")
print("       SAMPLE TRANSFORMED RECORDS")
print("========================================")

(
    valid_df
    .select(
        "event_timestamp",
        "method",
        "normalized_path",
        "request_type",
        "operation",
        "feature",
        "client_hash"
    )
    .show(
        5,
        truncate=False
    )
)


# ============================================================
# 23. Save cleaned data to S3
# ============================================================

print("\n========================================")
print("       SAVING CLEANED DATA TO S3")
print("========================================")

(
    valid_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print(
    f"Cleaned data saved to: "
    f"{OUTPUT_PATH}"
)


# ============================================================
# 24. Release cached data
# ============================================================

valid_df.unpersist()


# ============================================================
# 25. Stop Spark
# ============================================================

spark.stop()

print("\n========================================")
print("       TRANSFORMATION COMPLETE")
print("========================================")