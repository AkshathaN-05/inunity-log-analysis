import os

from pyspark.sql import SparkSession


def create_spark_session(app_name):
    """
    Create a Spark session with common configuration
    used by all InUnity Spark jobs.
    """

    aws_region = os.environ.get(
        "AWS_DEFAULT_REGION",
        "ap-south-1"
    )

    shuffle_partitions = os.environ.get(
        "SPARK_SHUFFLE_PARTITIONS",
        "8"
    )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # ---------------------------------------------
        # S3 connectivity
        # ---------------------------------------------
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.5.0"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            aws_region
        )

        # ---------------------------------------------
        # Performance tuning
        # ---------------------------------------------
        .config(
            "spark.sql.shuffle.partitions",
            shuffle_partitions
        )
        .config(
            "spark.default.parallelism",
            shuffle_partitions
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark