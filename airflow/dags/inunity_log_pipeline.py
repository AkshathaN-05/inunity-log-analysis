import os
import subprocess
import sys

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


# ============================================================
# Configuration
# ============================================================

AWS_CONN_ID = "aws_default"
AWS_REGION = "ap-south-1"

SPARK_JOBS_DIR = "/opt/airflow/spark_jobs"

SPARK_SHUFFLE_PARTITIONS = "8"


# ============================================================
# Spark job runner
# ============================================================

def run_spark_job(script_name):
    """
    Retrieve AWS credentials from the Airflow connection
    and execute the required PySpark job.
    """

    hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION
    )

    credentials = hook.get_credentials()

    if credentials is None:
        raise RuntimeError(
            "Could not retrieve AWS credentials from "
            f"Airflow connection: {AWS_CONN_ID}"
        )

    # --------------------------------------------------------
    # Prepare environment for Spark
    # --------------------------------------------------------

    environment = os.environ.copy()

    environment["AWS_ACCESS_KEY_ID"] = (
        credentials.access_key
    )

    environment["AWS_SECRET_ACCESS_KEY"] = (
        credentials.secret_key
    )

    environment["AWS_DEFAULT_REGION"] = AWS_REGION

    environment["SPARK_SHUFFLE_PARTITIONS"] = (
        SPARK_SHUFFLE_PARTITIONS
    )

    if credentials.token:
        environment["AWS_SESSION_TOKEN"] = (
            credentials.token
        )

    # --------------------------------------------------------
    # Locate Spark job
    # --------------------------------------------------------

    script_path = os.path.join(
        SPARK_JOBS_DIR,
        script_name
    )

    if not os.path.isfile(script_path):
        raise FileNotFoundError(
            f"Spark job not found: {script_path}"
        )

    print(
        f"Starting Spark job: {script_name}"
    )

    # --------------------------------------------------------
    # Execute Spark job
    # --------------------------------------------------------

    result = subprocess.run(
        [
            sys.executable,
            "-u",
            script_path
        ],
        env=environment,
        check=True
    )

    print(
        f"Spark job completed successfully: "
        f"{script_name}"
    )

    return result.returncode


# ============================================================
# DAG definition
# ============================================================

with DAG(
    dag_id="inunity_log_pipeline",

    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="Asia/Kolkata"
    ),

    # Run every day at 08:00 AM IST
    schedule=None,

    catchup=False,

    tags=[
        "inunity",
        "log-analysis"
    ],

    description=(
        "Daily InUnity log analysis pipeline "
        "using PySpark and AWS S3."
    )

) as dag:

    # ========================================================
    # TASK 1 — START
    # ========================================================

    start = EmptyOperator(
        task_id="start"
    )

    # ========================================================
    # TASK 2 — READ LOG FILE FROM S3
    # ========================================================

    read_log_file = PythonOperator(
        task_id="read_log_file_from_s3",

        python_callable=run_spark_job,

        op_args=[
            "read_logs.py"
        ]
    )

    # ========================================================
    # TASK 3 — CLEANING + TRANSFORMATION
    # ========================================================

    cleaning_transformation = PythonOperator(
        task_id="cleaning_and_transformation",

        python_callable=run_spark_job,

        op_args=[
            "transform_logs.py"
        ]
    )

    # ========================================================
    # TASK 4 — ANALYTICS
    # ========================================================

    analytics = PythonOperator(
        task_id="analytics",

        python_callable=run_spark_job,

        op_args=[
            "analytics.py"
        ]
    )

    # ========================================================
    # TASK DEPENDENCY
    # ========================================================

    (
        start
        >> read_log_file
        >> cleaning_transformation
        >> analytics
    )