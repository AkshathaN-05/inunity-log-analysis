
import os
import builtins
from io import BytesIO
from datetime import datetime
from zoneinfo import ZoneInfo
from xml.sax.saxutils import escape

import boto3

from pyspark import StorageLevel
from pyspark.sql.functions import (
    col,
    count,
    desc,
    round,
    avg,
    stddev,
    countDistinct,
    min as spark_min,
    max as spark_max,
)

from spark_session import create_spark_session

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)


# ============================================================
# 1. CONFIGURATION
# ============================================================

BUCKET_NAME = "inunity-log-analytics-dev"

CLEANED_PATH = f"s3a://{BUCKET_NAME}/cleaned/"
ANALYTICS_BASE_PATH = f"s3a://{BUCKET_NAME}/analytics/"

LOCAL_REPORT_DIR = "/opt/airflow/reports"
S3_REPORT_PREFIX = "reports/"


# ============================================================
# 2. AWS CREDENTIAL VALIDATION
# ============================================================

aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

if not aws_access_key or not aws_secret_key:
    raise RuntimeError(
        "AWS credentials were not provided to the Spark task."
    )


# ============================================================
# 3. CREATE SPARK SESSION
# ============================================================

spark = create_spark_session("InUnity-Log-Analytics")


try:
    # ========================================================
    # 4. READ CLEANED DATA FROM S3
    # ========================================================

    print("\n========================================")
    print("       INUNITY LOG ANALYTICS")
    print("========================================")

    print("\nReading cleaned data from:")
    print(CLEANED_PATH)

    df = spark.read.parquet(CLEANED_PATH)

    # ========================================================
    # 5. CACHE CLEANED DATASET
    # ========================================================

    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    # ========================================================
    # 6. FINAL SUMMARY METRICS
    # ========================================================

    summary = (
        df.agg(
            count("*").alias("total_requests"),
            countDistinct("operation").alias("unique_operations"),
            countDistinct("client_hash").alias("unique_clients"),
            countDistinct("feature").alias("unique_features"),
            spark_min("event_date").alias("start_date"),
            spark_max("event_date").alias("end_date"),
        )
        .first()
    )

    total_requests = summary["total_requests"] or 0

    print("\n========================================")
    print("FINAL ANALYTICS SUMMARY")
    print("========================================")

    print("Total Requests:", summary["total_requests"])
    print("Unique API Operations:", summary["unique_operations"])
    print("Unique Clients:", summary["unique_clients"])
    print("Unique Features:", summary["unique_features"])
    print("Start Date:", summary["start_date"])
    print("End Date:", summary["end_date"])

    # ========================================================
    # 7. TOP API OPERATIONS
    # ========================================================

    print("\n========================================")
    print("1. TOP API OPERATIONS")
    print("========================================")

    operation_counts = (
        df.groupBy("operation")
        .agg(count("*").alias("request_count"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # Materialize the cached operation counts.
    operation_counts.count()

    top_operations = operation_counts.orderBy(
        desc("request_count")
    )

    top_operations.show(20, truncate=False)

    # ========================================================
    # 8. API TRAFFIC PERCENTAGE
    # ========================================================

    print("\n========================================")
    print("2. API TRAFFIC PERCENTAGE")
    print("========================================")

    # Avoid division by zero if the cleaned dataset is empty.
    percentage_denominator = total_requests if total_requests else 1

    traffic_percentage = operation_counts.withColumn(
        "traffic_percentage",
        round(
            (col("request_count") / percentage_denominator) * 100,
            2,
        ),
    )

    (
        traffic_percentage
        .orderBy(desc("traffic_percentage"))
        .show(20, truncate=False)
    )

    # ========================================================
    # 9. TRAFFIC BY HOUR
    # ========================================================

    print("\n========================================")
    print("3. TRAFFIC BY HOUR")
    print("========================================")

    traffic_by_hour = (
        df.groupBy("event_hour")
        .agg(count("*").alias("request_count"))
    )

    traffic_by_hour.orderBy("event_hour").show(
        24, truncate=False
    )

    # ========================================================
    # 10. TOP CLIENTS
    # ========================================================

    print("\n========================================")
    print("4. TOP CLIENTS")
    print("========================================")

    top_clients = (
        df.groupBy("client_hash")
        .agg(count("*").alias("request_count"))
    )

    (
        top_clients
        .orderBy(desc("request_count"))
        .show(20, truncate=False)
    )

    # ========================================================
    # 11. ENDPOINT SPIKE CANDIDATES
    # ========================================================

    print("\n========================================")
    print("5. ENDPOINT SPIKE DETECTION")
    print("========================================")

    minute_traffic = (
        df.groupBy(
            "event_date",
            "event_hour",
            "event_minute",
            "operation",
        )
        .agg(count("*").alias("request_count"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # Materialize cached minute traffic.
    minute_traffic.count()

    endpoint_statistics = (
        minute_traffic
        .groupBy("operation")
        .agg(
            avg("request_count").alias("average_requests"),
            stddev("request_count").alias("stddev_requests"),
        )
    )

    spikes = (
        minute_traffic
        .join(
            endpoint_statistics,
            on="operation",
            how="left",
        )
        .withColumn(
            "spike_threshold",
            col("average_requests")
            + (3 * col("stddev_requests")),
        )
        .filter(
            col("request_count") > col("spike_threshold")
        )
    )

    spikes.orderBy(desc("request_count")).show(
        20, truncate=False
    )

    # ========================================================
    # 12. FEATURE USAGE
    # ========================================================

    print("\n========================================")
    print("6. FEATURE USAGE")
    print("========================================")

    feature_usage = (
        df.groupBy("feature")
        .agg(count("*").alias("request_count"))
    )

    (
        feature_usage
        .orderBy(desc("request_count"))
        .show(30, truncate=False)
    )

    # ========================================================
    # 13. REQUEST TYPE DISTRIBUTION
    # ========================================================

    print("\n========================================")
    print("7. REQUEST TYPE DISTRIBUTION")
    print("========================================")

    request_type_distribution = (
        df.groupBy("request_type")
        .agg(count("*").alias("request_count"))
    )

    (
        request_type_distribution
        .orderBy(desc("request_count"))
        .show(truncate=False)
    )

    # ========================================================
    # 14. DATE RANGE
    # ========================================================

    print("\n========================================")
    print("DATE RANGE")
    print("========================================")

    (
        df.select("event_date")
        .distinct()
        .orderBy("event_date")
        .show(10, truncate=False)
    )

    # ========================================================
    # 15. WRITE ANALYTICS RESULTS TO S3
    # ========================================================

    print("\n========================================")
    print("WRITING ANALYTICS RESULTS TO S3")
    print("========================================")

    analytics_outputs = [
        (operation_counts, "top_operations/"),
        (traffic_percentage, "traffic_percentage/"),
        (traffic_by_hour, "traffic_by_hour/"),
        (top_clients, "top_clients/"),
        (spikes, "endpoint_spikes/"),
        (feature_usage, "feature_usage/"),
        (
            request_type_distribution,
            "request_type_distribution/",
        ),
    ]

    for result_df, output_folder in analytics_outputs:
        output_path = ANALYTICS_BASE_PATH + output_folder

        print("Writing:", output_path)

        (
            result_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(output_path)
        )

    print("\nAnalytics results successfully written to S3.")

    # ========================================================
    # 16. GENERATE PDF REPORT
    # ========================================================

    print("\n========================================")
    print("GENERATING ANALYTICS PDF REPORT")
    print("========================================")

    report_time = datetime.now(ZoneInfo("Asia/Kolkata"))

    filename = (
        "analytics_report_"
        + report_time.strftime("%Y%m%d_%H%M%S")
        + ".pdf"
    )

    local_pdf_path = os.path.join(
        LOCAL_REPORT_DIR,
        filename,
    )

    s3_pdf_key = S3_REPORT_PREFIX + filename

    # The directory must be mounted to the Windows project folder
    # in docker-compose.yaml.
    os.makedirs(LOCAL_REPORT_DIR, exist_ok=True)

    pdf_buffer = BytesIO()

    document = SimpleDocTemplate(
        pdf_buffer,
        pagesize=landscape(A4),
        rightMargin=12 * mm,
        leftMargin=12 * mm,
        topMargin=12 * mm,
        bottomMargin=12 * mm,
    )

    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        alignment=TA_CENTER,
        fontSize=20,
        leading=25,
        spaceAfter=8,
    )

    subtitle_style = ParagraphStyle(
        "ReportSubtitle",
        parent=styles["Normal"],
        alignment=TA_CENTER,
        fontSize=9,
        leading=12,
        textColor=colors.grey,
    )

    section_style = ParagraphStyle(
        "ReportSection",
        parent=styles["Heading2"],
        fontSize=13,
        leading=16,
        spaceBefore=10,
        spaceAfter=6,
    )

    cell_style = ParagraphStyle(
        "ReportCell",
        parent=styles["Normal"],
        fontSize=7,
        leading=9,
        wordWrap="CJK",
    )

    header_style = ParagraphStyle(
        "ReportHeader",
        parent=cell_style,
        fontName="Helvetica-Bold",
        textColor=colors.white,
    )

    story = [
        Paragraph("InUnity Log Analytics Report", title_style),
        Paragraph(
            "Generated on "
            + report_time.strftime(
                "%d %B %Y, %I:%M:%S %p IST"
            ),
            subtitle_style,
        ),
        Spacer(1, 8 * mm),
    ]

    def display_value(value):
        """Convert values to safe, readable PDF text."""
        if value is None:
            text_value = "N/A"
        else:
            text_value = str(value)

        return escape(text_value)

    def add_report_table(
        section_title,
        headers,
        rows,
        column_widths=None,
    ):
        """Add a formatted table to the report."""

        story.append(
            Paragraph(
                escape(section_title),
                section_style,
            )
        )

        if not rows:
            story.append(
                Paragraph(
                    "No records available for this section.",
                    styles["Normal"],
                )
            )
            story.append(Spacer(1, 4 * mm))
            return

        table_data = [
            [
                Paragraph(display_value(header), header_style)
                for header in headers
            ]
        ]

        for row in rows:
            table_data.append(
                [
                    Paragraph(display_value(value), cell_style)
                    for value in row
                ]
            )

        table = Table(
            table_data,
            colWidths=column_widths,
            repeatRows=1,
            hAlign="LEFT",
        )

        table.setStyle(
            TableStyle(
                [
                    (
                        "BACKGROUND",
                        (0, 0),
                        (-1, 0),
                        colors.HexColor("#243B53"),
                    ),
                    (
                        "TEXTCOLOR",
                        (0, 0),
                        (-1, 0),
                        colors.white,
                    ),
                    (
                        "GRID",
                        (0, 0),
                        (-1, -1),
                        0.4,
                        colors.HexColor("#BCCCDC"),
                    ),
                    (
                        "VALIGN",
                        (0, 0),
                        (-1, -1),
                        "TOP",
                    ),
                    (
                        "LEFTPADDING",
                        (0, 0),
                        (-1, -1),
                        5,
                    ),
                    (
                        "RIGHTPADDING",
                        (0, 0),
                        (-1, -1),
                        5,
                    ),
                    (
                        "TOPPADDING",
                        (0, 0),
                        (-1, -1),
                        5,
                    ),
                    (
                        "BOTTOMPADDING",
                        (0, 0),
                        (-1, -1),
                        5,
                    ),
                    (
                        "ROWBACKGROUNDS",
                        (0, 1),
                        (-1, -1),
                        [
                            colors.white,
                            colors.HexColor("#F0F4F8"),
                        ],
                    ),
                ]
            )
        )

        story.append(table)
        story.append(Spacer(1, 5 * mm))

    # --------------------------------------------------------
    # A. Overall summary
    # --------------------------------------------------------

    summary_rows = [
        ["Total Requests", summary["total_requests"]],
        ["Unique API Operations", summary["unique_operations"]],
        ["Unique Clients", summary["unique_clients"]],
        ["Unique Features", summary["unique_features"]],
        ["Start Date", summary["start_date"]],
        ["End Date", summary["end_date"]],
    ]

    add_report_table(
        "1. Overall Summary",
        ["Metric", "Value"],
        summary_rows,
        [90 * mm, 90 * mm],
    )

    # --------------------------------------------------------
    # B. Top API operations
    # --------------------------------------------------------

    operation_rows = [
        [row["operation"], row["request_count"]]
        for row in top_operations.limit(20).collect()
    ]

    add_report_table(
        "2. Top API Operations",
        ["Operation", "Request Count"],
        operation_rows,
        [180 * mm, 40 * mm],
    )

    # --------------------------------------------------------
    # C. API traffic percentage
    # --------------------------------------------------------

    traffic_rows = [
        [
            row["operation"],
            row["request_count"],
            row["traffic_percentage"],
        ]
        for row in (
            traffic_percentage
            .orderBy(desc("traffic_percentage"))
            .limit(20)
            .collect()
        )
    ]

    add_report_table(
        "3. API Traffic Percentage",
        ["Operation", "Requests", "Traffic (%)"],
        traffic_rows,
        [130 * mm, 45 * mm, 45 * mm],
    )

    # --------------------------------------------------------
    # D. Traffic by hour
    # --------------------------------------------------------

    hour_rows = [
        [row["event_hour"], row["request_count"]]
        for row in (
            traffic_by_hour
            .orderBy("event_hour")
            .limit(24)
            .collect()
        )
    ]

    add_report_table(
        "4. Traffic by Hour",
        ["Hour", "Request Count"],
        hour_rows,
        [90 * mm, 90 * mm],
    )

    # --------------------------------------------------------
    # E. Top clients
    # --------------------------------------------------------

    client_rows = [
        [row["client_hash"], row["request_count"]]
        for row in (
            top_clients
            .orderBy(desc("request_count"))
            .limit(20)
            .collect()
        )
    ]

    add_report_table(
        "5. Top Clients",
        ["Client Hash", "Request Count"],
        client_rows,
        [150 * mm, 50 * mm],
    )

    # --------------------------------------------------------
    # F. Endpoint spike candidates
    # --------------------------------------------------------

    spike_rows = []

    for row in (
        spikes
        .orderBy(desc("request_count"))
        .limit(20)
        .collect()
    ):
        threshold = row["spike_threshold"]

        # IMPORTANT:
        # Use builtins.round for a normal Python float.
        # The imported Spark round() accepts Spark columns.
        rounded_threshold = (
            builtins.round(threshold, 2)
            if threshold is not None
            else None
        )

        spike_rows.append(
            [
                row["event_date"],
                row["event_hour"],
                row["event_minute"],
                row["operation"],
                row["request_count"],
                rounded_threshold,
            ]
        )

    add_report_table(
        "6. Endpoint Spike Candidates",
        [
            "Date",
            "Hour",
            "Minute",
            "Operation",
            "Requests",
            "Spike Threshold",
        ],
        spike_rows,
        [
            28 * mm,
            15 * mm,
            17 * mm,
            95 * mm,
            25 * mm,
            35 * mm,
        ],
    )

    # --------------------------------------------------------
    # G. Feature usage
    # --------------------------------------------------------

    feature_rows = [
        [row["feature"], row["request_count"]]
        for row in (
            feature_usage
            .orderBy(desc("request_count"))
            .limit(20)
            .collect()
        )
    ]

    add_report_table(
        "7. Feature Usage",
        ["Feature", "Request Count"],
        feature_rows,
        [150 * mm, 50 * mm],
    )

    # --------------------------------------------------------
    # H. Request type distribution
    # --------------------------------------------------------

    request_type_rows = [
        [row["request_type"], row["request_count"]]
        for row in (
            request_type_distribution
            .orderBy(desc("request_count"))
            .collect()
        )
    ]

    add_report_table(
        "8. Request Type Distribution",
        ["Request Type", "Request Count"],
        request_type_rows,
        [120 * mm, 80 * mm],
    )

    # --------------------------------------------------------
    # Build and validate the PDF
    # --------------------------------------------------------

    document.build(story)

    pdf_bytes = pdf_buffer.getvalue()
    pdf_buffer.close()

    if not pdf_bytes.startswith(b"%PDF"):
        raise RuntimeError(
            "PDF generation failed: invalid PDF content."
        )

    # --------------------------------------------------------
    # 17. SAVE PDF TO YOUR PC
    # --------------------------------------------------------

    with open(local_pdf_path, "wb") as pdf_file:
        pdf_file.write(pdf_bytes)

    print("\nPDF saved locally:", local_pdf_path)

    # --------------------------------------------------------
    # 18. UPLOAD THE SAME PDF TO S3
    # --------------------------------------------------------

    s3_client = boto3.client(
        "s3",
        region_name=os.environ.get(
            "AWS_DEFAULT_REGION",
            "ap-south-1",
        ),
    )

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_pdf_key,
        Body=pdf_bytes,
        ContentType="application/pdf",
    )

    print(
        "PDF uploaded to S3:",
        f"s3://{BUCKET_NAME}/{s3_pdf_key}",
    )

    print(
        "\nPDF report generated, saved locally, "
        "and uploaded successfully."
    )

    print("\n========================================")
    print("       ANALYTICS COMPLETED")
    print("========================================")

finally:
    # Release cached datasets and stop Spark even if the
    # analytics or PDF generation fails.
    try:
        if "operation_counts" in locals():
            operation_counts.unpersist()

        if "minute_traffic" in locals():
            minute_traffic.unpersist()

        if "df" in locals():
            df.unpersist()
    finally:
        spark.stop()
