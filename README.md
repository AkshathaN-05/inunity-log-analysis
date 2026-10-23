# InUnity Log Analysis Project

## 1. Project Overview

The InUnity Log Analysis Project processes application log data and generates useful analytics from it. The project uses Apache Airflow to manage the workflow and PySpark to read, process, and analyze the logs.

The processed data and analytics outputs are stored in Amazon S3. A PDF report is also generated to present the analysis results in a readable format.

## 2. Project Objectives

- Process application log data.
- Clean and transform logs into a structured format.
- Analyze log data to identify traffic patterns and useful statistics.
- Store processed data and outputs in Amazon S3.
- Generate a PDF report containing analytics results.
- Automate and manage the processing workflow using Apache Airflow.

## 3. Technologies Used

| Technology | Purpose |
|---|---|
| Python | Implements the processing and analytics logic |
| Apache Airflow | Orchestrates the log-processing workflow |
| Apache Spark (PySpark) | Processes and analyzes log data |
| Amazon S3 | Stores log data and generated outputs |
| PostgreSQL | Supports Airflow metadata storage |
| Docker Compose | Runs the project services in containers |
| Parquet | Stores structured processed data |
| PDF report | Presents analytics results in a readable format |

## 4. Project Architecture

```text
Application Log Data
        |
        v
Apache Airflow
(Workflow Orchestration)
        |
        v
PySpark Processing
        |
        v
Log Cleaning and Transformation
        |
        v
Structured Processed Data
        |
        v
Log Analytics
        |
        v
Analytics Outputs and PDF Report
        |
        v
Amazon S3
```

## 5. Project Workflow

1. **Workflow orchestration:** Apache Airflow manages the execution of the pipeline.
2. **Log reading:** PySpark reads the available log data.
3. **Data transformation:** The logs are cleaned and converted into a structured format.
4. **Data storage:** Processed data is stored in Parquet format.
5. **Analytics:** The pipeline calculates useful statistics and summarizes log activity.
6. **Report generation:** The analysis results are used to generate a PDF report.
7. **Output storage:** Generated outputs are stored in Amazon S3.

## 6. Analytics Performed

The analytics component is designed to summarize log activity, including:

- Request and operation counts
- Traffic distribution
- Hourly traffic patterns
- Client-related statistics
- Endpoint statistics
- Request-type distribution
- Minute-level traffic analysis

The PDF report provides a readable summary of the generated analytics.

## 7. Project Structure

```text
inunity-log-analysis/
│
├── airflow/
│   ├── dags/
│   │   └── inunity_log_pipeline.py
│   └── Dockerfile
│
├── spark_jobs/
│   ├── read_logs.py
│   ├── transform_logs.py
│   ├── analytics.py
│   └── spark_session.py
│
├── reports/
│   └── analytics_report_20261015_091049.pdf
│
├── docker-compose.yaml
├── .gitignore
└── README.md
```

## 8. Running the Project

The project uses Docker Compose to run its services.

1. Configure the required environment variables in the local `.env` file.
2. Configure the required AWS connection in Airflow.
3. Start the services from the project directory:

   ```bash
   docker compose up -d
   ```

4. Check the status of the running services:

   ```bash
   docker compose ps
   ```

5. Open the Airflow interface and trigger the pipeline.
6. Check the generated analytics report and configured Amazon S3 output location.

**Note:** AWS credentials and passwords should be configured locally and must not be committed to GitHub.

## 9. Project Output

The project generates a PDF report containing a summary of the log analytics.

An example report is maintained in the `reports/` directory:

`analytics_report_20261015_091049.pdf`

The pipeline also stores configured processing and analytics outputs in Amazon S3.

## 10. Future Enhancements

- Add additional log analytics and visualizations.
- Improve monitoring and error handling.
- Extend the pipeline to process additional log formats.
- Explore anomaly detection as a future enhancement.

## 11. Author

**Akshatha N**

