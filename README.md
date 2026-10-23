# InUnity Log Analysis Project

## 1. Project Overview

The InUnity Log Analysis Project is a data-processing pipeline designed to process application log data and generate useful analytical summaries.

Application logs contain information about requests, operations, clients, endpoints, and traffic over time. Analyzing these logs helps us understand application activity and identify useful patterns in the data.

This project uses Apache Airflow to orchestrate the workflow and PySpark to process and analyze the log data. Amazon S3 is used to store the input data and configured outputs. The project also generates a PDF report to present the analytics results in a readable format.

The pipeline is containerized using Docker Compose to make it easier to run the required services.

## 2. Project Objectives

The main objectives of this project are:

- Read application log data from the configured source.
- Clean and transform raw logs into a structured format.
- Process large amounts of log data using PySpark.
- Calculate useful statistics about application activity and traffic.
- Store processed data and analytics outputs in Amazon S3.
- Generate a PDF report containing the analytics results.
- Organize the processing steps using Apache Airflow.
- Maintain project documentation and generated outputs in GitHub.

## 3. Technologies Used

| Technology | Purpose |
|---|---|
| Python | Implements the data-processing and analytics logic |
| Apache Airflow | Orchestrates and manages pipeline tasks |
| Apache Spark / PySpark | Processes, transforms, and analyzes log data |
| Amazon S3 | Stores input data and configured output files |
| PostgreSQL | Stores Airflow metadata |
| Docker | Runs application services in containers |
| Docker Compose | Starts and manages the project's services |
| Parquet | Stores structured processed data |
| PDF | Presents analytics results in a readable report |
| Git and GitHub | Tracks source-code changes and hosts the project repository |

## 4. Project Architecture

The project follows a pipeline-based architecture. Each stage performs a specific task and passes its output to the next stage.

```text
                 Application Log Data
                          |
                          v
                 Apache Airflow DAG
              (Workflow Orchestration)
                          |
                          v
                 Read Logs using
                      PySpark
                          |
                          v
                 Clean and Transform
                    the Log Data
                          |
                          v
                Structured Data Output
                    (Parquet)
                          |
                          v
                 Analytics Processing
                          |
                          v
                 Analytics Results
                          |
                          v
                   PDF Report
                          |
                          v
                    Amazon S3
               (Configured Outputs)
```

**Architecture explanation**

- **Application logs:** The source data processed by the pipeline.
- **Apache Airflow:** Coordinates the execution of the pipeline tasks.
- **PySpark:** Reads, transforms, and analyzes the log data.
- **Parquet:** Provides a structured format for storing processed data.
- **Analytics processing:** Calculates statistics and summarizes log activity.
- **PDF report:** Presents the generated analytics in a human-readable format.
- **Amazon S3:** Stores data and outputs according to the project configuration.

The diagram is a high-level representation. The exact task dependencies and storage locations should match the implementation in the Airflow DAG and Spark scripts.

## 5. Project Directory Structure

The main project files are organized as follows:

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

### Purpose of the important files

**`airflow/dags/inunity_log_pipeline.py`**

Defines the Airflow DAG responsible for organizing the pipeline tasks and their execution order.

**`spark_jobs/read_logs.py`**

Contains the log-reading logic used by the processing workflow.

**`spark_jobs/transform_logs.py`**

Contains the logic for transforming raw log data into a structured format.

**`spark_jobs/analytics.py`**

Contains the analytics logic used to calculate statistics from the processed logs and generate analytical outputs.

**`spark_jobs/spark_session.py`**

Provides Spark session setup used by the Spark jobs.

**`airflow/Dockerfile`**

Defines how the Airflow container image is built for the project.

**`docker-compose.yaml`**

Defines the services used to run the project, including the relevant Airflow and PostgreSQL services.

**`reports/`**

Contains the generated PDF report included as a project artifact.

**`.gitignore`**

Specifies files that should not be tracked by Git, such as local environment files, credentials, temporary files, and generated files that are not intended for version control.

## 6. End-to-End Project Workflow

### Step 1: Start the project services

Docker Compose is used to start the services required by the pipeline.

Airflow provides the interface for managing and triggering pipeline execution, while PostgreSQL supports Airflow metadata storage.

### Step 2: Read the application logs

The log-reading stage loads the available input data from the configured source.

The source location and input format depend on the project's configuration and implementation.

### Step 3: Transform the logs

The transformation stage prepares the data for analysis.

Depending on the actual log format and implemented transformation rules, this can include parsing fields, handling missing values, standardizing values, and preparing structured records.

### Step 4: Store processed data

The transformed data can be written in Parquet format for efficient storage and further processing.

The output destination is determined by the Spark job configuration.

### Step 5: Perform analytics

The analytics stage processes the structured data and calculates statistics related to log activity.

These statistics help summarize requests, clients, endpoints, request types, and traffic patterns.

### Step 6: Generate the PDF report

The project generates a PDF report containing a readable summary of the analytics results.

The report can be reviewed independently of the pipeline execution interface.

### Step 7: Store the outputs

The configured output files are stored in Amazon S3.

The exact S3 locations and output filenames are determined by the project configuration and execution.

### Step 8: Review the results

The generated report and processed data can be reviewed to understand the results of the log analysis.

The report included in the repository serves as an example of a generated project output.

## 7. Analytics Performed

The analytics component is intended to summarize application log activity.

The areas covered by the project include:

### 7.1 Request and operation counts

Summarizes the number of requests or operations recorded in the processed log data.

### 7.2 Traffic distribution

Helps understand how the recorded traffic is distributed across the available categories or time periods.

### 7.3 Hourly traffic analysis

Summarizes traffic by hour to help identify periods of higher or lower activity.

### 7.4 Client-related statistics

Provides statistics about clients represented in the log data, depending on the fields available in the input.

### 7.5 Endpoint statistics

Summarizes activity across the available application endpoints.

### 7.6 Request-type distribution

Shows how requests are distributed across the request types represented in the logs.

### 7.7 Minute-level traffic analysis

Provides a more detailed view of traffic at the minute level.

The precise metrics, grouping fields, and calculations should be confirmed against `spark_jobs/analytics.py`. The sections above describe the analytics areas discussed for this project; they do not imply that every possible metric is present in every generated report.

## 8. Generated Output

The project generates a PDF report containing a summary of the log analytics.

The example report committed to this repository is:

`reports/analytics_report_20261015_091049.pdf`

This file can be opened directly to review the generated report.

The pipeline also produces configured processing and analytics outputs for storage in Amazon S3.

**Important:** The committed PDF is an example output from a particular run. Its contents may differ from reports generated by later executions.

## 9. Prerequisites

Before running the project, ensure the following are available:

- Git
- Docker Desktop or a compatible Docker Engine
- Docker Compose
- Access to the project's required AWS resources
- Appropriate AWS credentials and permissions
- The project source code and configuration files

The required Docker, Python, Spark, and Airflow versions should be confirmed from the project configuration files.

## 10. Configuration and Security

The project requires appropriate configuration for the services and resources it uses.

### Environment variables

Local passwords and other environment-specific settings should be stored in a local `.env` file when required by the Compose configuration.

The `.env` file must not be committed to GitHub.

### AWS configuration

The AWS connection and credentials must be configured according to the project's implementation. The Airflow pipeline uses the configured AWS connection, while Spark jobs may use AWS environment variables.

AWS credentials must never be hardcoded in source files or included in the public repository.

### Amazon S3 permissions

The configured AWS identity must have the required permissions to read input data and write the intended outputs.

### Public repository safety

Before uploading source code or reports to a public repository, check that they do not contain credentials, sensitive log contents, customer information, or other confidential information.

## 11. Running the Project

The following are the basic commands for starting and checking the Docker Compose services.

Run them from the project root directory.

### Step 1: Start the services

```bash
docker compose up -d
```

This starts the services defined in the Compose configuration.

### Step 2: Check service status

```bash
docker compose ps
```

Check that the required services are running.

### Step 3: Open Airflow

Open the Airflow web interface using the port configured in `docker-compose.yaml`.

Log in using the credentials configured for the local environment.

### Step 4: Trigger the pipeline

Locate the project DAG in the Airflow interface and trigger it according to the project's DAG configuration.

The DAG's actual schedule and trigger settings should be checked in `airflow/dags/inunity_log_pipeline.py`.

### Step 5: Check task execution

Review the DAG run and individual task statuses in Airflow. If a task fails, inspect its logs to identify the cause.

### Step 6: Review the outputs

Check the configured output location in Amazon S3 and review the generated PDF report.

**Note:** Successful execution depends on the correct environment configuration, AWS permissions, available input data, and the project's task configuration.

## 12. Troubleshooting

### Docker services do not start

Check the Docker installation and inspect the service status.

```bash
docker compose ps
```

Review service logs if a container exits unexpectedly.

```bash
docker compose logs
```

### Airflow task fails

Open the failed task in the Airflow interface and review its logs. Check the input location, configuration, credentials, and error message.

### AWS access errors

Verify the configured AWS connection or environment variables and confirm that the AWS identity has the required S3 permissions.

Do not paste credentials into issue reports or commit them to GitHub.

### Expected output is missing

Check the task execution status and inspect the configured output paths in the DAG and Spark scripts.

## 13. Version Control and GitHub

Git is used to track changes to the project source code, while GitHub hosts the repository.

The public repository contains the project documentation, source files, and the selected example PDF report.

Repository link:

https://github.com/AkshathaN-05/inunity-log-analysis

The `.gitignore` file helps prevent local configuration, credentials, temporary files, and other unwanted artifacts from being uploaded.

## 14. Future Enhancements

Potential future improvements include:

- Additional analytics and visualizations.
- Better pipeline monitoring and error handling.
- Support for additional log formats.
- More detailed reporting.
- Anomaly detection as a future enhancement, if implemented and validated.
- Improved documentation of configuration and deployment.

These are possible extensions and should not be considered completed features unless they are implemented.

## 15. Conclusion

The InUnity Log Analysis Project provides a structured approach to processing application logs, calculating useful analytics, and generating a PDF report.

By combining Python, PySpark, Apache Airflow, Docker Compose, and Amazon S3, the project organizes log processing into manageable stages and provides outputs that can be reviewed and used for further analysis.

This repository documents the project structure, workflow, technologies, execution procedure, and example report to support future maintenance and development.

## Author

**Akshatha N**
