# Data Engineering Portfolio

A collection of production-style scripts demonstrating hands-on experience
across the modern data stack — Airflow, Azure Databricks, Snowflake, and
Denodo — as a Lead Data Engineer.

## Contents

| File | Description |
|---|---|
| [`airflow/snowflake_daily_extract.py`](airflow/snowflake_daily_extract.py) | Airflow DAG that extracts daily data from Snowflake into staging, with retry logic and failure alerting. |
| [`databricks/optimize_delta_job.py`](databricks/optimize_delta_job.py) | Azure Databricks Spark job that processes and optimizes Delta tables (repartitioning, Z-ORDER) for large-scale data workloads. |
| [`denodo/denodo_backup_monitor.py`](denodo/denodo_backup_monitor.py) | Automated Denodo health-check and VDP backup script with alerting, used to safeguard data virtualization layer reliability. |

## Notes

These snippets are simplified, sanitized versions of patterns used in
production environments — credentials, connection strings, and
environment-specific configuration have been removed/redacted.

## Contact

Ana | anamika1yadav@gmail.com | https://www.linkedin.com/in/anamikayadav07/
