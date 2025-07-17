# Automated LMS ETL Pipeline with AWS and Redshift

This project mimicks an end-to-end ETL pipeline that automates the ingestion and transformation of learning management system (LMS) data into Amazon Redshift. The pipeline uses an AWS Step Function to orchestrate Lambda, Glue, and Redshift tasks.

---

## Tech Stack
- AWS Lambda (Python)
- AWS S3 (File Transfer target)
- AWS Glue Tables & Jobs (Parquet conversion)
- AWS Step Functions (Orchestration)
- Amazon Redshift (Data warehouse)
- dbt (Transformations and data modeling)
- SQL (Stored Procedures)

---

## Workflow Overview

1. **Delta files** are exported daily from an LMS.
2. Files are uploaded via a **File Transfer Process** to a designated S3 bucket.
3. **Step Function** is triggered:
   - **Lambda function** validates, cleans, and moves the file.
   - **Glue Job** converts the CSV to Parquet and stores it in a transformed S3 location.
   - **Lambda or Redshift Step** triggers stored procedures to load and merge data into final Redshift tables.
4. **dbt models** run nightly to transform raw tables into analytics-ready formats.

---

## Architecture Diagram
![Pipeline Diagram](architecture_diagram.png)

---

## Sample Data
Located in `/sample_data/`, these are mock LMS delta files:
- `lms_user_delta.csv`
- `lms_courses_delta.csv`

---

## Key Code Files

- **`lambda/process_lms_file.py`**  
  Parses CSVs, removes nulls, standardizes column names, and triggers Glue job.

- **`glue/convert_to_parquet.py`**  
  Simple Glue script to convert raw CSVs into partitioned Parquet files.

- **`sql/stored_procedures.sql`**  
  Contains Redshift stored procedures for UPSERT logic.

- **`dbt/models/`**  
  Models for `stg_lms_users`, `stg_lms_courses`, and final `fct_course_completion`.

---

## Business Impact
This automated solution reduced data latency from 24 hours to under 2 hours, eliminated manual uploads, and enabled near real-time analytics on training metrics across the org.

---

## 🔒 Note
All data shown is mock/simulated for demonstration purposes.
