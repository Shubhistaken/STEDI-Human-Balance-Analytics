# STEDI Human Balance Analytics

## Overview
This project builds a **data lakehouse solution** on AWS for analyzing human balance sensor data. The data is ingested, transformed, and curated using **AWS Glue, Spark, and Athena** to train a machine learning model.

## Architecture
- **Landing Zone** → Raw JSON data stored in **S3**.
- **Trusted Zone** → Filtered & cleaned data stored in **AWS Glue Tables**.
- **Curated Zone** → Final processed tables, ready for ML training.

## Technologies Used
- **AWS Glue** (ETL jobs, schema inference)
- **AWS S3** (Data storage)
- **AWS Athena** (Querying data)
- **Apache Spark** (Transformations)
- **Python** (ETL scripts)

## Repository Structure
```
STEDI-Human-Balance-Analytics/
│── data/
│   ├── customer_landing.sql
│   ├── accelerometer_landing.sql
│   ├── step_trainer_landing.sql
│── scripts/
│   ├── customer_landing_to_trusted.py
│   ├── accelerometer_landing_to_trusted.py
│   ├── step_trainer_trusted.py
│   ├── customer_trusted_to_curated.py
│   ├── machine_learning_curated.py
│── screenshots/
│   ├── customer_landing.png
│   ├── accelerometer_landing.png
│   ├── step_trainer_landing.png
│   ├── customer_trusted.png
│   ├── accelerometer_trusted.png
│   ├── step_trainer_trusted.png
│   ├── customer_curated.png
│   ├── machine_learning_curated.png
│── README.md
│── .gitignore
```

## Steps Performed
### 1. Landing Zone
- **Glue Tables Created:**
  - `customer_landing`
  - `accelerometer_landing`
  - `step_trainer_landing`
- **Athena Validation:**
  - `customer_landing`: 956 rows
  - `accelerometer_landing`: 81,273 rows
  - `step_trainer_landing`: 28,680 rows

### 2. Trusted Zone
- **Transformations Applied:**
  - Removed invalid records (e.g., blank `shareWithResearchAsOfDate` values).
  - Joined privacy tables.
- **Athena Validation:**
  - `customer_trusted`: 482 rows
  - `accelerometer_trusted`: 40,981 rows
  - `step_trainer_trusted`: 14,460 rows

### 3. Curated Zone
- **Transformations Applied:**
  - Filtered customers with accelerometer data.
  - Joined sensor readings on timestamps.
  - Removed personally identifiable information (PII).
- **Athena Validation:**
  - `customer_curated`: 464 rows
  - `machine_learning_curated`: 34,437 rows

## How to Run
1. Upload **JSON files** to S3 (`s3://your-bucket/landing/`).
2. Run the Glue jobs in the following order:
   - `customer_landing_to_trusted.py`
   - `accelerometer_landing_to_trusted.py`
   - `step_trainer_trusted.py`
   - `customer_trusted_to_curated.py`
   - `machine_learning_curated.py`
3. Validate results in **AWS Athena**.

## Screenshots
See the `screenshots/` folder for query results.

## Author
Shubham Pandey - https://github.com/Shubhistaken

