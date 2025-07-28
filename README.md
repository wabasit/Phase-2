# ETL Pipeline with AWS Step Functions and Glue

This project implements an ETL data pipeline using **AWS Step Functions** and **AWS Glue Jobs**. The pipeline performs data extraction, transformation, validation, and loading from a data lake (S3) into a Redshift data warehouse using multiple Glue jobs orchestrated with a Step Function workflow.

---

## Features

- Four modular AWS Glue Jobs:
  1. **Extract Metadata**
  2. **Extract Streaming Data**
  3. **Validate Schema**
  4. **Load to Redshift**
- Orchestrated using AWS Step Functions with fallback and fail-safe logic.
- Source: Amazon S3 (`raw/`)
- Target: Amazon Redshift (`presentation.*`)
- Error handling: Built-in catch and retry logic

---

## Architecture

S3 → Glue Job 1 → Glue Job 2 → Glue Job 3 → Glue Job 4 → Redshift
(load_aurora_s3)
↓
(s3_to_redshift)
↓
(redshift_raw_to_curated)
↓
(kpi_curated_to_presentation)


---

## Project Structure For Cloud

```text
data/
  └── apartment_attributes/
  └── apartments/
  └── bookings/
  └── user_viewing

## Technologies
- AWS Glue
- AWS Step Functions
- Amazon Redshift
- Amazon S3
- Python / PySpark

## Testing
Each Glue job is tested independently using development endpoints. The Step Function flow is verified using mock and real datasets in S3.