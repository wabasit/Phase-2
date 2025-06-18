```markdown
# 🧑‍💻 User Guide: Step Functions ETL Pipeline with AWS Glue

This document provides instructions for deploying and running the ETL pipeline using AWS Step Functions and Glue.

---

## 🚀 1. Prerequisites

- AWS account with permissions for:
  - S3
  - Glue
  - Redshift
  - Step Functions
- AWS CLI configured
- Raw data uploaded to S3 (`s3://your-bucket/raw/`)
- Glue connection to Redshift established
- IAM roles for Glue and Step Functions set up

---

## 🛠️ 2. Set Up

### 2.1 Create Glue Jobs

Upload each script under `glue_jobs/` into AWS Glue Studio as a separate job:

1. `load_aurora_s3.py`
2. `s3_to_redshift.py`
3. `redshift_raw_to_curated.py`
4. `kpi_curated_to_presentation.py`

Each job must:
- Use the same IAM role
- Target the appropriate S3/Redshift location

### 2.2 Create the State Machine

1. Navigate to **AWS Step Functions**
2. Click **Create State Machine**
3. Choose **Author with code**
4. Paste the contents of `step_function_definition.json`
5. Replace job names with your actual Glue job names
6. Configure IAM permissions

---

## 🧪 3. Running the Pipeline

1. Upload your raw files to `s3://your-bucket/raw/`
2. Go to **Step Functions**
3. Click on your created workflow
4. Choose **Start Execution**
5. Monitor progress

Each job will run in sequence:
- `load_aurora_s3` → `s3_to_redshift` → `redshift_raw_to_curated` → `kpi_curated_to_presentation`

Any failure will be caught, and error messages logged.

---

## 4. Troubleshooting

- **Glue job fails?**
  - Check Glue logs in **CloudWatch**
  - Verify IAM role permissions and Redshift connectivity

- **Step Function stuck?**
  - Review state transitions in execution history
  - Check for missing input parameters or failures

- **Redshift not updating?**
  - Validate that the schema matches
  - Confirm Redshift cluster is accessible and in `Available` state

---

## 5. Security Tips

- Do NOT hardcode AWS credentials.
- Use `.env` or AWS Secrets Manager
- Limit IAM permissions using least privilege principle

---

## 6. Monitoring

- Step Functions → Execution History
- Glue Jobs → Logs via CloudWatch
- Redshift → System tables and query logs

---