# etl-aws-pyspark-airflow

AWS region: `us-east-1`

## Day 1
- Python 3.10+ with venv
- Java 11 installed for PySpark
- Packages pinned via requirements.txt
- Project structure created

## How to activate env
```bash
# Windows PowerShell
.\venv\Scripts\Activate.ps1

# Day 2 — AWS Account, IAM & S3 Data Lake

- Region: us-east-1
- IAM user: yourname-dev-admin (AdministratorAccess for dev only)
- CLI profile: etl-dev
- S3 bucket: <yourname>-etl-lake-dev-1234
- Default encryption: SSE-S3 (AES-256)
- Public access: Blocked (all 4 settings)
- Folders: raw/, curated/

## CLI Verification
aws sts get-caller-identity
aws s3 ls s3://<your-bucket>/

## Screenshots captured
- IAM user summary (no secrets)
- S3 bucket Properties → Default encryption (SSE-S3)
- S3 Objects view showing raw/ and curated/

