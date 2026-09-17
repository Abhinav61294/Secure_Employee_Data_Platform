# 🔐 Secure Employee Data Platform

A cloud-based data engineering project built on **Google Cloud Platform (GCP)** to ingest, validate, transform, secure, and analyze employee data from multiple sources.

## 🏗️ Architecture

```text
PostgreSQL / CSV / REST API
            ↓
      Cloud Storage
            ↓
   Apache Beam / Dataflow
   ├── Validation
   ├── Deduplication
   ├── PII Masking
   └── Transformation
            ↓
        BigQuery
            ↓
        Dataform
            ↓
    Analytics Marts
            ↓
      Looker Studio

Cloud Composer / Airflow → Orchestration
IAM + Secret Manager    → Security
Terraform               → Infrastructure
GitHub Actions          → CI
🛠️ Tech Stack

Cloud: GCP, Cloud Storage, BigQuery, Dataflow, Cloud Composer
Data Engineering: Python, Apache Beam, PostgreSQL, Dataform, SQL
Security & Operations: IAM, Secret Manager, Cloud Monitoring, Cloud Logging
Analytics: Looker Studio
DevOps: Terraform, Git, GitHub Actions

🔄 Pipeline
Ingestion — Employee, payroll, and compliance data are collected from CSV, PostgreSQL, and REST API sources.
Processing — Apache Beam/Dataflow validates records, removes duplicates, masks PII, and transforms the data.
Warehouse — Processed data is organized in BigQuery using layered data architecture.
Transformation — Dataform builds curated analytical models and performs data quality assertions.
Orchestration — Cloud Composer / Airflow coordinates Dataflow, BigQuery, and Dataform tasks.
Analytics — Looker Studio provides interactive employee and salary analytics.
🔐 Security & Data Quality
PII masking during data processing
IAM-based service account access
Secret Manager for application secrets
Duplicate detection
Employee ID, email, and date validation
Dataform assertions for curated data quality
Sensitive credentials excluded from Git
📊 Dashboard

The Looker Studio dashboard provides:

Total employee count
Active / inactive / leave distribution
Employees by department
Employees by location
Average salary by pay grade
Interactive employment-status filtering
📈 Validation
Check	Result
Input records	50,000
Valid/masked records	49,000
Invalid records	1,000
Duplicate test	✅ Passed
PII masking	✅ Verified
Final integrated employees	49,684
Airflow pipeline	✅ Successful
GitHub Actions CI	✅ Passed
☁️ Infrastructure & CI

Terraform configuration is included for required GCP services, while GitHub Actions performs automated Python syntax and Terraform formatting checks.

💰 Cost Optimization

After completing pipeline validation and capturing portfolio evidence, the major GCP runtime and storage resources were removed to prevent unnecessary ongoing cloud costs.

The source code and infrastructure configuration remain available in this repository for reference and reproducibility.

📁 Project Structure
.github/        → CI/CD
ingestion/      → Data ingestion & Beam pipelines
data_quality/   → Validation
orchestration/  → Airflow DAGs
dataform/       → SQL transformations
terraform/      → Infrastructure as Code
sql/            → SQL scripts
tests/          → Tests
docs/           → Documentation
🎯 Key Concepts

Data Engineering: ETL/ELT • Batch Processing • Data Quality • Data Warehousing • Data Integration

GCP: BigQuery • Dataflow • Cloud Storage • Cloud Composer • IAM

Engineering: Apache Beam • Dataform • Airflow • Terraform • GitHub Actions

👨‍💻 Author

Abhinav

GCP / Data Engineering Portfolio Project
