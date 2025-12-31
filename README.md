# 📡 CoreTelecoms Customer Experience Platform

![CI/CD Pipeline](https://github.com/BidexTech/coretelecoms-customer-experience-platform/actions/workflows/ci_pipeline.yml/badge.svg)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.1.0-017CEE?style=flat&logo=apache-airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=flat&logo=docker&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?style=flat&logo=snowflake&logoColor=white)

## Project Overview / Background

The Core Telecoms Customer Experience Platform is designed to centralize, clean, and transform multi-channel customer complaints data to support business analytics and reporting. The project integrates call center logs, social media complaints, website complaint forms, customers and agent information into a unified data model. The Gold layer aggregates data for dashboards and analytics to monitor customer complaints trends and agent performance.

This project implements a modern ELT pipeline orchestrated with Apache Airflow.
Data from five independent sources is ingested into an S3 data lake (RAW zone), loaded into a Snowflake data warehouse as RAW tables, and transformed with dbt into CURATED and GOLD layers following the medallion architecture.

---

## Project Structure
| Folder             | Purpose                                      |
| ------------------ | -------------------------------------------- |
| `dags/`            | Airflow ingestion & dbt orchestration DAGs   |
| `dbt/`             | Modular SQL models (Staging, Marts)          |
| `terraform-infra/` | AWS & Snowflake infrastructure as code       |
| `snowflake/`       | Initial Snowflake DDL bootstrapping scripts  |
| `src/`             | Custom Python ingestion and enrichment logic |

```
coretelecoms-customer-experience-platform/
├── .github
├── .dockerignore
├── README.md
├── .gitignore
├── .env
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
│
├── dags/                             
│   ├── ingestion_and_transform_dag.py
│  
│
├── dbt/telecoms_project                             
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── raw/
│   │   ├── curated/
│   │   └── gold/
│   ├── seeds/
│   ├── snapshots/
│   └── target/                       
│
├── src/                   
│   ├── credentials/
│   ├── ingestion/
│       ├── agents_ingest.py
│       ├── cutomers_ingest.py
│       ├── social_medial_ingest.py
│       ├── call_center_ingest.py
│       ├── web_comaplaints_ingest.py
│       └── s3_ingestion.py
│   
│
├── snowflake/
│   └── data_integ_load_command/
│       ├── copy_procedure.sql
│       ├── data_integration.sql
│       ├── database.sql
│       ├── query_data.sql
│       ├── stage.sql
│       ├── table.sql
│       └── warehouse.sql
│
├── terraform-infra/                 
    ├── backend.tf
    ├── locals.tf
    ├── provider.tf
    ├── s3.tf
    └── snowflake.tf


```

---

## Architecture Diagram

```
          +-----------------+
          | Source Systems  |
          | (Call, Social,  |
          |  Website)       |
          +--------+--------+
                   |
                   v
           +-----------------+
           | S3 Raw Storage  |
           +--------+--------+
                   |
                   v
           +-----------------+
           | Snowflake       |
           | RAW Layer   |
           +--------+--------+
                   |
                   v
           +-----------------+
           | Snowflake       |
           | CURATED Layer   |
           +--------+--------+
                   |
                   v
           +-----------------+
           | Snowflake GOLD  |
           | Aggregations    |
           +--------+--------+
                   |
                   v
           +-----------------+
           | Analytics & ML  |
           | Dashboards      |
           +-----------------+
```

---

## Choice of Tools and Technology

This project utilizes a Modern Data Stack (MDS) architecture, ensuring scalability, reliability, and portability:

* **Infrastructure:** Terraform (AWS S3 & Snowflake policy document and role setup).
* **Orchestration:** Apache Airflow 3.1.0 (Running in Docker with CeleryExecutor).
* **Transformation:** dbt (Data Build Tool) for modular SQL modeling.
* **Data Warehouse:** Snowflake (Multi-cluster compute).
* **CI/CD:** GitHub Actions (Linting, Validation, and Automated Docker Builds).
* **Monitoring:** Slack & SMTP integration for pipeline health alerts.

---

## ⚙️ CI/CD Pipeline Logic

> **Broken code never hits production.**

## Continuous Integration (CI)

Every push to the `dev` branch triggers automated quality checks:

- **flake8** — Python linting  
- **sqlfluff** — SQL linting  
- **terraform validate** — Infrastructure configuration validation  

---

## Continuous Deployment (CD)

Merges to the `main` branch trigger a GitHub Actions workflow that:

- Builds a new production Docker image  
- Pushes the image to Docker Hub:

```bash
bidextech/coretelecoms-platform:1.0.0
```
##  Quick Start: Local Deployment

### 1. Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/)
* A Snowflake Account
* AWS IAM Credentials
* DBT-core

### 2. Setup
Clone the repository and prepare your environment:
```bash
git clone https://github.com/BidexTech/coretelecoms-customer-experience-platform.git
cd coretelecoms-customer-experience-platform
cp .env.example .env  # Update with your Snowflake credentials for dbt communication

# Build and start all services:

docker-compose up -d --build
```
| Tool           | URL                                            | Credentials                                  |
| -------------- | ---------------------------------------------- | -------------------------------------------- |
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | `admin / admin`                              |
| **dbt Docs**   | Run inside container: `dbt docs generate`      | Generates data lineage & model documentation |

### 3. Add connection in Airflow UI

| Connection ID    | Type      | Description                                             |
| ---------------- | --------- | ------------------------------------------------------- |
| `aws_default`    | AWS       | Pulls raw CSV/JSON logs from S3 landing zones           |
| `snowflake_conn` | Snowflake | Executes dbt models and performs data loading           |
| `slack_conn`     | HTTP      | Sends pipeline failure/success alerts to `#data-alerts` |
| `smtp_conn`   | Email     | Sends daily performance PDF reports to business leads   |

## Notes

* The platform is designed for easy extension for additional data sources and analytical metrics.
