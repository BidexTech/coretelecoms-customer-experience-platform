# Core Telecoms Customer Experience Platform

## Project Overview / Background

The Core Telecoms Customer Experience Platform is designed to centralize, clean, and transform multi-channel customer complaints data to support business analytics and reporting. The project integrates call center logs, social media complaints, website complaint forms, customers and agent information into a unified data model. The Gold layer aggregates data for dashboards and analytics to monitor customer complaints trends and agent performance.

This project implements a modern ELT pipeline orchestrated with Apache Airflow.
Data from five independent sources is ingested into an S3 data lake (RAW zone), loaded into a Snowflake data warehouse as RAW tables, and transformed with dbt into CURATED and GOLD layers following the medallion architecture.

---

## Project Structure

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

* **Python**: Data ingestion, transformation helpers.
* **dbt**: Transformations, testing, and documentation.
* **Snowflake**: Cloud data warehouse for storage and analytics.
* **Airflow**: Orchestration of ingestion and transformation pipelines.
* **AWS S3**: Raw and intermediate storage.
* **GitHub**: Version control and collaboration.

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository_url>
cd coretelecoms-customer-experience-platform
```

### 2. Set up Python Environment

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

### 3. Configure dbt

* Update `profiles.yml` with Snowflake credentials.
* Ensure `CURATED` and `GOLD` schemas exist and your ETL role has privileges.
* Example materialization in `dbt_project.yml`:

```yaml
models:
  telecoms_project:
    curated:
      +schema: CURATED
      +materialized: table
    gold:
      +schema: GOLD
      +materialized: table
```

### 4. Run dbt Locally

```bash
dbt deps
dbt clean
dbt run --models curated
dbt run --models gold
dbt test
dbt docs generate
dbt docs serve
```

### 5. Airflow Orchestration

* Place DAGs in `dags/` folder.
* Example DAG tasks:

  * Ingest RAW data → S3 → Snowflake
  * Run dbt CURATED models
  * Run dbt GOLD models
  * Run dbt tests
* Use `PythonOperator` for ingestion and `BashOperator` for dbt commands.

---

## Notes

* Keep large files, compiled dbt artifacts (`target/`, `logs/`) out of Git using `.gitignore`.
* Schema macro used to dynamically switch between CURATED and GOLD layers.
* The platform is designed for easy extension for additional data sources and analytical metrics.
