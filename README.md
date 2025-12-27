# ImpactU Airflow ETL

Central repository for Apache Airflow DAGs for the Extraction, Transformation, and Loading (ETL) processes of the ImpactU project.

## 🚀 Description
This project orchestrates data collection from various scientific and academic sources, its processing using the [Kahi](https://github.com/colav/Kahi) tool, and its subsequent loading into query systems such as MongoDB and Elasticsearch.

## 📂 Project Structure
The repository is organized by data lifecycle stages:

*   `extract/`: Extraction logic for sources like OpenAlex, ORCID, ROR, etc.
*   `transform/`: Transformation and normalization processes (Kahi).
*   `load/`: Loading scripts to final destinations.
*   `deploys/`: Deployment configurations per environment (dev, prod).
*   `backups/`: Database backup automation.
*   `tests/`: Integration and data quality tests.

## 📋 Requirements and Architecture
For details on design principles (Checkpoints, Idempotency, Parallelism), see the [System Requirements](REQUIREMENTS.md) document.

## 🛠 DAG Naming Standard
To maintain consistency in the Airflow interface, we follow this convention:

| Type | Format | Example |
| :--- | :--- | :--- |
| **Extraction** | `extract_{source}` | `extract_openalex` |
| **Transformation** | `transform_{entity}` | `transform_sources` |
| **Loading** | `load_{db}_{env}` | `load_mongodb_production` |
| **Deployment** | `deploy_{service}_{env}` | `deploy_mongodb_production` |
| **Backup** | `backup_{db}_{name}` | `backup_mongodb_kahi` |
| **Tests** | `tests_{service}` | `tests_kahi` |

## ⚙️ Configuration and Development
*(Section under construction)*

### Prerequisites
*   Docker & Docker Compose
*   Apache Airflow 3.1.5
*   Python 3.12+

### Installation
1. Clone the repository.
2. Configure environment variables in a `.env` file.
3. Start the environment with Docker Compose.

---
**Colav - ImpactU**
