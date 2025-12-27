# System Requirements - ImpactU Airflow ETL

This document details the technical and functional requirements for the ImpactU extraction, transformation, and loading (ETL) system.

## 1. Fundamental Requirements (Defined)

### 1.1 Centralization in MongoDB
* All data sources, regardless of their origin or format (API, SQL, NoSQL, flat files), must be normalized or stored in their raw form within a centralized **MongoDB** instance.

### 1.2 Package Structure for Large Volumes
* The system must be designed as a modular package that allows efficient management of large data volumes, avoiding memory bottlenecks and optimizing I/O.

### 1.3 Checkpoint Support
* It is critical that extraction processes support checkpoints. If a task fails or stops, it must be able to resume from the last successfully saved point, avoiding processing already downloaded data.

### 1.4 Parallel Execution
* The system must leverage Airflow's capabilities to execute multiple extraction and loading tasks simultaneously, optimizing total execution time.

### 1.5 Fault Tolerance
* The system must be resilient. Failures in a data source should not stop the entire pipeline, and automatic retry mechanisms must exist.

### 1.6 Integrity and No Duplication
* Mechanisms must be implemented to avoid data corruption and, above all, information duplication in MongoDB (Idempotency).

---

## 2. Additional Recommendations (Advanced Data Engineering)

To ensure the system is enterprise-grade and maintainable in the long term, we will develop the following points:

### 2.1 Strict Idempotency
* Each DAG must be designed such that if executed multiple times for the same period or dataset, the final state of the database is identical, without creating duplicate records.

### 2.2 Data Quality Validation
* Implement a validation layer post-extraction and pre-loading. Verify minimum schemas, mandatory fields, and data types to prevent "garbage data" from contaminating the data lake.

### 2.3 Secrets and Configuration Management
* Do not hardcode credentials. Use **Airflow Connections** and **Variables**, or integrate with a secrets manager (HashiCorp Vault, AWS Secrets Manager) to handle API keys and DB passwords.

### 2.4 Proactive Monitoring and Alerts
* Configure notifications (Slack, Email, or Discord) for critical failures. Additionally, implement structured logs that allow tracking data lineage.

### 2.5 Backfilling Strategy
* The system must allow easy historical data loading using `start_date` and `catchup` in Airflow, ensuring consistent data partitioning.

### 2.6 Source Abstraction Layer
* Create a base class or interface for extractors. This will facilitate adding new sources (e.g., a new API) following the same checkpoint and logging pattern without reinventing the wheel.

### 2.7 Containerization and Orchestration
* Ensure the entire environment is reproducible using **Docker**, facilitating identical deployment across different environments (Dev, Test, Prod).
