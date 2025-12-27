# ScimagoJR Extractor

This module handles the extraction of journal ranking data from ScimagoJR.

## Features
*   **Checkpoints**: Uses a MongoDB collection (`etl_checkpoints`) to track which years have already been processed.
*   **Idempotency**: If a year is re-executed, the system updates existing records or inserts new ones (upsert), avoiding duplicates.
*   **Normalization**: Converts ScimagoJR's CSV/XLS format into MongoDB-ready JSON documents.

## Extraction Logic
*   **Smart Download**: If the data file for a specific year is missing from the local cache, it will be downloaded automatically, regardless of the `force_redownload` parameter.
*   **Data Synchronization**:
    *   **Missing Records**: If records for a year are missing in the database, they are inserted.
    *   **Existing Records**: If a record already exists (matched by `Sourceid` and `year`), it is updated to ensure it matches the source data, maintaining data integrity.

## Usage
The extractor can be called from an Airflow DAG or as a standalone script.

```python
from extract.scimagojr.scimagojr_extractor import ScimagoJRExtractor
extractor = ScimagoJRExtractor(mongodb_uri, db_name)
extractor.run(1999, 2023)
```
