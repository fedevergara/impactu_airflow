# Staff Extractor

This module handles the extraction of **staff files** from a Google Drive folder structure and stores **raw rows** into MongoDB.

## Features
*   **Checkpoints (optional)**: Can record completion per institution (e.g., `staff_<RORID>`) if your `BaseExtractor` implements `save_checkpoint()` (commonly backed by an `etl_checkpoints` collection).
*   **Idempotency**: Avoids reprocessing when the latest Drive file for an institution (by `drive_file_id` + `drive_modified_time`) has already been loaded.
*   **Raw Extraction Only**: Stores Excel rows **as-is** (plus minimal metadata).
*   **Latest-File Strategy**: Selects the latest file using Drive `modifiedTime` (more reliable than parsing timestamps from filenames).
*   **Efficient Bulk Writes**: Loads rows via MongoDB bulk operations for performance.

## Extraction Strategy

The extractor follows a robust multi-stage process to ensure data integrity and repeatability:

1.  **Folder Discovery (Institutions)**  
    Lists first-level subfolders under the Drive root folder. Each folder is expected to follow the naming convention:  
    `RORID_INSTITUTION_NAME` (e.g., `03bp5hc83_Universidad_de_Antioquia`).

2.  **Latest File Selection (per Institution)**  
    Inside each institution folder, it lists Excel files (`.xlsx` / `.xls`).  
    The **latest** file is selected by Google Drive metadata field `modifiedTime`.

3.  **Persistent Caching**  
    The selected Excel is downloaded and stored in a local cache directory (default: `/tmp/impactu_airflow_cache/staff`).  
    This improves reproducibility and makes it easier to debug ingestion runs.

4.  **Raw Excel → JSON Records**
    *   The Excel is read with `pandas.read_excel(..., dtype=str)` to preserve identifiers/codes exactly.
    *   NaN values are converted to `None` for MongoDB compatibility.
    *   Each row becomes one JSON-like dict (one MongoDB document).

5.  **Replace-by-Institution Load (Bulk Upsert)**
    *   If `keep_only_latest_per_institution=True`, the extractor **deletes prior docs** for the institution.
    *   It then writes the latest matrix rows using bulk operations (`bulk_write`) for throughput.
    *   Each document `_id` is deterministic:  
        `<institution_id>:<drive_file_id>:<row_number>`  
        ensuring stable re-runs.

6.  **Checkpoint Save (optional)**  
    After successfully processing an institution, the extractor can mark it as completed using:  
    `save_checkpoint(f"staff_{institution_id}", "completed")`

## Usage

The extractor can be called from an Airflow DAG or as a standalone script.

```python
from extract.staff_extractor import StaffExtractor

extractor = StaffExtractor(
    mongodb_uri=mongodb_uri,
    db_name=db_name,
    drive_root_folder_id="drive_root_folder_id",
    google_token_pickle="/path/to/drive_token.pickle",
)

extractor.run(force=False)
