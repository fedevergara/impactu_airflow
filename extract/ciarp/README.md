# CIARP Extractor

This module extracts **CIARP Excel files** from a Google Drive folder and stores **raw rows** in MongoDB.

## Structure
Files are organized under a root folder with **one subfolder per institution**:

```
<ROOT>/RORID_INSTITUTION_NAME/ciarp_...
```

## Naming Convention
Files should follow:

```
ciarp_RORID_DD_MM_YYYY_HH:MM.xlsx
```

Example:
```
ciarp_006jxzx88_27_01_2026_14:41.xlsx
```

The **latest** file per ROR ID is selected using Drive `modifiedTime` (more reliable than filenames).

## Features
- **Raw Extraction Only**: Stores rows as-is plus minimal metadata.
- **Latest-by-Name Strategy**: Uses the datetime in the filename (not Drive `modifiedTime`) to select the latest.
- **Backup Before Load**: If data exists, creates a timestamped backup collection before loading.
- **Bulk Upserts**: Efficient batch writes to MongoDB.

## Usage

```python
from extract.ciarp.ciarp_extractor import CiarpExtractor

extractor = CiarpExtractor(
    mongodb_uri=mongodb_uri,
    db_name=db_name,
    drive_root_folder_id="drive_root_folder_id",
    google_token_pickle="/path/to/drive_token.pickle",
)

extractor.run(force=False)
```
