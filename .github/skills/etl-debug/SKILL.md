---
name: etl-debug
description: "Debug and fix ETL pipeline failures in ImpactU Airflow. Use when: a DAG fails or retries infinitely, data is missing or duplicated in MongoDB, a checkpoint is stuck or corrupt, local cache is stale, bulk_write raises duplicate key or sort kwargs errors, MongoHook connection fails, Kahi import errors."
argument-hint: "DAG name or error description (e.g. doaj_capture failed, duplicate key error, checkpoint stuck)"
---

# Debugging ETL Pipelines in ImpactU

## Diagnostic Flow

### 1. Identify the failure type from Airflow task logs

| Log pattern | Likely cause |
|---|---|
| `ImportError` or `ModuleNotFoundError` in DAG | Top-level import in DAG file — must be late (inside task function) |
| `MongoServerError: E11000 duplicate key` | Unique index missing or `create_indexes()` not called |
| `Connection refused` / `ServerSelectionTimeoutError` | `mongo_default` Airflow Connection not configured |
| `TypeError: add_update() got unexpected keyword argument 'sort'` | mongomock + pymongo 4.x compat issue in tests |
| Task endlessly retrying with HTTP 429 / 503 | Rate limiting — add `time.sleep()` between requests |
| Collection empty after successful run | Checkpoint advanced but bulk_write silently had 0 ops |

### 2. Inspect etl_checkpoints

```python
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017")
db = client["your_db_name"]

# View all checkpoints for a source
list(db.etl_checkpoints.find({"_id": {"$regex": "^your_source"}}))
```

### 3. Reset a checkpoint (force full re-extraction)

```python
# Reset one entity
db.etl_checkpoints.delete_one({"_id": "your_source_entity_type"})

# Reset all checkpoints for a source
db.etl_checkpoints.delete_many({"_id": {"$regex": "^your_source"}})
```

Then re-trigger the DAG from the Airflow UI.

### 4. Clear local cache

```bash
# Default cache locations
rm -rf /opt/airflow/cache/{source}/
rm -rf /tmp/impactu_airflow_cache/{source}/
```

Or trigger the DAG with `{"keep_cache": false}` to force a fresh download.

### 5. Debug with cache kept

Trigger the DAG with `{"keep_cache": true}` to inspect downloaded files without re-fetching.

### 6. Fix duplicate key errors

```python
# Check existing indexes
db.your_collection.index_information()

# If unique index is missing, rebuild it:
# 1. First deduplicate if needed
# 2. Then call extractor.create_indexes() manually
```

The problem is always one of:
- `create_indexes()` not called in `__init__`
- Natural key field name mismatch between index and upsert filter

### 7. Fix mongomock sort kwargs error (tests only)

Add this patch at the **top of the test file**, before any imports from the extractor:

```python
from mongomock.collection import BulkOperationBuilder
from typing import cast, Any

_cls = cast(Any, BulkOperationBuilder)
if hasattr(_cls, "add_update"):
    orig = _cls.add_update
    def patched(self, f, d, upsert=False, multi=False, **kw):
        kw.pop("sort", None)
        return orig(self, f, d, upsert, multi, **kw)
    _cls.add_update = patched
```

Reference: `tests/etl/test_scimagojr.py` (lines 1-20).

### 8. Verify after fix

```bash
pytest tests/etl/test_{source}.py -v
pytest tests/etl/test_dag_integrity.py -v
ruff check extract/{source}/ dags/{source}_capture.py
```
