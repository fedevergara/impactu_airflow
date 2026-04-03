---
name: new-extractor
description: "Add a new bibliographic data source to ImpactU Airflow end-to-end. Use when: creating a new extractor class, adding a capture DAG, implementing openalex/orcid/wikidata/scienti/horus or any new API or file-based source. Covers: extractor class, indexes, checkpoint resume, DAG with parallel tasks, mongomock tests."
argument-hint: "Name of the data source (e.g. openalex, orcid, wikidata)"
---

# Adding a New Data Source to ImpactU

## When to Use
- Adding any new bibliographic database, API, or file-based source
- Creating the extractor class + DAG + tests end-to-end

## Step 1 — Create the extractor module

```
extract/{source}/__init__.py               (empty)
extract/{source}/{source}_extractor.py
```

Inherit from `BaseExtractor` (`extract/base_extractor.py`).
**Reference implementation:** `extract/scimagojr/scimagojr_extractor.py`

Minimum structure:
```python
class {Source}Extractor(BaseExtractor):
    def __init__(self, mongodb_uri, db_name, collection_name="{source}", client=None, ...):
        super().__init__(mongodb_uri, db_name, collection_name, client=client)
        self.create_indexes()  # must be last line of __init__

    def create_indexes(self) -> None:
        self.collection.create_index([("{natural_key}", 1)], unique=True, background=True)

    def run(self, entity_type: str = "default", **kwargs) -> dict:
        checkpoint = self.get_checkpoint(f"{source}_{entity_type}")
        # ... paginate from checkpoint, upsert batches ...
        ops = [UpdateOne({"id": doc["id"]}, {"$set": doc}, upsert=True) for doc in batch]
        if ops:
            self.collection.bulk_write(ops, ordered=False)
        self.save_checkpoint(f"{source}_{entity_type}", last_value)
        return {"inserted": len(ops)}
```

### Pagination patterns by API type

| API type | Checkpoint value | Resume strategy |
|---|---|---|
| Cursor (OpenAlex) | `meta.next_cursor` string | Pass as `cursor=<checkpoint>` |
| Offset (ORCID) | integer offset | Pass as `start=<checkpoint>` |
| SPARQL (Wikidata) | integer offset | `OFFSET <checkpoint>` in query |
| ID-sequential (Scienti, Horus) | last processed integer ID | iterate IDs from checkpoint; stop after N consecutive 404s |

## Step 2 — Create the DAG

`dags/{source}_capture.py`

**Reference DAGs:**
- API/dump source → `dags/doaj_capture.py`
- ID-sequential → `dags/dam_capture.py`
- External library → `dags/dspace_capture.py`

Mandatory structure:
```python
from datetime import timedelta
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

with DAG(
    dag_id="{source}_capture",
    default_args={"owner": "impactu", "retries": 2, "retry_delay": timedelta(hours=1)},
    params={
        "mongo_conn_id": Param("mongo_default", type="string"),
        "db_name": Param("{source}", type="string"),
    },
    schedule=None,
    catchup=False,
) as dag:

    def run_extraction(entity_type: str, **context):
        from extract.{source}.{source}_extractor import {Source}Extractor  # late import

        hook = MongoHook(context["params"]["mongo_conn_id"])
        extractor = {Source}Extractor(
            mongodb_uri=hook.uri,
            db_name=context["params"]["db_name"],
            client=hook.get_conn(),
        )
        try:
            return extractor.run(entity_type=entity_type)
        finally:
            extractor.close()

    PythonOperator.partial(task_id="extract", python_callable=run_extraction).expand(
        op_args=[["entity_a"], ["entity_b"]]  # one parallel task per entity
    )
```

## Step 3 — Write tests

`tests/etl/test_{source}.py`

**Always** copy the BulkOperationBuilder patch from `tests/etl/test_scimagojr.py` (lines 1-20).

Minimum test coverage:
1. `test_extractor_initialization` — assert `db_name`, `collection_name`, indexes called
2. `test_run_inserts_documents` — mock HTTP response, assert docs appear in mongomock collection
3. `test_checkpoint_is_saved` — run once, assert `etl_checkpoints` has entry with correct value
4. `test_checkpoint_resume` — set checkpoint, run again, assert no re-fetch of prior data
5. `test_run_idempotent` — run twice with same data, assert collection count unchanged

## Step 4 — Register in integrity test

Add the new DAG id to the expected list in `tests/etl/test_dag_integrity.py`.

## Step 5 — Verify

```bash
pytest tests/etl/test_dag_integrity.py -v
pytest tests/etl/test_{source}.py -v
ruff check extract/{source}/ dags/{source}_capture.py
```
