---
name: new-dag
description: "Create a new Airflow DAG for ImpactU. Use when: adding a transform DAG (Kahi), load DAG (Elasticsearch/MongoDB), backup DAG (mongodump), or orchestrator DAG (TriggerDagRunOperator). Covers naming conventions, default_args, Params, parallel tasks with expand(), Kahi import pattern, Elasticsearch provider."
argument-hint: "DAG type and name (e.g. transform_sources, load_elasticsearch_production, backup_mongodb_kahi)"
---

# Creating a New DAG in ImpactU

## When to Use
- New transformation DAG (`transform_{entity}`)
- New load DAG (`load_{db}_{env}`)
- New backup DAG (`backup_{db}_{name}`)
- New orchestrator DAG that chains other DAGs

For **capture DAGs**, use the `new-extractor` skill instead.

## Required Elements for Every DAG

```python
from datetime import timedelta
from airflow import DAG
from airflow.sdk import Param

default_args = {
    "owner": "impactu",
    "retries": 2,
    "retry_delay": timedelta(hours=1),
}

with DAG(
    dag_id="<type>_<name>",       # follow naming convention below
    default_args=default_args,
    params={ ... },               # all config via Param(), never hardcoded
    schedule=None,                # or cron string
    catchup=False,
) as dag:
    ...
```

**Naming convention:**

| Type | Format | Example |
|---|---|---|
| Transform | `transform_{entity}` | `transform_sources` |
| Load | `load_{db}_{env}` | `load_elasticsearch_production` |
| Backup | `backup_{db}_{name}` | `backup_mongodb_kahi` |
| Orchestrator | `{scope}_data_capture` | `institutional_data_capture` |

## Transform DAG (Kahi via PythonOperator)

Import Kahi **inside the task function** — never at module level.

```python
def transform_batch(source_collection: str, **context):
    from kahi import KahiX  # late import
    from airflow.providers.mongo.hooks.mongo import MongoHook

    hook = MongoHook(context["params"]["mongo_conn_id"])
    client = hook.get_conn()
    db = client[context["params"]["db_name"]]

    checkpoint_key = f"transform_{entity}_{source_collection}"
    # read raw docs from db[source_collection] starting from checkpoint
    # normalize with KahiX
    # upsert into db["kahi.{entity}"]
    # save_checkpoint(checkpoint_key, last_processed_id)

PythonOperator.partial(task_id="transform", python_callable=transform_batch).expand(
    op_args=[[col] for col in ["scimagojr", "doaj", "publindex"]]
)
```

Reference: `dags/doaj_capture.py` for the parallel expand() structure.

## Load DAG (Elasticsearch)

Requires `apache-airflow-providers-elasticsearch` in `pyproject.toml`.

```python
def load_entity(entity: str, **context):
    from airflow.providers.mongo.hooks.mongo import MongoHook
    from elasticsearch import Elasticsearch, helpers

    mongo = MongoHook(context["params"]["mongo_conn_id"])
    es = Elasticsearch(hosts=[context["params"]["es_host"]])
    db = mongo.get_conn()[context["params"]["kahi_db"]]

    # cursor over kahi.{entity}, resuming from checkpoint
    # helpers.bulk(es, actions)
    # save checkpoint after each batch

PythonOperator.partial(task_id="load", python_callable=load_entity).expand(
    op_args=[["sources"], ["affiliations"], ["person"], ["works"]]
)
```

## Backup DAG (mongodump)

```python
from airflow.providers.standard.operators.bash import BashOperator

backup = BashOperator(
    task_id="mongodump",
    bash_command=(
        "mongodump "
        "--uri='{{ conn.mongo_default.get_uri() }}' "
        "--db='{{ params.db_name }}' "
        "--out='/opt/airflow/backups/{{ ds_nodash }}'"
    ),
)
```

Recommended schedule: `schedule="@weekly"`.

## Orchestrator DAG (TriggerDagRunOperator)

Reference: `dags/institutional_data_capture.py`

```python
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

trigger_a = TriggerDagRunOperator(
    task_id="trigger_source_a",
    trigger_dag_id="source_a_capture",
    conf={"mongo_conn_id": "{{ params.mongo_conn_id }}"},
    wait_for_completion=True,
)
```

## After Creating

1. Add the new DAG id to `tests/etl/test_dag_integrity.py`
2. Run:
```bash
pytest tests/etl/test_dag_integrity.py -v
ruff check dags/{new_dag}.py
```
