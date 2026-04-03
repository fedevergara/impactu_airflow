"""OpenAlex S3 snapshot capture DAG.

Workflow
--------
1. ``sync_snapshot``  — ``aws s3 sync s3://openalex openalex-snapshot``
   (run from ``snapshot_base_dir``; idempotent, only fetches delta)
2. ``load_entity``    — parallel PythonOperator per entity type; reads the
   downloaded .gz NDJSON partitions and upserts into MongoDB using
   ``OpenAlexExtractor``.  Each entity resumes from its own checkpoint.

Parallelism: one Airflow task per OpenAlex entity type (authors, concepts,
funders, institutions, publishers, sources, topics, works).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

from extract.openalex.openalex_extractor import DEFAULT_MAX_WORKERS

_DEFAULT_SNAPSHOT_BASE = "/storage/raw_data/openalex"

with DAG(
    dag_id="openalex_capture",
    default_args={
        "owner": "impactu",
        "retries": 2,
        "retry_delay": timedelta(hours=1),
    },
    params={
        "mongo_conn_id": Param(
            "mongodb_default",
            type="string",
            description="Airflow MongoDB connection ID",
        ),
        "db_name": Param(
            "openalex",
            type="string",
            description="Target MongoDB database name",
        ),
        "snapshot_base_dir": Param(
            _DEFAULT_SNAPSHOT_BASE,
            type="string",
            description=(
                "Base directory where the sync runs. "
                "The snapshot lands at <snapshot_base_dir>/openalex-snapshot/"
            ),
        ),
        "chunk_size": Param(
            500,
            type="integer",
            description="Documents per bulk_write batch",
        ),
        "max_workers": Param(
            DEFAULT_MAX_WORKERS,
            type="integer",
            description="Parallel threads per entity type for partition loading (default: 72)",
        ),
        "drop_db": Param(
            False,
            type="boolean",
            description="Drop the target database before loading (full reload, resets checkpoints)",
        ),
    },
    schedule=None,
    catchup=False,
    tags=["openalex", "capture"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1 — S3 sync (BashOperator, idempotent)
    # ------------------------------------------------------------------
    sync_snapshot = BashOperator(
        task_id="sync_snapshot",
        bash_command=(
            'cd "{{ params.snapshot_base_dir }}" && '
            "aws s3 sync s3://openalex openalex-snapshot --no-sign-request --delete"
        ),
        doc_md=(
            "Sync the full OpenAlex S3 snapshot to the local filesystem. "
            "Only changed/new files are downloaded; deleted files are removed."
        ),
    )

    # ------------------------------------------------------------------
    # Task 2 — Drop DB once (only when drop_db=True)
    # ------------------------------------------------------------------
    def prepare_db(**context: Any) -> None:
        """Drop the target database and all its checkpoints if drop_db=True.

        Uses the MongoHook directly — avoids re-creating the database as a
        side-effect of extractor index creation.
        """
        params = context["params"]
        if not params.get("drop_db", False):
            return

        import logging

        log = logging.getLogger("airflow.task.prepare_db")
        hook = MongoHook(params["mongo_conn_id"])
        client = hook.get_conn()
        db_name = params["db_name"]
        log.warning("Dropping database '%s' and all its checkpoints.", db_name)
        client.drop_database(db_name)
        log.info("Database '%s' dropped successfully.", db_name)

    prepare = PythonOperator(
        task_id="prepare_db",
        python_callable=prepare_db,
    )

    # ------------------------------------------------------------------
    # Task 3 — Load ALL entities with a single shared thread pool
    # ------------------------------------------------------------------
    def load_all_entities(**context: Any) -> dict[str, Any]:
        """Load all OpenAlex entities using one unified thread pool.

        All pending .gz files across every entity type are fed to a single
        ThreadPoolExecutor so CPU threads are never idle waiting for a small
        entity to finish before a large one can use them.
        """
        from extract.openalex.openalex_extractor import OpenAlexExtractor  # late import

        params = context["params"]
        snapshot_dir = f"{params['snapshot_base_dir']}/openalex-snapshot"

        hook = MongoHook(params["mongo_conn_id"])
        return OpenAlexExtractor.run_all(
            db_name=params["db_name"],
            snapshot_dir=snapshot_dir,
            client=hook.get_conn(),
            chunk_size=params["chunk_size"],
            max_workers=params["max_workers"],
        )

    load_entities = PythonOperator(
        task_id="load_all_entities",
        python_callable=load_all_entities,
    )

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------
    sync_snapshot >> prepare >> load_entities
