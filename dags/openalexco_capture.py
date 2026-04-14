"""OpenAlexCO capture DAG.

Executes the Colombia cut: reads from the full ``openalex`` MongoDB database
and writes the Colombia-filtered data to ``openalexco``.

Steps
-----
1. ``cut_works_authorship``  — works with Colombian authors/institutions.
2. ``cut_works_sources``     — works published in Colombian sources.
3. ``cut_works_dois``        — works matched by DOI across auxiliary DBs.
4. ``cut_works_minciencias`` — works matched by Minciencias Gruplac similarity.
5. ``dedup_works``           — deduplication via aggregate.
6. ``cut_authors``           — authors referenced by the cut works.
7. ``copy_aux_collections``  — verbatim copy of concepts, funders, institutions, etc.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

with DAG(
    dag_id="openalexco_capture",
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
        "db_in": Param(
            "openalex",
            type="string",
            description="Source OpenAlex MongoDB database name",
        ),
        "db_out": Param(
            "openalexco",
            type="string",
            description="Target Colombia-cut MongoDB database name",
        ),
        "es_index": Param(
            "openalex_index",
            type="string",
            description="Elasticsearch index for Minciencias similarity search",
        ),
        "es_uri": Param(
            "http://localhost:9200",
            type="string",
            description="Elasticsearch URI",
        ),
        "es_user": Param(
            "elastic",
            type="string",
            description="Elasticsearch username",
        ),
        "es_password": Param(
            "colav",
            type="string",
            description="Elasticsearch password",
        ),
        "jobs": Param(
            72,
            type="integer",
            description="Parallel workers for joblib tasks",
        ),
        "backend": Param(
            "threading",
            type="string",
            description="joblib backend (threading or loky)",
        ),
        "drop_db": Param(
            True,
            type="boolean",
            description="Drop the target database before processing (full rebuild)",
        ),
    },
    schedule=None,
    catchup=False,
    tags=["openalexco", "capture"],
) as dag:
    # ------------------------------------------------------------------
    # Helper: build extractor inside the task (avoid parse-time imports)
    # ------------------------------------------------------------------
    def _make_extractor(**context: Any):
        from extract.openalexco.openalexco_extractor import OpenAlexCOExtractor

        params = context["params"]
        hook = MongoHook(mongo_conn_id=params["mongo_conn_id"])
        client = hook.get_conn()
        return OpenAlexCOExtractor(
            db_in=params["db_in"],
            db_out=params["db_out"],
            es_index=params["es_index"],
            jobs=params["jobs"],
            backend=params["backend"],
            es_uri=params["es_uri"],
            es_auth=(params["es_user"], params["es_password"]),
            client=client,
        )

    # ------------------------------------------------------------------
    # Task 0 — Drop target DB
    # ------------------------------------------------------------------
    def prepare_db(**context: Any) -> None:
        import logging

        params = context["params"]
        if not params.get("drop_db", True):
            return
        log = logging.getLogger("airflow.task.prepare_db")
        hook = MongoHook(mongo_conn_id=params["mongo_conn_id"])
        client = hook.get_conn()
        db_out = params["db_out"]
        log.warning("Dropping database '%s' before Colombia cut.", db_out)
        client.drop_database(db_out)
        log.info("Database '%s' dropped successfully.", db_out)

    t_prepare = PythonOperator(
        task_id="prepare_db",
        python_callable=prepare_db,
    )

    # ------------------------------------------------------------------
    # Task 1 — Works by authorship
    # ------------------------------------------------------------------
    def cut_works_authorship(**context: Any) -> None:
        ext = _make_extractor(**context)
        ext._cut_works_by_authorship()

    t_authorship = PythonOperator(
        task_id="cut_works_authorship",
        python_callable=cut_works_authorship,
    )

    # ------------------------------------------------------------------
    # Task 2 — Works from Colombian sources
    # ------------------------------------------------------------------
    def cut_works_sources(**context: Any) -> None:
        ext = _make_extractor(**context)
        ext._cut_works_by_sources()

    t_sources = PythonOperator(
        task_id="cut_works_sources",
        python_callable=cut_works_sources,
    )

    # ------------------------------------------------------------------
    # Task 3 — DOI-based cut
    # ------------------------------------------------------------------
    def cut_works_dois(**context: Any) -> None:
        ext = _make_extractor(**context)
        ext._cut_works_by_dois()

    t_dois = PythonOperator(
        task_id="cut_works_dois",
        python_callable=cut_works_dois,
    )

    # ------------------------------------------------------------------
    # Task 4 — Minciencias similarity cut
    # ------------------------------------------------------------------
    def cut_works_minciencias(**context: Any) -> None:
        ext = _make_extractor(**context)
        ext._cut_works_by_minciencias()

    t_minciencias = PythonOperator(
        task_id="cut_works_minciencias",
        python_callable=cut_works_minciencias,
    )

    # ------------------------------------------------------------------
    # Task 5 — Dedup
    # ------------------------------------------------------------------
    def dedup_works(**context: Any) -> None:
        ext = _make_extractor(**context)
        ext._dedup_works()

    t_dedup = PythonOperator(
        task_id="dedup_works",
        python_callable=dedup_works,
    )

    # ------------------------------------------------------------------
    # Task 6 — Authors
    # ------------------------------------------------------------------
    def cut_authors(**context: Any) -> None:
        ext = _make_extractor(**context)
        ext._cut_authors()

    t_authors = PythonOperator(
        task_id="cut_authors",
        python_callable=cut_authors,
    )

    # ------------------------------------------------------------------
    # Task 7 — Auxiliary collections copy
    # ------------------------------------------------------------------
    def copy_aux_collections(**context: Any) -> None:
        ext = _make_extractor(**context)
        ext._copy_auxiliary_collections()

    t_aux = PythonOperator(
        task_id="copy_aux_collections",
        python_callable=copy_aux_collections,
    )

    # ------------------------------------------------------------------
    # Dependencies: sequential pipeline
    # ------------------------------------------------------------------
    (
        t_prepare
        >> t_authorship
        >> t_sources
        >> t_dois
        >> t_minciencias
        >> t_dedup
        >> t_authors
        >> t_aux
    )
