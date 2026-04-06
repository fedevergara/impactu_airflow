"""Load OpenAlex works from MongoDB into Elasticsearch using mohan.Similarity.

Workflow
--------
1. ``load_works`` — reads ``openalex.works`` from MongoDB, parses titles
   (MathML / HTML), builds documents and bulk-indexes them into Elasticsearch
   via ``mohan.Similarity``.  The index is recreated from scratch on each run.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

with DAG(
    dag_id="openalex_es_load",
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
            description="MongoDB database name",
        ),
        "collection_name": Param(
            "works",
            type="string",
            description="MongoDB collection to read works from",
        ),
        "es_conn_id": Param(
            "elasticsearch_default",
            type="string",
            description="Airflow Elasticsearch connection ID (host/port/login/password resolved from it)",
        ),
        "es_host": Param(
            "",
            type="string",
            description="Override Elasticsearch host URI. If empty, resolved from es_conn_id.",
        ),
        "es_user": Param(
            "",
            type="string",
            description="Override Elasticsearch username. If empty, resolved from es_conn_id.",
        ),
        "es_password": Param(
            "",
            type="string",
            description="Override Elasticsearch password. If empty, resolved from es_conn_id.",
        ),
        "es_index": Param(
            "openalex_index",
            type="string",
            description="Elasticsearch index name",
        ),
        "bulk_size": Param(
            1000,
            type="integer",
            description="Number of documents per bulk insert",
        ),
    },
    schedule=None,
    catchup=False,
    tags=["openalex", "load", "elasticsearch"],
) as dag:

    def load_works(**context: Any) -> None:
        """Read works from MongoDB and index them into Elasticsearch."""
        import logging

        from airflow.providers.mongo.hooks.mongo import MongoHook
        from kahi_impactu_utils.String import parse_html, parse_mathml
        from mohan.Similarity import Similarity

        params = context["params"]
        es_index = params["es_index"]
        bulk_size = params["bulk_size"]

        # ------------------------------------------------------------------ #
        # Elasticsearch connection — resolve from Airflow connection if not   #
        # overridden via params                                                #
        # ------------------------------------------------------------------ #
        es_host = params.get("es_host", "").strip()
        es_user = params.get("es_user", "").strip()
        es_password = params.get("es_password", "").strip()

        if not es_host:
            from airflow.sdk.bases.hook import BaseHook

            conn = BaseHook.get_connection(params["es_conn_id"])
            scheme = conn.schema or "http"
            host = conn.host or "localhost"
            port = conn.port or 9200
            es_host = f"{scheme}://{host}:{port}"
            es_user = es_user or conn.login or ""
            es_password = es_password or conn.password or ""

        es_auth = (es_user, es_password) if es_user else None
        s = Similarity(
            es_index,
            es_uri=es_host,
            es_auth=es_auth,
        )
        s.delete_index(es_index)

        # ------------------------------------------------------------------ #
        # MongoDB connection via MongoHook                                     #
        # ------------------------------------------------------------------ #
        hook = MongoHook(params["mongo_conn_id"])
        client = hook.get_conn()
        collection = client[params["db_name"]][params["collection_name"]]

        cursor = collection.find(
            {"title": {"$exists": True}},
            {
                "title": 1,
                "primary_location.source": 1,
                "publication_year": 1,
                "biblio": 1,
                "authorships": 1,
                "_id": 1,
            },
        )

        es_entries: list[dict] = []
        counter = 0
        count_nones = 0

        for doc in cursor:
            if doc.get("title") is None:
                count_nones += 1
                continue

            title = parse_mathml(doc["title"])
            title = parse_html(title)

            primary_location = doc.get("primary_location") or {}
            source_info = primary_location.get("source") or {}
            source_name = source_info.get("display_name", "") if source_info else ""

            biblio = doc.get("biblio") or {}
            authors = [
                a["author"]["display_name"]
                for a in doc.get("authorships", [])
                if "display_name" in a.get("author", {})
            ]

            work = {
                "title": title,
                "source": source_name,
                "year": doc.get("publication_year", ""),
                "volume": biblio.get("volume", ""),
                "issue": biblio.get("issue", ""),
                "first_page": biblio.get("first_page", ""),
                "last_page": biblio.get("last_page", ""),
                "authors": authors,
            }

            es_entries.append(
                {
                    "_index": es_index,
                    "_id": str(doc["_id"]),
                    "_source": work,
                }
            )

            if len(es_entries) >= bulk_size:
                s.insert_bulk(es_entries)
                es_entries = []

            counter += 1
            if counter % 1000 == 0:
                logging.info("Progress: %d works indexed", counter)

        if es_entries:
            s.insert_bulk(es_entries)

        logging.info("Done. Total indexed: %d | Skipped (no title): %d", counter, count_nones)

    load_works_task = PythonOperator(
        task_id="load_works",
        python_callable=load_works,
    )
