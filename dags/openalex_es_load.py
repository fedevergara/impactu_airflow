"""Load OpenAlex works from MongoDB into Elasticsearch using mohan.Similarity.

Workflow
--------
1. ``delete_index``   — drops and recreates the Elasticsearch index.
2. ``prepare_chunks`` — counts documents in MongoDB and returns a list of
   ``{"offset": N, "chunk_size": M}`` dicts for dynamic fan-out.
3. ``load_chunk``     — N parallel tasks, each indexing one slice of the cursor.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param, task


def _resolve_es(params: dict) -> tuple[str, tuple[str, str] | None]:
    """Return (es_host, es_auth) resolving from connection if not overridden."""
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
    return es_host, es_auth


def _get_mongo_collection(params: dict) -> Any:
    """Return a pymongo Collection using MongoHook."""
    from airflow.providers.mongo.hooks.mongo import MongoHook

    hook = MongoHook(params["mongo_conn_id"])
    client = hook.get_conn()
    return client[params["db_name"]][params["collection_name"]]


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
            description="Airflow Elasticsearch connection ID",
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
        "num_workers": Param(
            4,
            type="integer",
            description="Number of parallel indexing tasks",
        ),
    },
    schedule=None,
    catchup=False,
    tags=["openalex", "load", "elasticsearch"],
) as dag:

    def delete_index(**context: Any) -> None:
        """Drop and recreate the Elasticsearch index."""
        from mohan.Similarity import Similarity

        params = context["params"]
        es_host, es_auth = _resolve_es(params)
        es_index = params["es_index"]

        s = Similarity(es_index, es_uri=es_host, es_auth=es_auth)
        s.delete_index(es_index)
        logging.info("Deleted index '%s'", es_index)

    @task
    def prepare_chunks(**context: Any) -> list[dict]:
        """Count documents and return a list of {offset, chunk_size} dicts."""
        params = context["params"]
        num_workers = int(params["num_workers"])
        collection = _get_mongo_collection(params)

        total = collection.count_documents({"title": {"$exists": True}})
        logging.info("Total documents with title: %d", total)

        if total == 0:
            return [{"offset": 0, "chunk_size": 0}]

        chunk_size = (total + num_workers - 1) // num_workers
        chunks = [{"offset": i * chunk_size, "chunk_size": chunk_size} for i in range(num_workers)]
        logging.info("Split into %d chunks of ~%d docs each", len(chunks), chunk_size)
        return chunks

    def load_chunk(offset: int, chunk_size: int, **context: Any) -> None:
        """Index one slice of the MongoDB cursor into Elasticsearch."""
        from kahi_impactu_utils.String import parse_html, parse_mathml
        from mohan.Similarity import Similarity

        params = context["params"]
        es_host, es_auth = _resolve_es(params)
        es_index = params["es_index"]
        bulk_size = int(params["bulk_size"])

        s = Similarity(es_index, es_uri=es_host, es_auth=es_auth)
        collection = _get_mongo_collection(params)

        cursor = (
            collection.find(
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
            .skip(offset)
            .limit(chunk_size)
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

            es_entries.append(
                {
                    "_index": es_index,
                    "_id": str(doc["_id"]),
                    "_source": {
                        "title": title,
                        "source": source_name,
                        "year": doc.get("publication_year", ""),
                        "volume": biblio.get("volume", ""),
                        "issue": biblio.get("issue", ""),
                        "first_page": biblio.get("first_page", ""),
                        "last_page": biblio.get("last_page", ""),
                        "authors": authors,
                    },
                }
            )

            if len(es_entries) >= bulk_size:
                s.insert_bulk(es_entries)
                es_entries = []

            counter += 1
            if counter % 10000 == 0:
                logging.info("Chunk offset=%d progress: %d docs indexed", offset, counter)

        if es_entries:
            s.insert_bulk(es_entries)

        logging.info(
            "Chunk offset=%d done. Indexed: %d | Skipped (no title): %d",
            offset,
            counter,
            count_nones,
        )

    delete_task = PythonOperator(task_id="delete_index", python_callable=delete_index)
    chunks = prepare_chunks()
    load_tasks = PythonOperator.partial(
        task_id="load_chunk",
        python_callable=load_chunk,
    ).expand(op_kwargs=chunks)

    delete_task >> chunks >> load_tasks
