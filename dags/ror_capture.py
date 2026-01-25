"""Import ROR data from Zenodo into MongoDB (renamed to ror_capture).

Download the latest *ror-data.json* from the specified Zenodo record and load
into MongoDB `ror` database, collection `ror_stage` by default.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import re
import zipfile
from datetime import datetime
from typing import Any

import requests
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)


def _find_latest_ror_file(files: list[dict[str, Any]]) -> dict[str, Any] | None:
    pattern = re.compile(r"ror-data\.json$", re.IGNORECASE)
    matched = [f for f in files if pattern.search(f.get("key", ""))]
    if matched:
        matched.sort(key=lambda f: f.get("updated", f.get("created", "")))
        return matched[-1]
    if files:
        return files[-1]
    return None


def download_ror_to_path(record_id: int = 18260365) -> str:
    api_url = f"https://zenodo.org/api/records/{record_id}"
    log.info("Fetching Zenodo record %s", record_id)
    res = requests.get(api_url, timeout=30)
    res.raise_for_status()
    record = res.json()
    files = record.get("files", [])
    file_entry = _find_latest_ror_file(files)
    if not file_entry:
        raise RuntimeError(f"No files found on Zenodo record {record_id}")

    links = file_entry.get("links", {})
    download_url = links.get("download") or links.get("self")
    if not download_url:
        raise RuntimeError(f"No download link for file {file_entry}")

    log.info("Downloading %s", download_url)
    dl = requests.get(download_url, timeout=120)
    dl.raise_for_status()

    filename = file_entry.get("key", f"ror-{record_id}")

    airflow_home = os.environ.get("AIRFLOW_HOME", os.getcwd())
    dest_dir = os.path.join(airflow_home, "ror_import", str(record_id))
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, filename)
    with open(dest_path, "wb") as fh:
        fh.write(dl.content)

    log.info("Saved file to %s", dest_path)
    return dest_path


def load_ror_from_path(
    file_path: str,
    mongo_conn_id: str = "mongodb_default",
    db_name: str | None = None,
    collection_name: str = "ror_stage",
) -> int:
    if not file_path or str(file_path).lower() == "none":
        airflow_home = os.environ.get("AIRFLOW_HOME", os.getcwd())
        base = os.path.join(airflow_home, "ror_import")
        latest = None
        latest_mtime = 0.0
        for root, _, files in os.walk(base if os.path.isdir(base) else "."):
            for fn in files:
                p = os.path.join(root, fn)
                try:
                    m = os.path.getmtime(p)
                except Exception:
                    m = 0
                if m > latest_mtime:
                    latest_mtime = m
                    latest = p
        if not latest:
            raise RuntimeError(f"No downloaded ROR file found in {base}")
        file_path = latest

    filename = os.path.basename(file_path)
    if filename.lower().endswith(".zip"):
        with zipfile.ZipFile(file_path, "r") as zf:
            candidates = [
                n for n in zf.namelist() if re.search(r"ror-data\.json$", n, re.IGNORECASE)
            ]
            if not candidates:
                candidates = [n for n in zf.namelist() if n.lower().endswith(".json")]
            if not candidates:
                raise RuntimeError(f"No JSON file found inside zip {file_path}")
            member = candidates[-1]
            with zf.open(member) as fh:
                data = json.load(io.TextIOWrapper(fh, encoding="utf-8"))
    elif filename.lower().endswith(".gz"):
        with gzip.open(file_path, "rt", encoding="utf-8") as fh:
            data = json.load(fh)
    else:
        with open(file_path, "rt", encoding="utf-8") as fh:
            data = json.load(fh)

    hook = MongoHook(mongo_conn_id=mongo_conn_id)
    client = hook.get_conn()
    conn = getattr(hook, "connection", None)
    if db_name is None:
        db_name = getattr(conn, "schema", None)
        if not db_name:
            raise ValueError(
                "MongoDB database not provided. Set `db_name` in the DAG params or provide it in the connection schema."
            )
    db = client[db_name]
    coll = db[collection_name]

    if isinstance(data, dict):
        if "items" in data and isinstance(data["items"], list):
            docs = data["items"]
        else:
            docs = [data]
    elif isinstance(data, list):
        docs = data
    else:
        raise RuntimeError(f"Unexpected JSON structure in file {file_path}")

    if not docs:
        log.info("No documents to insert")
        return 0

    inserted = 0
    if all(isinstance(d, dict) and d.get("id") for d in docs):
        for doc in docs:
            doc_id = doc.get("id")
            coll.replace_one({"id": doc_id}, doc, upsert=True)
            inserted += 1
    else:
        coll.delete_many({})
        coll.insert_many(docs)
        inserted = len(docs)

    log.info("Inserted/updated %d documents into %s.%s", inserted, db_name, collection_name)
    return inserted


default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": 300,
}


with DAG(
    dag_id="ror_capture",
    default_args=default_args,
    description="Download latest ROR JSON from Zenodo and load into MongoDB",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ror", "import"],
    params={"mongo_conn_id": "mongodb_default", "mongo_db": "ror"},
) as dag:
    download_task = PythonOperator(
        task_id="download_ror",
        python_callable=download_ror_to_path,
        op_kwargs={"record_id": 18260365},
        do_xcom_push=True,
    )

    load_task = PythonOperator(
        task_id="load_ror",
        python_callable=load_ror_from_path,
        op_kwargs={
            "file_path": "{{ ti.xcom_pull(task_ids='download_ror') }}",
            "mongo_conn_id": "{{ params.mongo_conn_id }}",
            "db_name": "{{ params.mongo_db }}",
            "collection_name": "ror_stage",
        },
    )

    download_task >> load_task
