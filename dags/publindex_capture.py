"""Capture Publindex journals: national (via Yuku) + international (Scienti API).

Two tasks:
- `download_publindex_national`: uses the `deps/Yuku` library at runtime to download
  national journals (Yuku must expose a suitable method; this task will call
  `download_publindex_national` or `download_publindex` if available).
- `download_publindex_international`: hits the Scienti API
  `https://scienti.minciencias.gov.co/publindex/api/publico/revistasHomologadas/{id}`
  iterating `id` from `start_index` upward until `max_missing_streak` consecutive
  non-200 responses are observed (some intermediate ids may be absent).

Results are written to MongoDB (db `publindex` by default) in collections
`publindex.national` and `publindex.international`.

Warning: the international loop can be large; tune `max_missing_streak`.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime, timedelta
from typing import Any

import requests
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator

DEFAULTS = {
    "mongo_conn_id": "mongodb_default",
    "mongo_db": "publindex",
    "start_index": 1,
    "max_missing_streak": 200,
    "max_index": None,
}
DEFAULTS["publindex_id"] = "mwmn-inyg"


def _import_yuku():
    repo_root = os.path.dirname(os.path.dirname(__file__))
    yuku_path = os.path.join(repo_root, "deps", "Yuku")
    if yuku_path not in sys.path:
        sys.path.insert(0, yuku_path)
    try:
        from yuku.Yuku import Yuku

        return Yuku
    except Exception:
        raise


def _make_yuku_instance(mongo_conn_id: str, mongo_db: str, mongodb_uri: str | None = None):
    ycls = _import_yuku()
    return ycls(mongo_db=mongo_db, mongodb_uri=mongodb_uri)


def download_publindex_national(
    mongo_conn_id: str, mongo_db: str, publindex_id: str = "mwmn-inyg", **_kwargs: Any
) -> None:
    """Call Yuku to download national Publindex journals.

    The Yuku API used here is best-effort: it will try `download_publindex_national`
    first, then `download_publindex` as a fallback. If Yuku does not expose those
    methods, the task will log and exit.
    """
    log = logging.getLogger(__name__)
    # Resolve Mongo URI from Airflow connection so Yuku can construct MongoClient
    mongodb_uri: str | None = None
    try:
        hook = MongoHook(mongo_conn_id=mongo_conn_id)
        conn = getattr(hook, "connection", None)
        try:
            if conn and hasattr(conn, "get_uri"):
                mongodb_uri = conn.get_uri()
        except Exception:
            mongodb_uri = None

        # normalize common alternate schemes
        if isinstance(mongodb_uri, str):
            if mongodb_uri.startswith("mongo+srv://"):
                mongodb_uri = mongodb_uri.replace("mongo+srv://", "mongodb+srv://", 1)
            elif mongodb_uri.startswith("mongo://"):
                mongodb_uri = mongodb_uri.replace("mongo://", "mongodb://", 1)

        if not mongodb_uri and conn:
            host = getattr(conn, "host", None)
            port = getattr(conn, "port", None)
            login = getattr(conn, "login", None)
            password = getattr(conn, "password", None)
            schema = getattr(conn, "schema", None)
            if host:
                creds = (f"{login}:{password}@" if password else f"{login}@") if login else ""
                mongodb_uri = f"mongodb://{creds}{host}"
                if port:
                    mongodb_uri += f":{port}"
                if schema:
                    mongodb_uri += f"/{schema}"
    except Exception:
        log.exception(
            "Failed to resolve MongoDB connection for Yuku; proceeding without explicit URI"
        )

    try:
        y = _make_yuku_instance(mongo_conn_id, mongo_db, mongodb_uri=mongodb_uri)
    except Exception:
        log.exception("Failed to import or instantiate Yuku; national Publindex skipped")
        return

    try:
        log.info("Calling Yuku.download(%s,'publindex') to fetch national Publindex", publindex_id)
        y.download(publindex_id, "publindex")
        log.info("Yuku.download finished")
        return
    except Exception:
        log.exception("Yuku.download() failed")
        return


def download_publindex_international(
    mongo_conn_id: str,
    mongo_db: str,
    start_index: int = 1,
    max_missing_streak: int = 20,
    max_index: int | None = None,
    **_kwargs: Any,
) -> int:
    """Iterate Scienti API ids and insert found records into MongoDB.

    Returns number of records inserted.
    """
    log = logging.getLogger(__name__)
    try:
        start_index = int(start_index)
    except Exception:
        start_index = 1
    try:
        max_missing_streak = int(max_missing_streak)
    except Exception:
        max_missing_streak = 20
    # Normalize max_index which may be templated as string 'None'
    if isinstance(max_index, str):
        if max_index.lower() in ("none", "null", ""):
            max_index = None
        else:
            try:
                max_index = int(max_index)
            except Exception:
                max_index = None

    session = requests.Session()
    hook = MongoHook(mongo_conn_id=mongo_conn_id)
    client = hook.get_conn()
    conn = getattr(hook, "connection", None)
    if mongo_db is None:
        mongo_db = getattr(conn, "schema", None) or "publindex"

    db = client[mongo_db]
    coll = db.get_collection("international")

    base = "https://scienti.minciencias.gov.co/publindex/api/publico/revistasHomologadas/"
    idx = start_index
    missing = 0
    inserted = 0

    while True:
        if max_index and idx > int(max_index):
            log.info("Reached max_index %s; stopping", max_index)
            break

        url = f"{base}{idx}"
        try:
            r = session.get(url, timeout=30)
        except Exception:
            log.exception("HTTP error for %s", url)
            missing += 1
            if missing >= max_missing_streak:
                log.info("Max missing streak %s reached; stopping", max_missing_streak)
                break
            idx += 1
            continue

        if r.status_code == 200:
            try:
                data = r.json()
            except Exception:
                log.exception("Invalid JSON at %s", url)
                missing += 1
                if missing >= max_missing_streak:
                    break
                idx += 1
                continue

            # API sometimes returns a list; handle both list and dict
            if isinstance(data, list):
                if len(data) == 0:
                    missing += 1
                    if missing >= max_missing_streak:
                        break
                    idx += 1
                    continue
                data_item = data[0]
            elif isinstance(data, dict):
                data_item = data
            else:
                # unexpected type
                missing += 1
                if missing >= max_missing_streak:
                    break
                idx += 1
                continue

            # Upsert by id if present, else insert
            rec_id = data_item.get("id") if isinstance(data_item, dict) else None
            if rec_id is None:
                rec_id = data_item.get("codigo") if isinstance(data_item, dict) else None
            if rec_id is None:
                rec_id = idx
            # Normalize ISSNs: ensure `issns` is a list of strings
            try:
                issns = None
                # common keys where ISSN info may appear
                for key in (
                    "issns",
                    "issn",
                    "issnPrint",
                    "issnElectronico",
                    "issn_electronico",
                    "issn_print",
                ):
                    if key in data_item and data_item.get(key):
                        issns = data_item.get(key)
                        break

                normalized: list[str] = []
                if issns is None:
                    normalized = []
                elif isinstance(issns, list):
                    # list of strings or dicts
                    for v in issns:
                        if isinstance(v, str):
                            s = v.strip()
                            if s:
                                normalized.append(s)
                        elif isinstance(v, dict):
                            # try common subkeys
                            for sub in ("value", "issn", "codigo", "id"):
                                if sub in v and isinstance(v[sub], str):
                                    s = v[sub].strip()
                                    if s:
                                        normalized.append(s)
                                        break
                elif isinstance(issns, str):
                    # split on common separators
                    parts = [p.strip() for p in re.split(r"[,;|/\\]+", issns) if p.strip()]
                    normalized = parts
                else:
                    # fallback: try to stringify
                    s = str(issns).strip()
                    normalized = [s] if s else []

                # dedupe while preserving order
                seen = set()
                deduped: list[str] = []
                for v in normalized:
                    if v not in seen:
                        seen.add(v)
                        deduped.append(v)
                data_item["issns"] = deduped
            except Exception:
                logging.exception("Failed to normalize ISSNs for record %s", rec_id)

            try:
                coll.replace_one({"_id": rec_id}, data_item, upsert=True)
                inserted += 1
                log.info("Inserted/updated international id=%s", rec_id)
            except Exception:
                log.exception("Failed to write record id=%s", rec_id)

            missing = 0
        else:
            # treat 404/204/other as missing
            log.debug("Non-200 (%s) for %s", r.status_code, url)
            missing += 1
            if missing >= max_missing_streak:
                log.info(
                    "Max missing streak %s reached at idx=%s; stopping", max_missing_streak, idx
                )
                break

        idx += 1

    log.info("International fetch finished: inserted=%d", inserted)
    return inserted


default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="publindex_capture",
    default_args=default_args,
    description="Download national (Yuku) and international Publindex journals",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    params=DEFAULTS,
) as dag:
    t_national = PythonOperator(
        task_id="download_publindex_national",
        python_callable=download_publindex_national,
        op_kwargs={
            "mongo_conn_id": "{{ params.mongo_conn_id }}",
            "mongo_db": "{{ params.mongo_db }}",
            "publindex_id": "{{ params.publindex_id }}",
        },
    )

    t_international = PythonOperator(
        task_id="download_publindex_international",
        python_callable=download_publindex_international,
        op_kwargs={
            "mongo_conn_id": "{{ params.mongo_conn_id }}",
            "mongo_db": "{{ params.mongo_db }}",
            "start_index": "{{ params.start_index }}",
            "max_missing_streak": "{{ params.max_missing_streak }}",
            "max_index": "{{ params.max_index }}",
        },
    )

    t_national >> t_international
