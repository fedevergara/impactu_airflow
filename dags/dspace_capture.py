from __future__ import annotations

import importlib.util
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "oxomoc_colombia_config.py")


def load_config(path: str = CONFIG_PATH) -> dict:
    spec = importlib.util.spec_from_file_location("oxomoc_config", path)
    if spec is None or spec.loader is None:
        raise FileNotFoundError(f"Cannot load config module from {path}")

    cfg = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cfg)
    return getattr(cfg, "endpoints", {}) or {}


def ensure_oxomoc_import():
    try:
        import oxomoc  # noqa: F401
    except Exception:
        # try to load local ../Oxomoc if present
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        oxomoc_path = os.path.abspath(os.path.join(repo_root, "..", "Oxomoc"))
        if os.path.isdir(oxomoc_path):
            sys.path.insert(0, oxomoc_path)
        else:
            # also try an egg-style package path
            sys.path.insert(0, os.path.abspath(os.path.join(repo_root, "..")))

    # Monkeypatch lxml XPathElementEvaluator.evaluate if missing (compatibility across lxml versions)
    try:
        from lxml import etree as _etree

        if not hasattr(_etree.XPathElementEvaluator, "evaluate"):

            def _evaluate(self, expr, **kwargs):
                return self.xpath(expr, **kwargs)

            _etree.XPathElementEvaluator.evaluate = _evaluate
    except Exception:
        # best-effort: ignore if lxml not available yet
        pass


def run_endpoint(
    endpoint_name: str,
    mongo_conn_id: str = "mongodb_default",
    mongodb_uri: str | None = None,
    mongo_db: str | None = None,
    jobs: int | None = None,
):
    """Task function: create checkpoint if needed and harvest the endpoint."""
    ensure_oxomoc_import()
    # Import inside function to avoid hard import-time dependency for CI/static tools
    from oxomoc.harvester import OxomocHarvester

    # Resolve Mongo connection from Airflow hook if provided
    if mongo_conn_id and (mongodb_uri is None or mongo_db is None):
        hook = MongoHook(mongo_conn_id=mongo_conn_id)
        # get_conn() returns a pymongo.MongoClient; hook.connection exposes Airflow Connection
        conn = getattr(hook, "connection", None)
        try:
            mongodb_uri = conn.get_uri() if conn and hasattr(conn, "get_uri") else mongodb_uri
        except Exception:
            mongodb_uri = mongodb_uri
        if mongo_db is None:
            mongo_db = getattr(conn, "schema", None)
            if not mongo_db:
                raise ValueError(
                    "MongoDB database not provided. Set `mongo_db` in the DAG op_kwargs or connection schema."
                )

    endpoints = load_config()
    if endpoint_name not in endpoints:
        raise ValueError(f"Endpoint {endpoint_name} not found in config")

    endpoint_cfg = endpoints[endpoint_name]

    # instantiate harvester with only this endpoint (assumes checkpoint exists)
    harvester = OxomocHarvester(
        {endpoint_name: endpoint_cfg}, mongo_db=mongo_db, mongodb_uri=mongodb_uri
    )
    harvester.run(jobs=jobs)


def create_checkpoint(
    endpoint_name: str,
    mongo_conn_id: str = "mongodb_default",
    mongodb_uri: str | None = None,
    mongo_db: str | None = None,
):
    """Create or update the checkpoint for a given endpoint using oxomoc checkpoint classes."""
    ensure_oxomoc_import()
    from oxomoc.checkpoint import OxomocCheckPoint

    # Resolve Mongo connection from Airflow hook if provided
    if mongo_conn_id and (mongodb_uri is None or mongo_db is None):
        hook = MongoHook(mongo_conn_id=mongo_conn_id)
        conn = getattr(hook, "connection", None)
        try:
            mongodb_uri = conn.get_uri() if conn and hasattr(conn, "get_uri") else mongodb_uri
        except Exception:
            mongodb_uri = mongodb_uri
        if mongo_db is None:
            mongo_db = getattr(conn, "schema", None)
            if not mongo_db:
                raise ValueError(
                    "MongoDB database not provided. Set `mongo_db` in the DAG op_kwargs or connection schema."
                )

    endpoints = load_config()
    if endpoint_name not in endpoints:
        raise ValueError(f"Endpoint {endpoint_name} not found in config")

    cfg = endpoints[endpoint_name]
    ckp_cfg = cfg.get("checkpoint", {})
    enabled = ckp_cfg.get("enabled", False)
    if not enabled:
        # nothing to do
        return

    ckp = OxomocCheckPoint(mongodb_uri)

    metadata = cfg.get("metadataPrefix", "oai_dc")
    # Always call the non-selective create signature; selective mode removed.
    ckp.create(cfg["url"], mongo_db, endpoint_name, metadata)


default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="dspace_capture",
    default_args=default_args,
    description="Weekly harvest of DSpace endpoints using oxomoc library",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    params={"mongo_conn_id": "mongodb_default", "mongo_db": "dspace"},
) as dag:
    endpoints = load_config()

    for name, cfg in endpoints.items():
        if not cfg.get("enabled", False):
            continue

        create_task_id = f"create_checkpoint_{name}"
        harvest_task_id = f"harvest_{name}"

        create_task = PythonOperator(
            task_id=create_task_id,
            python_callable=create_checkpoint,
            op_kwargs={
                "endpoint_name": name,
                "mongo_conn_id": "{{ params.mongo_conn_id }}",
                "mongo_db": "{{ params.mongo_db }}",
            },
        )

        harvest_task = PythonOperator(
            task_id=harvest_task_id,
            python_callable=run_endpoint,
            op_kwargs={
                "endpoint_name": name,
                "mongo_conn_id": "{{ params.mongo_conn_id }}",
                "mongo_db": "{{ params.mongo_db }}",
            },
        )

        create_task >> harvest_task
