from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

from extract.ciarp.ciarp_extractor import CiarpExtractor

default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(hours=1),
}


def _coerce_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "1", "yes", "y", "t"):
            return True
        if lowered in ("false", "0", "no", "n", "f"):
            return False
    return bool(value)


def run_ciarp_extraction(**kwargs: dict) -> None:
    """
    Extract CIARP files from Google Drive and load into MongoDB.

    Notes
    -----
    Uses MongoHook to get MongoDB connection from Airflow connections.
    Google Drive credentials are expected to be available as a pickle file path.
    """
    params = kwargs.get("params", {})
    dag_run = kwargs.get("dag_run")
    if dag_run and getattr(dag_run, "conf", None):
        for key, value in dag_run.conf.items():
            if value not in (None, ""):
                params[key] = value

    drive_root_folder_id = params.get("drive_root_folder_id") or Variable.get(
        "ciarp_drive_root_folder_id", default_var=""
    )
    drive_subfolder_name = params.get("drive_subfolder_name") or Variable.get(
        "ciarp_drive_subfolder_name", default_var="ciarp"
    )
    dump_dir = params.get("dump_dir") or Variable.get("ciarp_dump_dir", default_var="")
    google_token_pickle = params.get("google_token_pickle")
    cache_dir = params.get("cache_dir", "/tmp/impactu_airflow_cache/ciarp")

    force = _coerce_bool(params.get("force", False))
    keep_only_latest_per_institution = _coerce_bool(params.get("keep_only_latest_per_institution", True))
    backup_existing = _coerce_bool(params.get("backup_existing", True))

    if not drive_root_folder_id:
        raise ValueError("Missing required param: drive_root_folder_id")
    if not google_token_pickle:
        raise ValueError("Missing required param: google_token_pickle")

    hook = MongoHook(mongo_conn_id="mongodb_default")
    client = hook.get_conn()
    db_name = hook.connection.schema or "institutional"

    extractor = CiarpExtractor(
        mongodb_uri="",
        db_name=db_name,
        drive_root_folder_id=drive_root_folder_id,
        subfolder_name=drive_subfolder_name or None,
        google_token_pickle=google_token_pickle,
        collection_name="ciarp",
        client=client,
        cache_dir=cache_dir,
        keep_only_latest_per_institution=keep_only_latest_per_institution,
        backup_existing=backup_existing,
    )

    try:
        extractor.dump_collection_if_exists(dump_dir, filename_prefix="ciarp")
        stats = extractor.process_all_files(force=force)
        print(f"CIARP extraction finished. Stats: {stats}")
    finally:
        extractor.close()


with DAG(
    "extract_ciarp",
    default_args=default_args,
    description="Extract CIARP files from Google Drive and load into MongoDB",
    schedule="0 3 * * 1",
    catchup=False,
    tags=["extract", "ciarp"],
    params={
        "drive_root_folder_id": Param(
            "",
            type="string",
            description="Google Drive folder ID containing CIARP files",
        ),
        "drive_subfolder_name": Param(
            "ciarp",
            type="string",
            description="Optional subfolder name under drive_root_folder_id (e.g., Ciarp)",
        ),
        "dump_dir": Param(
            "",
            type="string",
            description="Optional directory for dumping the ciarp collection before load",
        ),
        "google_token_pickle": Param(
            "",
            type="string",
            description="Path to Google Drive credentials pickle file (read-only access)",
        ),
        "cache_dir": Param(
            "/tmp/impactu_airflow_cache/ciarp",
            type="string",
            description="Local cache directory for downloaded CIARP files",
        ),
        "force": Param(
            False,
            type="boolean",
            description="Reprocess institutions even if the latest file was already loaded",
        ),
        "keep_only_latest_per_institution": Param(
            True,
            type="boolean",
            description="Delete previous docs for each institution before loading the latest file",
        ),
        "backup_existing": Param(
            True,
            type="boolean",
            description="Create a backup collection before loading if data exists",
        ),
    },
) as dag:
    extract_task = PythonOperator(
        task_id="ciarp_extractor",
        python_callable=run_ciarp_extraction,
    )
