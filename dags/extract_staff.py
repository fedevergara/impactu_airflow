from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

from extract.staff.staff_extractor import StaffExtractor

default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(hours=1),
}


def run_staff_extraction(**kwargs: dict) -> None:
    """
    Extract staff files from Google Drive and load into MongoDB.

    Notes
    -----
    Uses MongoHook to get MongoDB connection from Airflow connections.
    Google Drive credentials are expected to be available as a pickle file path.
    """
    params = kwargs.get("params", {})

    drive_root_folder_id = params.get("drive_root_folder_id") or Variable.get(
        "staff_drive_root_folder_id", default_var=""
    )
    google_token_pickle = params.get("google_token_pickle")
    cache_dir = params.get("cache_dir", "/opt/airflow/cache/staff")

    force = params.get("force", False)
    keep_only_latest_per_institution = params.get(
        "keep_only_latest_per_institution", True)

    if not drive_root_folder_id:
        raise ValueError("Missing required param: drive_root_folder_id")
    if not google_token_pickle:
        raise ValueError("Missing required param: google_token_pickle")

    # Use MongoHook to get the connection
    hook = MongoHook(mongo_conn_id="mongodb_default")
    client = hook.get_conn()
    db_name = hook.connection.schema or "impactu"

    extractor = StaffExtractor(
        mongodb_uri="",
        db_name=db_name,
        drive_root_folder_id=drive_root_folder_id,
        google_token_pickle=google_token_pickle,
        collection_name="staff",
        client=client,
        cache_dir=cache_dir,
        keep_only_latest_per_institution=keep_only_latest_per_institution,
    )

    try:
        stats = extractor.process_all_institutions(force=force)
        # Airflow log-friendly
        print(f"Staff extraction finished. Stats: {stats}")
    finally:
        extractor.close()


with DAG(
    "extract_staff",
    default_args=default_args,
    description="Extract staff files from Google Drive and load into MongoDB",
    schedule="0 2 * * 1",
    catchup=False,
    tags=["extract", "staff"],
    params={
        "drive_root_folder_id": Param(
            "",
            type="string",
            description="Google Drive root folder ID containing institution subfolders",
        ),
        "google_token_pickle": Param(
            "/opt/airflow/keys/drive_token.pickle",
            type="string",
            description="Path to Google Drive credentials pickle file (read-only access)",
        ),
        "cache_dir": Param(
            "/opt/airflow/cache/staff",
            type="string",
            description="Local cache directory for downloaded staff files",
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
    },
) as dag:
    extract_task = PythonOperator(
        task_id="extract_and_load_staff",
        python_callable=run_staff_extraction,
    )
