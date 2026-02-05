import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

from extract.doaj.doaj_extractor import DoajExtractor


def _compute_period(date_value: datetime) -> tuple[int, int]:
    month = date_value.month
    semester = 1 if month <= 6 else 2
    return date_value.year, semester


def run_doaj_extraction(**kwargs: dict) -> None:
    params = kwargs.get("params", {})

    api_key = params.get("api_key") or Variable.get(
        "doaj_api_key", default_var="")
    if not api_key:
        api_key = Variable.get("DOAJ_API_KEY", default_var="") or os.environ.get(
            "DOAJ_API_KEY", ""
        )
    if not api_key:
        raise ValueError(
            "Missing DOAJ API key (Airflow Variable doaj_api_key or DOAJ_API_KEY)")

    download_journals = params.get("download_journals", True)
    download_articles = params.get("download_articles", False)
    chunk_size = int(params.get("chunk_size", 1000))
    force_download = params.get("force_download", False)

    cache_dir = params.get("cache_dir") or "/tmp/impactu_airflow_cache/doaj"
    keep_cache = params.get("keep_cache", False)
    journals_collection = params.get("journals_collection", "stage")
    articles_collection = params.get("articles_collection", "articles")

    period_year = params.get("period_year") or 0
    period_semester = params.get("period_semester") or 0

    if period_year > 0 and period_semester > 0:
        year = int(period_year)
        semester = int(period_semester)
    else:
        logical_date = (
            kwargs.get("data_interval_start") or kwargs.get(
                "logical_date") or kwargs.get("execution_date") or datetime.now(timezone.utc)
        )
        year, semester = _compute_period(logical_date)

    db_name = f"doaj_{year}_{semester}"

    hook = MongoHook(mongo_conn_id="mongodb_default")
    client = hook.get_conn()

    extractor = DoajExtractor(
        mongodb_uri="",
        db_name=db_name,
        client=client,
        cache_dir=cache_dir,
        api_key=api_key,
        keep_cache=keep_cache,
        journals_collection=journals_collection,
        articles_collection=articles_collection,
    )

    try:
        stats = extractor.run(
            api_key=api_key,
            download_journals=download_journals,
            download_articles=download_articles,
            chunk_size=chunk_size,
            force_download=force_download,
        )
        print(f"DOAJ extraction finished. Stats: {stats}")
    finally:
        extractor.close()


default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(hours=1),
}


with DAG(
    "extract_doaj",
    default_args=default_args,
    description="Extract DOAJ public data dumps (journals and articles) into MongoDB",
    schedule=Variable.get("doaj_schedule", default_var="@monthly"),
    catchup=False,
    tags=["extract", "doaj"],
    params={
        "api_key": Param(
            "",
            type="string",
            description="DOAJ API key (overrides Airflow Variables)",
        ),
        "download_journals": Param(
            True,
            type="boolean",
            description="Download and load journal dump",
        ),
        "download_articles": Param(
            False,
            type="boolean",
            description="Download and load article dump",
        ),
        "chunk_size": Param(
            1000,
            type="integer",
            description="Bulk write chunk size",
        ),
        "force_download": Param(
            False,
            type="boolean",
            description="Force re-download even if cached",
        ),
        "cache_dir": Param(
            "/tmp/impactu_airflow_cache/doaj",
            type="string",
            description="Optional cache directory for downloads/extractions",
        ),
        "keep_cache": Param(
            False,
            type="boolean",
            description="Keep cache files after successful extraction",
        ),
        "journals_collection": Param(
            "stage",
            type="string",
            description="Mongo collection for DOAJ journals",
        ),
        "articles_collection": Param(
            "articles",
            type="string",
            description="Mongo collection for DOAJ articles",
        ),
        "period_year": Param(
            0,
            type="integer",
            description="Override DB year (e.g. 2025)",
        ),
        "period_semester": Param(
            0,
            type="integer",
            description="Override DB semester (1 or 2)",
        ),
    },
) as dag:
    extract_task = PythonOperator(
        task_id="doaj_extractor",
        python_callable=run_doaj_extraction,
    )
