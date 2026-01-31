"""DAG to download Minciencias open data using the Yuku library.

This DAG uses the `deps/Yuku` submodule as a library (imported at runtime)
to download CVLAC and GRUPLAC datasets and to scrape CVLAC profiles.

Default dataset IDs are taken from the Yuku README but can be overridden
via DAG params.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

DEFAULTS = {
    "mongodb_uri": "mongodb://localhost:27017/",
    "mongo_db": "dam",
    "cvlac_dataset_id": "bqtm-4y2h",
    "gruplac_production_id": "33dq-ab5a",
    "gruplac_groups_id": "hrhc-c4wu",
}


def _import_yuku():
    # Ensure the deps/Yuku package is importable when running in Airflow
    repo_root = os.path.dirname(os.path.dirname(__file__))
    yuku_path = os.path.join(repo_root, "deps", "Yuku")
    if yuku_path not in sys.path:
        sys.path.insert(0, yuku_path)
    # yuku package exposes yuku/Yuku.py
    from yuku.Yuku import Yuku

    return Yuku


def _make_yuku_instance(mongodb_uri: str, mongo_db: str):
    yuku_cls = _import_yuku()
    return yuku_cls(mongo_db=mongo_db, mongodb_uri=mongodb_uri)


def download_gruplac_groups(
    dataset_id: str, mongodb_uri: str, mongo_db: str, **_kwargs: Any
) -> None:
    y = _make_yuku_instance(mongodb_uri, mongo_db)
    y.download_gruplac_groups(dataset_id)


def download_gruplac_production(
    dataset_id: str, mongodb_uri: str, mongo_db: str, **_kwargs: Any
) -> None:
    y = _make_yuku_instance(mongodb_uri, mongo_db)
    y.download_gruplac_production(dataset_id)


def download_cvlac_data(dataset_id: str, mongodb_uri: str, mongo_db: str, **_kwargs: Any) -> None:
    y = _make_yuku_instance(mongodb_uri, mongo_db)
    y.download_cvlac_data(dataset_id)


def download_cvlac_profiles(
    mongodb_uri: str, mongo_db: str, use_raw: bool = False, **_kwargs: Any
) -> None:
    y = _make_yuku_instance(mongodb_uri, mongo_db)
    y.download_cvlac_profile(use_raw=use_raw)


default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="dam_capture",
    default_args=default_args,
    description="Download Minciencias (DAM) datasets via Yuku",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    params=DEFAULTS,
) as dag:
    gruplac_groups = PythonOperator(
        task_id="gruplac_groups",
        python_callable=download_gruplac_groups,
        op_kwargs={
            "dataset_id": "{{ params.gruplac_groups_id }}",
            "mongodb_uri": "{{ params.mongodb_uri }}",
            "mongo_db": "{{ params.mongo_db }}",
        },
    )

    gruplac_production = PythonOperator(
        task_id="gruplac_production",
        python_callable=download_gruplac_production,
        op_kwargs={
            "dataset_id": "{{ params.gruplac_production_id }}",
            "mongodb_uri": "{{ params.mongodb_uri }}",
            "mongo_db": "{{ params.mongo_db }}",
        },
    )

    cvlac_data = PythonOperator(
        task_id="cvlac_data",
        python_callable=download_cvlac_data,
        op_kwargs={
            "dataset_id": "{{ params.cvlac_dataset_id }}",
            "mongodb_uri": "{{ params.mongodb_uri }}",
            "mongo_db": "{{ params.mongo_db }}",
        },
    )

    cvlac_profiles = PythonOperator(
        task_id="cvlac_profiles",
        python_callable=download_cvlac_profiles,
        op_kwargs={
            "mongodb_uri": "{{ params.mongodb_uri }}",
            "mongo_db": "{{ params.mongo_db }}",
            "use_raw": False,
        },
    )

    # Order: groups -> production -> cvlac data -> profiles (profiles need data)
    gruplac_groups >> gruplac_production >> cvlac_data >> cvlac_profiles
