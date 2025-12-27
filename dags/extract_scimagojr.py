from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models.param import Param
from datetime import datetime, timedelta
from extract.scimagojr.scimagojr_extractor import ScimagoJRExtractor

default_args = {
    'owner': 'impactu',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_extraction_by_year(year, **kwargs):
    # Get params from DAG
    force_redownload = kwargs['params'].get('force_redownload', False)
    chunk_size = kwargs['params'].get('chunk_size', 1000)
    
    # Use MongoHook to get the connection
    hook = MongoHook(mongo_conn_id='mongodb_default')
    client = hook.get_conn()
    db_name = hook.connection.schema or 'impactu'
    
    extractor = ScimagoJRExtractor(None, db_name, client=client)
    try:
        extractor.process_year(year, force_redownload=force_redownload, chunk_size=chunk_size)
    finally:
        extractor.close()

with DAG(
    'extract_scimagojr',
    default_args=default_args,
    description='Extract data from ScimagoJR and load into MongoDB',
    schedule='@yearly',
    catchup=False,
    tags=['extract', 'scimagojr'],
    params={
        "force_redownload": Param(False, type="boolean", description="Force data download even if already in cache or database"),
        "chunk_size": Param(1000, type="integer", description="Number of records to insert in each bulk operation"),
    },
) as dag:

    years = list(range(1999, datetime.now().year + 1))

    extract_task = PythonOperator.partial(
        task_id='extract_and_load_scimagojr',
        python_callable=run_extraction_by_year,
    ).expand(op_kwargs=[{'year': year} for year in years])
