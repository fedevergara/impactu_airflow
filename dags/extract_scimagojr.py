from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
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

def run_extraction(**kwargs):
    # Use MongoHook to get the connection
    hook = MongoHook(mongo_conn_id='mongodb_default')
    client = hook.get_conn()
    db_name = hook.connection.schema or 'impactu'
    
    # Range of years: from 1999 to current year
    start_year = 1999
    end_year = datetime.now().year
    
    extractor = ScimagoJRExtractor(None, db_name, client=client)
    # The extractor now uses the airflow task logger automatically
    try:
        extractor.run(start_year, end_year)
    finally:
        extractor.close()

with DAG(
    'extract_scimagojr',
    default_args=default_args,
    description='Extract data from ScimagoJR and load into MongoDB',
    schedule='@yearly',
    catchup=False,
    tags=['extract', 'scimagojr'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_and_load_scimagojr',
        python_callable=run_extraction,
    )
