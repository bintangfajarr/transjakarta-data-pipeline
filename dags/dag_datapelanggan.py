
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from extract import run_extract
from transform import run_transform
from load import run_load

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 17),
    'email': ['data-engineer@transjakarta.co.id'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transjakarta_pelanggan_pipeline',
    default_args=default_args,
    description='Pipeline ETL untuk data pelanggan Transjakarta',
    schedule_interval='0 7 * * *',  # Setiap hari jam 07:00
    catchup=False,
    tags=['transjakarta', 'etl', 'pelanggan'],
)

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=run_extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=run_transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=run_load,
    provide_context=True,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

start_task >> extract_task >> transform_task >> load_task >> end_task