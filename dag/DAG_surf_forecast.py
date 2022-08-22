from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from ETL_surf_forecast import run_postgresql_etl, run_gsheets_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 8, 14, 6, 0, 0, tz = "America/Los_Angeles"),
    'email': ['cullenim3@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'surf_forecast_dag',
    default_args=default_args,
    description='surf forecast DAG',
    schedule_interval=timedelta(days=1),
)

update_postgresql_db = PythonOperator(
    task_id='postgresql_etl',
    python_callable=run_postgresql_etl,
    dag=dag,
)

update_google_sheets = PythonOperator(
    task_id='google_sheets_etl',
    python_callable=run_gsheets_etl,
    dag=dag,
)

update_postgresql_db >> update_google_sheets