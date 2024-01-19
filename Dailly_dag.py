from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from Dailly_func import run_extract_stocks

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,1,12),
    'email': ['karyne.uk@live.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

dag = DAG(
    'extract_stocks_data_daily',
    default_args=default_args,
    description = 'Daily data extraction from Brazil Market Stocks',
    schedule_interval = "@daily"
)

run_etl = PythonOperator(
    task_id='daily_extraction',
    python_callable=run_extract_stocks,
    dag=dag
)

run_etl