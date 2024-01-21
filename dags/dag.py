from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# get token_url, client_id, client_secret, refresh_token, access_token, expires_at from airflow variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 21),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'dag',
    default_args=default_args,
    description='SpotifyTunesTracker',
    schedule_interval=timedelta(days=1),
)

def just_a_function():
    print("I'm going to show you something :)")

run_etl = PythonOperator(
    task_id='spotify_tunes_tracker_etl',
    python_callable=just_a_function,
    dag=dag,
)

run_etl