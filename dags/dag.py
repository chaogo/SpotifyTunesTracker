from datetime import datetime, timedelta
import sqlite3
from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import sqlalchemy


# configuration variables - temporary solution
INITIAL_ACCESS_TOKEN = 'BQAxIEm3bBeRetDbSYz59EFzwQQ-W8DvQ0UiYez15aMT63cXVG6CrKjAf6QExPbZB122MrIjCXv7rzx3c_bviYUzH5fL8vSvRv_eQ4a1gLaWATaAezfdX-cu06eCSyRqKgbtAGseUPLZZDG9BKsd95E24B5CW2L54qi4kM0LgvK3KTKQo15yj16piYBJerjI-wO-A2uWbiY'
INITIAL_ACCESS_TOKEN_EXPIRES_AT = '1705800834.688717'
REFRESH_TOKEN = 'AQC-CUF-zmajDdUJx6UNLwMYbh4xe7greHBfGY8jh3mBWICoLafcQgy-Iit-ock4ymFC9HDapitTTS96DU7N9WwzZjIafyCVpdcLGqC9AU4YbfJ9SfKB3xPg7TF9TMvpERg'
CLIENT_ID = 'ca23a84ad19a4992a2338c327f457338'
CLIENT_SECRET = 'c087704f66894d31b1a2e32ac3a220dc'
DATABASE = 'listening_history.sqlite'

# save configurations to Airflow Variables
Variable.set('access_token', INITIAL_ACCESS_TOKEN)
Variable.set('refresh_token', REFRESH_TOKEN)
Variable.set('expires_at', INITIAL_ACCESS_TOKEN_EXPIRES_AT)
Variable.set('client_id', CLIENT_ID)
Variable.set('client_secret', CLIENT_SECRET)
Variable.set('database', DATABASE)
print("printing...", Variable.get('expires_at'))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 22),
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
    schedule=timedelta(days=1),
)

def refresh_token():
  # check if current access token has expired
  print("printing...", float(Variable.get('expires_at')))
  if datetime.now().timestamp() > float(Variable.get('expires_at')):
    print(Variable.get('refresh_token'), Variable.get('client_id'), Variable.get('client_secret'))
    req_body = {
        'grant_type': 'refresh_token',
        'refresh_token': Variable.get('refresh_token'),
        'client_id': Variable.get('client_id'),
        'client_secret': Variable.get('client_secret'), 
    }

    print(req_body)

    # make the HTTP request to refresh the token
    response = requests.post('https://accounts.spotify.com/api/token', data=req_body)

    # Update Airflow Variables of token info
    new_token_info = response.json()
    Variable.set("access_token", new_token_info['access_token'])
    Variable.set("expires_at", datetime.now().timestamp()+new_token_info['expires_in'])
    print(Variable.get('access_token'), Variable.get('expires_at'))

  return

def perform_etl():
 # extract all songs listened for the last 24 hours

  header = {
    "Authorization" : f"Bearer {Variable.get('access_token')}"
  }

  after_timestamp = int((datetime.now() - timedelta(days=1)).timestamp()) * 1000
  limit = 50

  # make the HTTP request to get recently played data
  response = requests.get(f'https://api.spotify.com/v1/me/player/recently-played?after={after_timestamp}&limit={limit}', headers=header)
  data = response.json()

  print(data)

  # transform and validate

  tune_names = []
  artist_names = []
  played_at_list = []

  for tune in data['items']:
    tune_names.append(tune['track']['name'])
    artist_names.append(tune['track']['album']['artists'][0]['name'])
    played_at_list.append(tune['played_at'])

  tune_dict = {
    'tune_name': tune_names,
    'artist_name': artist_names,
    'played_at': played_at_list,
  }

  tune_df = pd.DataFrame(tune_dict, columns = ['tune_name', 'artist_name', 'played_at'])
  
  print(tune_df)

  # Primary Key Check
  if not pd.Series(tune_df['played_at']).is_unique:
      raise Exception("Primary Key check is violated")

  # null check
  if tune_df.isnull().values.any():
      raise Exception("Null values found")
  
  # Load the transformed data into sqlite database

  engine = sqlalchemy.create_engine(f"sqlite:///{Variable.get('database')}")
  conn = sqlite3.connect(Variable.get('database'))
  cursor = conn.cursor()

  sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tunes(
        tune_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
  """

  cursor.execute(sql_query)
  tune_df.to_sql(name='my_played_tunes', con=engine, index=False, if_exists='append')

  conn.close()
  print('Load listening history yesterday to the database successfully')


# Define the tasks
refresh_token_task = PythonOperator(
    task_id='refresh_token',
    python_callable=refresh_token,
    dag=dag,
)

perform_etl_task = PythonOperator(
    task_id='perform_etl',
    python_callable=perform_etl,
    dag=dag,
)

refresh_token_task >> perform_etl_task

# for testing purpose
if __name__ == '__main__':
   print('testing...')
   refresh_token()
