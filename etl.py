import datetime
import sqlite3
import pandas as pd
import requests
import urllib
import sqlalchemy


URL_RECENTLY_PLAYED = 'https://api.spotify.com/v1/me/player/recently-played'
DATABASE= "listening_history.sqlite"

def extract_transform_load(access_token):
  # extract all songs listened for the last 24 hours

  header = {
    "Authorization" : f"Bearer {access_token}"
  }
  
  params = {
    'after': f'{int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp()) * 1000}', # "after" timestamp in miliseconds
    'limit': 50
  }

  url = f"{URL_RECENTLY_PLAYED}?{urllib.parse.urlencode(params)}"

  response = requests.get(url, headers=header)
  data = response.json()

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

  engine = sqlalchemy.create_engine(f'sqlite:///{DATABASE}')
  conn = sqlite3.connect(DATABASE)
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
  
  return data
