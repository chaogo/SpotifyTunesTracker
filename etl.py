import datetime
import pandas as pd
import requests
import urllib


URL_RECENTLY_PLAYED = 'https://api.spotify.com/v1/me/player/recently-played'

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
  
  return data
