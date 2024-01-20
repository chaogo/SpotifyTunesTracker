import datetime
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
    'limit': 20
  }

  url = f"{URL_RECENTLY_PLAYED}?{urllib.parse.urlencode(params)}"

  response = requests.get(url, headers=header)
  data = response.json()
  
  return data
