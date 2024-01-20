import requests
import urllib
import datetime
from flask import Flask, jsonify, redirect, request, session


app = Flask(__name__)
app.secret_key = 'g6f7d8-h7gf8d9-n8b9vc0-0n9m876nbvcx' # an arbitrary string as it's required when access session

CLIENT_ID = 'ca23a84ad19a4992a2338c327f457338'
CLIENT_SECRET = 'c087704f66894d31b1a2e32ac3a220dc'
REDIRECT_URI = 'http://localhost:8080/callback'

AUTH_URL = 'https://accounts.spotify.com/authorize'
TOKEN_URL = 'https://accounts.spotify.com/api/token'
API_BASE_URL = 'https://api.spotify.com/v1/'

@app.route('/')
def index():
  return "Welcome to SpotifyTunesTracker <a href='/login'>Login with Spotify</a>"

@app.route('/login')
def login():
  scope = 'user-read-recently-played'

  params = {
    'client_id': CLIENT_ID,
    'response_type': 'code',
    'scope': scope,
    'redirect_uri': REDIRECT_URI,
    'show_dialog': True # for testing purpose
  }

  auth_url = f"{AUTH_URL}?{urllib.parse.urlencode(params)}"

  return redirect(auth_url) 


@app.route('/callback')
def callback():
  """
  As it said in the documentation, "If the user accepts your request, then the user is redirected back to the application using the redirect_uri passed on the authorized request describe."
  For a sucessful request, the response would be e.g. https://my-domain.com/callback?code=NApCCg..BkWtQ&state=34fFs29kd09
  For a failed request, the response would be e.g.  https://my-domain.com/callback?code=NApCCg..BkWtQ&state=34fFs29kd09
  """
  if 'error' in request.args:
    return jsonify({"error": request.args['error']})
  
  if 'code' in request.args:
    print(request.args['code'])
    req_body = {
      'code': request.args['code'],
      'grant_type': 'authorization_code',
      'redirect_uri': REDIRECT_URI,
      'client_id': CLIENT_ID,
      'client_secret': CLIENT_SECRET
    }

    response = requests.post(TOKEN_URL, data=req_body)
    token_info = response.json()

    session['access_token'] = token_info['access_token']
    session['refresh_token'] = token_info['refresh_token']
    session['expires_at'] = datetime.datetime.now().timestamp() + token_info['expires_in']

    return redirect('/recently-played')


@app.route('/refresh-token')
def refresh_token():
  print('token refreshing')
  req_body = {
    'grant_type': 'refresh_token',
    'refresh_token': session['refresh_token'],
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET
  }

  response = requests.post(TOKEN_URL, data=req_body)
  new_token_info = response.json()
  new_token_info = {
      # refresh token will never be updated after it comes with the access token for the first time
      'access_token': new_token_info['access_token'],
      'expires_at': datetime.datetime.now().timestamp() + new_token_info['expires_in']
  }

  session.update(new_token_info)

  return redirect('recently-played')


@app.route('/recently-played')
def get_recently_played_tunes():
  if 'access_token' not in session:
    return redirect('/login')
  
  if datetime.datetime.now().timestamp() > session['expires_at']:
    return redirect('/refresh-token')
  
  # TODO: request the tunes played yesterday

  return "retrieving the tunes played yesterday"

if __name__ == '__main__':
  app.run(host='0.0.0.0', port='8080', debug=True)
