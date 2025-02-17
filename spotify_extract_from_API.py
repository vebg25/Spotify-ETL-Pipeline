import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))
    pl_id = 'spotify:playlist:5RIbzhG2QqdkaP24iXLnZX'
    data = sp.playlist_tracks(pl_id)
    print(data)

    filename = 'spotify_raw_' + str(datetime.now()) + '.json'
    client= boto3.client('s3')
    client.put_object(Body=json.dumps(data),
        Bucket='aws-bucket-etl-spotify-veb',
        Key=f'raw_data/to_processed/{filename}')