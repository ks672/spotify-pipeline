import logging
import os
import json
import requests
import azure.functions as func
from datetime import datetime
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def spotify_time_trigger(myTimer: func.TimerRequest) -> None:
    logging.info("Starting Spotify extraction...")

    # Environment variables (loaded from Azure)
    CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
    CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
    STORAGE_CONN = os.getenv("STORAGE_CONNECTION_STRING")
    CONTAINER = os.getenv("CONTAINER_NAME")
    #TEST = os.getenv("AzureWebJobsStorage")
    #TEST2 = os.getenv("AzureWebJobsFeatureFlags")

    # 1. Get Spotify token (Client Credentials OAuth2)
    token_url = "https://accounts.spotify.com/api/token"
    token_data = {"grant_type": "client_credentials"}
    token_response = requests.post(token_url, data=token_data,
                                   auth=(CLIENT_ID, CLIENT_SECRET))
    token = token_response.json().get("access_token")

    #logging.info(f"test: {TEST}\n test2: {TEST2} \n CLIENT_ID: {CLIENT_ID}\n CLIENT_SECRET: {CLIENT_SECRET}\n STORAGE_CONN: {STORAGE_CONN}\n CONTAINER: {CONTAINER}")
    #logging.info(f" token_url: {token_url}\n token_data: {token_data} \n token_response: {token_response} \n token: {token}")

    if not token:
        logging.error("Failed to get access token from Spotify")
        return

    headers = {"Authorization": f"Bearer {token}"}
    logging.info(f" headers: {headers}")


    # 2. Example API endpoint – get new releases
    spotify_url = "https://api.spotify.com/v1/browse/new-releases?limit=50"
    spotify_response = requests.get(spotify_url, headers=headers)

    if spotify_response.status_code != 200:
        logging.error(f"Spotify API error: {spotify_response.text}")
        return

    data = spotify_response.json()

    # 3. Save to Blob Storage
    blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN)
    container_client = blob_service.get_container_client(CONTAINER)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    blob_name = f"new_releases_raw/new_releases_{timestamp}.json"

    #logging.info(f"blob_service: {blob_service}\n container_client: {container_client}")
    #logging.info(f" timestamp: {timestamp}\n blob_name: {blob_name}")

    container_client.upload_blob(
    name=blob_name,
    data=json.dumps(data, indent=2),
    overwrite=True
)

    logging.info(f"Successfully saved: {blob_name}")