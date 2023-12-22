import os
import requests
import pandas as pd
import yaml
from google.cloud import bigquery
from google.oauth2 import service_account
import functions_framework
from datetime import datetime, timedelta
import pytz
import json

# Function to fetch data from API
def fetch_api_data(chain, address):
    api_url = f"https://api.dexscreener.com/latest/dex/pairs/{chain}/{address}"
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from {api_url}: Status code {response.status_code}")

# Function to flatten JSON and convert to DataFrame
def flatten_json_to_dataframe(data):
    pairs = data['pairs']
    df = pd.json_normalize(pairs, sep='_')  

    df = df.drop(columns=['url'])

    if 'labels' in df.columns:
        df['labels'] = df['labels'].apply(json.dumps)

    df['priceUsd'] = df['priceUsd'].astype(float)
    df['priceNative'] = df['priceNative'].astype(float)

    return df

def get_service_account():
    service_account_info_string = os.environ.get('SERVICE_ACCOUNT_INFO')
    service_account_info = json.loads(service_account_info_string)

    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client, credentials

def insert_data_into_bigquery(dataset_id, table_id, dataframe):
    client, _ = get_service_account()
    table_ref = client.dataset(dataset_id).table(table_id)
    job = client.load_table_from_dataframe(dataframe, table_ref)
    job.result()  # Wait for the job to complete

    if job.errors is None:
        return 'Data inserted successfully!'
    else:
        return f'Encountered errors: {job.errors}'

# Function to check if the pair was added within the last 60 days
def is_recently_added(time_added_str):
    time_added = datetime.fromisoformat(time_added_str).replace(tzinfo=pytz.UTC)
    sixty_days_ago = datetime.now(pytz.UTC) - timedelta(days=60)
    return time_added > sixty_days_ago

def add_timestamp(df):
    df['timestamp'] = datetime.now(pytz.UTC)
    return df

@functions_framework.http
def main_function(request):
    dataset_id = 'dev_momentum'
    table_id = 'raw'

    with open('pairs.yaml', 'r') as file:
        pairs_config = yaml.safe_load(file)
    
    for pair in pairs_config['pairs']:
        if is_recently_added(pair['timeAdded']):
            chain = pair['chain']
            address = pair['address']

            api_data = fetch_api_data(chain, address)
            dataframe = flatten_json_to_dataframe(api_data)
            dataframe = add_timestamp(dataframe)
            insert_data_into_bigquery(dataset_id, table_id, dataframe)

    return 'All recent data inserted successfully!'
