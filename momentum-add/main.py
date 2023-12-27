from datetime import datetime
from google.cloud import firestore
from flask import jsonify
import os
import pytz
import functions_framework
import requests

project_id = os.environ.get("GCP_PROJECT")
database_id = os.environ.get("FIRESTORE_DATABASE")
db = firestore.Client(project=project_id, database=database_id)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

def send_message(chat_id, message):
    base_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": chat_id, "text": message}
    response = requests.post(base_url, data=data)
    return response

@functions_framework.http
def telegram_bot_webhook(request):
    """
    Responds to HTTP requests from a Telegram bot.
    Expects messages in the format: <pair_address> <name>
    """

    try:
        request_json = request.get_json(silent=True, force=True)
        if not request_json:
            # Even in the case of bad request, return 200 OK to stop retries
            return jsonify({"error": "No JSON payload"}), 200

        # Parse the body to get message text and chat ID
        text = request_json['message']['text']
        chat_id = request_json['message']['chat']['id']

        print(f"Received message: {text} from chat ID: {chat_id}")

        # Directly get the data from url
        # Example entry: https://dexscreener.com/arbitrum/0x90ff2b6b6a2eb3c68d883bc276f6736b1262fa50
        # Split the string by space to separate the token name and the URL
        _, _, _, chainId, pair_address = text.split('/')
        api_info = requests.get(f"https://api.dexscreener.com/latest/dex/pairs/{chainId}/{pair_address}")
        name = api_info.json()['pairs'][0]['baseToken']['name']

        if not name:
            raise Exception("Failed to obtain name from pair address.")

        # Firestore logic
        current_time = datetime.now(pytz.UTC)
        doc_ref = db.collection("momentum").document(str(current_time))
        doc_ref.set({'name': name, 'chainId': chainId, 'pairAddress': pair_address, 'addedTime': current_time})
        send_message(chat_id, f"Pair {pair_address} with name {name} added successfully at {current_time}.")

    except ValueError:
        send_message(chat_id, "Invalid format.")
        # Return 200 OK to stop retries
        return jsonify({"error": "Invalid format"}), 200

    except Exception as e:
        send_message(chat_id, f"Failed to add data: {str(e)}")
        # Return 200 OK to stop retries
        return jsonify({"error": f"Internal error: {str(e)}"}), 200

    return jsonify({"message": "Processed successfully"}), 200