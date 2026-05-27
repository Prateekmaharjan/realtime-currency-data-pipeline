import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
import os

# Load API key from .env file
load_dotenv()
API_KEY = os.getenv("EXCHANGE_RATE_API_KEY")

# Initialize Kafka producer with JSON serialization
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def fetch_exchange_rates():
    # Fetch live exchange rates based in USD from ExchangeRate API
    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"
    response = requests.get(url)
    return response.json()

def send_to_kafka(data):
    # Extract only the currencies relevant to operations
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    target_currencies = ["NPR", "EUR", "GBP", "JPY", "INR", "AUD"]

    for currency in target_currencies:
        rate = data["conversion_rates"].get(currency)
        if rate:
            message = {
                "base_currency": "USD",
                "target_currency": currency,
                "exchange_rate": rate,
                "timestamp": timestamp
            }
            producer.send('exchange-rates', value=message)
            print(f"Sent → {message}")

print("Producer running. Fetching exchange rates every 10 seconds. Ctrl+C to stop.\n")

while True:
    try:
        data = fetch_exchange_rates()
        send_to_kafka(data)
        producer.flush()
        time.sleep(10)

    except KeyboardInterrupt:
        print("\nShutting down producer.")
        producer.close()
        break

    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)
