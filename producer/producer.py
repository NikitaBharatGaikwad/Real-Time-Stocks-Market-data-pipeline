import time
import json
import requests
from kafka import KafkaProducer

# ------------------------------
# Finnhub API key
# ------------------------------
API_KEY = "d4ber09r01qnomk4i43gd4ber09r01qnomk4i440"

# Base URL for stock quotes
BASE_URL = "https://finnhub.io/api/v1/quote"

# List of stock symbols
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL"]

# ------------------------------
# Kafka producer setup
# ------------------------------
# Using Docker-exposed port 29092
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",  # ✅ Docker Kafka port
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ------------------------------
# Function to fetch stock data
# ------------------------------
def fetch(symbol):
    """
    Fetch stock data for a symbol from Finnhub API
    """
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()  # raise error if request fails
    data = r.json()
    data["symbol"] = symbol
    data["fetched_at"] = int(time.time())
    return data

# ------------------------------
# Main loop to produce messages
# ------------------------------
print("Kafka Producer Started...")

while True:
    for s in SYMBOLS:
        try:
            data = fetch(s)
            print("Producing:", data)
            producer.send("stock-quotes", value=data)
        except Exception as e:
            print("Error fetching or producing data:", e)

    producer.flush()  # send all messages
    time.sleep(5)  # wait 5 seconds before next batch
