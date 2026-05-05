from dotenv import load_dotenv
import os
from confluent_kafka import Producer
from simulator import simulate_price
import json
from datetime import datetime, timezone
import time

load_dotenv()

CONFLUENT_BOOTSTRAP_SERVERS = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
CONFLUENT_API_KEY = os.getenv('CONFLUENT_API_KEY')
CONFLUENT_API_SECRET = os.getenv('CONFLUENT_API_SECRET')

config = {
    'bootstrap.servers': CONFLUENT_BOOTSTRAP_SERVERS,
    'sasl.username': CONFLUENT_API_KEY,
    'sasl.password': CONFLUENT_API_SECRET,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN'
}

def delivery_callback(err, msg):
    if err:
        print(f'Error:{err}')
    else:
        print(f'Delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}')

producer = Producer(config)

prices = {
    'AAPL': 180.0,
    'MSFT': 380.0,
    'GOOGL': 140.0,
    'AMZN': 175.0,
    'TSLA': 250.0
}

try:
    while True:
        for ticker, price in prices.items():
            new_price = simulate_price(price)
            prices[ticker] = new_price
            message = {
                'ticker': ticker,
                'price': new_price,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }

            producer.produce(
                topic = f'prices.{ticker}',
                key = ticker,
                value = json.dumps(message),
                on_delivery = delivery_callback
            )
        producer.poll(0)
        time.sleep(1)
except KeyboardInterrupt:
    print("Shutting down")
finally:
    producer.flush()
