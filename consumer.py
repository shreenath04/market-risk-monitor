from dotenv import load_dotenv
import os
from confluent_kafka import Consumer

load_dotenv()

CONFLUENT_BOOTSTRAP_SERVERS = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
CONFLUENT_API_KEY = os.getenv('CONFLUENT_API_KEY')
CONFLUENT_API_SECRET = os.getenv('CONFLUENT_API_SECRET')

config = {
    'bootstrap.servers': CONFLUENT_BOOTSTRAP_SERVERS,
    'sasl.username': CONFLUENT_API_KEY,
    'sasl.password': CONFLUENT_API_SECRET,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'group.id': 'var-calculator',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(config)
consumer.subscribe(['prices.AAPL', 'prices.MSFT', 'prices.GOOGL', 'prices.AMZN', 'prices.TSLA'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            print(f'Error: {msg.error()}')
            continue
        print(f'Received: {msg.topic()} - {msg.value().decode("utf-8")}')
except KeyboardInterrupt:
    print("Shutting Down")
finally:
    consumer.close() 