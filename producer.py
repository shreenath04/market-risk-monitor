from dotenv import load_dotenv
import os
from confluent_kafka import Producer

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

producer.produce(
    topic = 'prices.AAPL',
    key = 'AAPL',
    value = '{"price": 150.23, "timestamp": "..."}',
    on_delivery = delivery_callback
)
producer.flush()