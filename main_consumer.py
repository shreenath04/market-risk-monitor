from dotenv import load_dotenv
import os
from confluent_kafka import Consumer
from var_calculator import PortfolioVaR
from parquet_sink import ParquetSink
import json

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

portfolio = PortfolioVaR()
sink = ParquetSink(batch_size=100)

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'Error: {msg.error()}')
            continue
        
        data = json.loads(msg.value().decode('utf-8'))
        ticker = data['ticker']
        price = data['price']
        timestamp = data['timestamp']

        portfolio.update(ticker, price)
        var = portfolio.ticker_vars.get(ticker)

        sink.add_record(ticker, price, timestamp, var if var is not None else 0.0)
        portfolio.check_threshold()

except KeyboardInterrupt:
    print("Shutting Down")
finally:
    sink.flush()
    consumer.close() 