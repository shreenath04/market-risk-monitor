import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

class ParquetSink:
    def __init__(self, batch_size=100):
        self.buffer = {'ticker': [], 'price': [], 'timestamp': [], 'var':[]}
        self.batch_size = batch_size
        self.schema = pa.schema([
            pa.field('ticker', pa.string()),
            pa.field('price', pa.float64()),
            pa.field('timestamp', pa.string()),
            pa.field('var', pa.float64())
        ])    
    
    def add_record(self, ticker, price, timestamp, var):
        self.buffer['ticker'].append(ticker)
        self.buffer['price'].append(price)
        self.buffer['timestamp'].append(timestamp)
        self.buffer['var'].append(var)
        if len(self.buffer['ticker']) >= self.batch_size:
            self.flush()
    
    def flush(self):
        if len(self.buffer['ticker']) == 0:
            return
        table = pa.Table.from_pydict(self.buffer, schema=self.schema)
        pq.write_to_dataset(table, root_path='data/', partition_cols=['ticker'])
        print(f'Written {len(self.buffer["ticker"])} records to Parquet')
        self.buffer = {'ticker': [], 'price': [], 'timestamp': [], 'var': []}
