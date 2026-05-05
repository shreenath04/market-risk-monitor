# Real-Time Market Risk Monitor

A production-style streaming data pipeline that simulates live stock price feeds, calculates portfolio risk in real time, and persists results to a partitioned data lake — built with Apache Kafka, Python, and PyArrow.

> Built to demonstrate end-to-end data engineering skills: streaming ingestion, real-time computation, and scalable storage — the same stack used at major financial institutions.

---

## What It Does

This system continuously streams simulated price data for a 5-stock portfolio (AAPL, MSFT, GOOGL, AMZN, TSLA), calculates Value at Risk (VaR) on every tick, fires alerts when portfolio risk exceeds a threshold, and writes all data to a partitioned Parquet data lake.

**Key capabilities:**
- Real-time price streaming via Apache Kafka on Confluent Cloud
- Geometric Brownian Motion (GBM) price simulation — the same model used in Black-Scholes options pricing
- Rolling 50-tick Value at Risk (VaR) calculation at 95% confidence
- Portfolio-level risk aggregation across 5 tickers
- Threshold-based alerting when portfolio VaR exceeds -2%
- Batch-optimized Parquet writes partitioned by ticker — production data lake structure

---

## Architecture

```
GBM Simulator
     │
     ▼
main_producer.py  ──►  Confluent Cloud Kafka  ──►  main_consumer.py
                        (5 topics, 1 per ticker)          │
                                                          ├──► PortfolioVaR
                                                          │    (rolling VaR + alerts)
                                                          │
                                                          └──► ParquetSink
                                                               (batched writes)
                                                                    │
                                                                    ▼
                                                             data/ticker=AAPL/
                                                             data/ticker=MSFT/
                                                             data/ticker=GOOGL/
                                                             ...
```

Two independent processes communicate exclusively through Kafka — no direct coupling between producer and consumer.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message Broker | Apache Kafka (Confluent Cloud) |
| Price Simulation | Geometric Brownian Motion (NumPy) |
| Stream Processing | Python, confluent-kafka |
| Risk Calculation | Historical Simulation VaR |
| Storage Format | Apache Parquet (PyArrow) |
| Data Lake Structure | Hive-style partitioning by ticker |
| Config Management | python-dotenv |

---

## The Finance Behind It

**Geometric Brownian Motion** models stock price evolution as:

```
S(t+1) = S(t) * exp((μ - σ²/2) * dt + σ * √dt * Z)
```

Where μ is drift (10% annual), σ is volatility (20% annual), dt is one trading day (1/252), and Z is a standard normal random variable. This is the foundational model behind Black-Scholes options pricing.

**Value at Risk (VaR)** answers: *"What is the maximum I can lose at a 95% confidence level?"*

Using Historical Simulation — the rolling window of the last 50 price returns is collected, sorted, and the 5th percentile is taken as the VaR. Portfolio VaR is aggregated across all 5 tickers. A breach below -2% fires a real-time alert.

---

## Project Structure

```
kafka-project/
├── simulator.py          # GBM price simulation
├── producer.py           # Single-message test producer
├── consumer.py           # Single-topic test consumer
├── var_calculator.py     # PriceWindow + PortfolioVaR classes
├── parquet_sink.py       # Batched Parquet writer
├── main_producer.py      # Full streaming producer loop
├── main_consumer.py      # Full streaming consumer + risk engine
├── data/                 # Generated Parquet files
│   ├── ticker=AAPL/
│   ├── ticker=MSFT/
│   ├── ticker=GOOGL/
│   ├── ticker=AMZN/
│   └── ticker=TSLA/
├── .env                  # Credentials (use your own)
└── .gitignore
```

---

## Getting Started

### Prerequisites

- Python 3.10+
- A free [Confluent Cloud](https://confluent.io/get-started) account

### 1. Clone the repo

```bash
git clone https://github.com/yourusername/kafka-project.git
cd kafka-project
```

### 2. Create and activate virtual environment

```bash
python3 -m venv kafka-project
source kafka-project/bin/activate
```

### 3. Install dependencies

```bash
pip install confluent-kafka python-dotenv pyarrow numpy
```

### 4. Set up Confluent Cloud

1. Create a free Basic cluster on Confluent Cloud
2. Create 5 topics: `prices.AAPL`, `prices.MSFT`, `prices.GOOGL`, `prices.AMZN`, `prices.TSLA`
3. Generate an API key with Kafka cluster scope

### 5. Configure credentials

Create a `.env` file in the project root:

```
CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
CONFLUENT_API_KEY=your_api_key
CONFLUENT_API_SECRET=your_api_secret
```

### 6. Run the pipeline

**Terminal 1 — start the producer:**
```bash
python3 main_producer.py
```

**Terminal 2 — start the consumer:**
```bash
python3 main_consumer.py
```

You will see real-time delivery confirmations in Terminal 1 and VaR alerts + Parquet flush confirmations in Terminal 2.

---

## Sample Output

**Producer:**
```
Delivered to prices.AAPL[0] @ offset 42
Delivered to prices.MSFT[0] @ offset 38
Delivered to prices.TSLA[0] @ offset 41
```

**Consumer:**
```
ALERT: Portfolio VaR -0.0287 exceeds threshold!!
ALERT: Portfolio VaR -0.0352 exceeds threshold!!
Written 100 records to Parquet
```

**Parquet schema:**
```
price      float64
timestamp  string (ISO 8601 UTC)
var        float64
```

---

## Design Decisions

**Why GBM instead of a real data API?** Free-tier market data APIs impose rate limits and 15-minute delays. GBM produces unlimited, continuous, statistically realistic price streams — allowing the Kafka pipeline to run indefinitely without hitting API constraints. In production, this simulator would be replaced by a real-time market data feed.

**Why batch Parquet writes instead of row-by-row?** Parquet is optimized for columnar bulk I/O. Writing one row per message would generate thousands of tiny files and destroy read performance. Batching at 100 records balances latency with write efficiency — the same pattern used in production data lakes.

**Why partition by ticker?** Hive-style partitioning (`ticker=AAPL/`) allows query engines like AWS Athena, Spark, and Snowflake to skip irrelevant partitions entirely during reads. For a single-ticker query across millions of records, this can reduce I/O by orders of magnitude.

**Why separate producer and consumer processes?** This mirrors production architecture — a price feed service and a risk engine are independent systems. Kafka decouples them so either can scale, fail, or restart independently without affecting the other.

---

## Future Improvements

- Seed rolling windows with historical OHLCV data for immediate VaR accuracy on startup
- Add Avro serialization with Confluent Schema Registry for schema enforcement
- Replace GBM simulator with real-time market data feed (Alpaca, Polygon.io)
- Deploy producer and consumer as containerized services on Kubernetes
- Add Kafka consumer lag monitoring and alerting
- Extend to multi-factor risk models (Delta-Normal VaR)
