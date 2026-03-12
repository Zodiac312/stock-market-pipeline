# Stock Market Analytics Pipeline

A real-time data pipeline that ingests stock market data using yfinance, streams it through Apache Kafka, and stores raw data in MongoDB.

## Stack
- Python, yfinance, Pandas
- Apache Kafka (KRaft mode)
- MongoDB
- Docker

## How to run
1. Start infrastructure: docker-compose up -d
2. Run ingestion: python ingest.py
3. Run consumer: python kafka-consumer.py
