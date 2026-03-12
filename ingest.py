import yfinance as yf
import pandas as pd
import json
from kafka import KafkaProducer
from pymongo import MongoClient
import copy


#Stocks to be monitored
tickers = ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA", "META", "ORCL", "TSM"]

#Fetching OHLCV of stocks mentioned above in one go
df = yf.download(tickers, period="1d", interval="1m")

#Converting Wide form to long form
df = df.stack(future_stack=True)

#converitng the index into a column
df.reset_index(inplace=True)

#making column names simpler
df.columns = [col.lower() for col in df.columns]
df.rename(columns={'ticker':'symbol', 'datetime': 'timestamp'}, inplace=True)

#adding the time the data was fetched at to each row
df['fetched_at'] = pd.Timestamp.utcnow()

#organising the columns
df = df.reindex(columns=['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'fetched_at'])

#rounding off prices to two decimals
df[['open','high','low','close']] = df[['open','high','low','close']].apply(lambda x:x.round(2))

#converting timestamp and fetched_at columns to string
df[['timestamp', 'fetched_at']] = df[['timestamp', 'fetched_at']].astype('str')

#converting dataframe to dictionary 
rows = df.to_dict('records')

#kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

#publishing each row to kafka topic
for row in rows:
    message_bytes = json.dumps(row).encode('utf-8')
    key_bytes = row['symbol'].encode('utf-8')
    try:
        producer.send('stock-ticks', key=key_bytes, value=message_bytes)
    except:
        print('Producer could not send')
        break

producer.flush()

#success-message
print(f"Published {len(rows)} messages to stock-ticks")

#connect to mongodb
connecturl='mongodb://localhost:27017/'

with MongoClient(connecturl) as conn:
    db = conn.stock_pipeline
    collection = db.stock_ticks_raw
    collection.insert_many(rows)



