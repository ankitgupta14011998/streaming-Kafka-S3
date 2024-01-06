import pandas as pd
from kafka import KafkaProducer
from json import dumps
import json
from time import sleep


producer = KafkaProducer(bootstrap_servers=['65.1.85.11:9092'],
                        value_serializer = lambda x: dumps(x).encode('utf-8'))

df=pd.read_csv('stock_market_data.csv')

while True:
    stock_data = df.sample(1).to_dict(orient='records')[0]
    producer.send('demo-topic', value=stock_data)