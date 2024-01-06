import pandas as pd
from json import dumps,loads
import json
from kafka import KafkaConsumer
from s3fs import S3FileSystem
consumer = KafkaConsumer('demo-topic',
                        bootstrap_servers = ['65.1.85.11:9092'],
                        value_deserializer = lambda x : dumps(x).decode('utf-8'))

s3=S3FileSystem()

for count,i in enumerate(consumer):
    with s3.open('s3://stock-market-project-ankit/stock_market_{}.json'.format(count)):
        json.dump(i.value, file)