from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('stock-ticks', bootstrap_servers='localhost:9092', value_deserializer=lambda x: json.loads(x.decode('utf-8')), key_deserializer=lambda x: x.decode('utf-8'))

for message in consumer:
    print('Topic: ', message.topic , '\n',
          'Partition: ', message.partition , '\n',
          'offset: ', message.offset , '\n',
          'key: ', message.key , '\n',
          'value: ', message.value)