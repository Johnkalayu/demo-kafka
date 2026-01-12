from confluent_kafka import Consumer
import json


consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-tracker',
    'auto.offset.reset': 'earliest'
}


consumer = Consumer(consumer_config)
consumer.subscribe(['orders'])

print('consume is running from me stope him .....')

while True;
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print('error:', msg.error())

    value = msg.value().decode('utf-8') 
    order = json.loads(value)
    print(f'order complite: {order['quantity']} x {order['item']} from {order['user']}')