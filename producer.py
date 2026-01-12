from confluent_kafka import Producer
import uuid
import json 

producer_config = {

    'bootstarp.server',
    'localhost:9092'
}


producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f' deliver falied: {err}')
    else:
        print(f'delivered {msg.value().decode('utf-8')}')
        print(dir(msg))
            

order = {
    'order_id': str(uuid.uuid4()),
    'user': "john",
    'item': 'AK47',
    'quantity': 50
}


value = json.dumps(order).encode('utf-8')


producer.produce(
    topic='oriders',
    value=value,
    callback=delivery_report

)

producer.flush()