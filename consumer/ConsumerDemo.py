from confluent_kafka import Consumer
from sys import argv
from confluent_kafka import  KafkaException
import os

bserver = argv[1]
topic = argv[2]
group = argv[3]


conf = {
    'bootstrap.servers': bserver,
    'group.id': group,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([topic, 'nsefo-topic'])

while True:
    try:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print('no records received. continuing')
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            key = msg.key()
            value = msg.value()
            print('key {}, value- partition: {}, offset: {}, message: {}'.format(
                key, msg.partition(), msg.offset(), msg.value()
            ))
    except KeyboardInterrupt:
        break

print('shutting down customer')
consumer.close()

