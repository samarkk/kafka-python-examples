import os

from confluent_kafka import Consumer
from sys import argv
from confluent_kafka import  KafkaException
from confluent_kafka import  TopicPartition

# bserver = argv[1]
# topic = argv[2]
# group = argv[3]

bserver = os.environ['bserver']
topic = 'nsefo-topic'
group = 'cg1'

conf = {
    'bootstrap.servers': bserver,
    'group.id': group
}

consumer = Consumer(conf)
tpart = TopicPartition(topic, 0, 40000)
consumer.assign([tpart])
consumer.seek(tpart)

while True:
    try:
        msg = consumer.poll(timeout=10.0)
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


