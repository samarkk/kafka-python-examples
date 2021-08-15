from confluent_kafka import Producer
import os

bserver = os.environ['bserver']
p = Producer({'bootstrap.servers': bserver})

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delievered to {} {}'.format(msg.topic(), msg.partition()))

p.produce('ide-topic', 'message from the ide', callback=delivery_report)

p.flush()