from confluent_kafka import Producer
from sys import argv


bserver = argv[1]
topic = argv[2]
file = argv[3]

producer_config = {'bootstrap.servers': bserver}
producer = Producer(producer_config)

folines = open(file).readlines()[1:]
for line in folines:
    print(line)
    key = line.split(',')
    producer.produce(topic, line)

producer.flush()