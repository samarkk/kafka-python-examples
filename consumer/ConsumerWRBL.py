import confluent_kafka
from confluent_kafka import Consumer
import os
from confluent_kafka import TopicPartition
from confluent_kafka import  KafkaException
import time
from sys import argv

bserver = argv[1]
topic = argv[2]
group = argv[3]

# bserver = os.environ['bserver']
# topic = 'first-topic'
# group = 'new-group'

conf = {
    'bootstrap.servers': bserver,
    'group.id': group,
    'enable.auto.commit': False
}

consumer = Consumer(conf)

# create a dictionary for the partitions of the topic partitions
def get_partitions_dict(consumer, topic):
    metadata = consumer.list_topics(topic, timeout=10)
    partitions = [TopicPartition(topic, partition)
                  for partition in metadata.topics[topic].partitions]
    partition_commits = consumer.committed(partitions)
    return {cp.partition:cp.offset for cp in partition_commits}

partitions_dict = get_partitions_dict(consumer, topic)
adict = {}
def revoke_cb(consumer, partitions):
    print('revoke callback in action')
    print('Revoked Partitions:')
    [print('Topic: {}, Partition: {}, Offset: {}'.format(tp.topic, tp.partition, tp.offset))
     for tp in consumer.committed(partitions)]
    partitions_and_offsets = [TopicPartition(topic, k, v) for k, v in adict.items()]
    print('About to make the following commits:')
    [print('Topic: {}, Partition: {}, Offset: {}'.format(tp.topic, tp.partition, tp.offset))
     for tp in partitions_and_offsets]
    consumer.commit(asynchronous=True)

def assign_cb(consumer, partitions):
    print('Partitions assigned:')
    [print('Topic: {}, Partition: {}, Offset: {}'.format(tp.topic, tp.partition, tp.offset))
     for tp in consumer.committed(partitions)]



consumer.subscribe([topic], on_revoke= revoke_cb,on_assign=assign_cb)

while True:
    try:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            # print('no records received. continuing')
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            key = msg.key()
            value = msg.value()
            print('key {}, value- partition: {}, offset: {}, message: {}'.format(
                key, msg.partition(), msg.offset(), msg.value()
            ))
            adict[msg.partition()] = msg.offset()

    except KeyboardInterrupt:
        break

print('shutting down customer')
consumer.close()

