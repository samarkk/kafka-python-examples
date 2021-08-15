# create kafka consumer
import confluent_kafka
from confluent_kafka import Consumer
import os
from confluent_kafka import TopicPartition
from confluent_kafka import  KafkaException

bserver = os.environ['bserver']
topic = 'first-topic'
group = 'new-group'

conf = {
    'bootstrap.servers': bserver,
    'group.id': group,
    'enable.auto.commit': False
}
consumer = Consumer(conf)

# function to return partitions and  offsets
def list_partitions_and_offsets(consumer, topic):
    metadata = consumer.list_topics(topic, timeout=10)
    partitions = [TopicPartition(topic, partition)
                  for partition in metadata.topics[topic].partitions]
    return consumer.committed(partitions, timeout = 10)
    # return partitions

# function to print offsets - check for lags too
def print_partitions_and_offsets(partitions):
    for partition in partitions:
        print('Topic {}, Partition {}, Offset: {}'.format(
            partition.topic, partition.partition, partition.offset
        ))

def print_lags(committed):
    print("%-50s  %9s  %9s" % (
        "{} [{}]".format('Topic', 'Partiton'), 'offset', 'lag'))
    for partition in committed:
        # Get the partitions low and high watermark offsets.
        (lo, hi) = consumer.get_watermark_offsets(partition, timeout=10, cached=False)

        if partition.offset == confluent_kafka.OFFSET_INVALID:
            offset = "-"
        else:
            offset = "%d" %  (partition.offset)

        if hi < 0:
            lag = "no hwmark"  # Unlikely
        elif partition.offset < 0:
            # No committed offset, show total message count as lag.
            # The actual message count may be lower due to compaction
            # and record deletions.
            lag = "%d" % (hi - lo)
        else:
            lag = "%d" % (hi - partition.offset)

        print("%-50s  %9s  %9s" % (
            "{} [{}]".format(partition.topic, partition.partition), offset, lag))

def get_offsets(metadata):
    # metadata = consumer.list_topics(topic, timeout=10)
    partitions = [TopicPartition(topic, partition) for partition in metadata.topics[topic].partitions]
    print(partitions)
    new_offsets = map(lambda tp: TopicPartition(tp.topic, tp.partition, 0), partitions)
    new_offsets_list = map(lambda tp: (tp.topic, tp.partition, tp.offset), new_offsets)
    print(list(new_offsets_list))
    return list(new_offsets_list)

# consumer.subscribe([topic])
partitions_and_offsets = list_partitions_and_offsets(consumer, topic)
print('Printing the partitions and offsets for the committed partitions')
print_partitions_and_offsets(partitions_and_offsets)
new_offsets = [tp for tp in map(lambda tp: TopicPartition(tp.topic, tp.partition, 0), partitions_and_offsets)]
print('Printing the new partition offsets that we want to commit')
print_partitions_and_offsets(new_offsets)
print('now commiting the new offsets')
# consumer.subscribe([topic])
consumer.commit(offsets=new_offsets, asynchronous=False)
# print_partitions_and_offsets(list_partitions_and_offsets(consumer, topic))
print('the lags after committing the new partitions')
print_lags(partitions_and_offsets)
# metadata = consumer.list_topics(topic, timeout=10)
# new_offsets = get_offsets(metadata)
# function to reset offsets

consumer.subscribe([topic])
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
    except KeyboardInterrupt:
        print('keyboard interrupt')
        break

print('now committing offsets')
consumer.commit(asynchronous=False)
print('shutting down customer')
consumer.close()
