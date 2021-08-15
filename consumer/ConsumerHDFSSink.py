from confluent_kafka import Consumer
from hdfs import InsecureClient
from sys import argv
from confluent_kafka import KafkaException
from hdfs.util import HdfsError

bserver = argv[1]
topic = argv[2]
group = argv[3]
hdfs_address = argv[4]
hdfs_path = argv[5]

conf = {
    'bootstrap.servers': bserver,
    'group.id': group,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([topic])

hdfs_client = InsecureClient(hdfs_address)

# check if file exists and if it does not, create it
try:
    file_exists = hdfs_client.status(hdfs_path)
except HdfsError as hdfs_err:
    print(hdfs_err)
    with hdfs_client.write(hdfs_path) as writer:
        writer.write('')

# while True:
#     try:
#         msg = consumer.poll(timeout=10.0)
#         if msg is None:
#             print('no records received. continuing')
#             continue
#         if msg.error():
#             raise KafkaException(msg.error())
#         else:
#             key = msg.key()
#             value = msg.value()
#             with hdfs_client.write(hdfs_path, append=True) as writer:
#                 writer.write(value)
#     except KeyboardInterrupt:
#         break
batch_no = 1
batch_records = 0
line = ''
total_records = 0
batch_size = 100

while True:
    try:
        msg = consumer.poll(timeout=10.0)
        if msg is None:
            print('no records received. continuing')
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # if we write line by line, will take forever
            # so we follow a batch approach
            # and cumulate the messages into a large string
            # and write that to hdfs
            key = msg.key()
            value = msg.value()
            line += str(value).replace('b', '').replace("'", '') + '\n'
            batch_records += 1
            total_records += 1
            if batch_records % batch_size == 0:
                print('sending {} records for batch no {} and total records written so far {}' \
                      .format(batch_records, batch_no, total_records))
                batch_no += 1
                with hdfs_client.write(hdfs_path, encoding='utf-8', append=True) as writer:
                    writer.write(line)
                    batch_records = 0
                    line = ''
    except KeyboardInterrupt:
        break

print('shutting down customer')
consumer.close()
