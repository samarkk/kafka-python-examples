from cassandra import cluster
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel
from confluent_kafka import Consumer
from sys import argv
from confluent_kafka import KafkaException
from confluent_kafka import TopicPartition
import os
import time
import datetime

# bserver = argv[1]
# topic = argv[2]
# group = argv[3]

bserver = os.environ['bserver']
topic = 'nsefo-topic'
group = 'cass-group'

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

from datetime import datetime

month_dict = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
              'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}


def convertStringToDateTime(x):
    dt = int(x[:2])
    mnth = month_dict[x[3:6]]
    year = int(x[7:11])
    return datetime(year, mnth, dt, 0, 0, 0)


convertStringToDateTime('31-OCT-2018')

month_str_dict = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06',
                  'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}


def tstoDate(x):
    mnth = month_str_dict[x[3:6]]
    return x[7:11] + '-' + mnth + '-' + x[:2]


print(tstoDate('31-OCT-2019'))


def generate_cass_insert_statement(foline):
    lp = str(foline).split(',')
    statement_to_execute = '''
insert into finks.fotable(
    symbol ,\
    expiry ,\
    trdate ,\
    instrument ,\
    option_typ ,\
    strike_pr,\
    chgoi,\
    contracts,\
    cpr,\
    oi,\
    trdval,\
    tstamp) VALUES \
    ('{}','{}','{}','{}','{}',{},{},{},{},{},{}, {})'''.format(
        lp[1], tstoDate(lp[2].upper()), tstoDate(lp[14].upper()),
        lp[0], lp[4], lp[3], lp[13], lp[10], lp[8], lp[12], lp[11], int(round(time.time()))*1000
    )
    return statement_to_execute

conf = {
    'bootstrap.servers': bserver,
    'group.id': group,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}


consumer = Consumer(conf)
consumer.subscribe([topic])

records_processed = 0
batch_number = 0
batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)

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
            value = str(msg.value(), encoding='utf-8')
            # print('key {}, value- partition: {}, offset: {}, message: {}'.format(
            #     key, msg.partition(), msg.offset(), msg.value()
            # ))
            cass_exec_statement = generate_cass_insert_statement(value)
            # print('statement', cass_exec_statement)
            batch.add(cass_exec_statement)
            records_processed += 1
            # print('record processed {} and batch length'.format(
            #     records_processed, len(batch)
            # ))
            if(records_processed % 100 == 0):
                # print('if records % entered')
                session.execute(batch)
                consumer.commit(asynchronous=False)
                batch.clear()
                batch_number += 1
                print("Batch no {} processed, total records processed: {}".format(
                    batch_number, records_processed
                ))
    except KeyboardInterrupt:
        break

print('shutting down customer')
consumer.close()
