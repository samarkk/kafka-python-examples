from confluent_kafka import avro
from sys import argv
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import KafkaException

bserver = argv[1]
topic = argv[2]
schema_registry_host = argv[3]

avro_v1_schema_str = '''
{
  "type": "record",
  "namespace": "com.example",
  "name": "Customer",
  "version": "1",
  "fields": [
    { "name": "first_name", "type": "string", "doc": "First Name of Customer" },
    { "name": "last_name", "type": "string", "doc": "Last Name of Customer" },
    { "name": "age", "type": "int", "doc": "Age at the time of registration" },
    { "name": "height", "type": "float", "doc": "Height at the time of registration in cm" },
    { "name": "weight", "type": "float", "doc": "Weight at the time of registration in kg" },
    { "name": "automated_email", "type": "boolean",
     "default": true, "doc": "Field indicating if the user is enrolled in marketing emails" }
  ]
}
'''
avro_v2_schema_str = '''
{
  "type": "record",
  "namespace": "com.example",
  "name": "Customer",
  "version": "2",
  "fields": [
    { "name": "first_name", "type": "string", "doc": "First Name of Customer" },
    { "name": "last_name", "type": "string", "doc": "Last Name of Customer" },
    { "name": "age", "type": "int", "doc": "Age at the time of registration" },
    { "name": "height", "type": "float", "doc": "Height at the time of registration in cm" },
    { "name": "weight", "type": "float", "doc": "Weight at the time of registration in kg" },
    { "name": "phone_number", "type": ["null", "string"], "default": null, "doc": "optional phone number"},
    { "name": "email", "type": "string", "default": "missing@example.com", "doc": "email address"}
  ]
}
'''
avro_key_schema_str = '''{
    "namespace": "com.example",
    "name": "key",
    "type": "record",
    "fields": [
        {
            "name": "key",
            "type": "string"
        }
    ]
}'''


def create_consumer(conf, schemastr):
    reader_schema = avro.loads(schemastr)
    consumer = AvroConsumer(conf, reader_value_schema=reader_schema)
    return consumer


conf = {'bootstrap.servers': bserver,
        'schema.registry.url': schema_registry_host,
        'group.id': 'delgr-2',
        'auto.offset.reset': 'earliest'
        }
# use v1 schema string - forward compatible - reads values produced with v2 schema
# use v2 schema string - backward compatible - reads values produced with v1 schema
consumer = create_consumer(conf, avro_v2_schema_str)

consumer.subscribe([topic])

while True:
    try:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            key = msg.key()
            record = msg.value()
            print('key:' + str(key) + ',value: ' + str(record) +
                  ' and type of record: ' + str(type(record)))
            print("first_name: {}, last_name {}".format(
                record['first_name'], record['last_name']))

    except SerializerError as e:
        print('Message deserializationn failed {}'.format(e))
        continue

    except KeyboardInterrupt:
        break

print('shutting down customer')
consumer.close()
