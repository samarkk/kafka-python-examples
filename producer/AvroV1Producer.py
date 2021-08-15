from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from sys import argv

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

avro_v1_key_schema_str = '''{
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


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


producer_config = {
    'bootstrap.servers': bserver,
    'on_delivery': delivery_report,
    'schema.registry.url': schema_registry_host
}
key_schema = avro.loads(avro_v1_key_schema_str)
value_schema = avro.loads(avro_v1_schema_str)
avro_producer = AvroProducer(producer_config,
                             default_value_schema=value_schema, default_key_schema=key_schema)

avro_producer.produce(topic=topic,key={"key": "Raman Shastri"},
                      value={"first_name": 'Raman', 'last_name': 'Shastri', 'age': 34,
                             'height': 174.9, 'weight': 68.4})

avro_producer.flush()
