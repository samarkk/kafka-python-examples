# WC - With Callback
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from sys import argv
from uuid import uuid4

bserver = argv[1]
topic = argv[2]
schema_registry_host = argv[3]


class Customer(object):
    def __init__(self,first_name=None,last_name=None,age=None,height=None,
                 weight=None,automated_email=None):
        self.first_name = first_name
        self.last_name = last_name
        self.age = age
        self.height = height
        self.weight = weight
        self.automated_email = automated_email
        self.id = uuid4()

    def to_dict(self):
        return {
            "first_name": self.first_name,
            "last_name": self.last_name,
            "age": self.age,
            "height": self.height,
            "weight": self.weight,
            "automated_email": self.automated_email
        }


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
record_schema = avro.loads(avro_v1_schema_str)

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


def delivery_report(err, msg, obj):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message {} delivery failed for Customer {} with error {}'.format(
            obj.id, obj.first_name + obj.last_name, err))
    else:
        print('Message {} successfully prodcued to {} [{}] at offset {}'.format(
            obj.id, msg.topic(), msg.partition(), msg.offset()))


producer_config = {
    'bootstrap.servers': bserver,
    'on_delivery': delivery_report,
    'schema.registry.url': schema_registry_host
}

customer = Customer('Raman', 'Shastri', 34, 174.9, 68.4)

avro_producer = AvroProducer(producer_config,
                             default_value_schema=record_schema)

avro_producer.produce(topic=topic,value=customer.to_dict(),
                      callback=lambda err, msg, obj=customer: delivery_report(err,msg,obj))

avro_producer.flush()
