from confluent_kafka import  avro
from confluent_kafka.avro import AvroProducer
from sys import argv

bserver = argv[1]
topic = argv[2]
schema_registry_host= argv[3]
file_loc = argv[4]

nsefo_key_schema_str = '''{
"namespace": "com.example",
"name": "key",
"type": "record",
"fields": [
    {
        "name": "fokey",
        "type": "string"
    }
]
}'''
nsefo_schema_str = '''{
     "type": "record",
     "namespace": "com.example",
     "name": "Nseforec",
     "version": "1",
     "fields": [
       { "name": "instrument", "type": "string", "doc": "Future or option broadly" },
       { "name": "symbol", "type": "string", "doc": "Name of the stock traded" },
       { "name": "expiry_dt", "type": "string", "doc": "Expiry date of contract" },
       { "name": "strike_pr", "type": "float", "doc": "Strike price for options, 0 for Futures" },
       { "name": "option_typ", "type": "string", "doc": "CE, PE, XX" },
       { "name": "openpr", "type": "float", "doc": "opening price" },
       { "name": "highpr", "type": "float", "doc": "highest price in the day" },
       { "name": "lowpr", "type": "float", "doc": "lowest price in the day" },
       { "name": "closepr", "type": "float", "doc": "closing price" },
       { "name": "settlepr", "type": "float", "doc": "settlement price" },
       { "name": "contracts", "type": "int", "doc": "number of contracts" },
       { "name": "valinlakh", "type": "float", "doc": "total value traded in lakhs" },
       { "name": "openint", "type":"int", "doc": "total positions open" },
       { "name": "chginoi", "type": "int", "doc": "change in open interest in the day" },
       { "name": "tmstamp", "type": "string", "doc": "trade date in DD-MMMM-YYYY format"}
     ]
}'''

def create_key(foline):
    splits = foline.split(',')
    return {"fokey" : ''.join([splits[1],splits[2],splits[14],splits[0],splits[4],splits[3]]) }

def format_date(dt):
    mname = dt[3:6].upper()
    calendar = {"JAN": "01", "FEB": "02", "MAR":"03", "APR": "04",
                "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
                "SEP": "09", "OCT": "10","NOV": "11", "DEC": "12"}
    dt_with_mno = dt.upper().replace(mname, calendar[mname])
    ldt = len(dt_with_mno)
    return dt_with_mno[ldt-4:ldt] + dt_with_mno[2:6] + dt_with_mno[:2]

def create_value(foline):
    splits = foline.split(",")
    value = {
        "instrument": splits[0],
        "symbol": splits[1],
        "expiry_dt": format_date(splits[2]),
        "strike_pr": float(splits[3]),
        "option_typ": splits[4],
        "openpr": float(splits[5]),
        "highpr": float(splits[6]),
        "lowpr": float(splits[7]),
        "closepr": float(splits[8]),
        "settlepr": float(splits[9]),
        "contracts": int(splits[10]),
        "valinlakh": float(splits[11]),
        "openint": int(splits[12]),
        "chginoi": int(splits[13]),
        "tmstamp": format_date(splits[14])
    }
    return value

key_schema = avro.loads(nsefo_key_schema_str)
value_schema = avro.loads(nsefo_schema_str)

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
    'schema.registry.url': schema_registry_host,
}

avro_producer = AvroProducer(producer_config, default_value_schema=value_schema, default_key_schema=key_schema)

nsefo_lines = open(file_loc).readlines()[1:]
for line in nsefo_lines:
    avro_producer.produce(topic=topic,key=create_key(line), value=create_value(line))

avro_producer.flush()
