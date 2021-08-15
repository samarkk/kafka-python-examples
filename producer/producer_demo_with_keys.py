from confluent_kafka import Producer

p = Producer({'bootstrap.servers': '172.22.85.85:9092'})


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message {} delievered to {} {}'.format(msg.key(),  msg.topic(), msg.partition()))


for x in range(20):
    message_to_send = 'message from the ide no: ' + str(x)
    p.produce('ide-topic-wkey', message_to_send,
              callback=delivery_report, key="id-" + str(x))

p.flush()
