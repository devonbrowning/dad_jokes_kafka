from confluent_kafka import Consumer
import typing as typ

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':62680'


def run(group_id: str, process_message: typ.Callable[[str], None]):
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'group.id': group_id}
    consumer = Consumer(conf)
    try:
        consumer.subscribe([KAFKA_TOPIC])
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f'Consumer {group_id} encountered error receiving a message from {msg.topic()} topic: {msg.error()}')
            else:
                process_message(msg.value().decode())
    except Exception as e:
        print(f'Uncaught exception in {group_id} consumer: {e}')
    finally:
        consumer.close()