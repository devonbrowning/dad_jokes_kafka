import time
import requests
from confluent_kafka import Producer
import socket


KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':62680'
DELAY_TIME = 5
URL = 'https://icanhazdadjoke.com/'


def delivery_callback(err, msg):
    if err:
        print(f'Message failed delivery: {err}')
    else:
        print(f'Message "{msg.value().decode()}" delivered to {msg.topic()}')


def main():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': socket.gethostname()}
    producer = Producer(conf)
    try:
        while True:
            response = requests.get(URL, headers={'Accept': 'application/json'})
            joke = response.json()['joke']
            producer.produce(KAFKA_TOPIC, value=joke, callback=delivery_callback)
            producer.flush()
            time.sleep(DELAY_TIME)
    except Exception as e:
        print(f'Uncaught error in producer: {e}')
    finally:
        producer.flush()


if __name__ == '__main__':
    main()
