""" Send messages to rabbitmq (producer)"""

import json

import pika

QUEUE = 'switchbox'
EXCHANGE = 'switchbox'
EXCHANGE_TYPE = 'direct'

def connect():
    """ Connect to rabbitmq """
    credentials = pika.PlainCredentials('guest', 'guest')

    parameters = pika.ConnectionParameters(
            host='localhost',
            port=5672,
            virtual_host='/',
            credentials=credentials
    )

    return pika.BlockingConnection(parameters)

def send(profile):
    """ Sends a message to rabbitmq """

    connection = connect()

    channel = connection.channel()

    channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type=EXCHANGE_TYPE,
            passive=False,
            durable=True,
            auto_delete=False)

    channel.queue_declare(queue=QUEUE)

    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key='switchbox',
        body=json.dumps(profile))

    connection.close()
