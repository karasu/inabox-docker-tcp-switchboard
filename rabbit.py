""" Send messages to rabbitmq (producer)"""

import json

import pika

QUEUE = 'switchboard'

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

    channel.queue_declare(queue=QUEUE)

    channel.basic_publish(
        exchange='',
        routing_key=QUEUE,
        body=json.dumps(profile))

    connection.close()
