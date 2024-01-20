""" Send messages to rabbitmq """

import json

import pika

def connect():
    """ Connect to rabbitmq """
    connection =  pika.BlockingConnection(
        pika.ConnectionParameters(
            host='localhost',
            port=5672,
            virtual_host='/',
            credentials=pika.PlainCredentials('guest', 'guest')
        )
    )
    return connection

def send(profile):
    """ Sends a message to rabbitmq """

    connection = connect()
    channel = connection.channel()
    channel.queue_declare(queue="switchbox")

    channel.basic_publish(
        exchange='',
        routing_key='switchbox',
        body=json.dumps(profile))

    connection.close()
