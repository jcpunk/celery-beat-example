#!/usr/bin/env python3

import datetime

from kombu import Connection, Exchange, Queue
from kombu.pools import producers

from celeryconfig import broker_url

exchange = Exchange("example_topic_exchange", "topic")
queue = Queue("", exchange=exchange, auto_delete=True)
_con = Connection(broker_url)


def send_messages():
    with producers[_con].acquire(block=True) as producer:

        print("Got connection: {0!r}".format(producer.connection.as_uri()))
        print("Sending message to the bus:")

        producer.publish(
            f"message for example {datetime.datetime.now()}",
            routing_key="example",
            exchange=exchange,
            declare=[exchange, queue],
        )

        producer.publish(
            f"message for example.text {datetime.datetime.now()}",
            routing_key="example.text",
            exchange=exchange,
            declare=[exchange, queue],
        )

        producer.publish(
            f"message for test.code.example {datetime.datetime.now()}",
            routing_key="test.code.example",
            exchange=exchange,
            declare=[exchange, queue],
        )

        producer.publish(
            f"message for test.code.example.thing {datetime.datetime.now()}",
            routing_key="test.code.example.thing",
            exchange=exchange,
            declare=[exchange, queue],
        )


if __name__ == "__main__":
    send_messages()
