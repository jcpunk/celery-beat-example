#!/usr/bin/env python3

import datetime

from kombu import Connection, Exchange, Queue

from celeryconfig import broker_url

exchange = Exchange("example_topic_exchange", "topic")
queue = Queue("", exchange=exchange, auto_delete=True)

with Connection(broker_url) as conn:
    producer = conn.Producer(serializer="json")

    producer.publish(
        f"message for example {datetime.datetime.now()}",
        routing_key="example",
        exchange=exchange,
        maybe_declare=[queue],
    )
    producer.publish(
        f"message for example.text {datetime.datetime.now()}",
        routing_key="example.text",
        exchange=exchange,
        maybe_declare=[queue],
    )
    producer.publish(
        f"message for test.code.example {datetime.datetime.now()}",
        routing_key="test.code.example",
        exchange=exchange,
        maybe_declare=[queue],
    )
    producer.publish(
        f"message for test.code.example.thing {datetime.datetime.now()}",
        routing_key="test.code.example.thing",
        exchange=exchange,
        maybe_declare=[queue],
    )
