#!/usr/bin/env python3

from kombu import Connection, Exchange, Queue

from celeryconfig import broker_url

exchange = Exchange("example_topic_exchange", "topic")
queue = Queue("", exchange=exchange)

with Connection(broker_url) as conn:
    producer = conn.Producer(serializer="json")

    producer.publish(
        "message for example",
        routing_key="example",
        exchange=exchange,
        maybe_declare=[queue],
    )
    producer.publish(
        "message for example.text",
        routing_key="example.text",
        exchange=exchange,
        maybe_declare=[queue],
    )
    producer.publish(
        "message for test.code.example",
        routing_key="test.code.example",
        exchange=exchange,
        maybe_declare=[queue],
    )
    producer.publish(
        "message for test.code.example.thing",
        routing_key="test.code.example.thing",
        exchange=exchange,
        maybe_declare=[queue],
    )
