#!/usr/bin/env python3

from kombu import Connection, Exchange
from kombu.common import Broadcast

from celeryconfig import broker_url

exchange = Exchange("example_fanout_exchange", "fanout")
queue = Broadcast("", exchange=exchange)

with Connection(broker_url) as conn:
    producer = conn.Producer(serializer="json")
    producer.publish("message", exchange=exchange, declare=[queue])
