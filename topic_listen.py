#!/usr/bin/env python3

import threading

from kombu import Connection, Exchange, Queue

from celeryconfig import broker_url


def simple_listen():
    exchange = Exchange("example_topic_exchange", "topic")
    queue = Queue("", exchange=exchange, routing_key="*.*.example")
    with Connection(broker_url) as conn:

        def print_msg(body, message):
            print(f"Message on bus: {body}")
            message.ack()

        with conn.Consumer(queue, callbacks=[print_msg]):
            print("Waiting for message to appear on the bus:")
            conn.drain_events()


if __name__ == "__main__":
    print("Creating Threads")
    for _ in range(10):
        _t = threading.Thread(target=simple_listen)
        _t.start()
