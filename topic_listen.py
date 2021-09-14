#!/usr/bin/env python3

import threading
import time

from kombu import Connection, Exchange, Queue

from celeryconfig import broker_url


def simple_listen():
    exchange = Exchange("example_topic_exchange", "topic")
    # subtopics are not automatically subscribed
    queue = Queue("", exchange=exchange, routing_key="*.*.example", auto_delete=True)
    with Connection(broker_url) as conn:

        def print_msg(body, message):
            print(f"Message on bus: {body}")
            message.ack()

        with conn.Consumer(queue, callbacks=[print_msg]):
            print("Waiting for message to appear on the bus:")
            while True:
                conn.drain_events()
                time.sleep(2)


if __name__ == "__main__":
    print("Creating Threads")
    threads = []
    for _ in range(10):
        _t = threading.Thread(target=simple_listen)
        _t.start()
        threads.append(_t)

    for _t in threads:
        _t.join()
