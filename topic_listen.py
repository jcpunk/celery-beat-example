#!/usr/bin/env python3

import socket
import threading
import time

from kombu import Connection, Exchange, Queue
from kombu.pools import connections as listen_connections

from celeryconfig import broker_url

LATER = int(time.time()) + 10


def simple_listen():
    exchange = Exchange("example_topic_exchange", "topic")
    # subtopics are not automatically subscribed
    queue = Queue("", exchange=exchange, routing_key="*.*.example", auto_delete=True)
    _cl = Connection(broker_url)
    with listen_connections[_cl].acquire(block=True) as conn:

        print("Got connection: {0!r}".format(conn.as_uri()))

        def print_msg(body, message):
            print(f"Message on bus: {body}")
            message.ack()

        with conn.Consumer(queue, callbacks=[print_msg]):
            print("Waiting for message to appear on the bus:")
            while int(time.time()) < LATER:
                try:
                    conn.drain_events(timeout=2)
                except TimeoutError:
                    # no events found in time
                    pass
                except socket.timeout:
                    # no events found in time
                    pass


if __name__ == "__main__":
    print("Creating Threads")
    threads = []
    for _ in range(10):
        _t = threading.Thread(target=simple_listen)
        _t.start()
        threads.append(_t)

    for _t in threads:
        _t.join()

    print("\nThreads reached their time limit I set in the code\n")
