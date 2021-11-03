#!/usr/bin/env python3

import datetime
import socket
import threading
import time

from kombu import Connection, Exchange, Queue, pools
from kombu.pools import connections
from kombu.pools import producers

from celeryconfig import broker_url

# default limit is 10, which is a bit low
pools.set_limit(100)

exchange = Exchange("example_topic_exchange", "topic")
queue = Queue("", exchange=exchange, auto_delete=True)
_con = Connection(broker_url)

LATER = int(time.time()) + 10


def simple_listen():
    exchange = Exchange("example_topic_exchange", "topic")
    # subtopics are not automatically subscribed
    queue = Queue("", exchange=exchange, routing_key="*.*.example", auto_delete=True)
    _cl = Connection(broker_url)
    with connections[_cl].acquire(block=True) as conn:

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
    print("Creating Threads")
    threads = []
    for _ in range(10):
        _t = threading.Thread(target=simple_listen)
        _t.start()
        threads.append(_t)

    send = threading.Thread(target=send_messages)
    send.start()
    send.join()

    for _t in threads:
        _t.join()

    print("\nThreads reached their time limit I set in the code\n")
