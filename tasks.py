import logging

from celery import Celery
from kombu import Connection, Exchange, Queue

app = Celery("tasks")
app.config_from_object("celeryconfig")

from celeryconfig import broker_url


@app.task(bind=True, max_retries=2)
def add(self, x, y):
    """docstring"""
    logging.info(f"I AM LOG FOR `add` {self.request.id} - {x} + {y}")
    try:
        return x + y
    except Exception:
        raise add.retry(countdown=1)


@app.task(bind=True, max_retries=2)
def add_to_amqp(self, x, y):
    """docstring"""
    logging.info(f"I AM LOG FOR `add_to_amqp` {self.request.id} - {x} + {y}")
    try:
        exchange = Exchange("example_exchange", "direct")
        exchange = Exchange("example.exchange", "topic")
        queue = Queue("example_queue", exchange=exchange, routing_key="add")
        with Connection(broker_url) as conn:
            producer = conn.Producer(serializer="json")
            producer.publish(
                add(x, y), exchange=exchange, routing_key="add", declare=[queue]
            )
    except Exception:
        raise add_to_amqp.retry(countdown=1)
