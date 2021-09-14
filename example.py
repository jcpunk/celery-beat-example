#!/usr/bin/env python3

import sys
import time

from celery import chain, group, schedules, signature
from celery.result import ResultBase
from kombu import Connection, Exchange, Queue
from redbeat import RedBeatSchedulerEntry

from celeryconfig import broker_url, redbeat_key_prefix
from tasks import add, add_to_amqp, app

print("Flower may be running at http://localhost:5555\n\n")

run_schedule = schedules.schedule(run_every=3)
scheduled_task = RedBeatSchedulerEntry(
    "task_name_key",
    task="tasks.add",
    schedule=run_schedule,
    options={"expires": 10},
    args=[15, 14],
    app=app,
)
scheduled_task.save()


print(f"Scheduled task {scheduled_task.key}")
print(f"Scheduled task due at {scheduled_task.due_at}")

time.sleep(1)

print(f"Running function as is: {add(2,2)}")

print("\n\n")

print("Running in celery with no delay:")
result = add.apply_async((2, 3))
time.sleep(1)
print(f"  is task complete? {result.ready()}")
print(f"  is task successful? {result.successful()}")
print(f"  task result: {result.get(timeout=1)}")

print("\n\n")

print("Running in celery with delay:")
result_delay = add.apply_async((3, 3), countdown=7, expires=10, track_started=True)
print(f"  is task complete? {result_delay.ready()}")
print(f"  is task successful? {result_delay.successful()}")
print(f"  task result: {result_delay.get()}")

print("\n\n")

print("Get a previous task result:")
old_task = add.AsyncResult(result.id)
print(f"  is task complete? {old_task.ready()}")
print(f"  is task successful? {old_task.successful()}")
print(f"  task result: {old_task.get()}")

print("\n\n")

print("Finding and ending scheduled task")
task_cleanup = RedBeatSchedulerEntry.from_key(
    f"{redbeat_key_prefix}task_name_key", app=app
)
task_cleanup.delete()

print("\n\n")

print("Running second task if and only if success on first")
lresult = add.apply_async((4, 3), link=add.s(8))  # second task of 7 + 8
print(f"  is task complete? {lresult.ready()}")
print(f"  is task successful? {lresult.successful()}")
print(f"  first task result: {lresult.get()}")
print(f"  second task result: {lresult.children[0].get()}")

print("\n\n")

print("Running second task if and only if success on first")
lresult2 = add.apply_async((4, 4), link=add.s(8))  # second task of 8 + 8
print(f"  is task complete? {lresult2.ready()}")
print(f"  is task successful? {lresult2.successful()}")

for v in lresult2.collect():
    if isinstance(v, (ResultBase, tuple)):
        print(f"  task result: {v[1]}")

# link_error is the error handler you can use

print("\n\n")

print("Run tasks in sequence")
res_chain = chain(add.s(2, 2), add.s(4), add.s(8))()
print(f"  is task complete? {res_chain.ready()}")
print(f"  is task successful? {res_chain.successful()}")
print(f"  task result: {res_chain.get()}")

print("\n\n")

print("Run tasks in parallel")
res_group = group(add.s(i, i) for i in range(10))()
print(f"  is task waiting? {res_group.waiting()}")
print(f"  is task complete? {res_group.ready()}")
print(f"  is task successful? {res_group.successful()}")
print(f"  task result: {res_group.get()}")

print("\n\n")

print("Run tasks as a map")
res_map = add.map([range(10), range(100)])
print(f"  is task waiting? {res_group.waiting()}")
print(f"  is task complete? {res_group.ready()}")
print(f"  is task successful? {res_group.successful()}")
print(f"  task result: {res_group.get()}")

print("\n\n")

print("Run tasks with chunks, and increasing delays")
res_chunk_task = add.chunks(zip(range(100), range(100)), 10).group()
res_chunk = res_chunk_task.skew(start=1, stop=4).apply_async()
print(f"  is task complete? {res_chunk.ready()}")
print(f"  is task successful? {res_chunk.successful()}")
print(f"  is task waiting? {res_chunk.waiting()}")
print(f"  task result: {res_chunk.get()}")

print("\n\n")

add_to_amqp.apply_async((7, 7), countdown=7)
exchange = Exchange("example_exchange", "direct")
queue = Queue("", exchange=exchange, routing_key="add", auto_delete=True)
with Connection(broker_url) as conn:

    def print_msg(body, message):
        print(f"Message on bus: {body}")
        message.ack()

    with conn.Consumer(queue, callbacks=[print_msg]):
        print("Waiting for message to appear on the bus:")
        conn.drain_events()  # finish after one event it parsed

print("\n\n")

print("Running in celery with a stack trace:")
result_fail = add.apply_async((2, "string"))
print(f"  is task complete? {result_fail.ready()}")
print(f"  is task successful? {result_fail.successful()}")
try:
    print(f"  task result: {result_fail.get()}")
except Exception:
    print(f"  is task failed? {result_fail.failed()}")
    print(f"Client Traceback:\n{result_fail.traceback}\n", file=sys.stderr)
    raise
