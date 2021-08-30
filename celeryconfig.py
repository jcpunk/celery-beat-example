broker_url = "amqp://localhost"
result_backend = "redis://localhost:6379/0"

redbeat_redis_url = "redis://localhost:6379/1"
redbeat_key_prefix = "example_namespace."

task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]
timezone = "America/Chicago"
enable_utc = True

# if you have tblib
# you can just set task_remote_tracebacks = True
