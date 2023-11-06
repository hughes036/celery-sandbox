# celery-sandbox

From following: https://docs.celeryq.dev/en/stable/getting-started/first-steps-with-celery.html#rabbitmq

**Setup**:

Run RabbitMQ container:
```
docker run -d -p <server_port>:5672 rabbitmq
```

Run test celery consumer (`tasks.py`):
```
celery -A tasks worker --loglevel=INFO
```

Run test celery producer (`producer.py`)
```
python producer.py
```

