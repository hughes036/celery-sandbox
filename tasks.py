from celery import Celery
import time
import requests
app = Celery('tasks', broker='pyamqp://guest@dev-aics-tfp-002:8080', backend='rpc://',) # using rabbitMQ instance as backend

# Configure Celery to use threads for concurrency
app.conf.update(
    task_concurrency=4,  # Use 4 threads for concurrency
    worker_prefetch_multiplier=1  # Prefetch one task at a time
)

@app.task
def add(x, y):
    return x + y

@app.task
def mul(x, y):
    return x * y

@app.task
def xsum(numbers):
    return sum(numbers)

@app.task
def cpuBoundTask(sleep_time=1):
    time.sleep(sleep_time)
    return f"slept for {sleep_time} seconds"

@app.task
def ioBoundTask():
    x = requests.get('https://w3schools.com')
    return str(x.status_code)