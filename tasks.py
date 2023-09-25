from celery import Celery

app = Celery('tasks', broker='pyamqp://guest@dev-aics-tfp-002:8080')

@app.task
def add(x, y):
    return x + y


@app.task
def mul(x, y):
    return x * y


@app.task
def xsum(numbers):
    return sum(numbers)