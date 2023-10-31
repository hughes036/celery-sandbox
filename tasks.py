from celery import Celery

app = Celery('tasks', broker='pyamqp://aics:aics@dev-aics-tfp-002:80')

@app.task
def add(x, y):
    return x + y


@app.task
def mul(x, y):
    return x * y


@app.task
def xsum(numbers):
    return sum(numbers)