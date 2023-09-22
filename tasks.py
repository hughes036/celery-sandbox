from celery import Celery

app = Celery('tasks', broker='pyamqp://guest@10.141.0.54')

@app.task
def add(x, y):
    return x + y