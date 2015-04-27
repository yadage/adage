from celery import Celery
app = Celery(broker = 'redis://', backend = 'redis://')
