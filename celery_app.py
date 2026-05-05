import os

from celery import Celery
from kombu import Queue

BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "cache+memory://")

celery_app = Celery(
    "email_security",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
    include=[
        "tasks.url_tasks",
        "tasks.body_tasks",
        "tasks.headers_tasks",
        "tasks.attachment_tasks",
    ],
)

celery_app.conf.update(
    task_queues=(
        Queue("url_queue"),
        Queue("body_queue"),
        Queue("headers_queue"),
        Queue("attachments_queue"),
    ),
    task_routes={
        "tasks.analyze_urls": {"queue": "url_queue"},
        "tasks.analyze_body": {"queue": "body_queue"},
        "tasks.analyze_headers": {"queue": "headers_queue"},
        "tasks.analyze_attachments": {"queue": "attachments_queue"},
    },
    task_default_queue="default",
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    worker_enable_remote_control=False,
    task_ignore_result=True,
    timezone="UTC",
    enable_utc=True,
)
