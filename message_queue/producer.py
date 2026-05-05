from celery_app import celery_app
from database import SessionLocal
import models


def enqueue_email_analysis(email_id: int) -> None:
    db = SessionLocal()
    try:
        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if not email:
            return

        if not email.is_urls_queued:
            celery_app.send_task("tasks.analyze_urls", args=[email_id], queue="url_queue")
            email.is_urls_queued = True

        if not email.is_body_queued:
            celery_app.send_task("tasks.analyze_body", args=[email_id], queue="body_queue")
            email.is_body_queued = True

        if not email.is_headers_queued:
            celery_app.send_task("tasks.analyze_headers", args=[email_id], queue="headers_queue")
            email.is_headers_queued = True

        if not email.is_attachments_queued:
            celery_app.send_task("tasks.analyze_attachments", args=[email_id], queue="attachments_queue")
            email.is_attachments_queued = True

        db.commit()
    finally:
        db.close()
