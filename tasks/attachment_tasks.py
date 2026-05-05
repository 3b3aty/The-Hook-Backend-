from datetime import datetime, timezone

from celery_app import celery_app
from database import SessionLocal
import models
from services.analysis_service import analyze_attachments_api
from redis_pubsub import publish_event
from tasks.utils import get_receiver_user_id, maybe_finalize_email, serialize_attachments


@celery_app.task(name="tasks.analyze_attachments")
def analyze_attachments(email_id: int) -> dict:
    db = SessionLocal()
    try:
        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if not email:
            return {"status": "missing"}

        if email.attachments_status in {"DONE", "FAILED", "PROCESSING"}:
            return {"status": "skipped"}

        email.status = "PROCESSING"
        email.attachments_status = "PROCESSING"
        db.commit()

        attachments = (
            db.query(models.Attachments)
            .filter(models.Attachments.email_id == email_id)
            .all()
        )
        if not attachments:
            email.attachments_status = "DONE"
            final_payload = maybe_finalize_email(db, email)
            db.commit()
            user_id = get_receiver_user_id(db, email_id)
            if user_id:
                publish_event(
                    {
                        "user_id": user_id,
                        "type": "partial_update",
                        "email_id": email_id,
                        "field": "attachments",
                        "status": email.attachments_status,
                        "attachments": [],
                    }
                )
                if final_payload:
                    final_payload["user_id"] = user_id
                    publish_event(final_payload)
            return {"status": "no_attachments"}

        payload = [
            {
                "file_name": row.file_name,
                "file_type": row.file_type,
                "file_size": row.file_size,
                "hash_sha256": row.hash_sha256,
            }
            for row in attachments
        ]
        analyze_attachments_api(email, payload)
        now = datetime.now(timezone.utc)

        for row in attachments:
            row.status = "DONE"
            row.analyzed_at = now

        email.attachments_status = "DONE"
        final_payload = maybe_finalize_email(db, email)
        db.commit()

        user_id = get_receiver_user_id(db, email_id)
        if user_id:
            publish_event(
                {
                    "user_id": user_id,
                    "type": "partial_update",
                    "email_id": email_id,
                    "field": "attachments",
                    "status": email.attachments_status,
                    "attachments": serialize_attachments(attachments),
                }
            )
            if final_payload:
                final_payload["user_id"] = user_id
                publish_event(final_payload)
        return {"status": "done"}
    except Exception as exc:  # noqa: BLE001
        now = datetime.now(timezone.utc)
        attachments = (
            db.query(models.Attachments)
            .filter(models.Attachments.email_id == email_id)
            .all()
        )
        for row in attachments:
            row.status = "FAILED"
            row.analyzed_at = now

        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if email:
            email.attachments_status = "FAILED"
            final_payload = maybe_finalize_email(db, email)
        else:
            final_payload = None

        db.commit()
        user_id = get_receiver_user_id(db, email_id)
        if user_id:
            publish_event(
                {
                    "user_id": user_id,
                    "type": "partial_update",
                    "email_id": email_id,
                    "field": "attachments",
                    "status": "FAILED",
                    "error": str(exc),
                }
            )
            if final_payload:
                final_payload["user_id"] = user_id
                publish_event(final_payload)
        return {"status": "failed", "error": str(exc)}
    finally:
        db.close()
