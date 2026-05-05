import json
from datetime import datetime, timezone

from celery_app import celery_app
from database import SessionLocal
import models
from services.analysis_service import analyze_body_api
from redis_pubsub import publish_event
from tasks.utils import get_receiver_user_id, maybe_finalize_email, serialize_body


@celery_app.task(name="tasks.analyze_body")
def analyze_body(email_id: int) -> dict:
    db = SessionLocal()
    try:
        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if not email:
            return {"status": "missing"}

        if email.body_status in {"DONE", "FAILED", "PROCESSING"}:
            return {"status": "skipped"}

        email.status = "PROCESSING"
        email.body_status = "PROCESSING"
        db.commit()

        result = analyze_body_api(email)
        now = datetime.now(timezone.utc)

        body_row = (
            db.query(models.BodyClassification)
            .filter(models.BodyClassification.email_id == email_id)
            .first()
        )
        if not body_row:
            body_row = models.BodyClassification(email_id=email_id)
            db.add(body_row)

        body_row.verdict = result.get("verdict")
        confidence = result.get("confidence") or result.get("score")
        body_row.confidence = float(confidence) if confidence is not None else None
        body_row.status = "DONE"
        body_row.analyzed_at = now

        email.body_status = "DONE"
        final_payload = maybe_finalize_email(db, email)
        db.commit()

        user_id = get_receiver_user_id(db, email_id)
        if user_id:
            publish_event(
                {
                    "user_id": user_id,
                    "type": "partial_update",
                    "email_id": email_id,
                    "field": "body",
                    "status": email.body_status,
                    "body": serialize_body(body_row),
                }
            )
            if final_payload:
                final_payload["user_id"] = user_id
                publish_event(final_payload)
        return {"status": "done"}
    except Exception as exc:  # noqa: BLE001
        now = datetime.now(timezone.utc)
        body_row = (
            db.query(models.BodyClassification)
            .filter(models.BodyClassification.email_id == email_id)
            .first()
        )
        if not body_row:
            body_row = models.BodyClassification(email_id=email_id)
            db.add(body_row)

        body_row.status = "FAILED"
        body_row.verdict = "error"
        body_row.confidence = None
        body_row.analyzed_at = now

        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if email:
            email.body_status = "FAILED"
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
                    "field": "body",
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
