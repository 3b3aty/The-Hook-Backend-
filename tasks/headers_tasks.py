from datetime import datetime, timezone

from celery_app import celery_app
from database import SessionLocal
import models
from services.analysis_service import analyze_headers_api
from redis_pubsub import publish_event
from tasks.utils import get_receiver_user_id, maybe_finalize_email, normalize_reasons, serialize_headers


@celery_app.task(name="tasks.analyze_headers")
def analyze_headers(email_id: int) -> dict:
    db = SessionLocal()
    try:
        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if not email:
            return {"status": "missing"}

        if email.headers_status in {"DONE", "FAILED", "PROCESSING"}:
            return {"status": "skipped"}

        email.status = "PROCESSING"
        email.headers_status = "PROCESSING"
        db.commit()

        headers_row = (
            db.query(models.EmailHeaders)
            .filter(models.EmailHeaders.email_id == email_id)
            .first()
        )
        if not headers_row:
            headers_row = models.EmailHeaders(email_id=email_id)
            db.add(headers_row)

        result = analyze_headers_api(email, headers_row.raw_headers)
        now = datetime.now(timezone.utc)

        headers_row.verdict = result.get("verdict")
        headers_row.reasons = normalize_reasons(result.get("reasons"))
        headers_row.score = result.get("score")
        headers_row.status = "DONE"
        headers_row.analyzed_at = now

        email.headers_status = "DONE"
        final_payload = maybe_finalize_email(db, email)
        db.commit()

        user_id = get_receiver_user_id(db, email_id)
        if user_id:
            publish_event(
                {
                    "user_id": user_id,
                    "type": "partial_update",
                    "email_id": email_id,
                    "field": "headers",
                    "status": email.headers_status,
                    "headers": serialize_headers(headers_row),
                }
            )
            if final_payload:
                final_payload["user_id"] = user_id
                publish_event(final_payload)
        return {"status": "done"}
    except Exception as exc:  # noqa: BLE001
        now = datetime.now(timezone.utc)
        headers_row = (
            db.query(models.EmailHeaders)
            .filter(models.EmailHeaders.email_id == email_id)
            .first()
        )
        if not headers_row:
            headers_row = models.EmailHeaders(email_id=email_id)
            db.add(headers_row)

        headers_row.status = "FAILED"
        headers_row.verdict = "error"
        headers_row.reasons = [str(exc)]
        headers_row.analyzed_at = now

        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if email:
            email.headers_status = "FAILED"
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
                    "field": "headers",
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
