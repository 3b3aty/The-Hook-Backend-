from datetime import datetime, timezone

from celery_app import celery_app
from database import SessionLocal
import models
from services.analysis_service import analyze_urls_api
from redis_pubsub import publish_event
from tasks.utils import get_receiver_user_id, maybe_finalize_email, normalize_reasons, serialize_urls


@celery_app.task(name="tasks.analyze_urls")
def analyze_urls(email_id: int) -> dict:
    db = SessionLocal()
    try:
        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if not email:
            return {"status": "missing"}

        if email.urls_status in {"DONE", "FAILED", "PROCESSING"}:
            return {"status": "skipped"}

        email.status = "PROCESSING"
        email.urls_status = "PROCESSING"
        db.commit()

        urls = db.query(models.UrlsExtracted).filter(models.UrlsExtracted.email_id == email_id).all()
        url_list = [row.url for row in urls]

        if not url_list:
            email.urls_status = "DONE"
            final_payload = maybe_finalize_email(db, email)
            db.commit()

            user_id = get_receiver_user_id(db, email_id)
            if user_id:
                publish_event(
                    {
                        "user_id": user_id,
                        "type": "partial_update",
                        "email_id": email_id,
                        "field": "urls",
                        "status": email.urls_status,
                        "urls": [],
                    }
                )
                if final_payload:
                    final_payload["user_id"] = user_id
                    publish_event(final_payload)
            return {"status": "no_urls"}

        result = analyze_urls_api(email, url_list)
        verdict = result.get("verdict")
        reasons = normalize_reasons(result.get("reasons"))
        now = datetime.now(timezone.utc)

        for row in urls:
            row.verdict = verdict
            row.reasons = reasons
            row.status = "DONE"
            row.analyzed_at = now

        email.urls_status = "DONE"
        final_payload = maybe_finalize_email(db, email)
        db.commit()

        user_id = get_receiver_user_id(db, email_id)
        if user_id:
            publish_event(
                {
                    "user_id": user_id,
                    "type": "partial_update",
                    "email_id": email_id,
                    "field": "urls",
                    "status": email.urls_status,
                    "urls": serialize_urls(urls),
                }
            )
            if final_payload:
                final_payload["user_id"] = user_id
                publish_event(final_payload)
        return {"status": "done"}
    except Exception as exc:  # noqa: BLE001
        now = datetime.now(timezone.utc)
        urls = db.query(models.UrlsExtracted).filter(models.UrlsExtracted.email_id == email_id).all()
        for row in urls:
            row.status = "FAILED"
            row.reasons = [str(exc)]
            row.analyzed_at = now

        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if email:
            email.urls_status = "FAILED"
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
                    "field": "urls",
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
