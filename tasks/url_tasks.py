from datetime import datetime, timezone

from celery_app import celery_app
from database import SessionLocal
import models
from services.analysis_service import analyze_urls_api
from redis_pubsub import publish_event
from tasks.utils import (
    get_receiver_user_id,
    maybe_finalize_email,
    normalize_reasons,
    serialize_urls,
)


def _as_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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

        urls = (
            db.query(models.UrlsExtracted)
            .filter(models.UrlsExtracted.email_id == email_id)
            .all()
        )
        url_list = [row.url for row in urls]

        if not url_list:
            email.urls_status = "DONE"
            db.commit()

            email = db.query(models.Email).filter(models.Email.id == email_id).first()
            final_payload = maybe_finalize_email(db, email) if email else None
            if final_payload:
                db.commit()

            user_id = get_receiver_user_id(db, email_id)
            if user_id:
                publish_event(
                    {
                        "user_id": user_id,
                        "type": "partial_update",
                        "email_id": email_id,
                        "field": "urls",
                        "status": "DONE",
                        "urls": [],
                    }
                )
                if final_payload:
                    final_payload["user_id"] = user_id
                    publish_event(final_payload)
            return {"status": "no_urls"}

        api_results = analyze_urls_api(email, url_list)
        now = datetime.now(timezone.utc)

        for idx, row in enumerate(urls):
            result = api_results[idx] if idx < len(api_results) else None

            if result:
                cloud_verdict = (result.get("verdict") or "UNKNOWN").upper()
                row.verdict = cloud_verdict
                row.score = _as_float(result.get("score"))
                row.domain = result.get("domain")
                row.final_url = result.get("final_url")
                row.http_status = _as_int(result.get("http_status"))
                row.redirect_count = _as_int(result.get("redirect_count"))
                row.reasons = normalize_reasons(result.get("reasons"))
                row.status = "FAILED" if cloud_verdict == "ERROR" else "DONE"
            else:
                row.verdict = "ERROR"
                row.score = None
                row.reasons = ["No result returned from analysis endpoint"]
                row.status = "FAILED"

            row.analyzed_at = now

        email.urls_status = "DONE"
        db.commit()

        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        final_payload = maybe_finalize_email(db, email) if email else None
        if final_payload:
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
        urls = (
            db.query(models.UrlsExtracted)
            .filter(models.UrlsExtracted.email_id == email_id)
            .all()
        )
        for row in urls:
            row.status = "FAILED"
            row.reasons = [str(exc)]
            row.analyzed_at = now

        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if email:
            email.urls_status = "FAILED"
        else:
            final_payload = None

        db.commit()
        if email:
            email = db.query(models.Email).filter(models.Email.id == email_id).first()
            final_payload = maybe_finalize_email(db, email) if email else None
            if final_payload:
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
