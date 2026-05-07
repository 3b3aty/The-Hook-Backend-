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
            if user_id and final_payload:
                final_payload["user_id"] = user_id
                publish_event(final_payload)
            return {"status": "no_urls"}

        # Call cloud API — returns a list of per-URL results
        api_results = analyze_urls_api(email, url_list)

        # Build a lookup: url -> result for matching with DB rows
        result_map = {}
        for res in api_results:
            res_url = res.get("url") or res.get("final_url", "")
            result_map[res_url] = res

        now = datetime.now(timezone.utc)

        for row in urls:
            # Try exact match first, then fall back to positional
            result = result_map.get(row.url)
            if result is None:
                # Fallback: pop the first result from the list matching order
                idx = url_list.index(row.url) if row.url in url_list else None
                if idx is not None and idx < len(api_results):
                    result = api_results[idx]

            if result:
                cloud_verdict = (result.get("verdict") or "UNKNOWN").upper()
                row.verdict = cloud_verdict
                row.reasons = normalize_reasons(result.get("reasons"))
                row.status = "DONE"
            else:
                row.verdict = "ERROR"
                row.reasons = ["No result returned from analysis endpoint"]
                row.status = "FAILED"

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
