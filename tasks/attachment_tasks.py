from datetime import datetime, timezone

from celery_app import celery_app
from database import SessionLocal
import models
from services.analysis_service import analyze_attachment_api
from redis_pubsub import publish_event
from tasks.utils import (
    get_receiver_user_id,
    maybe_finalize_email,
    normalize_reasons,
    serialize_attachments,
)


def _store_static_analysis(db, attachment, result: dict) -> None:
    """Persist the cloud analysis result into the static_analysis table."""
    existing = (
        db.query(models.StaticAnalysis)
        .filter(models.StaticAnalysis.attach_id == attachment.id)
        .first()
    )
    payload = {
        "score": result.get("score"),
        "verdict": result.get("verdict"),
        "reasons": normalize_reasons(result.get("reasons")),
    }
    if existing:
        existing.score = payload["score"]
        existing.verdict = payload["verdict"]
        existing.reasons = payload["reasons"]
    else:
        db.add(
            models.StaticAnalysis(
                attach_id=attachment.id,
                score=payload["score"],
                verdict=payload["verdict"],
                reasons=payload["reasons"],
            )
        )


def _notify_attachments(db, email_id: int, status: str = "DONE", error=None):
    user_id = get_receiver_user_id(db, email_id)
    if not user_id:
        return None

    attachments = (
        db.query(models.Attachments)
        .filter(models.Attachments.email_id == email_id)
        .all()
    )
    payload = {
        "user_id": user_id,
        "type": "partial_update",
        "email_id": email_id,
        "field": "attachments",
        "status": status,
        "attachments": serialize_attachments(attachments),
    }
    if error:
        payload["error"] = error
    publish_event(payload)
    return user_id


@celery_app.task(name="tasks.analyze_attachments")
def analyze_attachments(email_id: int) -> dict:
    """Analyze all email attachments through the single cloud file endpoint."""
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
        now = datetime.now(timezone.utc)

        if not attachments:
            email.attachments_status = "DONE"
            db.commit()

            email = db.query(models.Email).filter(models.Email.id == email_id).first()
            final_payload = maybe_finalize_email(db, email) if email else None
            if final_payload:
                db.commit()
            user_id = _notify_attachments(db, email_id)
            if user_id and final_payload:
                final_payload["user_id"] = user_id
                publish_event(final_payload)
            return {"status": "no_attachments"}

        failures = []
        for row in attachments:
            if not row.file_url:
                row.status = "FAILED"
                row.analyzed_at = now
                failures.append(f"{row.file_name}: missing local file path")
                _store_static_analysis(
                    db,
                    row,
                    {
                        "score": None,
                        "verdict": "ERROR",
                        "reasons": ["Missing local file path"],
                    },
                )
                continue

            result = analyze_attachment_api(row.file_url, row.file_name)
            if "error" in result:
                row.status = "FAILED"
                row.analyzed_at = now
                failures.append(f"{row.file_name}: {result['error']}")
                _store_static_analysis(
                    db,
                    row,
                    {
                        "score": None,
                        "verdict": "ERROR",
                        "reasons": [result["error"]],
                    },
                )
                continue

            if result.get("sha256"):
                row.hash_sha256 = result.get("sha256")
            _store_static_analysis(db, row, result)
            row.status = "DONE"
            row.analyzed_at = now

        email.attachments_status = "DONE"
        db.commit()

        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        final_payload = maybe_finalize_email(db, email) if email else None
        if final_payload:
            db.commit()

        user_id = _notify_attachments(
            db,
            email_id,
            status="DONE",
            error="; ".join(failures) if failures else None,
        )
        if user_id and final_payload:
            final_payload["user_id"] = user_id
            publish_event(final_payload)
        return {
            "status": "done",
            "failed_attachments": len(failures),
            "total_attachments": len(attachments),
        }

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
        else:
            final_payload = None
        db.commit()

        if email:
            email = db.query(models.Email).filter(models.Email.id == email_id).first()
            final_payload = maybe_finalize_email(db, email) if email else None
            if final_payload:
                db.commit()

        user_id = _notify_attachments(db, email_id, status="FAILED", error=str(exc))
        if user_id and final_payload:
            final_payload["user_id"] = user_id
            publish_event(final_payload)
        return {"status": "failed", "error": str(exc)}
    finally:
        db.close()
