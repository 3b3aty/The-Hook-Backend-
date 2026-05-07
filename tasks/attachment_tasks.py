from datetime import datetime, timezone

from celery.exceptions import Retry as CeleryRetry
from celery_app import celery_app
from database import SessionLocal
import models
from services.analysis_service import upload_attachment, check_attachment_status
from redis_pubsub import publish_event
from tasks.utils import get_receiver_user_id, maybe_finalize_email, normalize_reasons, serialize_attachments


def _store_static_analysis(db, attachment, result: dict) -> None:
    """Persist the cloud analysis result into the static_analysis table."""
    existing = (
        db.query(models.StaticAnalysis)
        .filter(models.StaticAnalysis.attach_id == attachment.id)
        .first()
    )
    if existing:
        existing.score = result.get("score")
        existing.verdict = result.get("verdict")
        existing.reasons = normalize_reasons(result.get("reasons"))
    else:
        db.add(
            models.StaticAnalysis(
                attach_id=attachment.id,
                score=result.get("score"),
                verdict=result.get("verdict"),
                reasons=normalize_reasons(result.get("reasons")),
            )
        )


def _notify_attachments(db, email_id, email, status="DONE", error=None):
    """Send a partial_update event via Redis pub/sub."""
    user_id = get_receiver_user_id(db, email_id)
    if not user_id:
        return

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


@celery_app.task(name="tasks.analyze_attachments", bind=True, max_retries=60)
def analyze_attachments(self, email_id: int, pending_tasks: dict = None) -> dict:
    """Analyse email attachments via the cloud endpoint.

    Phase 1 (pending_tasks is None):
        Upload each attachment file. If the API returns immediately
        (task_id=null, file already analyzed), store the result right away.
        Otherwise collect task_ids for polling.

    Phase 2 (pending_tasks is provided):
        Poll GET /status/{task_id} for each pending attachment.
        If any are still processing, re-queue via self.retry().
    """
    db = SessionLocal()
    try:
        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if not email:
            return {"status": "missing"}

        # Only gate on first invocation, not retries
        if pending_tasks is None and email.attachments_status in {"DONE", "FAILED", "PROCESSING"}:
            return {"status": "skipped"}

        # ── Phase 1: Upload ──────────────────────────────────────────────
        if pending_tasks is None:
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
                if user_id and final_payload:
                    final_payload["user_id"] = user_id
                    publish_event(final_payload)
                return {"status": "no_attachments"}

            pending_tasks = {}
            now = datetime.now(timezone.utc)

            for row in attachments:
                if not row.file_url:
                    row.status = "FAILED"
                    row.analyzed_at = now
                    continue

                result = upload_attachment(row.file_url, row.file_name)

                if "error" in result:
                    row.status = "FAILED"
                    row.analyzed_at = now
                    continue

                task_id = result.get("task_id")
                if task_id is None:
                    # File was already analyzed — result is inline
                    _store_static_analysis(db, row, result)
                    row.status = "DONE"
                    row.analyzed_at = now
                else:
                    # Queued for analysis — need to poll later
                    pending_tasks[str(row.id)] = task_id

            db.commit()

            if not pending_tasks:
                email.attachments_status = "DONE"
                final_payload = maybe_finalize_email(db, email)
                db.commit()
                user_id = _notify_attachments(db, email_id, email)
                if user_id and final_payload:
                    final_payload["user_id"] = user_id
                    publish_event(final_payload)
                return {"status": "done"}

        # ── Phase 2: Poll pending tasks ──────────────────────────────────
        still_pending = {}
        now = datetime.now(timezone.utc)

        for att_id_str, task_id in pending_tasks.items():
            status_resp = check_attachment_status(task_id)

            if "error" in status_resp:
                att = db.query(models.Attachments).filter(models.Attachments.id == int(att_id_str)).first()
                if att:
                    att.status = "FAILED"
                    att.analyzed_at = now
                continue

            state = (status_resp.get("state") or "").upper()

            if state == "SUCCESS":
                att = db.query(models.Attachments).filter(models.Attachments.id == int(att_id_str)).first()
                if att:
                    inner = status_resp.get("result", {})
                    _store_static_analysis(db, att, inner)
                    att.status = "DONE"
                    att.analyzed_at = now
            elif state in {"PENDING", "STARTED", "RETRY"}:
                still_pending[att_id_str] = task_id
            else:
                # FAILURE or unknown state
                att = db.query(models.Attachments).filter(models.Attachments.id == int(att_id_str)).first()
                if att:
                    att.status = "FAILED"
                    att.analyzed_at = now

        db.commit()

        if still_pending:
            # Re-queue: the worker is freed, task runs again after countdown
            raise self.retry(
                countdown=15,
                kwargs={"email_id": email_id, "pending_tasks": still_pending},
            )

        # All attachments resolved
        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if email:
            email.attachments_status = "DONE"
            final_payload = maybe_finalize_email(db, email)
        else:
            final_payload = None
        db.commit()

        user_id = _notify_attachments(db, email_id, email)
        if user_id and final_payload:
            final_payload["user_id"] = user_id
            publish_event(final_payload)
        return {"status": "done"}

    except CeleryRetry:
        # Let Celery's retry mechanism propagate
        raise
    except self.MaxRetriesExceededError:
        # Timed out waiting for cloud analysis
        now = datetime.now(timezone.utc)
        attachments = (
            db.query(models.Attachments)
            .filter(models.Attachments.email_id == email_id)
            .all()
        )
        for row in attachments:
            if row.status not in {"DONE", "FAILED"}:
                row.status = "FAILED"
                row.analyzed_at = now

        email = db.query(models.Email).filter(models.Email.id == email_id).first()
        if email:
            email.attachments_status = "FAILED"
            final_payload = maybe_finalize_email(db, email)
        else:
            final_payload = None
        db.commit()

        user_id = _notify_attachments(db, email_id, email, status="FAILED", error="Max retries exceeded")
        if user_id and final_payload:
            final_payload["user_id"] = user_id
            publish_event(final_payload)
        return {"status": "failed", "error": "Max retries exceeded waiting for attachment analysis"}

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

        user_id = _notify_attachments(db, email_id, email, status="FAILED", error=str(exc))
        if user_id and final_payload:
            final_payload["user_id"] = user_id
            publish_event(final_payload)
        return {"status": "failed", "error": str(exc)}
    finally:
        db.close()
