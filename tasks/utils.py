from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import json

import models


SUSPICIOUS_VERDICTS = {"phishing", "malicious", "suspicious", "fail"}


def normalize_reasons(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return [str(item) for item in parsed]
            except json.JSONDecodeError:
                pass
        return [text]
    return [str(value)]


def get_receiver_user_id(db, email_id: int) -> Optional[int]:
    interface = (
        db.query(models.Interface)
        .filter(models.Interface.email_id == email_id, models.Interface.receiver_id.isnot(None))
        .first()
    )
    return interface.receiver_id if interface else None


def serialize_urls(rows: List[models.UrlsExtracted]) -> List[Dict[str, Any]]:
    return [
        {
            "url": row.url,
            "verdict": row.verdict,
            "reasons": normalize_reasons(row.reasons),
            "status": row.status,
        }
        for row in rows
    ]


def serialize_body(row: Optional[models.BodyClassification]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "verdict": row.verdict,
        "confidence": row.confidence,
        "status": row.status,
    }


def serialize_headers(row: Optional[models.EmailHeaders]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "verdict": row.verdict,
        "score": row.score,
        "reasons": normalize_reasons(row.reasons),
        "status": row.status,
    }


def serialize_attachments(rows: List[models.Attachments]) -> List[Dict[str, Any]]:
    return [
        {
            "file_name": row.file_name,
            "file_type": row.file_type,
            "file_size": row.file_size,
            "hash_sha256": row.hash_sha256,
            "status": row.status,
        }
        for row in rows
    ]


def collect_results(db, email_id: int) -> Dict[str, Any]:
    urls = db.query(models.UrlsExtracted).filter(models.UrlsExtracted.email_id == email_id).all()
    body = (
        db.query(models.BodyClassification)
        .filter(models.BodyClassification.email_id == email_id)
        .first()
    )
    headers = (
        db.query(models.EmailHeaders)
        .filter(models.EmailHeaders.email_id == email_id)
        .first()
    )
    attachments = (
        db.query(models.Attachments)
        .filter(models.Attachments.email_id == email_id)
        .all()
    )

    return {
        "urls": serialize_urls(urls),
        "body": serialize_body(body),
        "headers": serialize_headers(headers),
        "attachments": serialize_attachments(attachments),
    }


def compute_final_verdict(results: Dict[str, Any]) -> Tuple[float, str]:
    verdicts: List[str] = []

    for item in results.get("urls", []):
        if item.get("verdict"):
            verdicts.append(str(item["verdict"]))

    body = results.get("body")
    if body and body.get("verdict"):
        verdicts.append(str(body["verdict"]))

    headers = results.get("headers")
    if headers and headers.get("verdict"):
        verdicts.append(str(headers["verdict"]))

    verdict_lower = {v.lower() for v in verdicts}
    if verdict_lower & SUSPICIOUS_VERDICTS:
        return 85.0, "PHISHING"

    return 5.0, "SAFE"


def maybe_finalize_email(db, email: models.Email) -> Optional[Dict[str, Any]]:
    done_statuses = {"DONE", "FAILED"}
    if email.status == "ANALYZED":
        return None

    if (
        email.urls_status in done_statuses
        and email.body_status in done_statuses
        and email.headers_status in done_statuses
        and email.attachments_status in done_statuses
    ):
        results = collect_results(db, email.id)
        risk_score, final_verdict = compute_final_verdict(results)
        email.status = "ANALYZED"
        email.risk_score = risk_score
        email.final_verdict = final_verdict
        email.is_hooked = final_verdict == "PHISHING"
        email.analyzed_at = datetime.now(timezone.utc)
        category_name = email.category.name if email.category else None
        return {
            "type": "analysis_complete",
            "email_id": email.id,
            "status": email.status,
            "risk_score": email.risk_score,
            "final_verdict": email.final_verdict,
            "is_hooked": email.is_hooked,
            "is_trash": email.is_trash,
            "is_starred": email.is_starred,
            "category": category_name,
            **results,
        }

    return None
