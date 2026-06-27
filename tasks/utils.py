from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import json
import logging

import models

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Verdict keyword sets used for mapping sub-analysis verdicts to scores
# ---------------------------------------------------------------------------
_MALICIOUS_VERDICTS = {"phishing", "malicious"}
_SUSPICIOUS_VERDICTS = {"suspicious", "fail", "softfail"}
_CLEAN_VERDICTS = {"clean", "safe", "benign", "legitimate", "pass", "neutral"}

# Component weights (must sum to 1.0)
_WEIGHT_HEADERS = 0.30
_WEIGHT_URLS = 0.30
_WEIGHT_ATTACHMENTS = 0.25
_WEIGHT_BODY = 0.15


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


# ---------------------------------------------------------------------------
# Weighted multi-signal scoring
# ---------------------------------------------------------------------------

def _verdict_to_score(verdict: Optional[str]) -> float:
    """Map a sub-analysis verdict string to a 0.0–1.0 severity score."""
    if not verdict:
        return 0.0
    v = verdict.strip().lower()
    if v in _MALICIOUS_VERDICTS:
        return 1.0
    if v in _SUSPICIOUS_VERDICTS:
        return 0.5
    if v in _CLEAN_VERDICTS:
        return 0.0
    # Unknown verdict — treat as mildly suspicious rather than malicious
    return 0.25


def _score_headers(headers: Optional[Dict[str, Any]]) -> Optional[float]:
    """Return a normalized 0.0–1.0 score for the headers component.

    Returns None if the component should be excluded from scoring
    (no data or analysis failed).
    """
    if not headers:
        return None
    status = (headers.get("status") or "").upper()
    if status == "FAILED":
        return None

    # The header analysis engine returns a score 0–100
    raw_score = headers.get("score")
    if raw_score is not None:
        return min(max(float(raw_score) / 100.0, 0.0), 1.0)

    # Fallback to verdict mapping if no numeric score
    return _verdict_to_score(headers.get("verdict"))


def _score_urls(url_items: List[Dict[str, Any]]) -> Optional[float]:
    """Return a normalized 0.0–1.0 score for the URLs component.

    Returns None if there are no URLs (weight should be redistributed).
    """
    if not url_items:
        return None

    # Only consider URLs that were actually analyzed
    analyzed = [u for u in url_items if (u.get("status") or "").upper() not in ("FAILED",)]
    if not analyzed:
        return None

    total_score = 0.0
    for item in analyzed:
        total_score += _verdict_to_score(item.get("verdict"))

    # Average severity across all analyzed URLs
    return total_score / len(analyzed)


def _score_attachments(attachment_items: List[Dict[str, Any]], db=None, email_id: int = 0) -> Optional[float]:
    """Return a normalized 0.0–1.0 score for the attachments component.

    Returns None if there are no attachments (weight should be redistributed).
    Tries to use static analysis scores from DB when available.
    """
    if not attachment_items:
        return None

    # Try to get richer data from static_analysis table
    static_scores: List[float] = []
    if db and email_id:
        attachments_db = (
            db.query(models.Attachments)
            .filter(models.Attachments.email_id == email_id)
            .all()
        )
        for att in attachments_db:
            if att.status == "FAILED":
                continue
            if att.static_analysis and att.static_analysis.score is not None:
                # Cloud analysis score — normalize from 0–100 to 0–1
                static_scores.append(min(max(float(att.static_analysis.score) / 100.0, 0.0), 1.0))
            elif att.static_analysis and att.static_analysis.verdict:
                static_scores.append(_verdict_to_score(att.static_analysis.verdict))

    if static_scores:
        return max(static_scores)  # Take the worst attachment

    # Fallback: no static analysis data available — use serialized status only
    analyzed = [a for a in attachment_items if (a.get("status") or "").upper() not in ("FAILED",)]
    if not analyzed:
        return None

    # Without scores we can only check status — treat DONE as clean
    return 0.0


def _score_body(body: Optional[Dict[str, Any]]) -> Optional[float]:
    """Return a normalized 0.0–1.0 score for the body component.

    Returns None if the component should be excluded from scoring.
    """
    if not body:
        return None
    status = (body.get("status") or "").upper()
    if status == "FAILED":
        return None

    verdict = (body.get("verdict") or "").strip().lower()
    confidence = body.get("confidence")

    if confidence is not None:
        conf = min(max(float(confidence), 0.0), 1.0)
        # If the body is classified as clean/safe, the risk is inverse of confidence
        if verdict in _CLEAN_VERDICTS:
            return 1.0 - conf
        # If suspicious/malicious, confidence IS the risk level
        if verdict in _MALICIOUS_VERDICTS or verdict in _SUSPICIOUS_VERDICTS:
            return conf
        # Unknown verdict with confidence — treat as moderate
        return conf * 0.5

    # No confidence — fall back to verdict mapping
    return _verdict_to_score(verdict)


def compute_final_verdict(results: Dict[str, Any], db=None, email_id: int = 0) -> Tuple[float, str]:
    """Compute a weighted risk score and verdict from all analysis components.

    Each component contributes a normalized score (0.0–1.0) multiplied by its
    weight. Components with no data or FAILED status have their weight
    redistributed proportionally to the remaining components.

    Returns ``(risk_score, verdict)`` where risk_score is 0–100 and verdict
    is one of ``"SAFE"``, ``"SUSPICIOUS"``, or ``"PHISHING"``.
    """
    # Calculate each component score (None = exclude from scoring)
    component_scores: Dict[str, Optional[float]] = {
        "headers": _score_headers(results.get("headers")),
        "urls": _score_urls(results.get("urls", [])),
        "attachments": _score_attachments(results.get("attachments", []), db=db, email_id=email_id),
        "body": _score_body(results.get("body")),
    }

    base_weights = {
        "headers": _WEIGHT_HEADERS,
        "urls": _WEIGHT_URLS,
        "attachments": _WEIGHT_ATTACHMENTS,
        "body": _WEIGHT_BODY,
    }

    # Determine which components are active (have data)
    active: Dict[str, float] = {}
    for key, score in component_scores.items():
        if score is not None:
            active[key] = score

    if not active:
        # All components excluded — no data to score, return safe
        return 0.0, "SAFE"

    # Redistribute weights from excluded components to active ones
    total_active_weight = sum(base_weights[k] for k in active)
    if total_active_weight <= 0:
        return 0.0, "SAFE"

    weight_scale = 1.0 / total_active_weight

    # Compute weighted sum
    weighted_sum = 0.0
    for key, score in active.items():
        normalized_weight = base_weights[key] * weight_scale
        weighted_sum += score * normalized_weight

    # Scale to 0–100
    risk_score = round(weighted_sum * 100.0, 1)
    risk_score = min(max(risk_score, 0.0), 100.0)

    # Determine verdict
    if risk_score >= 60:
        verdict = "PHISHING"
    elif risk_score >= 30:
        verdict = "SUSPICIOUS"
    else:
        verdict = "SAFE"

    logger.info(
        "Final scoring for email %d: components=%s → risk=%.1f, verdict=%s",
        email_id, {k: f"{v:.2f}" for k, v in active.items()}, risk_score, verdict,
    )

    return risk_score, verdict


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
        risk_score, final_verdict = compute_final_verdict(results, db=db, email_id=email.id)
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
