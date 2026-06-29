from typing import Any, Dict, List, Optional
import html
import requests
import logging
import os
import re
from dotenv import load_dotenv
load_dotenv()
logger = logging.getLogger(__name__)

URL_ANALYSIS_ENDPOINT = os.getenv(
    "URL_ANALYSIS_ENDPOINT"
)
ATTACHMENT_ANALYSIS_ENDPOINT = os.getenv(
    "ATTACHMENT_ANALYSIS_ENDPOINT"
)
BODY_ANALYSIS_ENDPOINT = os.getenv(
    "BODY_ANALYSIS_ENDPOINT"
)
REQUEST_TIMEOUT = int(os.getenv("ANALYSIS_REQUEST_TIMEOUT", "45"))
ATTACHMENT_REQUEST_TIMEOUT = int(os.getenv("ATTACHMENT_ANALYSIS_TIMEOUT", "240"))
BODY_REQUEST_TIMEOUT = int(os.getenv("BODY_ANALYSIS_TIMEOUT", "90"))


def analyze_single_url(url: str) -> Dict[str, Any]:
    """Call the cloud URL analysis endpoint for a single URL.

    Returns the API response dict on success, or a fallback error dict.
    """
    try:
        response = requests.post(
            URL_ANALYSIS_ENDPOINT,
            json={"url": url},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        logger.error("URL analysis request failed for %s: %s", url, exc)
        return {
            "url": url,
            "verdict": "ERROR",
            "score": 0,
            "reasons": [f"Analysis request failed: {exc}"],
        }


def analyze_urls_api(email: Any, urls: List[str]) -> List[Dict[str, Any]]:
    """Analyse every URL via the cloud endpoint.

    Returns a list of per-URL result dicts, each containing at least:
        url, verdict, score, reasons
    """
    results: List[Dict[str, Any]] = []
    for url in urls:
        result = analyze_single_url(url)
        results.append(result)
    return results


def analyze_attachment_api(file_path: str, file_name: str) -> Dict[str, Any]:
    """Submit an attachment to the single cloud analysis endpoint."""
    try:
        with open(file_path, "rb") as f:
            response = requests.post(
                ATTACHMENT_ANALYSIS_ENDPOINT,
                files={"file": (file_name, f)},
                timeout=ATTACHMENT_REQUEST_TIMEOUT,
            )
        response.raise_for_status()
        return response.json()
    except FileNotFoundError:
        logger.error("Attachment file not found on disk: %s", file_path)
        return {"error": f"File not found: {file_path}"}
    except requests.RequestException as exc:
        logger.error("Attachment analysis failed for %s: %s", file_name, exc)
        return {"error": f"Attachment analysis request failed: {exc}"}


def _clean_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", raw)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


def _parse_confidence(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("%"):
            try:
                return float(text[:-1]) / 100.0
            except ValueError:
                return None
        try:
            number = float(text)
        except ValueError:
            return None
    else:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None

    if number > 1.0:
        number = number / 100.0
    return min(max(number, 0.0), 1.0)


def analyze_body_api(email: Any) -> Dict[str, Any]:
    """Classify the email body via the body-analysis service."""
    text = _clean_text(getattr(email, "body_full", "") or "")
    try:
        response = requests.post(
            BODY_ANALYSIS_ENDPOINT,
            json={"text": text},
            timeout=BODY_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        prediction = payload.get("prediction") or payload.get("verdict") or "UNKNOWN"
        return {
            "verdict": prediction,
            "confidence": _parse_confidence(payload.get("confidence")),
            "probabilities": payload.get("probabilities") or {},
            "class_id": payload.get("class_id"),
            "raw_response": payload,
        }
    except requests.RequestException as exc:
        logger.error("Body analysis request failed for email %s: %s", getattr(email, "id", None), exc)
        return {
            "verdict": "ERROR",
            "confidence": None,
            "probabilities": {},
            "error": f"Body analysis request failed: {exc}",
        }


def analyze_headers_api(email: Any, headers_raw: str | None) -> Dict[str, Any]:
    """Analyse email headers locally for phishing / spoofing indicators.

    Returns ``{"verdict": str, "score": int, "reasons": list[str]}``.
    """
    from services.header_analysis_engine import run_header_analysis

    return run_header_analysis(headers_raw)
