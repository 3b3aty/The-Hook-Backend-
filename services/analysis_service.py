from typing import Any, Dict, List
import time
import requests
import logging

logger = logging.getLogger(__name__)

CLOUD_BASE_URL = "http://52.23.226.104:8000"
URL_ANALYSIS_ENDPOINT = f"{CLOUD_BASE_URL}/analyze-url"
ATTACHMENT_UPLOAD_ENDPOINT = f"{CLOUD_BASE_URL}/upload"
ATTACHMENT_STATUS_ENDPOINT = f"{CLOUD_BASE_URL}/status"  # + /{task_id}
REQUEST_TIMEOUT = 30  # seconds per request


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


def upload_attachment(file_path: str, file_name: str) -> Dict[str, Any]:
    """Upload a file to the cloud attachment analysis endpoint (POST /upload).

    Returns the API response dict. Possible shapes:
    - Queued:  {"message": "File queued for analysis", "task_id": "<uuid>", "sha256": "..."}
    - Cached:  {"message": "File already analyzed", "task_id": null, "sha256": "...",
                "score": 0, "verdict": "Benign", "reasons": []}
    - Error:   {"error": "<message>"}
    """
    try:
        with open(file_path, "rb") as f:
            response = requests.post(
                ATTACHMENT_UPLOAD_ENDPOINT,
                files={"file": (file_name, f)},
                timeout=REQUEST_TIMEOUT,
            )
        response.raise_for_status()
        return response.json()
    except FileNotFoundError:
        logger.error("Attachment file not found on disk: %s", file_path)
        return {"error": f"File not found: {file_path}"}
    except requests.RequestException as exc:
        logger.error("Attachment upload failed for %s: %s", file_name, exc)
        return {"error": f"Upload request failed: {exc}"}


def check_attachment_status(task_id: str) -> Dict[str, Any]:
    """Poll the cloud analysis status for an uploaded attachment (GET /status/{task_id}).

    Returns the API response dict. Possible shapes:
    - Success: {"task_id": "...", "state": "SUCCESS",
                "result": {"score": 0, "verdict": "benign", "reasons": [...]}}
    - Pending: {"task_id": "...", "state": "PENDING", ...}
    - Error:   {"error": "<message>"}
    """
    try:
        response = requests.get(
            f"{ATTACHMENT_STATUS_ENDPOINT}/{task_id}",
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        logger.error("Attachment status check failed for task %s: %s", task_id, exc)
        return {"error": f"Status check failed: {exc}"}


def analyze_body_api(email: Any) -> Dict[str, Any]:
    time.sleep(2)
    return {
        "verdict": "clean",
        "confidence": 0.01,
    }


def analyze_headers_api(email: Any, headers_raw: str | None) -> Dict[str, Any]:
    """Analyse email headers locally for phishing / spoofing indicators.

    Returns ``{"verdict": str, "score": int, "reasons": list[str]}``.
    """
    from services.header_analysis_engine import run_header_analysis

    return run_header_analysis(headers_raw)
