from typing import Any, Dict, List
import time


def analyze_urls_api(email: Any, urls: List[str]) -> Dict[str, Any]:
    time.sleep(10)  
    return {
        "verdict": "clean" if not urls else "suspicious",
        "reasons": ["dummy-url-check"],
    }


def analyze_body_api(email: Any) -> Dict[str, Any]:
    time.sleep(2)
    return {
        "verdict": "clean",
        "confidence": 0.01,
    }


def analyze_headers_api(email: Any, headers_raw: str | None) -> Dict[str, Any]:
    
    return {
        "verdict": "clean",
        "reasons": "dummy-headers-check",
    }


def analyze_attachments_api(email: Any, attachments: List[Dict[str, Any]]) -> Dict[str, Any]:
    time.sleep(40)
    return {
        "verdict": "clean" if not attachments else "suspicious",
        "reasons": ["dummy-attachment-check"],
    }
