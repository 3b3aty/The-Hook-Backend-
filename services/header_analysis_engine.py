"""
Email Header Analysis Engine
Inspects raw email headers for phishing, spoofing, authentication failures,
infrastructure anomalies, and suspicious sender behaviour.
"""

import re
import ipaddress
import unicodedata
import logging
from typing import Any, Dict, List, Optional, Tuple
from email.utils import parseaddr, parsedate_to_datetime
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TRUSTED_BRANDS = {
    "paypal", "apple", "microsoft", "google", "amazon", "facebook", "meta",
    "netflix", "instagram", "whatsapp", "twitter", "linkedin", "dropbox",
    "adobe", "zoom", "slack", "github", "spotify", "uber", "airbnb",
    "chase", "wellsfargo", "bankofamerica", "citibank", "hsbc",
    "dhl", "fedex", "ups", "usps",
    "walmart", "target", "bestbuy", "ebay",
    "yahoo", "aol", "outlook", "icloud",
}

BRAND_DOMAINS: Dict[str, set] = {
    "paypal": {"paypal.com"},
    "apple": {"apple.com", "icloud.com"},
    "microsoft": {"microsoft.com", "outlook.com", "live.com", "hotmail.com"},
    "google": {"google.com", "gmail.com", "googlemail.com"},
    "amazon": {"amazon.com", "amazon.co.uk", "amazon.de", "amazon.fr"},
    "facebook": {"facebook.com", "fb.com", "facebookmail.com"},
    "meta": {"meta.com", "facebook.com", "facebookmail.com"},
    "netflix": {"netflix.com"},
    "instagram": {"instagram.com", "facebookmail.com"},
    "whatsapp": {"whatsapp.com"},
    "twitter": {"twitter.com", "x.com"},
    "linkedin": {"linkedin.com"},
    "dropbox": {"dropbox.com", "dropboxmail.com"},
    "adobe": {"adobe.com"},
    "zoom": {"zoom.us"},
    "slack": {"slack.com"},
    "github": {"github.com"},
    "spotify": {"spotify.com"},
    "uber": {"uber.com"},
    "chase": {"chase.com"},
    "dhl": {"dhl.com"},
    "fedex": {"fedex.com"},
    "ups": {"ups.com"},
    "ebay": {"ebay.com"},
    "yahoo": {"yahoo.com"},
}

SUSPICIOUS_MAILERS = {
    "king phisher", "gophish", "setoolkit", "swaks", "emkei",
    "zeta mailer", "turbomailer", "sendblaster", "mailwizz",
    "atomic mail sender", "gammadyne", "phpmailer 5", "leaf phpmailer",
    "exploit", "hackmail", "anonymousemail",
}

SUSPICIOUS_TLDS = {
    ".tk", ".ml", ".ga", ".cf", ".gq",
    ".top", ".xyz", ".click", ".link", ".work", ".date",
    ".racing", ".win", ".stream", ".review", ".loan",
    ".bid", ".trade", ".webcam", ".download", ".accountant",
    ".science", ".cricket", ".party", ".faith", ".gdn",
    ".zip", ".mov",
}

FREE_EMAIL_PROVIDERS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "mail.com", "protonmail.com", "zoho.com", "yandex.com", "gmx.com",
    "icloud.com", "live.com", "msn.com", "inbox.com", "fastmail.com",
    "tutanota.com", "mailinator.com", "guerrillamail.com", "tempmail.com",
}

KNOWN_TWO_PART_SLDS = {"co", "com", "org", "net", "gov", "edu", "ac"}

IP_RE = re.compile(r"\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b")

# Confusable Unicode → ASCII mappings (subset covering common homoglyphs)
_CONFUSABLES: Dict[str, str] = {
    "\u0430": "a", "\u0435": "e", "\u043e": "o", "\u0440": "p",
    "\u0441": "c", "\u0443": "y", "\u0445": "x", "\u04bb": "h",
    "\u0456": "i", "\u0458": "j", "\u043d": "h", "\u0455": "s",
    "\u0442": "t", "\u0432": "v", "\u043c": "m", "\u043a": "k",
    "\u0251": "a", "\u1d04": "c", "\u0261": "g", "\u026f": "m",
    "\u0270": "m", "\u0280": "r", "\u1d0f": "o",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_raw_headers(raw: str) -> Dict[str, List[str]]:
    """Parse RFC-822 style raw headers into ``{lowercase_name: [values]}``."""
    headers: Dict[str, List[str]] = {}
    current_name: Optional[str] = None
    current_value = ""

    for line in raw.splitlines():
        if not line:
            continue
        # Continuation line
        if line[0] in (" ", "\t"):
            if current_name is not None:
                current_value += " " + line.strip()
            continue
        # Store previous header
        if current_name is not None:
            headers.setdefault(current_name, []).append(current_value)
        # New header
        if ":" in line:
            name, _, value = line.partition(":")
            current_name = name.strip().lower()
            current_value = value.strip()
        else:
            current_name = None
            current_value = ""

    if current_name is not None:
        headers.setdefault(current_name, []).append(current_value)

    return headers


def _get_auth_result(header_map: Dict[str, List[str]], key: str) -> Optional[str]:
    """Extract an authentication result (spf/dkim/dmarc) from headers."""
    # Try Authentication-Results first
    auth_values = header_map.get("authentication-results", [])
    # Also try ARC-Authentication-Results
    auth_values += header_map.get("arc-authentication-results", [])
    combined = " ".join(auth_values)

    if key == "spf":
        for spf_val in header_map.get("received-spf", []):
            low = spf_val.strip().lower()
            for r in ("pass", "fail", "softfail", "neutral", "none", "temperror", "permerror"):
                if low.startswith(r):
                    return r

    pattern = rf"{key}\s*=\s*([a-zA-Z0-9_-]+)"
    match = re.search(pattern, combined, re.IGNORECASE)
    return match.group(1).lower() if match else None


def _extract_domain(addr_str: str) -> Optional[str]:
    """Extract the domain part from an email address or header value."""
    if not addr_str:
        return None
    cleaned = addr_str.strip().strip("<>")
    _, email_addr = parseaddr(cleaned)
    if not email_addr:
        email_addr = cleaned
    if "@" not in email_addr:
        return None
    return email_addr.rsplit("@", 1)[-1].lower().strip()


def _get_base_domain(domain: str) -> str:
    """Return the registrable (base) domain."""
    parts = domain.lower().rstrip(".").split(".")
    if len(parts) >= 3 and parts[-2] in KNOWN_TWO_PART_SLDS:
        return ".".join(parts[-3:])
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return domain


def _domains_related(d1: str, d2: str) -> bool:
    return _get_base_domain(d1) == _get_base_domain(d2)


def _looks_random(s: str) -> bool:
    """Heuristic: a string looks random if it has low vowel ratio and is long."""
    if len(s) < 10:
        return False
    vowels = sum(1 for c in s.lower() if c in "aeiou")
    ratio = vowels / len(s)
    digit_count = sum(1 for c in s if c.isdigit())
    return ratio < 0.15 or digit_count > len(s) * 0.5


def _has_non_ascii(s: str) -> bool:
    try:
        s.encode("ascii")
        return False
    except UnicodeEncodeError:
        return True


def _detect_homograph(domain: str) -> bool:
    """Return True if the domain contains confusable Unicode characters."""
    for char in domain:
        if char in _CONFUSABLES:
            return True
        if ord(char) > 127:
            cat = unicodedata.category(char)
            if cat.startswith("L"):  # Letter categories
                return True
    return False

# ---------------------------------------------------------------------------
# Individual Check Functions
# Each returns (score_delta, [reason_strings])
# ---------------------------------------------------------------------------

def _check_spf(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []
    result = _get_auth_result(hm, "spf")
    if result is None or result == "none":
        score += 30
        reasons.append("SPF record is missing or not configured")
    elif result == "fail":
        score += 30
        reasons.append("SPF validation failed — sender IP not authorized")
    elif result == "softfail":
        score += 15
        reasons.append("SPF soft-fail — sender IP not fully authorized")
    elif result in ("temperror", "permerror"):
        score += 10
        reasons.append(f"SPF check returned error ({result})")
    elif result == "neutral":
        score += 5
        reasons.append("SPF neutral — domain does not assert authorization")
    return score, reasons


def _check_dkim(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []
    result = _get_auth_result(hm, "dkim")
    if result is None or result == "none":
        score += 25
        reasons.append("DKIM signature is missing")
    elif result == "fail":
        score += 25
        reasons.append("DKIM signature verification failed")
    elif result in ("temperror", "permerror"):
        score += 10
        reasons.append(f"DKIM check returned error ({result})")
    return score, reasons


def _check_dmarc(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []
    result = _get_auth_result(hm, "dmarc")
    if result is None or result == "none":
        score += 35
        reasons.append("DMARC policy is missing — no domain alignment enforcement")
    elif result == "fail":
        score += 35
        reasons.append("DMARC validation failed — domain alignment not met")
    elif result in ("temperror", "permerror"):
        score += 10
        reasons.append(f"DMARC check returned error ({result})")
    return score, reasons


def _check_sender_consistency(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []
    from_vals = hm.get("from", [])
    from_domain = _extract_domain(from_vals[0]) if from_vals else None
    if not from_domain:
        return score, reasons

    # Return-Path
    rp_vals = hm.get("return-path", [])
    if rp_vals:
        rp_domain = _extract_domain(rp_vals[0])
        if rp_domain and not _domains_related(from_domain, rp_domain):
            # SendGrid-style bounce addresses are normal — check for known ESPs
            if not any(esp in rp_domain for esp in ("sendgrid", "amazonses", "mailgun", "mandrill", "postmark", "mcsv", "moengage")):
                score += 15
                reasons.append(
                    f"Return-Path domain ({rp_domain}) differs from From domain ({from_domain})"
                )

    # Reply-To
    rt_vals = hm.get("reply-to", [])
    if rt_vals:
        rt_domain = _extract_domain(rt_vals[0])
        if rt_domain and not _domains_related(from_domain, rt_domain):
            score += 20
            reasons.append(
                f"Reply-To domain ({rt_domain}) differs from From domain ({from_domain})"
            )

    # Sender
    s_vals = hm.get("sender", [])
    if s_vals:
        s_domain = _extract_domain(s_vals[0])
        if s_domain and not _domains_related(from_domain, s_domain):
            score += 15
            reasons.append(
                f"Sender header domain ({s_domain}) differs from From domain ({from_domain})"
            )

    return score, reasons


def _check_display_name_spoofing(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []
    from_vals = hm.get("from", [])
    if not from_vals:
        return score, reasons

    display_name, email_addr = parseaddr(from_vals[0])
    if not display_name or not email_addr or "@" not in email_addr:
        return score, reasons

    display_lower = display_name.lower()
    email_domain = email_addr.rsplit("@", 1)[-1].lower()

    for brand in TRUSTED_BRANDS:
        if brand not in display_lower:
            continue
        expected = BRAND_DOMAINS.get(brand, set())
        domain_ok = False
        if expected:
            domain_ok = any(_domains_related(email_domain, ed) for ed in expected)
        if not domain_ok and brand not in _get_base_domain(email_domain):
            score += 25
            reasons.append(
                f"Display name contains '{brand}' but email domain is '{email_domain}' "
                f"— possible brand impersonation"
            )
            break
    return score, reasons


def _check_received_headers(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []
    received = hm.get("received", [])
    if not received:
        score += 10
        reasons.append("No Received headers found — possible header stripping")
        return score, reasons

    for i, recv in enumerate(received):
        recv_lower = recv.lower()

        # Extract IPv4s
        ips = IP_RE.findall(recv)
        for ip_str in ips:
            try:
                ip_obj = ipaddress.ip_address(ip_str)
                # Only flag private IPs in outermost hop (i==0 = topmost Received)
                if ip_obj.is_private and i == 0 and "from" in recv_lower:
                    score += 10
                    reasons.append(f"Private IP ({ip_str}) in outermost Received header")
                    break
            except ValueError:
                continue

        # Malformed: missing both 'from' and 'by'
        if "from" not in recv_lower and "by" not in recv_lower and recv.strip():
            score += 5
            reasons.append("Malformed Received header (missing 'from'/'by' clauses)")

    if len(received) > 15:
        score += 10
        reasons.append(f"Excessive mail routing detected ({len(received)} hops)")

    return score, reasons


def _check_x_headers(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []

    # X-Originating-IP
    for val in hm.get("x-originating-ip", []):
        ip_str = val.strip().strip("[]")
        try:
            ip_obj = ipaddress.ip_address(ip_str)
            if ip_obj.is_private:
                score += 10
                reasons.append(f"X-Originating-IP is a private address ({ip_str})")
        except ValueError:
            pass

    # X-Mailer
    for val in hm.get("x-mailer", []):
        mailer_low = val.lower()
        for sus in SUSPICIOUS_MAILERS:
            if sus in mailer_low:
                score += 25
                reasons.append(f"Suspicious X-Mailer detected: {val}")
                break

    # X-Priority (1 = highest → urgency tactic)
    for val in hm.get("x-priority", []):
        try:
            prio = int(val.strip().split()[0])
            if prio == 1:
                score += 5
                reasons.append("Email marked as highest priority (X-Priority: 1) — urgency tactic")
        except (ValueError, IndexError):
            pass

    # X-Spam-Status
    for val in hm.get("x-spam-status", []):
        if val.strip().lower().startswith("yes"):
            score += 15
            reasons.append("Email flagged as spam by upstream filter (X-Spam-Status: Yes)")

    return score, reasons


def _check_mime_encoding(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []
    ct_vals = hm.get("content-type", [])
    cte_vals = hm.get("content-transfer-encoding", [])

    ct_lower = ct_vals[0].lower() if ct_vals else ""

    if cte_vals:
        enc = cte_vals[0].strip().lower()
        if enc == "base64" and "text/html" in ct_lower:
            score += 5
            reasons.append("HTML content encoded with base64 — may hide malicious content")
        elif enc not in ("7bit", "8bit", "quoted-printable", "base64", "binary"):
            score += 10
            reasons.append(f"Unusual Content-Transfer-Encoding: {cte_vals[0]}")

    # Unusual charsets
    unusual_charsets = ("koi8-r", "windows-1251", "gb2312", "gbk", "big5", "iso-2022-jp")
    for cs in unusual_charsets:
        if cs in ct_lower:
            score += 3
            reasons.append(f"Unusual character encoding detected: {cs}")
            break

    return score, reasons


def _check_time_analysis(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []
    date_vals = hm.get("date", [])
    if not date_vals:
        score += 5
        reasons.append("Missing Date header")
        return score, reasons

    try:
        email_date = parsedate_to_datetime(date_vals[0])
    except (TypeError, ValueError):
        score += 10
        reasons.append("Malformed Date header — cannot parse timestamp")
        return score, reasons

    now = datetime.now(timezone.utc)
    if email_date.tzinfo is None:
        email_date = email_date.replace(tzinfo=timezone.utc)

    # Future date
    if email_date > now + timedelta(hours=1):
        score += 15
        reasons.append("Email Date header is set in the future")

    # Very old date
    if email_date < now - timedelta(days=365):
        score += 10
        reasons.append("Email Date header is more than 1 year in the past")

    # Parse Received timestamps to detect impossible flow
    recv_timestamps: List[datetime] = []
    for recv in hm.get("received", []):
        m = re.search(r";\s*(.+?)(?:\s*\(.*?\))?\s*$", recv)
        if m:
            try:
                dt = parsedate_to_datetime(m.group(1).strip())
                recv_timestamps.append(dt)
            except (TypeError, ValueError):
                continue

    if recv_timestamps:
        latest = max(recv_timestamps)
        earliest = min(recv_timestamps)
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        if earliest.tzinfo is None:
            earliest = earliest.replace(tzinfo=timezone.utc)

        gap = abs((latest - email_date).total_seconds())
        if gap > 7 * 86400:
            score += 10
            reasons.append("Large time discrepancy between Date header and Received timestamps")

        # Internal hop time check — if any hop took > 24 h, flag
        sorted_ts = sorted(recv_timestamps)
        for j in range(1, len(sorted_ts)):
            hop_gap = abs((sorted_ts[j] - sorted_ts[j - 1]).total_seconds())
            if hop_gap > 86400:
                score += 5
                reasons.append("Unusually long delay between mail relay hops")
                break

    return score, reasons


def _check_domain_reputation(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []
    from_vals = hm.get("from", [])
    if not from_vals:
        return score, reasons

    display_name, email_addr = parseaddr(from_vals[0])
    if not email_addr or "@" not in email_addr:
        return score, reasons

    domain = email_addr.rsplit("@", 1)[-1].lower()

    # Suspicious TLD
    for tld in SUSPICIOUS_TLDS:
        if domain.endswith(tld):
            score += 10
            reasons.append(f"Sender uses suspicious TLD: {tld}")
            break

    # Free email provider with organizational display name
    if domain in FREE_EMAIL_PROVIDERS and display_name:
        org_words = (
            "inc", "ltd", "llc", "corp", "company", "bank", "support",
            "billing", "security", "admin", "helpdesk", "service",
            "official", "team", "department",
        )
        dl = display_name.lower()
        for w in org_words:
            if w in dl:
                score += 15
                reasons.append(
                    f"Free email provider ({domain}) used with organizational display name '{display_name}'"
                )
                break

    # Excessive subdomains
    parts = domain.split(".")
    if len(parts) > 3:
        score += 5
        reasons.append(f"Sender domain has excessive subdomains: {domain}")

    # Random-looking domain
    base_part = parts[0] if len(parts) >= 2 else domain
    if _looks_random(base_part):
        score += 10
        reasons.append(f"Sender domain appears randomly generated: {domain}")

    return score, reasons


def _check_homograph(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []
    from_vals = hm.get("from", [])
    if not from_vals:
        return score, reasons

    _, email_addr = parseaddr(from_vals[0])
    if not email_addr or "@" not in email_addr:
        return score, reasons

    domain = email_addr.rsplit("@", 1)[-1]

    if _has_non_ascii(domain) and _detect_homograph(domain):
        score += 40
        reasons.append(
            f"Homograph/Unicode spoofing detected in sender domain: {domain}"
        )

    # Also check Reply-To domain
    for val in hm.get("reply-to", []):
        rt_domain = _extract_domain(val)
        if rt_domain and _has_non_ascii(rt_domain) and _detect_homograph(rt_domain):
            score += 40
            reasons.append(f"Homograph/Unicode spoofing detected in Reply-To domain: {rt_domain}")
            break

    return score, reasons


def _check_missing_security_headers(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []
    has_auth = bool(hm.get("authentication-results") or hm.get("arc-authentication-results"))
    has_dkim_sig = bool(hm.get("dkim-signature"))
    has_received_spf = bool(hm.get("received-spf"))

    missing = []
    if not has_auth and not has_received_spf:
        missing.append("Authentication-Results / Received-SPF")
    if not has_dkim_sig:
        missing.append("DKIM-Signature")

    if missing:
        score += 15
        reasons.append(f"Missing security headers: {', '.join(missing)}")

    return score, reasons


def _check_message_id(hm: Dict[str, List[str]]) -> Tuple[int, List[str]]:
    score, reasons = 0, []
    mid_vals = hm.get("message-id", [])

    if not mid_vals:
        score += 15
        reasons.append("Missing Message-ID header")
        return score, reasons

    mid = mid_vals[0].strip().strip("<>")

    # Message-ID should contain '@'
    if "@" not in mid:
        score += 15
        reasons.append(f"Suspicious Message-ID format (missing '@'): {mid}")
        return score, reasons

    # Check domain portion
    mid_domain = mid.rsplit("@", 1)[-1].lower()
    from_vals = hm.get("from", [])
    if from_vals:
        from_domain = _extract_domain(from_vals[0])
        # It's common for ESPs to use their own domain in Message-ID,
        # so only flag if the domain looks really suspicious
        if from_domain and _has_non_ascii(mid_domain):
            score += 15
            reasons.append(f"Message-ID domain contains non-ASCII characters: {mid_domain}")

    return score, reasons


# ---------------------------------------------------------------------------
# Main Analysis Entry Point
# ---------------------------------------------------------------------------

_ALL_CHECKS = [
    _check_spf,
    _check_dkim,
    _check_dmarc,
    _check_sender_consistency,
    _check_display_name_spoofing,
    _check_received_headers,
    _check_x_headers,
    _check_mime_encoding,
    _check_time_analysis,
    _check_domain_reputation,
    _check_homograph,
    _check_missing_security_headers,
    _check_message_id,
]


def run_header_analysis(headers_raw: str | None) -> Dict[str, Any]:
    """Analyse raw email headers and return verdict, score, and reasons.

    Returns
    -------
    dict  –  ``{"verdict": str, "score": int, "reasons": list[str]}``
    """
    if not headers_raw or not headers_raw.strip():
        return {
            "verdict": "suspicious",
            "score": 15,
            "reasons": ["No headers available for analysis"],
        }

    header_map = _parse_raw_headers(headers_raw)

    total_score = 0
    all_reasons: List[str] = []

    for check_fn in _ALL_CHECKS:
        try:
            delta, reasons = check_fn(header_map)
            total_score += delta
            all_reasons.extend(reasons)
        except Exception:
            logger.exception("Header check %s raised an error", check_fn.__name__)

    # Cap
    total_score = min(total_score, 100)

    # Verdict
    if total_score >= 50:
        verdict = "phishing"
    elif total_score >= 20:
        verdict = "suspicious"
    else:
        verdict = "legitimate"

    return {
        "verdict": verdict,
        "score": total_score,
        "reasons": all_reasons,
    }
