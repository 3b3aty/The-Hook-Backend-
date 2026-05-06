from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import os
import asyncio
import base64
from contextlib import suppress
import hashlib
import json
import re
from datetime import datetime, timezone, timedelta
from email.utils import parseaddr, parsedate_to_datetime
from email.message import EmailMessage
from typing import Any, Dict, List, Optional
import requests
import jwt
from redis.asyncio import Redis

from sqlalchemy.orm import Session
from database import SessionLocal, engine
import models
from message_queue.producer import enqueue_email_analysis
from websocket.manager import manager
from redis_pubsub import REDIS_URL, REDIS_CHANNEL
from dotenv import load_dotenv
load_dotenv()

models.Base.metadata.create_all(bind=engine)

app = FastAPI()
bearer_scheme = HTTPBearer(auto_error=False)


async def _redis_listener() -> None:
    redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(REDIS_CHANNEL)
    try:
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            data = message.get("data")
            if not data:
                continue
            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                continue
            user_id = payload.get("user_id")
            if user_id is None:
                continue
            await manager.send_json(int(user_id), payload)
    except asyncio.CancelledError:
        pass
    finally:
        await pubsub.close()
        await redis_client.close()


@app.on_event("startup")
async def start_redis_listener() -> None:
    app.state.redis_task = asyncio.create_task(_redis_listener())


@app.on_event("shutdown")
async def stop_redis_listener() -> None:
    task = getattr(app.state, "redis_task", None)
    if task:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "")

JWT_SECRET = os.getenv("JWT_SECRET", "")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _decode_base64_url_bytes(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _decode_base64_url(data: str) -> str:
    return _decode_base64_url_bytes(data).decode("utf-8", errors="replace")


def extract_body(payload: Dict[str, Any]) -> str:
    plain_parts: List[str] = []
    html_parts: List[str] = []

    def walk(part: Dict[str, Any]) -> None:
        mime_type = part.get("mimeType", "")
        body = part.get("body", {})
        data = body.get("data")

        if data:
            text = _decode_base64_url(data)
            if mime_type == "text/plain":
                plain_parts.append(text)
            elif mime_type == "text/html":
                html_parts.append(text)

        for child in part.get("parts", []) or []:
            walk(child)

    walk(payload)

    if plain_parts:
        return "\n".join(plain_parts)
    if html_parts:
        return "\n".join(html_parts)

    fallback = payload.get("body", {}).get("data")
    return _decode_base64_url(fallback) if fallback else ""


def extract_headers(headers: List[Dict[str, str]]) -> Dict[str, Optional[str]]:
    header_map: Dict[str, List[str]] = {}
    raw_lines: List[str] = []

    for header in headers:
        name = header.get("name", "")
        value = header.get("value", "")
        if name:
            raw_lines.append(f"{name}: {value}")
            header_map.setdefault(name.lower(), []).append(value)

    auth_results = ", ".join(header_map.get("authentication-results", []))

    def extract_auth_result(key: str) -> Optional[str]:
        if not auth_results:
            return None
        match = re.search(rf"{key}=([a-zA-Z0-9_\-]+)", auth_results)
        return match.group(1) if match else None

    return {
        "raw_headers": "\n".join(raw_lines) if raw_lines else None,
        "return_path": (header_map.get("return-path") or [None])[0],
        "received_chain": "\n".join(header_map.get("received", [])) or None,
        "spf_result": extract_auth_result("spf"),
        "dkim_result": extract_auth_result("dkim"),
        "dmarc_result": extract_auth_result("dmarc"),
    }


def extract_urls(body: str) -> List[str]:
    if not body:
        return []
    urls = re.findall(r"https?://[^\s\"'<>]+", body)
    return list(dict.fromkeys(urls))


def _parse_email_date(headers: List[Dict[str, str]], fallback_ms: Optional[str]) -> Optional[datetime]:
    for header in headers:
        if header.get("name", "").lower() == "date":
            value = header.get("value")
            if value:
                try:
                    return parsedate_to_datetime(value)
                except (TypeError, ValueError):
                    break

    if fallback_ms:
        try:
            return datetime.fromtimestamp(int(fallback_ms) / 1000, tz=timezone.utc)
        except (TypeError, ValueError):
            return None
    return None


def _decode_jwt_token(token: str) -> Dict[str, Any]:
    normalized = token.strip()
    if normalized.lower().startswith("bearer "):
        normalized = normalized[7:]
    try:
        payload = jwt.decode(normalized, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=401, detail="JWT token expired") from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail="Invalid JWT token") from exc

    token_type = payload.get("type")
    if token_type and token_type != "access":
        raise HTTPException(status_code=401, detail="Invalid JWT token type")

    return payload


def _create_jwt_tokens(user_id: int, email: str) -> Dict[str, Any]:
    access_expiry = datetime.now(timezone.utc) + timedelta(minutes=15)
    refresh_expiry = datetime.now(timezone.utc) + timedelta(days=7)

    access_token = jwt.encode(
        {"user_id": user_id, "email": email, "type": "access", "exp": access_expiry},
        JWT_SECRET,
        algorithm="HS256",
    )
    refresh_token = jwt.encode(
        {"user_id": user_id, "email": email, "type": "refresh", "exp": refresh_expiry},
        JWT_SECRET,
        algorithm="HS256",
    )

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "access_expiry": access_expiry,
        "refresh_expiry": refresh_expiry,
    }


def _get_current_user(token: str, db: Session) -> models.User:
    payload = _decode_jwt_token(token)
    user_id = payload.get("user_id") or payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="JWT missing user_id")

    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


def get_current_user_from_auth(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: Session = Depends(get_db),
) -> models.User:
    if not credentials or not credentials.credentials:
        raise HTTPException(status_code=401, detail="Missing JWT token")
    return _get_current_user(credentials.credentials, db)


def _refresh_google_access_token(user: models.User, db: Session) -> str:
    if not user.refresh_token:
        raise HTTPException(status_code=401, detail="Missing Google refresh token")

    token_res = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": user.refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=15,
    )

    if not token_res.ok:
        raise HTTPException(status_code=502, detail="Failed to refresh Google access token")

    payload = token_res.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise HTTPException(status_code=502, detail="Google access token missing in refresh response")

    user.access_token = access_token
    expires_in = payload.get("expires_in")
    if expires_in:
        user.token_expiry = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))

    db.commit()
    return access_token


def _get_valid_google_access_token(user: models.User, db: Session) -> str:
    if not user.access_token:
        return _refresh_google_access_token(user, db)

    if user.token_expiry:
        expiry = user.token_expiry
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        if expiry <= datetime.now(timezone.utc):
            return _refresh_google_access_token(user, db)

    return user.access_token


def _gmail_get_json(user: models.User, db: Session, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    access_token = _get_valid_google_access_token(user, db)
    res = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        params=params,
        timeout=15,
    )

    if res.status_code == 401:
        access_token = _refresh_google_access_token(user, db)
        res = requests.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
            timeout=15,
        )

    if not res.ok:
        raise HTTPException(status_code=502, detail=f"Gmail API error: {res.text}")

    return res.json()


def _collect_attachments(payload: Dict[str, Any], out: List[Dict[str, Any]]) -> None:
    filename = payload.get("filename")
    body = payload.get("body", {})
    attachment_id = body.get("attachmentId")
    if filename and attachment_id:
        out.append(
            {
                "filename": filename,
                "mime_type": payload.get("mimeType"),
                "attachment_id": attachment_id,
                "size": body.get("size"),
            }
        )

    for part in payload.get("parts", []) or []:
        _collect_attachments(part, out)


def _serialize_email(email: models.Email) -> Dict[str, Any]:
    category_name = email.category.name if email.category else None
    return {
        "email_id": email.id,
        "gmail_message_id": email.gmail_message_id,
        "thread_id": email.thread_id,
        "subject": email.subject,
        "snippet": email.body_snippet,
        "body": email.body_full,
        "category": category_name,
        "date": email.date.isoformat() if email.date else None,
        "status": email.status,
        "risk_score": email.risk_score,
        "final_verdict": email.final_verdict,
        "urls_status": email.urls_status,
        "body_status": email.body_status,
        "headers_status": email.headers_status,
        "attachments_status": email.attachments_status,
        "is_read": email.is_read,
        "is_hooked": email.is_hooked,
        "is_trash": email.is_trash,
        "is_starred": email.is_starred,
    }


def _normalize_reasons(value: Any) -> List[str]:
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


def _serialize_urls(rows: List[models.UrlsExtracted]) -> List[Dict[str, Any]]:
    return [
        {
            "url": row.url,
            "verdict": row.verdict,
            "reasons": _normalize_reasons(row.reasons),
            "status": row.status,
        }
        for row in rows
    ]


def _serialize_body(row: Optional[models.BodyClassification]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "verdict": row.verdict,
        "confidence": row.confidence,
        "status": row.status,
    }


def _serialize_headers(row: Optional[models.EmailHeaders]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "verdict": row.verdict,
        "reasons": _normalize_reasons(row.reasons),
        "status": row.status,
    }


def _serialize_attachments(rows: List[models.Attachments]) -> List[Dict[str, Any]]:
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


def _collect_analysis_results(db: Session, email_id: int) -> Dict[str, Any]:
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
        "urls": _serialize_urls(urls),
        "body": _serialize_body(body),
        "headers": _serialize_headers(headers),
        "attachments": _serialize_attachments(attachments),
    }


def _send_email_received(user_id: int, email: models.Email) -> None:
    manager.send_json_sync(
        user_id,
        {
            "type": "email_received",
            "email": _serialize_email(email),
        },
    )


def _extract_category_name(label_ids: List[str]) -> str:
    if "CATEGORY_PROMOTIONS" in label_ids:
        return "Promotions"
    if "CATEGORY_SOCIAL" in label_ids:
        return "Social"
    if "CATEGORY_UPDATES" in label_ids:
        return "Updates"
    return "Primary"


def _get_or_create_category(db: Session, name: str) -> models.Category:
    category = db.query(models.Category).filter(models.Category.name == name).first()
    if not category:
        category = models.Category(name=name)
        db.add(category)
        db.flush()
    return category


def _load_user_emails(db: Session, user_id: int, limit: int = 20) -> List[models.Email]:
    return (
        db.query(models.Email)
        .join(models.Interface, models.Interface.email_id == models.Email.id)
        .filter(models.Interface.receiver_id == user_id)
        .order_by(models.Email.date.is_(None), models.Email.date.desc(), models.Email.created_at.desc())
        .limit(limit)
        .all()
    )


def _fetch_and_store_emails_for_user(
    user: models.User,
    db: Session,
    max_results: int = 50,
    notify_user_id: Optional[int] = None,
    send_each_email: bool = False,
) -> Dict[str, int]:
    list_url = "https://gmail.googleapis.com/gmail/v1/users/me/messages"
    list_payload = _gmail_get_json(
        user,
        db,
        list_url,
        params={"maxResults": max_results},
    )

    messages = list_payload.get("messages", []) or []
    stored = 0
    skipped = 0

    for item in messages:
        message_id = item.get("id")
        if not message_id:
            continue

        existing = db.query(models.Email).filter(models.Email.gmail_message_id == message_id).first()
        if existing:
            skipped += 1
            continue

        message_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}"
        message = _gmail_get_json(user, db, message_url, params={"format": "full"})

        payload = message.get("payload", {})
        headers = payload.get("headers", []) or []

        subject = next(
            (header.get("value") for header in headers if header.get("name", "").lower() == "subject"),
            None,
        )
        from_header = next(
            (header.get("value") for header in headers if header.get("name", "").lower() == "from"),
            "",
        )
        sender_name, sender_email = parseaddr(from_header)

        email_date = _parse_email_date(headers, message.get("internalDate"))
        body_full = extract_body(payload)
        snippet = message.get("snippet")

        labels = message.get("labelIds") or []
        category_name = _extract_category_name(labels)
        category = _get_or_create_category(db, category_name)

        email_record = models.Email(
            gmail_message_id=message_id,
            thread_id=message.get("threadId"),
            subject=subject,
            body_full=body_full,
            body_snippet=snippet,
            labels=None,
            date=email_date,
            category_id=category.id,
            status="PENDING",
            urls_status="PENDING",
            attachments_status="PENDING",
            body_status="PENDING",
            headers_status="PENDING",
        )
        db.add(email_record)
        db.flush()

        sender_user = None
        if sender_email:
            sender_user = db.query(models.User).filter(models.User.email == sender_email).first()
            if not sender_user:
                sender_user = models.User(
                    email=sender_email,
                    name=sender_name or None,
                    provider="external",
                )
                db.add(sender_user)
                db.flush()

        interface_record = models.Interface(
            sender_id=sender_user.id if sender_user else None,
            receiver_id=user.id,
            email_id=email_record.id,
        )
        db.add(interface_record)

        header_info = extract_headers(headers)
        header_record = models.EmailHeaders(
            email_id=email_record.id,
            return_path=header_info.get("return_path"),
            received_chain=header_info.get("received_chain"),
            spf_result=header_info.get("spf_result"),
            dkim_result=header_info.get("dkim_result"),
            dmarc_result=header_info.get("dmarc_result"),
            raw_headers=header_info.get("raw_headers"),
            status="PENDING",
        )
        db.add(header_record)

        urls = extract_urls(body_full)
        for url in urls:
            db.add(
                models.UrlsExtracted(
                    email_id=email_record.id,
                    url=url,
                    status="PENDING",
                )
            )

        attachment_meta: List[Dict[str, Any]] = []
        _collect_attachments(payload, attachment_meta)
        for attachment in attachment_meta:
            attachment_hash = None
            attachment_size = attachment.get("size")

            attachment_id = attachment.get("attachment_id")
            attachment_url = (
                f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}/attachments/{attachment_id}"
            )
            attachment_payload = _gmail_get_json(user, db, attachment_url)
            attachment_data = attachment_payload.get("data")
            if attachment_data:
                blob = _decode_base64_url_bytes(attachment_data)
                attachment_hash = hashlib.sha256(blob).hexdigest()
                if attachment_size is None:
                    attachment_size = len(blob)

            db.add(
                models.Attachments(
                    email_id=email_record.id,
                    file_name=attachment.get("filename"),
                    file_type=attachment.get("mime_type"),
                    file_size=attachment_size,
                    hash_sha256=attachment_hash,
                    status="PENDING",
                )
            )

        email_id = email_record.id
        db.commit()

        enqueue_email_analysis(email_id)
        if send_each_email and notify_user_id is not None:
            _send_email_received(notify_user_id, email_record)
        stored += 1

    user.last_email_sync = datetime.now(timezone.utc)
    db.commit()

    return {"stored": stored, "skipped": skipped, "fetched": len(messages)}


@app.get("/auth/google/login")
def login():
    url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        "&response_type=code"
        "&scope=openid email profile https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/gmail.send"
        "&access_type=offline"
        "&prompt=consent"
    )
    return RedirectResponse(url)


@app.get("/auth/google/callback")
def callback(code: str, db: Session = Depends(get_db)):
    token_res = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": REDIRECT_URI,
        },
    ).json()

    access_token = token_res["access_token"]
    refresh_token = token_res.get("refresh_token")
    expires_in = token_res.get("expires_in")

    user_info = requests.get(
        "https://www.googleapis.com/oauth2/v1/userinfo",
        headers={"Authorization": f"Bearer {access_token}"}
    ).json()

    email = user_info["email"]
    google_user_id = user_info.get("id")
    name = user_info.get("name")
    profile_picture = user_info.get("picture")

    user = db.query(models.User).filter(models.User.email == email).first()

    if not user:
        user = models.User(
            email=email,
            google_user_id=google_user_id,
            name=name,
            profile_picture=profile_picture,
            provider="google",
        )
        db.add(user)
        db.flush()
    else:
        if google_user_id:
            user.google_user_id = google_user_id
        if name:
            user.name = name
        if profile_picture:
            user.profile_picture = profile_picture
        user.provider = "google"

    user.access_token = access_token
    if refresh_token:
        user.refresh_token = refresh_token
    if expires_in:
        user.token_expiry = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))

    tokens = _create_jwt_tokens(user.id, email)
    user.refresh_token_jwt = tokens["refresh_token"]
    user.refresh_token_jwt_expiry = tokens["refresh_expiry"]
    user.last_login = datetime.now(timezone.utc)

    db.commit()

    expires_in = int((tokens["access_expiry"] - datetime.now(timezone.utc)).total_seconds())
    if expires_in < 0:
        expires_in = 0

    return {
        "jwt_token": tokens["access_token"],
        "refresh_token": tokens["refresh_token"],
        "expires_in": expires_in,
        "user": {
            "user_id": str(user.id),
            "google_id": user.google_user_id,
            "name": user.name,
            "email": user.email,
            "photo_url": user.profile_picture,
            "provider": "google",
        },
    }


@app.post("/auth/logout-and-reauth")
def logout_and_reauth(
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    user.access_token = None
    user.refresh_token = None
    user.token_expiry = None
    db.commit()

    return {
        "message": "Tokens cleared. Please go to /auth/google/login to re-authorize with the new permissions.",
        "redirect_url": "/auth/google/login",
    }


@app.post("/emails/send")
def send_email(
    recipients: List[str] = Body(...),
    subject: Optional[str] = Body(None),
    body: Optional[str] = Body(None),
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    if not recipients or not isinstance(recipients, list):
        raise HTTPException(status_code=400, detail="recipients must be a non-empty list of email addresses")

    # Build RFC822 message
    msg = EmailMessage()
    msg["From"] = user.email
    msg["To"] = ", ".join(recipients)
    if subject:
        msg["Subject"] = subject
    msg.set_content(body or "")

    raw_b64 = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    send_url = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
    access_token = _get_valid_google_access_token(user, db)
    headers = {"Authorization": f"Bearer {access_token}"}

    res = requests.post(send_url, headers=headers, json={"raw": raw_b64}, timeout=20)
    
    # Retry on 401 or 403 insufficient scopes
    if res.status_code in (401, 403):
        access_token = _refresh_google_access_token(user, db)
        headers = {"Authorization": f"Bearer {access_token}"}
        res = requests.post(send_url, headers=headers, json={"raw": raw_b64}, timeout=20)

    if not res.ok:
        error_detail = res.text
        # Check if it's a scope issue after retry
        if res.status_code == 403 and "insufficientPermissions" in error_detail:
            error_detail += " | RESOLUTION: Visit https://myaccount.google.com/permissions, revoke this app, then re-login via /auth/google/login"
        raise HTTPException(status_code=502, detail=f"Failed to send email: {error_detail}")

    payload = res.json()
    message_id = payload.get("id")
    thread_id = payload.get("threadId")

    if not message_id:
        raise HTTPException(status_code=502, detail="Gmail did not return message id")

    # Create a single Email record and Interface rows for each recipient
    existing = db.query(models.Email).filter(models.Email.gmail_message_id == message_id).first()
    if existing:
        # add any missing interfaces
        existing_receivers = {iface.receiver.email for iface in existing.interfaces if iface.receiver}
        for r in recipients:
            if r not in existing_receivers:
                r_user = db.query(models.User).filter(models.User.email == r).first()
                if not r_user:
                    r_user = models.User(email=r, provider="external")
                    db.add(r_user)
                    db.flush()
                db.add(models.Interface(sender_id=user.id, receiver_id=r_user.id, email_id=existing.id))
        db.commit()
        return {"email_id": existing.id, "gmail_message_id": message_id, "status": "exists_updated_receivers"}

    category = _get_or_create_category(db, "Primary")
    email_record = models.Email(
        gmail_message_id=message_id,
        thread_id=thread_id,
        subject=subject or "",
        body_full=body or "",
        body_snippet=(body or "")[:200],
        labels=None,
        date=datetime.now(timezone.utc),
        category_id=category.id,
        status="PENDING",
        urls_status="PENDING",
        attachments_status="PENDING",
        body_status="PENDING",
        headers_status="PENDING",
    )
    db.add(email_record)
    db.flush()

    for r in recipients:
        r_user = db.query(models.User).filter(models.User.email == r).first()
        if not r_user:
            r_user = models.User(email=r, provider="external")
            db.add(r_user)
            db.flush()
        db.add(models.Interface(sender_id=user.id, receiver_id=r_user.id, email_id=email_record.id))

    # Minimal header record
    db.add(models.EmailHeaders(email_id=email_record.id, status="PENDING"))

    # extract urls from body and persist
    urls = extract_urls(body or "")
    for u in urls:
        db.add(models.UrlsExtracted(email_id=email_record.id, url=u, status="PENDING"))

    db.commit()

    enqueue_email_analysis(email_record.id)

    return {"email_id": email_record.id, "gmail_message_id": message_id, "status": "sent"}




@app.patch("/emails/{email_id}/read")
def mark_email_read(
    email_id: int,
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    email = (
        db.query(models.Email)
        .join(models.Interface, models.Interface.email_id == models.Email.id)
        .filter(models.Email.id == email_id, models.Interface.receiver_id == user.id)
        .first()
    )
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")

    email.is_read = True
    db.commit()

    return {"email_id": email_id, "is_read": True}


@app.patch("/emails/{email_id}/trash")
def mark_email_trash(
    email_id: int,
    value: bool = True,
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    email = (
        db.query(models.Email)
        .join(models.Interface, models.Interface.email_id == models.Email.id)
        .filter(models.Email.id == email_id, models.Interface.receiver_id == user.id)
        .first()
    )
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")

    email.is_trash = value
    db.commit()

    return {"email_id": email_id, "is_trash": email.is_trash}


@app.patch("/emails/{email_id}/star")
def mark_email_starred(
    email_id: int,
    value: bool = True,
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    email = (
        db.query(models.Email)
        .join(models.Interface, models.Interface.email_id == models.Email.id)
        .filter(models.Email.id == email_id, models.Interface.receiver_id == user.id)
        .first()
    )
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")

    email.is_starred = value
    db.commit()

    return {"email_id": email_id, "is_starred": email.is_starred}


async def _send_initial_batch(user_id: int, max_results: int = 20) -> None:
    def _load_existing() -> Dict[str, Any]:
        db = SessionLocal()
        try:
            user = db.query(models.User).filter(models.User.id == user_id).first()
            if not user:
                return {"emails": [], "should_fetch": False}

            emails = _load_user_emails(db, user_id, max_results)
            return {"emails": [_serialize_email(email) for email in emails], "should_fetch": len(emails) == 0}
        finally:
            db.close()

    payload = await asyncio.to_thread(_load_existing)
    await manager.send_json(user_id, {"type": "initial_emails", "emails": payload["emails"]})

    if payload["should_fetch"]:
        asyncio.create_task(_stream_gmail_emails(user_id, max_results))


async def _stream_gmail_emails(user_id: int, max_results: int = 20) -> None:
    def _sync() -> None:
        db = SessionLocal()
        try:
            user = db.query(models.User).filter(models.User.id == user_id).first()
            if not user:
                return

            _fetch_and_store_emails_for_user(
                user,
                db,
                max_results,
                notify_user_id=user_id,
                send_each_email=True,
            )
        finally:
            db.close()

    await asyncio.to_thread(_sync)


@app.websocket("/ws/emails")
async def websocket_emails(websocket: WebSocket):
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=1008)
        return

    try:
        payload = _decode_jwt_token(token)
    except HTTPException:
        await websocket.close(code=1008)
        return

    user_id = payload.get("user_id") or payload.get("sub")
    if not user_id:
        await websocket.close(code=1008)
        return

    user_id_int = int(user_id)
    await manager.connect(user_id_int, websocket)
    init_task = asyncio.create_task(_send_initial_batch(user_id_int))
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(user_id_int)
    finally:
        init_task.cancel()


@app.websocket("/ws/updates")
async def websocket_updates(websocket: WebSocket):
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=1008)
        return

    try:
        payload = _decode_jwt_token(token)
    except HTTPException:
        await websocket.close(code=1008)
        return

    user_id = payload.get("user_id") or payload.get("sub")
    if not user_id:
        await websocket.close(code=1008)
        return

    user_id_int = int(user_id)
    await manager.connect(user_id_int, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(user_id_int)



