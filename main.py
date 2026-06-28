from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Body, Request, Query, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import os
import asyncio
import base64
from contextlib import suppress
import hashlib
import html
import json
import mimetypes
import re
import shutil
from datetime import datetime, timezone, timedelta
from email.utils import parseaddr, parsedate_to_datetime
from email.message import EmailMessage
from typing import Any, Dict, List, Optional, Union
import requests
from pydantic import BaseModel, Field
from urllib.parse import urlencode
import jwt
from redis.asyncio import Redis
from google.cloud import pubsub_v1
import threading
import logging

from sqlalchemy import or_
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

# Pub/Sub config (set GCP_PUBSUB_SUBSCRIPTION to like "projects/<project>/subscriptions/<sub>")
GCP_PUBSUB_SUBSCRIPTION = os.getenv("GCP_PUBSUB_SUBSCRIPTION", "")
GCP_PUBSUB_TOPIC = os.getenv("GCP_PUBSUB_TOPIC", "")
logger = logging.getLogger(__name__)


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


def _process_pubsub_notification(email_address: Optional[str], history_id: Optional[str]) -> None:
    """Run blocking fetch/store in a worker thread for a given email address."""
    db = SessionLocal()
    try:
        if not email_address:
            return
        user = db.query(models.User).filter(
            models.User.email == email_address).first()
        if not user:
            logger.info("Pub/Sub: user not found for email %s", email_address)
            return

        try:
            if history_id and user.last_history_id:
                try:
                    history_payload = _gmail_get_history(user, db, user.last_history_id)
                    records = history_payload.get("history", []) or []
                    if records:
                        _process_gmail_history_records(
                            user, db, records, notify_user_id=user.id
                        )
                    user.last_history_id = history_id
                    db.commit()
                    return
                except HTTPException as exc:
                    logger.warning(
                        "Gmail history sync failed for %s: %s",
                        email_address,
                        exc.detail,
                    )

            _fetch_and_store_emails_for_user(
                user,
                db,
                max_results=50,
                notify_user_id=user.id,
                send_each_email=True,
            )
            if history_id:
                user.last_history_id = history_id
            else:
                try:
                    profile = _gmail_get_profile(user, db)
                    if profile.get("historyId"):
                        user.last_history_id = profile.get("historyId")
                except Exception:
                    pass
            db.commit()
        except Exception:
            logger.exception(
                "Error fetching/storing emails for user %s", email_address)
    finally:
        db.close()


def _pubsub_callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """Callback for Pub/Sub messages from Gmail push notifications."""
    try:
        data = message.data.decode("utf-8")
        payload = json.loads(data)
    except Exception:
        logger.exception("Failed to decode Pub/Sub message")
        message.ack()
        return

    logger.info("Pub/Sub notification received: %s", payload)

    email_address = payload.get("emailAddress") or payload.get(
        "email") or payload.get("userId")
    history_id = payload.get("historyId")

    # Offload the heavy work to a thread to avoid blocking the Pub/Sub threads
    t = threading.Thread(target=_process_pubsub_notification, args=(
        email_address, history_id), daemon=True)
    t.start()

    message.ack()


async def _pubsub_listener() -> None:
    """Start a Google Pub/Sub subscriber and block until cancelled."""
    if not GCP_PUBSUB_SUBSCRIPTION:
        logger.info(
            "GCP_PUBSUB_SUBSCRIPTION not set; skipping Pub/Sub listener")
        return

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = GCP_PUBSUB_SUBSCRIPTION

    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=_pubsub_callback)
    app.state.pubsub_subscriber = subscriber
    app.state.pubsub_future = streaming_pull_future

    try:
        # This will block until the future is done/cancelled.
        await asyncio.to_thread(streaming_pull_future.result)
    except Exception:
        logger.exception("Pub/Sub listener stopped with exception")
    finally:
        try:
            streaming_pull_future.cancel()
        except Exception:
            pass
        try:
            subscriber.close()
        except Exception:
            pass


@app.post("/pubsub/push", tags = ['PubSub'])
async def pubsub_push(request: Request):
    """Endpoint to receive Pub/Sub push messages (no local GCP credentials required).

    Expected body format (Pub/Sub push): {"message": {"data": "<base64>", ...}, "subscription": "..."}
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    message = body.get("message") or {}

    data_b64 = message.get("data")
    payload = None
    if data_b64:
        try:
            decoded = base64.b64decode(data_b64).decode("utf-8")
            payload = json.loads(decoded)
        except Exception:
            # Not JSON inside data, try to treat as plain string
            try:
                payload = json.loads(data_b64)
            except Exception:
                payload = {"raw": decoded}
    else:
        # If no data, try attributes
        payload = message.get("attributes") or {}

    # Extract common fields and offload processing
    email_address = None
    history_id = None
    if isinstance(payload, dict):
        email_address = payload.get("emailAddress") or payload.get(
            "email") or payload.get("userId")
        history_id = payload.get("historyId")

    # Spawn thread to reuse existing sync logic
    t = threading.Thread(target=_process_pubsub_notification, args=(
        email_address, history_id), daemon=True)
    t.start()

    return {"status": "accepted"}


def _create_gmail_watch_for_user(user: models.User, db: Session) -> None:
    """Create a Gmail watch for a specific user using their credentials."""
    if not GCP_PUBSUB_TOPIC:
        logger.info("GCP_PUBSUB_TOPIC not set; skipping Gmail watch creation")
        return

    try:
        access_token = _get_valid_google_access_token(user, db)
    except Exception:
        logger.exception(
            "Failed to obtain access token for user %s", user.email)
        return

    url = "https://gmail.googleapis.com/gmail/v1/users/me/watch"
    body = {"topicName": GCP_PUBSUB_TOPIC, "labelIds": ["INBOX"]}
    try:
        res = requests.post(
            url, headers={"Authorization": f"Bearer {access_token}"}, json=body, timeout=15)
        if not res.ok:
            logger.warning(
                "Failed to create Gmail watch for %s: %s", user.email, res.text)
        else:
            logger.info("Gmail watch created for %s", user.email)
    except Exception:
        logger.exception(
            "Exception while creating Gmail watch for %s", user.email)


async def _ensure_watches_for_all_users() -> None:
    """Ensure Gmail watch is created for every Google user with tokens."""
    if not GCP_PUBSUB_TOPIC:
        logger.info("GCP_PUBSUB_TOPIC not set; skipping ensure watches")
        return

    def _sync():
        db = SessionLocal()
        try:
            users = db.query(models.User).filter(
                models.User.provider == "google").all()
            for u in users:
                if u.refresh_token or u.access_token:
                    _create_gmail_watch_for_user(u, db)
        finally:
            db.close()

    await asyncio.to_thread(_sync)


@app.on_event("startup")
async def start_redis_listener() -> None:
    app.state.redis_task = asyncio.create_task(_redis_listener())
    # Start Pub/Sub listener if configured
    app.state.pubsub_task = asyncio.create_task(_pubsub_listener())
    # Ensure Gmail watches exist for all google users
    app.state.gmail_watch_task = asyncio.create_task(
        _ensure_watches_for_all_users())


@app.on_event("shutdown")
async def stop_redis_listener() -> None:
    task = getattr(app.state, "redis_task", None)
    if task:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    # Stop Pub/Sub listener
    pubsub_future = getattr(app.state, "pubsub_future", None)
    if pubsub_future:
        try:
            pubsub_future.cancel()
        except Exception:
            pass
    pubsub_task = getattr(app.state, "pubsub_task", None)
    if pubsub_task:
        pubsub_task.cancel()
        with suppress(asyncio.CancelledError):
            await pubsub_task
    gmail_watch_task = getattr(app.state, "gmail_watch_task", None)
    if gmail_watch_task:
        gmail_watch_task.cancel()
        with suppress(asyncio.CancelledError):
            await gmail_watch_task

CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "")

JWT_SECRET = os.getenv("JWT_SECRET", "")

ATTACHMENTS_DIR = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "attachments_store")


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

    if html_parts:
        return "\n".join(html_parts)
    if plain_parts:
        return "\n".join(plain_parts)

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
        raise HTTPException(
            status_code=401, detail="JWT token expired") from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=401, detail="Invalid JWT token") from exc

    token_type = payload.get("type")
    if token_type and token_type != "access":
        raise HTTPException(status_code=401, detail="Invalid JWT token type")

    return payload


def _create_jwt_tokens(user_id: int, email: str) -> Dict[str, Any]:
    access_expiry = datetime.now(timezone.utc) + timedelta(days=7)
    refresh_expiry = datetime.now(timezone.utc) + timedelta(days=7)

    access_token = jwt.encode(
        {"user_id": user_id, "email": email, "type": "access", "exp": access_expiry},
        JWT_SECRET,
        algorithm="HS256",
    )
    refresh_token = jwt.encode(
        {"user_id": user_id, "email": email,
            "type": "refresh", "exp": refresh_expiry},
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
        raise HTTPException(
            status_code=401, detail="Missing Google refresh token")

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
        raise HTTPException(
            status_code=502, detail="Failed to refresh Google access token")

    payload = token_res.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise HTTPException(
            status_code=502, detail="Google access token missing in refresh response")

    user.access_token = access_token
    expires_in = payload.get("expires_in")
    if expires_in:
        user.token_expiry = datetime.now(
            timezone.utc) + timedelta(seconds=int(expires_in))

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
        raise HTTPException(
            status_code=502, detail=f"Gmail API error: {res.text}")

    return res.json()


def _gmail_get_profile(user: models.User, db: Session) -> Dict[str, Any]:
    profile_url = "https://gmail.googleapis.com/gmail/v1/users/me/profile"
    return _gmail_get_json(user, db, profile_url)


def _gmail_get_history(user: models.User, db: Session, start_history_id: str) -> Dict[str, Any]:
    history_url = "https://gmail.googleapis.com/gmail/v1/users/me/history"
    return _gmail_get_json(
        user,
        db,
        history_url,
        params={"startHistoryId": start_history_id},
    )


def _delete_local_email(db: Session, email: models.Email) -> None:
    attachment_dir = os.path.join(ATTACHMENTS_DIR, str(email.id))

    db.query(models.StaticAnalysis).filter(
        models.StaticAnalysis.attach_id.in_(
            db.query(models.Attachments.id).filter(
                models.Attachments.email_id == email.id)
        )
    ).delete(synchronize_session=False)
    db.query(models.DynamicAnalysis).filter(
        models.DynamicAnalysis.attach_id.in_(
            db.query(models.Attachments.id).filter(
                models.Attachments.email_id == email.id)
        )
    ).delete(synchronize_session=False)
    db.query(models.AnalysisTask).filter(
        models.AnalysisTask.email_id == email.id).delete(synchronize_session=False)
    db.query(models.BodyClassification).filter(
        models.BodyClassification.email_id == email.id).delete(synchronize_session=False)
    db.query(models.EmailHeaders).filter(
        models.EmailHeaders.email_id == email.id).delete(synchronize_session=False)
    db.query(models.UrlsExtracted).filter(
        models.UrlsExtracted.email_id == email.id).delete(synchronize_session=False)
    db.query(models.EmailDeadline).filter(
        models.EmailDeadline.email_id == email.id).delete(synchronize_session=False)
    db.query(models.UserAction).filter(
        models.UserAction.email_id == email.id).delete(synchronize_session=False)
    db.query(models.EmailLabel).filter(
        models.EmailLabel.email_id == email.id).delete(synchronize_session=False)
    db.query(models.Interface).filter(
        models.Interface.email_id == email.id).delete(synchronize_session=False)
    db.query(models.Attachments).filter(
        models.Attachments.email_id == email.id).delete(synchronize_session=False)
    db.delete(email)
    if os.path.isdir(attachment_dir):
        shutil.rmtree(attachment_dir, ignore_errors=True)


def _create_email_from_gmail_message(
    user: models.User,
    db: Session,
    message: Dict[str, Any],
    notify_user_id: Optional[int] = None,
    send_each_email: bool = False,
) -> Optional[models.Email]:
    message_id = message.get("id")
    if not message_id:
        return None

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
    to_header = next(
        (header.get("value") for header in headers if header.get("name", "").lower() == "to"),
        "",
    )
    sender_name, sender_email = parseaddr(from_header)
    receiver_name, receiver_email = parseaddr(to_header)

    email_date = _parse_email_date(headers, message.get("internalDate"))
    body_full = extract_body(payload)
    snippet = message.get("snippet")

    labels = message.get("labelIds") or []
    category_name = _extract_category_name(labels)
    category = _get_or_create_category(db, category_name)

    is_starred = "STARRED" in labels
    is_trash = "TRASH" in labels
    is_read = "UNREAD" not in labels

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
        delivery_status="sent",
        urls_status="PENDING",
        attachments_status="PENDING",
        body_status="PENDING",
        headers_status="PENDING",
        is_starred=is_starred,
        is_trash=is_trash,
        is_read=is_read,
    )
    db.add(email_record)
    db.flush()

    def _get_or_create_external_user(email_addr: str, display_name: str) -> Optional[models.User]:
        if not email_addr:
            return None
        found = db.query(models.User).filter(models.User.email == email_addr).first()
        if not found:
            found = models.User(
                email=email_addr,
                name=display_name or None,
                provider="external",
            )
            db.add(found)
            db.flush()
        return found

    sender_user = _get_or_create_external_user(sender_email, sender_name)
    receiver_user = _get_or_create_external_user(receiver_email, receiver_name)

    interface_record = models.Interface(
        sender_id=sender_user.id if sender_user else None,
        receiver_id=receiver_user.id if receiver_user else None,
        email_id=email_record.id,
    )
    db.add(interface_record)

    _apply_label_rules_to_email(
        db,
        receiver_id=receiver_user.id if receiver_user else None,
        sender_id=sender_user.id if sender_user else None,
        email_id=email_record.id,
    )

    header_info = extract_headers(headers)
    db.add(
        models.EmailHeaders(
            email_id=email_record.id,
            return_path=header_info.get("return_path"),
            received_chain=header_info.get("received_chain"),
            spf_result=header_info.get("spf_result"),
            dkim_result=header_info.get("dkim_result"),
            dmarc_result=header_info.get("dmarc_result"),
            raw_headers=header_info.get("raw_headers"),
            status="PENDING",
        )
    )

    urls = extract_urls(body_full)
    for url in urls:
        db.add(models.UrlsExtracted(email_id=email_record.id, url=url, status="PENDING"))

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
        file_path = None
        if attachment_data:
            blob = _decode_base64_url_bytes(attachment_data)
            attachment_hash = hashlib.sha256(blob).hexdigest()
            if attachment_size is None:
                attachment_size = len(blob)

            email_att_dir = os.path.join(ATTACHMENTS_DIR, str(email_record.id))
            os.makedirs(email_att_dir, exist_ok=True)
            file_path = os.path.join(email_att_dir, attachment.get("filename", "unknown"))
            with open(file_path, "wb") as f:
                f.write(blob)

        db.add(
            models.Attachments(
                email_id=email_record.id,
                file_name=attachment.get("filename"),
                file_type=attachment.get("mime_type"),
                file_size=attachment_size,
                hash_sha256=attachment_hash,
                file_url=file_path,
                status="PENDING",
            )
        )

    db.commit()

    enqueue_email_analysis(email_record.id)
    if send_each_email and notify_user_id is not None:
        _send_email_received(db, notify_user_id, email_record)

    return email_record


def _sync_gmail_message_state(
    user: models.User,
    db: Session,
    message: Dict[str, Any],
    notify_user_id: Optional[int] = None,
) -> bool:
    message_id = message.get("id")
    if not message_id:
        return False

    if message.get("labelIds") is None or not message.get("payload"):
        message_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}"
        message = _gmail_get_json(user, db, message_url, params={"format": "full"})

    existing = db.query(models.Email).filter(models.Email.gmail_message_id == message_id).first()
    if existing:
        return _sync_existing_email_with_gmail_message(
            db, existing, message, notify_user_id=notify_user_id
        )

    created = _create_email_from_gmail_message(
        user,
        db,
        message,
        notify_user_id=notify_user_id,
        send_each_email=True,
    )
    return created is not None


def _process_gmail_history_records(
    user: models.User,
    db: Session,
    records: List[Dict[str, Any]],
    notify_user_id: Optional[int] = None,
) -> int:
    updated = 0
    for record in records:
        for message_data in (
            record.get("messages", [])
            + record.get("messagesAdded", [])
            + record.get("messagesRemoved", [])
        ):
            if _sync_gmail_message_state(user, db, message_data, notify_user_id=notify_user_id):
                updated += 1

        for label_change in (
            record.get("labelsAdded", []) + record.get("labelsRemoved", [])
        ):
            message_payload = label_change.get("message") if isinstance(label_change, dict) else None
            if message_payload and _sync_gmail_message_state(
                user, db, message_payload, notify_user_id=notify_user_id
            ):
                updated += 1

        for deleted in record.get("messagesDeleted", []):
            message_payload = deleted.get("message") if isinstance(deleted, dict) else deleted
            if not isinstance(message_payload, dict):
                continue
            message_id = message_payload.get("id")
            if not message_id:
                continue
            existing = db.query(models.Email).filter(
                models.Email.gmail_message_id == message_id
            ).first()
            if existing:
                _delete_local_email(db, existing)
                updated += 1

    return updated


def _gmail_trash_message(user: models.User, db: Session, message_id: str) -> None:
    trash_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}/trash"
    access_token = _get_valid_google_access_token(user, db)
    res = requests.post(
        trash_url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )

    if res.status_code == 401:
        access_token = _refresh_google_access_token(user, db)
        res = requests.post(
            trash_url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )

    if not res.ok:
        detail = res.text
        if res.status_code == 403 and "insufficientPermissions" in detail:
            detail += " | RESOLUTION: Visit https://myaccount.google.com/permissions, revoke this app, then re-login via /auth/google/login"
        raise HTTPException(
            status_code=502, detail=f"Failed to trash Gmail message: {detail}")


def _gmail_delete_message(user: models.User, db: Session, message_id: str) -> None:
    delete_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}"
    access_token = _get_valid_google_access_token(user, db)
    res = requests.delete(
        delete_url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )

    if res.status_code == 401:
        access_token = _refresh_google_access_token(user, db)
        res = requests.delete(
            delete_url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )

    if not res.ok:
        detail = res.text
        if res.status_code == 403 and "insufficientPermissions" in detail:
            detail += " | RESOLUTION: Visit https://myaccount.google.com/permissions, revoke this app, then re-login via /auth/google/login"
        raise HTTPException(
            status_code=502, detail=f"Failed to delete Gmail message: {detail}")


def _gmail_modify_message_labels(
    user: models.User,
    db: Session,
    message_id: str,
    add_labels: Optional[List[str]] = None,
    remove_labels: Optional[List[str]] = None,
) -> None:
    modify_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}/modify"
    access_token = _get_valid_google_access_token(user, db)
    payload = {}
    if add_labels is not None:
        payload["addLabelIds"] = add_labels
    if remove_labels is not None:
        payload["removeLabelIds"] = remove_labels

    res = requests.post(
        modify_url,
        headers={"Authorization": f"Bearer {access_token}"},
        json=payload,
        timeout=15,
    )

    if res.status_code == 401:
        access_token = _refresh_google_access_token(user, db)
        res = requests.post(
            modify_url,
            headers={"Authorization": f"Bearer {access_token}"},
            json=payload,
            timeout=15,
        )

    if not res.ok:
        detail = res.text
        if res.status_code == 403 and "insufficientPermissions" in detail:
            detail += " | RESOLUTION: Visit https://myaccount.google.com/permissions, revoke this app, then re-login via /auth/google/login"
        raise HTTPException(
            status_code=502, detail=f"Failed to modify Gmail labels: {detail}")


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


def clean_email_body(raw: str) -> str:
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


def _serialize_email(email: models.Email) -> Dict[str, Any]:
    category_name = email.category.name if email.category else None
    body_full = email.body_full or ""
    body_clean = clean_email_body(body_full)
    date_epoch_ms = int(email.date.timestamp() * 1000) if email.date else None
    return {
        "email_id": email.id,
        "gmail_message_id": email.gmail_message_id,
        "thread_id": email.thread_id,
        "subject": email.subject,
        "snippet": email.body_snippet,
        "body": email.body_full,
        "body_clean": body_clean,
        "category": category_name,
        "date": email.date.isoformat() if email.date else None,
        "date_epoch_ms": date_epoch_ms,
        "status": email.status,
        "delivery_status": email.delivery_status,
        "risk_score": email.risk_score,
        "final_verdict": email.final_verdict,
        "user_act": email.user_act,
        "urls_status": email.urls_status,
        "body_status": email.body_status,
        "headers_status": email.headers_status,
        "attachments_status": email.attachments_status,
        "is_read": email.is_read,
        "is_hooked": email.is_hooked,
        "is_trash": email.is_trash,
        "is_starred": email.is_starred,
    }


def _serialize_sender(sender: Optional[models.User]) -> Optional[Dict[str, Any]]:
    if not sender:
        return None
    return {
        "user_id": sender.id,
        "email": sender.email,
        "name": sender.name,
        "photo_url": sender.profile_picture,
        "provider": sender.provider,
    }


def _get_sender_for_email(db: Session, email_id: int, receiver_id: int) -> Optional[models.User]:
    return (
        db.query(models.User)
        .join(models.Interface, models.Interface.sender_id == models.User.id)
        .filter(models.Interface.email_id == email_id, models.Interface.receiver_id == receiver_id)
        .first()
    )


def _serialize_email_for_user(db: Session, email: models.Email, receiver_id: int) -> Dict[str, Any]:
    payload = _serialize_email(email)
    sender = _get_sender_for_email(db, email.id, receiver_id)
    payload["sender"] = _serialize_sender(sender)
    return payload


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
        "score": row.score,
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
    urls = db.query(models.UrlsExtracted).filter(
        models.UrlsExtracted.email_id == email_id).all()
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


def _send_email_received(db: Session, user_id: int, email: models.Email) -> None:
    manager.send_json_sync(
        user_id,
        {
            "type": "email_received",
            "email": _serialize_email_for_user(db, email, user_id),
        },
    )


def _send_email_updated(db: Session, user_id: int, email: models.Email) -> None:
    manager.send_json_sync(
        user_id,
        {
            "type": "email_updated",
            "email": _serialize_email_for_user(db, email, user_id),
        },
    )


def _sync_existing_email_with_gmail_message(
    db: Session,
    email_record: models.Email,
    message: Dict[str, Any],
    notify_user_id: Optional[int] = None,
) -> bool:
    labels = message.get("labelIds") or []
    is_starred = "STARRED" in labels
    is_trash = "TRASH" in labels
    is_read = "UNREAD" not in labels

    changed = False
    if email_record.is_starred != is_starred:
        email_record.is_starred = is_starred
        changed = True
    if email_record.is_trash != is_trash:
        email_record.is_trash = is_trash
        changed = True
    if email_record.is_read != is_read:
        email_record.is_read = is_read
        changed = True

    if changed:
        db.commit()
        if notify_user_id is not None:
            _send_email_updated(db, notify_user_id, email_record)

    return changed


def _extract_category_name(label_ids: List[str]) -> str:
    if "CATEGORY_PROMOTIONS" in label_ids:
        return "Promotions"
    if "CATEGORY_SOCIAL" in label_ids:
        return "Social"
    if "CATEGORY_UPDATES" in label_ids:
        return "Updates"
    return "Primary"


def _get_or_create_category(db: Session, name: str) -> models.Category:
    category = db.query(models.Category).filter(
        models.Category.name == name).first()
    if not category:
        category = models.Category(name=name)
        db.add(category)
        db.flush()
    return category


def _apply_label_rules_to_email(
    db: Session,
    receiver_id: Optional[int],
    sender_id: Optional[int],
    email_id: int,
) -> None:
    if receiver_id is None or sender_id is None:
        return

    rules = db.query(models.LabelRule).filter(
        models.LabelRule.user_id == receiver_id,
        models.LabelRule.from_user_id == sender_id,
    ).all()
    if not rules:
        return

    existing_label_ids = {
        label_id
        for (label_id,) in db.query(models.EmailLabel.label_id)
        .filter(models.EmailLabel.email_id == email_id)
        .all()
    }

    rows_to_insert = [
        {"email_id": email_id, "label_id": rule.label_id}
        for rule in rules
        if rule.label_id not in existing_label_ids
    ]
    if rows_to_insert:
        db.bulk_insert_mappings(models.EmailLabel, rows_to_insert)


def _load_user_emails(db: Session, user_id: int, limit: int = 60) -> List[models.Email]:
    return (
        db.query(models.Email)
        .join(models.Interface, models.Interface.email_id == models.Email.id)
        .filter(models.Interface.receiver_id == user_id)
        .order_by(models.Email.date.is_(None), models.Email.date.desc(), models.Email.created_at.desc())
        .limit(limit)
        .all()
    )


def _normalize_string_list(value: Any) -> List[str]:
    if value is None:
        return []

    items = value if isinstance(value, list) else [value]
    normalized: List[str] = []

    for item in items:
        if item is None:
            continue
        if isinstance(item, str):
            text = item.strip()
            if not text:
                continue
            if text.startswith("["):
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    parsed = None
                if isinstance(parsed, list):
                    normalized.extend(
                        str(entry).strip() for entry in parsed if str(entry).strip()
                    )
                    continue
            if "," in text:
                normalized.extend(
                    part.strip() for part in text.split(",") if part.strip()
                )
            else:
                normalized.append(text)
        else:
            text = str(item).strip()
            if text:
                normalized.append(text)

    return normalized


class SendEmailRequest(BaseModel):
    recipients: Optional[List[str]] = Field(
        None,
        description="List of recipient email addresses",
    )
    subject: Optional[str] = Field(None, description="Email subject")
    body: Optional[str] = Field(None, description="Email body")
    delivery_status: Optional[str] = Field(
        "draft",
        description='Email send state: "draft" or "sent"',
    )


class VerdictActionRequest(BaseModel):
    state: str = Field(..., description="Target state for the user action")
    reason: str = Field(..., description="Reason for the user action")


def _normalize_int_list(value: Any) -> List[int]:
    ids: List[int] = []
    for item in _normalize_string_list(value):
        try:
            ids.append(int(item))
        except (TypeError, ValueError):
            continue
    return list(dict.fromkeys(ids))


@app.get("/emails", tags = ['emails'])
def get_emails(
    status: str = Query("all"),
    label_id: Optional[int] = Query(None, ge=1),
    from_user_id: Optional[int] = Query(None, ge=1),
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    normalized_status = status.strip().lower()
    if normalized_status not in {"draft", "sent", "all"}:
        raise HTTPException(
            status_code=400,
            detail='status must be one of "draft", "sent", or "all"',
        )

    query = (
        db.query(models.Email)
        .join(models.Interface, models.Interface.email_id == models.Email.id)
        .filter(
            (models.Interface.receiver_id == user.id) |
            (models.Interface.sender_id == user.id)
        )
    )

    if normalized_status != "all":
        query = query.filter(models.Email.delivery_status == normalized_status)

    if label_id is not None:
        query = (
            query.join(models.EmailLabel,
                       models.EmailLabel.email_id == models.Email.id)
            .join(models.Label, models.Label.id == models.EmailLabel.label_id)
            .filter(models.Label.id == label_id, models.Label.user_id == user.id)
        )

    if from_user_id is not None:
        query = query.filter(models.Interface.sender_id == from_user_id)

    emails = (
        query.order_by(
            models.Email.date.is_(None),
            models.Email.date.desc(),
            models.Email.created_at.desc(),
        )
        .all()
    )

    return {
        "emails": [_serialize_email_for_user(db, email, user.id) for email in emails],
    }


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
        params={
            "maxResults": max_results,
            "includeSpamTrash": "true",
            "q": "in:anywhere",
        },
    )

    messages = list_payload.get("messages", []) or []
    stored = 0
    skipped = 0
    updated = 0

    for item in messages:
        message_id = item.get("id")
        if not message_id:
            continue

        message_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}"
        message = _gmail_get_json(
            user, db, message_url, params={"format": "full"})

        existing = db.query(models.Email).filter(
            models.Email.gmail_message_id == message_id).first()
        if existing:
            if _sync_existing_email_with_gmail_message(
                db, existing, message, notify_user_id=notify_user_id
            ):
                updated += 1
            else:
                skipped += 1
            continue

        created_email = _create_email_from_gmail_message(
            user,
            db,
            message,
            notify_user_id=notify_user_id,
            send_each_email=send_each_email,
        )
        if created_email is not None:
            stored += 1

    user.last_email_sync = datetime.now(timezone.utc)
    db.commit()

    return {
        "stored": stored,
        "skipped": skipped,
        "updated": updated,
        "fetched": len(messages),
    }


@app.get("/auth/google/login", tags = ['login/logout'])
def login():
    url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        "&response_type=code"
        "&scope=openid email profile https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/gmail.modify"
        "&access_type=offline"
        "&prompt=consent"
    )
    return RedirectResponse(url)


@app.get("/auth/google/callback", tags = ['login/logout'])
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
        user.token_expiry = datetime.now(
            timezone.utc) + timedelta(seconds=int(expires_in))

    tokens = _create_jwt_tokens(user.id, email)
    user.refresh_token_jwt = tokens["refresh_token"]
    user.refresh_token_jwt_expiry = tokens["refresh_expiry"]
    user.last_login = datetime.now(timezone.utc)

    db.commit()

    expires_in = int((tokens["access_expiry"] -
                     datetime.now(timezone.utc)).total_seconds())
    if expires_in < 0:
        expires_in = 0

    payload = {
        "jwt_token": tokens["access_token"],
        "refresh_token": tokens["refresh_token"],
        "expires_in": expires_in,
        "user_id": str(user.id),
        "google_id": user.google_user_id or "",
        "name": user.name or "",
        "email": user.email,
        "photo_url": user.profile_picture or "",
        "provider": "google",
    }

    # Redirect to the mobile app deep link with the JSON data as query params.
    query = urlencode(payload)
    deep_link = f"phishingdetectorapp://auth/callback?{query}"
    # return RedirectResponse(deep_link)
    return payload


@app.post("/auth/logout-and-reauth", tags = ['login/logout'])
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


@app.post("/labels", tags = ['labels'])
def create_label(
    name: str = Body(...),
    color: Optional[str] = Body(None),
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    normalized_name = name.strip() if isinstance(name, str) else ""
    if not normalized_name:
        raise HTTPException(status_code=400, detail="name is required")

    normalized_color = None
    if color is not None:
        normalized_color = color.strip() if isinstance(
            color, str) else str(color).strip()
        if not normalized_color:
            normalized_color = None

    label = models.Label(
        user_id=user.id,
        name=normalized_name,
        color=normalized_color,
    )
    db.add(label)
    db.commit()
    db.refresh(label)

    return {
        "label_id": label.id,
        "name": label.name,
        "color": label.color,
        "created_at": label.created_at.isoformat() if label.created_at else None,
    }


@app.get("/labels", tags = ['labels'])
def get_labels(
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    labels = (
        db.query(models.Label)
        .filter(models.Label.user_id == user.id)
        .order_by(models.Label.created_at.desc(), models.Label.id.desc())
        .all()
    )

    return {
        "labels": [
            {
                "label_id": label.id,
                "name": label.name,
                "color": label.color,
                "created_at": label.created_at.isoformat() if label.created_at else None,
            }
            for label in labels
        ]
    }


@app.delete("/labels/{label_id}", tags = ['labels'])
def delete_label(
    label_id: int,
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    label = (
        db.query(models.Label)
        .filter(models.Label.id == label_id, models.Label.user_id == user.id)
        .first()
    )
    if not label:
        raise HTTPException(status_code=404, detail="Label not found")

    db.query(models.EmailLabel).filter(models.EmailLabel.label_id ==
                                       label.id).delete(synchronize_session=False)
    db.query(models.LabelRule).filter(models.LabelRule.label_id ==
                                      label.id).delete(synchronize_session=False)
    db.delete(label)
    db.commit()

    return {"label_id": label_id, "deleted": True}


def _get_owned_label(db: Session, user_id: int, label_id: int) -> Optional[models.Label]:
    return (
        db.query(models.Label)
        .filter(models.Label.id == label_id, models.Label.user_id == user_id)
        .first()
    )


@app.post("/label-rules", tags = ['label-rules'])
def create_label_rule(
    label_id: int = Body(..., ge=1),
    from_user_id: int = Body(..., ge=1),
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    label = _get_owned_label(db, user.id, label_id)
    if not label:
        raise HTTPException(status_code=404, detail="Label not found")

    from_user = db.query(models.User).filter(models.User.id == from_user_id).first()
    if not from_user:
        raise HTTPException(status_code=404, detail="User not found")

    existing_rule = (
        db.query(models.LabelRule)
        .filter(
            models.LabelRule.user_id == user.id,
            models.LabelRule.label_id == label_id,
            models.LabelRule.from_user_id == from_user_id,
        )
        .first()
    )
    if existing_rule:
        return {
            "rule_id": existing_rule.id,
            "label_id": existing_rule.label_id,
            "from_user_id": existing_rule.from_user_id,
            "created": False,
        }

    rule = models.LabelRule(
        user_id=user.id,
        label_id=label_id,
        from_user_id=from_user_id,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)

    # Backfill existing emails from this sender into the label
    existing_email_ids = [
        email_id
        for (email_id,) in db.query(models.Email.id)
        .join(models.Interface, models.Interface.email_id == models.Email.id)
        .filter(
            models.Interface.receiver_id == user.id,
            models.Interface.sender_id == from_user_id,
        )
        .all()
    ]

    if existing_email_ids:
        already_labeled_ids = {
            email_id
            for (email_id,) in db.query(models.EmailLabel.email_id)
            .filter(
                models.EmailLabel.label_id == label_id,
                models.EmailLabel.email_id.in_(existing_email_ids),
            )
            .all()
        }

        rows_to_insert = [
            {"email_id": email_id, "label_id": label_id}
            for email_id in existing_email_ids
            if email_id not in already_labeled_ids
        ]

        if rows_to_insert:
            db.bulk_insert_mappings(models.EmailLabel, rows_to_insert)
            db.commit()

    return {
        "rule_id": rule.id,
        "label_id": rule.label_id,
        "from_user_id": rule.from_user_id,
        "created": True,
    }


@app.get("/label-rules", tags = ['label-rules'])
def get_label_rules(
    label_id: int = Query(..., ge=1),
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    label = _get_owned_label(db, user.id, label_id)
    if not label:
        raise HTTPException(status_code=404, detail="Label not found")

    rules = (
        db.query(models.LabelRule)
        .join(models.User, models.User.id == models.LabelRule.from_user_id)
        .filter(models.LabelRule.user_id == user.id, models.LabelRule.label_id == label_id)
        .order_by(models.User.email.asc())
        .all()
    )

    return {
        "label_id": label_id,
        "label_name": label.name,
        "rules": [
            {
                "rule_id": rule.id,
                "label_id": rule.label_id,
                "from_user": _serialize_sender(rule.from_user),
            }
            for rule in rules
        ],
    }


@app.delete("/label-rules", tags = ['label-rules'])
def delete_label_rule(
    label_id: int = Body(..., ge=1),
    from_user_id: int = Body(..., ge=1),
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    label = _get_owned_label(db, user.id, label_id)
    if not label:
        raise HTTPException(status_code=404, detail="Label not found")

    rule = (
        db.query(models.LabelRule)
        .filter(
            models.LabelRule.user_id == user.id,
            models.LabelRule.label_id == label_id,
            models.LabelRule.from_user_id == from_user_id,
        )
        .first()
    )
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    db.delete(rule)
    db.commit()

    return {
        "rule_id": rule.id,
        "label_id": label_id,
        "from_user_id": from_user_id,
        "deleted": True,
    }


@app.post("/emails/send", tags = ['emails'])
async def send_email(
    request: Request,
    payload: Optional[SendEmailRequest] = Body(None),
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    content_type = (request.headers.get("content-type") or "").lower()
    recipients_raw: Any = None
    subject: Optional[str] = None
    body: Optional[str] = None
    delivery_status: str = "draft"

    if "multipart/form-data" in content_type or "application/x-www-form-urlencoded" in content_type:
        form = await request.form()
        recipients_raw = form.getlist("recipients") or form.getlist("to")
        subject = form.get("subject")
        body = form.get("body")
        delivery_status = form.get("delivery_status") or "draft"
    else:
        if payload is None:
            raise HTTPException(status_code=400, detail="Request body must be a JSON object")
        recipients_raw = payload.recipients
        subject = payload.subject
        body = payload.body
        delivery_status = payload.delivery_status or "draft"

    recipients = _normalize_string_list(recipients_raw)
    if not recipients:
        raise HTTPException(
            status_code=400, detail="recipients must be a non-empty list of email addresses")

    normalized_delivery_status = delivery_status.strip().lower()
    if normalized_delivery_status not in {"sent", "draft"}:
        raise HTTPException(
            status_code=400, detail='delivery_status must be either "sent" or "draft"')

    msg = EmailMessage()
    msg["From"] = user.email
    msg["To"] = ", ".join(recipients)
    if subject:
        msg["Subject"] = subject
    msg.set_content(body or "")

    category = _get_or_create_category(db, "Primary")

    email_record = models.Email(
        gmail_message_id=None,
        thread_id=None,
        subject=subject or "",
        body_full=body or "",
        body_snippet=(body or "")[:200],
        labels=None,
        date=datetime.now(timezone.utc),
        category_id=category.id,
        status="PENDING",
        delivery_status=normalized_delivery_status,
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
        db.add(models.Interface(sender_id=user.id,
               receiver_id=r_user.id, email_id=email_record.id))

    db.add(models.EmailHeaders(email_id=email_record.id, status="PENDING"))

    urls = extract_urls(body or "")
    for u in urls:
        db.add(models.UrlsExtracted(
            email_id=email_record.id, url=u, status="PENDING"))

    if normalized_delivery_status == "draft":
        db.commit()
        return {
            "email_id": email_record.id,
            "gmail_message_id": None,
            "status": "draft",
            "delivery_status": normalized_delivery_status,
        }

    raw_b64 = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    send_url = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
    access_token = _get_valid_google_access_token(user, db)
    headers = {"Authorization": f"Bearer {access_token}"}

    res = requests.post(send_url, headers=headers, json={"raw": raw_b64}, timeout=20)
    if res.status_code in (401, 403):
        access_token = _refresh_google_access_token(user, db)
        headers = {"Authorization": f"Bearer {access_token}"}
        res = requests.post(send_url, headers=headers, json={"raw": raw_b64}, timeout=20)

    if not res.ok:
        error_detail = res.text
        if res.status_code == 403 and "insufficientPermissions" in error_detail:
            error_detail += " | RESOLUTION: Visit https://myaccount.google.com/permissions, revoke this app, then re-login via /auth/google/login"
        raise HTTPException(status_code=502, detail=f"Failed to send email: {error_detail}")

    payload = res.json()
    message_id = payload.get("id")
    thread_id = payload.get("threadId")
    if not message_id:
        raise HTTPException(status_code=502, detail="Gmail did not return message id")

    existing = db.query(models.Email).filter(models.Email.gmail_message_id == message_id).first()
    if existing:
        existing.delivery_status = normalized_delivery_status
        existing.is_read = True
        existing_receivers = {
            iface.receiver.email for iface in existing.interfaces if iface.receiver}
        for r in recipients:
            if r not in existing_receivers:
                r_user = db.query(models.User).filter(models.User.email == r).first()
                if not r_user:
                    r_user = models.User(email=r, provider="external")
                    db.add(r_user)
                    db.flush()
                db.add(models.Interface(sender_id=user.id,
                       receiver_id=r_user.id, email_id=existing.id))
        db.commit()
        return {"email_id": existing.id, "gmail_message_id": message_id, "status": "exists_updated_receivers"}

    email_record.gmail_message_id = message_id
    email_record.thread_id = thread_id
    email_record.delivery_status = normalized_delivery_status
    email_record.is_read = True
    db.commit()

    enqueue_email_analysis(email_record.id)

    return {
        "email_id": email_record.id,
        "gmail_message_id": message_id,
        "status": "sent",
        "delivery_status": normalized_delivery_status,
    }


@app.patch("/emails/{email_id}/read", tags = ['flags'])
def mark_email_read(
    email_id: int,
    value: bool = True,
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    email = (
        db.query(models.Email)
        .join(models.Interface, models.Interface.email_id == models.Email.id)
        .filter(
            models.Email.id == email_id,
            or_(
                models.Interface.receiver_id == user.id,
                models.Interface.sender_id == user.id,
            ),
        )
        .first()
    )
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")

    # Sync current Gmail state into local DB so external changes are reflected
    if email.gmail_message_id:
        try:
            message_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{email.gmail_message_id}"
            message = _gmail_get_json(user, db, message_url, params={"format": "full"})
            _sync_existing_email_with_gmail_message(db, email, message, notify_user_id=user.id)
        except HTTPException as exc:
            logger.warning("Failed to sync Gmail state for %s: %s", email.gmail_message_id, str(exc.detail))

    if email.gmail_message_id:
        if value:
            _gmail_modify_message_labels(
                user,
                db,
                email.gmail_message_id,
                remove_labels=["UNREAD"],
            )
        else:
            _gmail_modify_message_labels(
                user,
                db,
                email.gmail_message_id,
                add_labels=["UNREAD"],
            )

    email.is_read = value
    db.commit()

    return {"email_id": email_id, "is_read": value}


@app.patch("/emails/{email_id}/trash", tags = ['flags'])
def mark_email_trash(
    email_id: int,
    value: bool = True,
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    email = (
        db.query(models.Email)
        .join(models.Interface, models.Interface.email_id == models.Email.id)
        .filter(
            models.Email.id == email_id,
            or_(
                models.Interface.receiver_id == user.id,
                models.Interface.sender_id == user.id,
            ),
        )
        .first()
    )
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")

    # First, sync current Gmail state so external label changes are reflected locally
    if email.gmail_message_id:
        try:
            message_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{email.gmail_message_id}"
            message = _gmail_get_json(user, db, message_url, params={"format": "full"})
            _sync_existing_email_with_gmail_message(db, email, message, notify_user_id=user.id)
        except HTTPException as exc:
            logger.warning("Failed to sync Gmail state for %s: %s", email.gmail_message_id, str(exc.detail))

    # If request asks to trash the email
    if value:
        # If it's already trash locally, perform permanent deletion (Gmail + DB)
        if email.is_trash:
            gmail_deleted = False
            if email.gmail_message_id:
                try:
                    _gmail_delete_message(user, db, email.gmail_message_id)
                    gmail_deleted = True
                except HTTPException as exc:
                    detail = str(exc.detail)
                    if "insufficientPermissions" in detail:
                        logger.warning(
                            "Gmail delete failed with insufficient permissions for %s, attempting trash fallback",
                            email.gmail_message_id,
                        )
                        try:
                            _gmail_trash_message(user, db, email.gmail_message_id)
                            gmail_deleted = True
                        except HTTPException:
                            logger.warning(
                                "Gmail trash fallback also failed for %s",
                                email.gmail_message_id,
                            )
                    else:
                        logger.warning(
                            "Gmail delete failed for %s: %s",
                            email.gmail_message_id,
                            detail,
                        )

            # Remove DB records and attachments
            attachment_dir = os.path.join(ATTACHMENTS_DIR, str(email.id))

            db.query(models.StaticAnalysis).filter(
                models.StaticAnalysis.attach_id.in_(
                    db.query(models.Attachments.id).filter(
                        models.Attachments.email_id == email.id)
                )
            ).delete(synchronize_session=False)
            db.query(models.DynamicAnalysis).filter(
                models.DynamicAnalysis.attach_id.in_(
                    db.query(models.Attachments.id).filter(
                        models.Attachments.email_id == email.id)
                )
            ).delete(synchronize_session=False)
            db.query(models.AnalysisTask).filter(
                models.AnalysisTask.email_id == email.id).delete(synchronize_session=False)
            db.query(models.BodyClassification).filter(
                models.BodyClassification.email_id == email.id).delete(synchronize_session=False)
            db.query(models.EmailHeaders).filter(
                models.EmailHeaders.email_id == email.id).delete(synchronize_session=False)
            db.query(models.UrlsExtracted).filter(
                models.UrlsExtracted.email_id == email.id).delete(synchronize_session=False)
            db.query(models.EmailDeadline).filter(
                models.EmailDeadline.email_id == email.id).delete(synchronize_session=False)
            db.query(models.UserAction).filter(models.UserAction.email_id ==
                                               email.id).delete(synchronize_session=False)
            db.query(models.EmailLabel).filter(models.EmailLabel.email_id ==
                                               email.id).delete(synchronize_session=False)
            db.query(models.Interface).filter(models.Interface.email_id ==
                                              email.id).delete(synchronize_session=False)
            db.query(models.Attachments).filter(models.Attachments.email_id ==
                                                email.id).delete(synchronize_session=False)
            db.query(models.Email).filter(models.Email.id ==
                                          email.id).delete(synchronize_session=False)
            db.commit()

            if os.path.isdir(attachment_dir):
                shutil.rmtree(attachment_dir, ignore_errors=True)

            return {"email_id": email_id, "deleted": True, "gmail_deleted": gmail_deleted}

        # Otherwise, mark it as trash in Gmail (and locally)
        if email.gmail_message_id:
            try:
                _gmail_trash_message(user, db, email.gmail_message_id)
            except HTTPException:
                # Fallback to modifying labels if direct trash fails
                try:
                    _gmail_modify_message_labels(
                        user,
                        db,
                        email.gmail_message_id,
                        add_labels=["TRASH"],
                    )
                except Exception as exc:
                    logger.warning("Failed to mark Gmail message as TRASH: %s", str(exc))

        email.is_trash = True
        db.commit()
        return {"email_id": email_id, "is_trash": True}

    # If request asks to un-trash the email, remove TRASH label
    if email.gmail_message_id:
        try:
            _gmail_modify_message_labels(
                user,
                db,
                email.gmail_message_id,
                remove_labels=["TRASH"],
            )
        except Exception as exc:
            logger.warning("Failed to remove TRASH label: %s", str(exc))

    email.is_trash = False
    db.commit()

    return {"email_id": email_id, "is_trash": email.is_trash}


@app.patch("/emails/{email_id}/star", tags = ['flags'])
def mark_email_starred(
    email_id: int,
    value: bool = True,
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    email = (
        db.query(models.Email)
        .join(models.Interface, models.Interface.email_id == models.Email.id)
        .filter(
            models.Email.id == email_id,
            or_(
                models.Interface.receiver_id == user.id,
                models.Interface.sender_id == user.id,
            ),
        )
        .first()
    )
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")

    # Sync Gmail state first to reflect external changes
    if email.gmail_message_id:
        try:
            message_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{email.gmail_message_id}"
            message = _gmail_get_json(user, db, message_url, params={"format": "full"})
            _sync_existing_email_with_gmail_message(db, email, message, notify_user_id=user.id)
        except HTTPException as exc:
            logger.warning("Failed to sync Gmail state for %s: %s", email.gmail_message_id, str(exc.detail))

    if email.gmail_message_id:
        if value:
            _gmail_modify_message_labels(
                user,
                db,
                email.gmail_message_id,
                add_labels=["STARRED"],
            )
        else:
            _gmail_modify_message_labels(
                user,
                db,
                email.gmail_message_id,
                remove_labels=["STARRED"],
            )

    email.is_starred = value
    db.commit()

    return {"email_id": email_id, "is_starred": email.is_starred}


@app.patch("/emails/{email_id}/verdict", tags = ['flags'])
def update_email_verdict(
    email_id: int,
    action: VerdictActionRequest = Body(...),
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    email = (
        db.query(models.Email)
        .join(models.Interface, models.Interface.email_id == models.Email.id)
        .filter(
            models.Email.id == email_id,
            or_(
                models.Interface.receiver_id == user.id,
                models.Interface.sender_id == user.id,
            ),
        )
        .first()
    )
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")

    from_state = email.user_act or email.final_verdict or ""
    user_action = models.UserAction(
        email_id=email.id,
        from_action=from_state,
        to_action=action.state,
        reasons=action.reason,
    )
    db.add(user_action)
    email.user_act = action.state
    db.commit()

    return {
        "email_id": email.id,
        "from_state": from_state,
        "to_action": action.state,
        "reasons": action.reason,
        "user_act": email.user_act,
    }


@app.patch("/emails/{email_id}/draft_edit", tags = ['emails'])
async def draft_edit(
    email_id: int,
    request: Request,
    payload: Optional[SendEmailRequest] = Body(None),
    user: models.User = Depends(get_current_user_from_auth),
    db: Session = Depends(get_db),
):
    email = (
        db.query(models.Email)
        .join(models.Interface, models.Interface.email_id == models.Email.id)
        .filter(models.Email.id == email_id, models.Interface.sender_id == user.id)
        .first()
    )
    if not email:
        raise HTTPException(status_code=404, detail="Draft email not found")

    if email.delivery_status != "draft":
        raise HTTPException(
            status_code=400, detail="Only draft emails can be edited")

    content_type = (request.headers.get("content-type") or "").lower()
    recipients_raw: Any = None
    subject: Optional[str] = None
    body: Optional[str] = None
    delivery_status: str = "draft"

    if "multipart/form-data" in content_type or "application/x-www-form-urlencoded" in content_type:
        form = await request.form()
        recipients_raw = form.getlist("recipients") or form.getlist("to")
        subject = form.get("subject")
        body = form.get("body")
        delivery_status = form.get("delivery_status") or "draft"
    else:
        if payload is None:
            raise HTTPException(status_code=400, detail="Request body must be a JSON object")
        recipients_raw = payload.recipients
        subject = payload.subject
        body = payload.body
        delivery_status = payload.delivery_status or "draft"

    recipients = _normalize_string_list(recipients_raw)
    if not recipients:
        raise HTTPException(
            status_code=400, detail="recipients must be a non-empty list of email addresses")

    normalized_delivery_status = delivery_status.strip().lower()
    if normalized_delivery_status not in {"sent", "draft"}:
        raise HTTPException(
            status_code=400, detail='delivery_status must be either "sent" or "draft"')

    email.subject = subject or email.subject
    email.body_full = body or email.body_full
    email.body_snippet = (email.body_full or "")[:200]
    email.delivery_status = normalized_delivery_status
    email.status = "PENDING"
    email.urls_status = "PENDING"
    email.attachments_status = "PENDING"
    email.body_status = "PENDING"
    email.headers_status = "PENDING"
    email.is_urls_queued = False
    email.is_attachments_queued = False
    email.is_body_queued = False
    email.is_headers_queued = False
    email.risk_score = 0.0
    email.final_verdict = None
    email.analyzed_at = None

    db.query(models.EmailHeaders).filter(
        models.EmailHeaders.email_id == email.id).delete(synchronize_session=False)
    db.query(models.UrlsExtracted).filter(
        models.UrlsExtracted.email_id == email.id).delete(synchronize_session=False)
    db.query(models.Interface).filter(
        models.Interface.email_id == email.id).delete(synchronize_session=False)

    db.add(models.EmailHeaders(email_id=email.id, status="PENDING"))

    urls = extract_urls(email.body_full or "")
    for url in urls:
        db.add(models.UrlsExtracted(email_id=email.id, url=url, status="PENDING"))

    for recipient_email in recipients:
        recipient_email = str(recipient_email or "").strip()
        if not recipient_email:
            continue
        recipient_user = db.query(models.User).filter(
            models.User.email == recipient_email).first()
        if not recipient_user:
            recipient_user = models.User(email=recipient_email, provider="external")
            db.add(recipient_user)
            db.flush()
        db.add(models.Interface(sender_id=user.id,
               receiver_id=recipient_user.id, email_id=email.id))

    if normalized_delivery_status == "draft":
        db.commit()
        return {
            "email_id": email.id,
            "status": "draft",
            "delivery_status": email.delivery_status,
            "subject": email.subject,
            "body": email.body_full,
            "recipients": recipients,
        }

    msg = EmailMessage()
    msg["From"] = user.email
    msg["To"] = ", ".join(recipients)
    if email.subject:
        msg["Subject"] = email.subject
    msg.set_content(email.body_full or "")

    raw_b64 = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    send_url = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
    access_token = _get_valid_google_access_token(user, db)
    headers = {"Authorization": f"Bearer {access_token}"}

    res = requests.post(send_url, headers=headers, json={"raw": raw_b64}, timeout=20)
    if res.status_code in (401, 403):
        access_token = _refresh_google_access_token(user, db)
        headers = {"Authorization": f"Bearer {access_token}"}
        res = requests.post(send_url, headers=headers, json={"raw": raw_b64}, timeout=20)

    if not res.ok:
        error_detail = res.text
        if res.status_code == 403 and "insufficientPermissions" in error_detail:
            error_detail += " | RESOLUTION: Visit https://myaccount.google.com/permissions, revoke this app, then re-login via /auth/google/login"
        db.rollback()
        raise HTTPException(status_code=502, detail=f"Failed to send email: {error_detail}")

    payload = res.json()
    message_id = payload.get("id")
    thread_id = payload.get("threadId")
    if not message_id:
        db.rollback()
        raise HTTPException(status_code=502, detail="Gmail did not return message id")

    existing = db.query(models.Email).filter(models.Email.gmail_message_id == message_id).first()
    if existing and existing.id != email.id:
        existing.delivery_status = normalized_delivery_status
        existing.is_read = True
        existing_receivers = {
            iface.receiver.email for iface in existing.interfaces if iface.receiver}
        for r in recipients:
            if r not in existing_receivers:
                r_user = db.query(models.User).filter(models.User.email == r).first()
                if not r_user:
                    r_user = models.User(email=r, provider="external")
                    db.add(r_user)
                    db.flush()
                db.add(models.Interface(sender_id=user.id,
                       receiver_id=r_user.id, email_id=existing.id))
        db.commit()
        return {"email_id": existing.id, "gmail_message_id": message_id, "status": "exists_updated_receivers"}

    email.gmail_message_id = message_id
    email.thread_id = thread_id
    email.is_read = True
    db.commit()

    enqueue_email_analysis(email.id)

    return {
        "email_id": email.id,
        "gmail_message_id": message_id,
        "status": "sent",
        "delivery_status": normalized_delivery_status,
    }


async def _send_initial_batch(user_id: int, max_results: int = 60) -> None:
    def _load_existing() -> Dict[str, Any]:
        db = SessionLocal()
        try:
            user = db.query(models.User).filter(
                models.User.id == user_id).first()
            if not user:
                return {"emails": [], "should_fetch": False}

            emails = _load_user_emails(db, user_id, max_results)
            serialized = [_serialize_email_for_user(
                db, email, user_id) for email in emails]
            return {"emails": serialized, "should_fetch": len(emails) == 0}
        finally:
            db.close()

    payload = await asyncio.to_thread(_load_existing)
    await manager.send_json(user_id, {"type": "initial_emails", "emails": payload["emails"]})

    if payload["should_fetch"]:
        asyncio.create_task(_stream_gmail_emails(user_id, max_results))


async def _stream_gmail_emails(user_id: int, max_results: int = 60) -> None:
    def _sync() -> None:
        db = SessionLocal()
        try:
            user = db.query(models.User).filter(
                models.User.id == user_id).first()
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
