import json
import os
from typing import Any, Dict, Optional

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_CHANNEL = os.getenv("REDIS_WS_CHANNEL", "ws_updates")

_redis_client: Optional[redis.Redis] = None


def _get_client() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    return _redis_client


def publish_event(payload: Dict[str, Any]) -> None:
    client = _get_client()
    client.publish(REDIS_CHANNEL, json.dumps(payload))
