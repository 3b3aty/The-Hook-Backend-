import asyncio
from typing import Any, Dict, Optional

from fastapi import WebSocket


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: Dict[int, WebSocket] = {}

    async def connect(self, user_id: int, websocket: WebSocket) -> None:
        await websocket.accept()
        self._connections[user_id] = websocket

    def disconnect(self, user_id: int) -> None:
        self._connections.pop(user_id, None)

    def has_connection(self, user_id: int) -> bool:
        return user_id in self._connections

    async def send_json(self, user_id: int, payload: Dict[str, Any]) -> None:
        websocket = self._connections.get(user_id)
        if websocket:
            await websocket.send_json(payload)

    def send_json_sync(self, user_id: int, payload: Dict[str, Any]) -> bool:
        websocket = self._connections.get(user_id)
        if not websocket:
            return False
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        try:
            if loop and loop.is_running():
                loop.create_task(websocket.send_json(payload))
            else:
                asyncio.run(websocket.send_json(payload))
        except RuntimeError:
            return False
        return True


manager = ConnectionManager()


def notify_user(user_id: Optional[int], payload: Dict[str, Any]) -> bool:
    if user_id is None:
        return False
    return manager.send_json_sync(user_id, payload)
