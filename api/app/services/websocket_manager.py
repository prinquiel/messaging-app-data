from typing import Dict, Set
from starlette.websockets import WebSocket
import asyncio
import json


class WebSocketManager:
    def __init__(self) -> None:
        self.chat_connections: Dict[int, Dict[int, Set[WebSocket]]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, chat_id: int, user_id: int, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            if chat_id not in self.chat_connections:
                self.chat_connections[chat_id] = {}
            if user_id not in self.chat_connections[chat_id]:
                self.chat_connections[chat_id][user_id] = set()
            self.chat_connections[chat_id][user_id].add(websocket)

    async def disconnect(self, chat_id: int, user_id: int, websocket: WebSocket) -> None:
        async with self._lock:
            chat_conns = self.chat_connections.get(chat_id)
            if chat_conns:
                user_conns = chat_conns.get(user_id)
                if user_conns and websocket in user_conns:
                    user_conns.remove(websocket)
                    if not user_conns:
                        chat_conns.pop(user_id, None)
                if not chat_conns:
                    self.chat_connections.pop(chat_id, None)

    async def broadcast(self, chat_id: int, message: str) -> None:
        async with self._lock:
            conns = [
                ws
                for user_conns in self.chat_connections.get(chat_id, {}).values()
                for ws in user_conns
            ]
        to_remove = []
        for ws in conns:
            try:
                await ws.send_text(message)
            except Exception:
                to_remove.append(ws)
        if to_remove:
            async with self._lock:
                chat_conns = self.chat_connections.get(chat_id, {})
                for user_id, user_conns in list(chat_conns.items()):
                    for ws in to_remove:
                        user_conns.discard(ws)
                    if not user_conns:
                        chat_conns.pop(user_id, None)
                if not chat_conns:
                    self.chat_connections.pop(chat_id, None)

    async def broadcast_presence(self, chat_id: int) -> None:
        async with self._lock:
            active_users = list(self.chat_connections.get(chat_id, {}).keys())
        event = json.dumps({"type": "presence", "chat_id": chat_id, "user_ids": active_users})
        await self.broadcast(chat_id, event)


manager = WebSocketManager()

