from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from sqlalchemy.orm import Session
import json

from app.database import SessionLocal
from app import models
from app.services.websocket_manager import manager
from app.security import decode_token


router = APIRouter()


def get_db_session() -> Session:
    return SessionLocal()


@router.websocket("/ws/chats/{chat_id}")
async def chat_ws(websocket: WebSocket, chat_id: int, token: str = Query(...)):
    try:
        payload = decode_token(token)
        user_id = payload.get("user_id")
        if not user_id:
            await websocket.close(code=4401)
            return
    except Exception:
        await websocket.close(code=4401)
        return

    await manager.connect(chat_id, user_id, websocket)
    await manager.broadcast_presence(chat_id)
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
                event_type = data.get("type") or "message"
            except Exception:
                event_type = "message"
                data = {"content": raw}

            if event_type == "typing":
                is_typing = bool(data.get("is_typing"))
                event = {
                    "type": "typing",
                    "sender_id": user_id,
                    "chat_id": chat_id,
                    "is_typing": is_typing,
                }
                await manager.broadcast(chat_id, json.dumps(event))
                continue

            content = data.get("content")
            if not content:
                await websocket.send_text(json.dumps({"type": "error", "message": "Contenido vacío"}))
                continue
            db: Session = get_db_session()
            try:
                chat = db.query(models.Chat).filter(models.Chat.id == chat_id).first()
                user = db.query(models.User).filter(models.User.id == user_id).first()
                if not chat or not user:
                    await websocket.send_text(json.dumps({"type": "error", "message": "Invalid chat or user"}))
                    continue

                membership = (
                    db.query(models.chat_members)
                    .filter(
                        models.chat_members.c.chat_id == chat_id,
                        models.chat_members.c.user_id == user_id,
                    )
                    .first()
                )
                if not membership:
                    await websocket.send_text(
                        json.dumps({"type": "error", "message": "No perteneces a este chat"})
                    )
                    continue
                message = models.Message(content=content, sender_id=user_id, chat_id=chat_id)
                db.add(message)
                db.commit()
                db.refresh(message)
                event = {
                    "type": "message",
                    "id": message.id,
                    "content": message.content,
                    "sender_id": message.sender_id,
                    "chat_id": message.chat_id,
                    "sent_at": message.sent_at.isoformat() if message.sent_at else None,
                }
                await manager.broadcast(chat_id, json.dumps(event))
            finally:
                db.close()
    except WebSocketDisconnect:
        await manager.disconnect(chat_id, user_id, websocket)
        await manager.broadcast_presence(chat_id)
    except Exception:
        await manager.disconnect(chat_id, user_id, websocket)
        await manager.broadcast_presence(chat_id)
        try:
            await websocket.close()
        except Exception:
            pass


