import json

from fastapi.testclient import TestClient

from app import models
from app.security import create_access_token, hash_password


def create_user(db, username: str) -> models.User:
    user = models.User(
        username=username,
        email=f"{username}@example.com",
        full_name=f"{username} Tester",
        password_hash=hash_password("secret123"),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def create_chat_with_members(db, users):
    chat = models.Chat(chat_type="private", name="ws", created_by=users[0].id)
    chat.members = users
    db.add(chat)
    db.commit()
    db.refresh(chat)
    return chat


def wait_for(ws, event_type: str):
    for _ in range(5):
        data = ws.receive_json()
        if data.get("type") == event_type:
            return data
    raise AssertionError(f"{event_type} not received")


def test_websocket_typing_and_message(client: TestClient, db_session):
    user_a = create_user(db_session, "socketA")
    user_b = create_user(db_session, "socketB")
    chat = create_chat_with_members(db_session, [user_a, user_b])

    token_a = create_access_token({"user_id": user_a.id})
    token_b = create_access_token({"user_id": user_b.id})

    with client.websocket_connect(f"/ws/chats/{chat.id}?token={token_a}") as ws_a:
        wait_for(ws_a, "presence")
        with client.websocket_connect(f"/ws/chats/{chat.id}?token={token_b}") as ws_b:
            wait_for(ws_a, "presence")
            wait_for(ws_b, "presence")

            ws_a.send_text(json.dumps({"type": "typing", "is_typing": True}))
            typing_event = wait_for(ws_b, "typing")
            assert typing_event["sender_id"] == user_a.id
            assert typing_event["is_typing"] is True

            ws_a.send_text(json.dumps({"content": "Hola"}))
            message_event = wait_for(ws_b, "message")
            assert message_event["content"] == "Hola"
            assert message_event["sender_id"] == user_a.id

