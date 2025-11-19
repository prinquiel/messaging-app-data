from fastapi.testclient import TestClient

from app import models
from app.security import hash_password


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


def test_me_chats_returns_only_member_chats(client: TestClient, db_session):
    user_a = create_user(db_session, "memberA")
    user_b = create_user(db_session, "memberB")
    other = create_user(db_session, "intruder")

    chat1 = models.Chat(chat_type="private", name="A", created_by=user_a.id, members=[user_a, user_b])
    chat2 = models.Chat(chat_type="group", name="B", created_by=user_a.id, members=[user_a, other])
    db_session.add_all([chat1, chat2])
    db_session.commit()

    res = client.post("/auth/login", json={"username": "memberA", "password": "secret123"})
    token = res.json()["access_token"]

    res_chats = client.get("/me/chats", headers={"Authorization": f"Bearer {token}"})
    assert res_chats.status_code == 200
    ids = [chat["id"] for chat in res_chats.json()]
    assert set(ids) == {chat1.id, chat2.id}


def test_create_private_chat_requires_two_members(client: TestClient):
    register_payload = {
        "username": "user1",
        "email": "user1@example.com",
        "full_name": "User One",
        "password": "secret123",
    }
    client.post("/auth/register", json=register_payload)
    login = client.post("/auth/login", json={"username": "user1", "password": "secret123"})
    token = login.json()["access_token"]

    chat_payload = {"chat_type": "private", "member_ids": []}
    res = client.post("/me/chats", json=chat_payload, headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 400

