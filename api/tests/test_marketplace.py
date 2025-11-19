from fastapi.testclient import TestClient


def register_and_login(client: TestClient, username: str) -> str:
    payload = {
        "username": username,
        "email": f"{username}@example.com",
        "full_name": f"{username} Tester",
        "password": "secret123",
    }
    res = client.post("/auth/register", json=payload)
    assert res.status_code == 201
    res = client.post("/auth/login", json={"username": username, "password": "secret123"})
    token = res.json()["access_token"]
    return token


def test_publish_marketplace_item_without_chat(client: TestClient):
    token = register_and_login(client, "seller1")

    payload = {
        "title": "Bicicleta montaña",
        "description": "Excelente estado",
        "price": 125000,
        "currency": "CRC",
    }
    res = client.post(
        "/marketplace/items",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert res.status_code == 201
    data = res.json()
    assert data["title"] == payload["title"]
    assert data["price"] == payload["price"]
    assert data["currency"] == "CRC"
    assert data["chat_id"] is not None
    assert data["message_id"] is not None


def test_contact_seller_creates_private_chat(client: TestClient):
    seller_token = register_and_login(client, "seller2")
    payload = {
        "title": "Laptop Pro",
        "description": "16GB RAM",
        "price": 350000,
        "currency": "CRC",
    }
    res = client.post(
        "/marketplace/items",
        json=payload,
        headers={"Authorization": f"Bearer {seller_token}"},
    )
    item = res.json()

    buyer_token = register_and_login(client, "buyer1")
    contact_res = client.post(
        f"/marketplace/items/{item['id']}/contact",
        headers={"Authorization": f"Bearer {buyer_token}"},
    )
    assert contact_res.status_code == 201
    chat = contact_res.json()
    assert chat["id"]
    # calling again should reuse chat
    contact_res2 = client.post(
        f"/marketplace/items/{item['id']}/contact",
        headers={"Authorization": f"Bearer {buyer_token}"},
    )
    assert contact_res2.status_code == 201
    assert contact_res2.json()["id"] == chat["id"]


def test_contact_seller_rejected_when_self(client: TestClient):
    token = register_and_login(client, "seller-self")
    payload = {
        "title": "Libro raro",
        "description": "Firmado",
        "price": 5555,
        "currency": "CRC",
    }
    res = client.post(
        "/marketplace/items",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )
    item = res.json()
    res_self = client.post(
        f"/marketplace/items/{item['id']}/contact",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert res_self.status_code == 400
    assert res_self.json()["detail"] == "Eres el vendedor de este producto"


