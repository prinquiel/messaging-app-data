from fastapi.testclient import TestClient


def _register(client: TestClient, username: str, password: str = "secret123") -> None:
    payload = {
        "username": username,
        "email": f"{username}@example.com",
        "full_name": f"{username} User",
        "password": password,
    }
    res = client.post("/auth/register", json=payload)
    assert res.status_code == 201


def test_register_and_login_flow(client: TestClient):
    _register(client, "tester")

    res = client.post(
        "/auth/login",
        json={"username": "tester", "password": "secret123"},
    )
    assert res.status_code == 200
    data = res.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


def test_register_duplicate_username(client: TestClient):
    _register(client, "dupuser")
    res = client.post(
        "/auth/register",
        json={
            "username": "dupuser",
            "email": "dup2@example.com",
            "full_name": "Dup User",
            "password": "secret123",
        },
    )
    assert res.status_code == 400
    assert "ya existe" in res.json()["detail"] or "already exists" in res.json()["detail"]


