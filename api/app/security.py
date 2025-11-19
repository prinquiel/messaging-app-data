import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app import models


MAX_BCRYPT_BYTES = 72
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = HTTPBearer(auto_error=True)

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "dev-secret-change-me")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "60"))


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _normalize_password(password: str) -> str:
    if password is None:
        raise ValueError("Password must be provided")
    encoded = password.encode("utf-8")
    if len(encoded) <= MAX_BCRYPT_BYTES:
        return password
    return encoded[:MAX_BCRYPT_BYTES].decode("utf-8", "ignore")


def hash_password(password: str) -> str:
    normalized = _normalize_password(password)
    return pwd_context.hash(normalized)


def verify_password(password: str, password_hash: str) -> bool:
    if not password_hash:
        return False
    normalized = _normalize_password(password)
    return pwd_context.verify(normalized, password_hash)


def create_access_token(subject: dict, expires_delta: Optional[timedelta] = None) -> str:
    now = datetime.now(timezone.utc)
    expire = now + (expires_delta or timedelta(minutes=JWT_EXPIRE_MINUTES))
    to_encode = {"exp": int(expire.timestamp()), "iat": int(now.timestamp()), **subject}
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return encoded_jwt


def decode_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
) -> models.User:
    token = credentials.credentials
    payload = decode_token(token)
    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    return user


