from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, selectinload

from app import models, schemas
from app.database import get_db
from app.security import get_current_user

router = APIRouter(prefix="", tags=["chats"])


def _exclude_internal_chats(query):
    return query.filter(
        (models.Chat.description.is_(None)) | (models.Chat.description != "__marketplace_seller__")
    )


@router.get("/me/chats", response_model=List[schemas.Chat])
def get_my_chats(
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    chats = (
        db.query(models.Chat)
        .join(models.chat_members, models.Chat.id == models.chat_members.c.chat_id)
        .filter(models.chat_members.c.user_id == current_user.id)
        .options(selectinload(models.Chat.members))
    )
    chats = _exclude_internal_chats(chats).order_by(models.Chat.created_at.desc()).all()
    return chats


@router.get("/users/{user_id}/chats", response_model=List[schemas.Chat])
def get_user_chats(user_id: int, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    chats = (
        db.query(models.Chat)
        .join(models.chat_members, models.Chat.id == models.chat_members.c.chat_id)
        .filter(models.chat_members.c.user_id == user_id)
        .options(selectinload(models.Chat.members))
    )
    chats = _exclude_internal_chats(chats).order_by(models.Chat.created_at.desc()).all()
    return chats


class MyChatCreate(schemas.ChatBase):
    member_ids: Optional[List[int]] = None


@router.post("/me/chats", response_model=schemas.ChatWithMembers, status_code=201)
def create_my_chat(
    payload: MyChatCreate,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    member_ids = payload.member_ids or []
    unique_member_ids = list(dict.fromkeys(member_ids))
    if current_user.id not in unique_member_ids:
        unique_member_ids.append(current_user.id)

    if len(unique_member_ids) < 2:
        raise HTTPException(status_code=400, detail="Un chat requiere al menos 2 participantes")
    if payload.chat_type == "private" and len(unique_member_ids) != 2:
        raise HTTPException(status_code=400, detail="Un chat privado solo admite 2 personas")
    if payload.chat_type == "group" and len(unique_member_ids) < 3:
        raise HTTPException(status_code=400, detail="Un chat grupal requiere al menos 3 miembros")

    members = db.query(models.User).filter(models.User.id.in_(unique_member_ids)).all()
    if len(members) != len(unique_member_ids):
        raise HTTPException(status_code=400, detail="Algunos usuarios no existen")

    chat_data = payload.model_dump(exclude={"member_ids"})
    chat = models.Chat(**chat_data, created_by=current_user.id)
    chat.members = members
    db.add(chat)
    db.commit()
    chat = (
        db.query(models.Chat)
        .options(selectinload(models.Chat.members))
        .filter(models.Chat.id == chat.id)
        .first()
    )
    return chat


