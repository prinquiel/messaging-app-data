from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Type
from pydantic import BaseModel
import math

from app.database import engine, get_db, Base
from app import models, schemas

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Messaging App API",
    description="API completa para aplicación de mensajería tipo WhatsApp/Discord",
    version="1.0.0"
)



def paginate_query(
    query,
    page: int,
    page_size: int,
    include_total: bool,
    schema_class: Optional[Type[BaseModel]] = None,
):
    """Paginación eficiente con metadata.

    - Evita count() salvo que include_total sea True
    - Obtiene page_size + 1 filas para calcular has_next sin costo extra
    """
    offset_value = (page - 1) * page_size
    rows = query.offset(offset_value).limit(page_size + 1).all()
    has_next = len(rows) > page_size
    items = rows[:page_size]

    # Convert ORM items to Pydantic dicts if a schema is provided
    if schema_class is not None:
        serialized_items = [
            schema_class.model_validate(item, from_attributes=True).model_dump()
            for item in items
        ]
    else:
        serialized_items = items

    result = {
        "items": serialized_items,
        "page": page,
        "page_size": page_size,
        "has_next": has_next,
        "next_page": page + 1 if has_next else None,
        "prev_page": page - 1 if page > 1 else None,
    }

    if include_total:
        total = query.count()
        total_pages = math.ceil(total / page_size) if page_size > 0 else 0
        result.update({
            "total": total,
            "total_pages": total_pages,
        })

    return result



@app.get("/")
async def root():
    return {
        "message": "Messaging App API",
        "version": "1.0.0",
        "endpoints": {
            "users": "/users",
            "chats": "/chats",
            "messages": "/messages",
            "docs": "/docs"
        }
    }


@app.get("/health")
async def health_check():
    return {"status": "healthy"}



@app.post("/users", response_model=schemas.User, status_code=201)
def create_user(user: schemas.UserCreate, db: Session = Depends(get_db)):
    """Crear un nuevo usuario"""
    # Verificar si el username o email ya existen
    existing_user = db.query(models.User).filter(
        (models.User.username == user.username) | (models.User.email == user.email)
    ).first()
    
    if existing_user:
        raise HTTPException(status_code=400, detail="Username o email ya existe")
    
    db_user = models.User(**user.model_dump())
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


@app.get("/users", response_model=dict)
def get_users(
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(50, ge=1, le=250, description="Cantidad de items por página (máximo 250)"),
    include_total: bool = Query(False, description="Incluir total y total_pages (usa count())"),
    db: Session = Depends(get_db)
):
    """Obtener lista de usuarios con paginación"""
    query = db.query(models.User).filter(models.User.is_active == True).order_by(models.User.created_at.desc())
    return paginate_query(query, page, page_size, include_total, schemas.User)


@app.get("/users/{user_id}", response_model=schemas.User)
def get_user(user_id: int, db: Session = Depends(get_db)):
    """Obtener un usuario por ID"""
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return user



@app.post("/chats", response_model=schemas.Chat, status_code=201)
def create_chat(chat: schemas.ChatCreate, db: Session = Depends(get_db)):
    """Crear un nuevo chat (privado o grupal)"""
    # Verificar que los usuarios existan
    users = db.query(models.User).filter(models.User.id.in_(chat.member_ids)).all()
    if len(users) != len(chat.member_ids):
        raise HTTPException(status_code=400, detail="Algunos usuarios no existen")
    
    # Crear el chat
    chat_data = chat.model_dump(exclude={'member_ids'})
    db_chat = models.Chat(**chat_data, created_by=chat.member_ids[0])
    db_chat.members = users
    
    db.add(db_chat)
    db.commit()
    db.refresh(db_chat)
    return db_chat


@app.get("/chats", response_model=dict)
def get_chats(
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(50, ge=1, le=250, description="Cantidad de items por página (máximo 250)"),
    chat_type: str = Query(None, description="Filtrar por tipo: 'private' o 'group'"),
    include_total: bool = Query(False, description="Incluir total y total_pages (usa count())"),
    db: Session = Depends(get_db)
):
    """Obtener lista de chats con paginación"""
    query = db.query(models.Chat).order_by(models.Chat.created_at.desc())
    
    if chat_type:
        query = query.filter(models.Chat.chat_type == chat_type)
    
    return paginate_query(query, page, page_size, include_total, schemas.Chat)


@app.get("/chats/{chat_id}", response_model=schemas.ChatWithMembers)
def get_chat(chat_id: int, db: Session = Depends(get_db)):
    """Obtener un chat por ID con sus miembros"""
    chat = db.query(models.Chat).filter(models.Chat.id == chat_id).first()
    if not chat:
        raise HTTPException(status_code=404, detail="Chat no encontrado")
    return chat


@app.get("/chats/{chat_id}/messages", response_model=dict)
def get_chat_messages(
    chat_id: int,
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(50, ge=1, le=250, description="Cantidad de items por página (máximo 250)"),
    include_total: bool = Query(False, description="Incluir total y total_pages (usa count())"),
    db: Session = Depends(get_db)
):
    """Obtener mensajes de un chat específico con paginación"""
    # Verificar que el chat existe
    chat = db.query(models.Chat).filter(models.Chat.id == chat_id).first()
    if not chat:
        raise HTTPException(status_code=404, detail="Chat no encontrado")
    
    query = db.query(models.Message).filter(
        models.Message.chat_id == chat_id,
        models.Message.is_deleted == False
    ).order_by(models.Message.sent_at.desc())
    
    return paginate_query(query, page, page_size, include_total, schemas.Message)



@app.post("/messages", response_model=schemas.Message, status_code=201)
def create_message(message: schemas.MessageCreate, db: Session = Depends(get_db)):
    """Crear un nuevo mensaje"""
    # Verificar que el chat y usuario existen
    chat = db.query(models.Chat).filter(models.Chat.id == message.chat_id).first()
    if not chat:
        raise HTTPException(status_code=404, detail="Chat no encontrado")
    
    user = db.query(models.User).filter(models.User.id == message.sender_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    db_message = models.Message(**message.model_dump())
    db.add(db_message)
    db.commit()
    db.refresh(db_message)
    return db_message


@app.get("/messages", response_model=dict)
def get_messages(
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(50, ge=1, le=250, description="Cantidad de items por página (máximo 250)"),
    include_total: bool = Query(False, description="Incluir total y total_pages (usa count())"),
    db: Session = Depends(get_db)
):
    """Obtener todos los mensajes con paginación"""
    query = db.query(models.Message).filter(
        models.Message.is_deleted == False
    ).order_by(models.Message.sent_at.desc())
    
    return paginate_query(query, page, page_size, include_total, schemas.Message)


@app.get("/messages/{message_id}", response_model=schemas.MessageWithSender)
def get_message(message_id: int, db: Session = Depends(get_db)):
    """Obtener un mensaje por ID con información del remitente"""
    message = db.query(models.Message).filter(models.Message.id == message_id).first()
    if not message:
        raise HTTPException(status_code=404, detail="Mensaje no encontrado")
    return message



@app.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    """Obtener estadísticas generales de la plataforma"""
    total_users = db.query(models.User).count()
    active_users = db.query(models.User).filter(models.User.is_active == True).count()
    total_chats = db.query(models.Chat).count()
    total_messages = db.query(models.Message).filter(models.Message.is_deleted == False).count()
    
    return {
        "total_users": total_users,
        "active_users": active_users,
        "total_chats": total_chats,
        "total_messages": total_messages
    }
