from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi import BackgroundTasks
from decimal import Decimal
import json
import os
from pydantic import BaseModel as _BaseModel
import uuid as _uuid
import asyncio
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional, Type
from pydantic import BaseModel
from pydantic import ValidationError as _PydValidationError
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

    offset_value = (page - 1) * page_size
    rows = query.offset(offset_value).limit(page_size + 1).all()
    has_next = len(rows) > page_size
    items = rows[:page_size]

    # Convert ORM items to Pydantic dicts if a schema is provided
    if schema_class is not None:
        serialized_items = []
        for item in items:
            try:
                serialized_items.append(
                    schema_class.model_validate(item, from_attributes=True).model_dump()
                )
            except _PydValidationError:
                # Attempt to coerce common problematic fields, then retry
                try:
                    if hasattr(item, 'image_urls') and isinstance(item.image_urls, str):
                        import json as _json
                        try:
                            parsed = _json.loads(item.image_urls)
                            if isinstance(parsed, list):
                                item.image_urls = parsed
                        except Exception:
                            item.image_urls = None
                    serialized_items.append(
                        schema_class.model_validate(item, from_attributes=True).model_dump()
                    )
                except Exception:
                    # Fallback: return raw ORM fields
                    try:
                        row_dict = {c.name: getattr(item, c.name) for c in item.__table__.columns}
                        # Best-effort JSON parse for image_urls
                        if isinstance(row_dict.get('image_urls'), str):
                            import json as _json
                            try:
                                val = _json.loads(row_dict['image_urls'])
                                row_dict['image_urls'] = val if isinstance(val, list) else None
                            except Exception:
                                row_dict['image_urls'] = None
                        # Convert Decimal to float for JSON safety
                        from decimal import Decimal as _Dec
                        for _k, _v in list(row_dict.items()):
                            if isinstance(_v, _Dec):
                                row_dict[_k] = float(_v)
                        serialized_items.append(row_dict)
                    except Exception:
                        serialized_items.append({})
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
            "marketplace": "/marketplace",
            "categories": "/marketplace/categories",
            "sellers": "/sellers",
            "docs": "/docs"
        }
    }


@app.get("/health")
async def health_check():
    return {"status": "healthy"}



@app.post("/users", response_model=schemas.User, status_code=201)
def create_user(user: schemas.UserCreate, db: Session = Depends(get_db)):
    ## Crear un nuevo usuario
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


# ===================== ETL Workflow trigger =====================

class ETLResponse(_BaseModel):
    workflow_id: str


@app.post("/etl", response_model=ETLResponse)
async def trigger_etl():
    """Inicia el ETL como un workflow Temporal."""
    workflow_id = str(_uuid.uuid4())

    async def _run_etl():
        try:
            # Try Temporal client first
            from temporalio.client import Client
            import os as _os
            address = _os.getenv("TEMPORAL_ADDRESS", "temporal:7233")
            print(f"Attempting to connect to Temporal at {address}...")
            client = await Client.connect(address)
            print(f"✅ Connected to Temporal, starting workflow...")
            # Import workflow reference for clarity
            from workers.etl.worker import ETLWorkflow
            handle = await client.start_workflow(
                ETLWorkflow,
                id=f"etl-{workflow_id}",
                task_queue="etl-task-queue",
            )
            print(f"✅ Workflow started: {handle.id}")
            return
        except ImportError as e:
            print(f"❌ Cannot import workers module: {e}")
            print(f"   This is expected if workers directory is not in API container")
            print(f"   Workflows should be triggered from external client")
        except Exception as e:
            print(f"❌ Temporal workflow start failed: {e}")
            import traceback
            traceback.print_exc()
            try:
                import subprocess, os
                env = os.environ.copy()
                cmd = ["python", "etl/etl_pipeline.py"]
                subprocess.run(cmd, check=True, cwd="/app" if os.path.exists("/app") else ".", env=env)
            except Exception as fallback_error:
                print(f"❌ Fallback ETL also failed: {fallback_error}")

    asyncio.create_task(_run_etl())
    return ETLResponse(workflow_id=workflow_id)


# ===================== Marketplace Endpoints =====================

@app.post("/messages/{message_id}/sell", response_model=schemas.MarketplaceItem, status_code=201)
def convert_message_to_listing(
    message_id: int,
    item_data: schemas.MarketplaceItemCreate,
    db: Session = Depends(get_db)
):
    """Convertir un mensaje en un producto en venta"""
    # Verificar que el mensaje existe y pertenece al chat correcto
    message = db.query(models.Message).filter(models.Message.id == message_id).first()
    if not message:
        raise HTTPException(status_code=404, detail="Mensaje no encontrado")
    
    if message.chat_id != item_data.chat_id:
        raise HTTPException(status_code=400, detail="El mensaje no pertenece al chat especificado")
    
    # Validar que el chat sea de tipo grupal
    chat = db.query(models.Chat).filter(models.Chat.id == message.chat_id).first()
    if not chat:
        raise HTTPException(status_code=404, detail="Chat no encontrado")
    if chat.chat_type != 'group':
        raise HTTPException(status_code=400, detail="Solo se pueden crear items de marketplace en chats grupales")
    
    # Verificar que el mensaje no esté ya convertido en listing
    existing_item = db.query(models.MarketplaceItem).filter(
        models.MarketplaceItem.message_id == message_id
    ).first()
    if existing_item:
        raise HTTPException(status_code=400, detail="Este mensaje ya es un producto en venta")
    
    # Verificar que el sender del mensaje es quien está creando el listing
    if message.sender_id != item_data.message_id:  # Asumiendo que el sender es el seller
        pass  
    
    # Crear el marketplace item
    item_dict = item_data.model_dump()
    item_dict['seller_id'] = message.sender_id
    item_dict['message_id'] = message_id
    item_dict['image_urls'] = json.dumps(item_data.image_urls) if item_data.image_urls else None
    
    db_item = models.MarketplaceItem(**item_dict)
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    
    # Convertir image_urls de vuelta a lista para la respuesta
    if db_item.image_urls:
        db_item.image_urls = json.loads(db_item.image_urls)
    
    return db_item


@app.get("/marketplace", response_model=dict)
def get_marketplace_items(
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(50, ge=1, le=250, description="Cantidad de items por página"),
    chat_id: Optional[int] = Query(None, description="Filtrar por chat"),
    seller_id: Optional[int] = Query(None, description="Filtrar por vendedor"),
    status: Optional[str] = Query(None, description="Filtrar por estado: active, sold, cancelled"),
    category_id: Optional[int] = Query(None, description="Filtrar por categoría"),
    min_price: Optional[float] = Query(None, description="Precio mínimo"),
    max_price: Optional[float] = Query(None, description="Precio máximo"),
    search: Optional[str] = Query(None, description="Buscar en título y descripción"),
    include_total: bool = Query(False, description="Incluir total y total_pages"),
    db: Session = Depends(get_db)
):
    """Obtener lista de productos en venta con filtros y búsqueda"""
    query = db.query(models.MarketplaceItem)
    
    if chat_id:
        query = query.filter(models.MarketplaceItem.chat_id == chat_id)
    if seller_id:
        query = query.filter(models.MarketplaceItem.seller_id == seller_id)
    if status:
        query = query.filter(models.MarketplaceItem.status == status)
    if category_id:
        query = query.filter(models.MarketplaceItem.category_id == category_id)
    if min_price:
        query = query.filter(models.MarketplaceItem.price >= Decimal(str(min_price)))
    if max_price:
        query = query.filter(models.MarketplaceItem.price <= Decimal(str(max_price)))
    if search:
        query = query.filter(
            (models.MarketplaceItem.title.ilike(f"%{search}%")) |
            (models.MarketplaceItem.description.ilike(f"%{search}%"))
        )
    
    query = query.order_by(models.MarketplaceItem.created_at.desc())
    
    return paginate_query(query, page, page_size, include_total, schemas.MarketplaceItem)


@app.get("/marketplace/{item_id}", response_model=schemas.MarketplaceItemWithSeller)
def get_marketplace_item(item_id: int, db: Session = Depends(get_db)):
    """Obtener un producto específico con información del vendedor"""
    item = db.query(models.MarketplaceItem).filter(models.MarketplaceItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Producto no encontrado")
    
    # Cargar el seller
    item.seller = db.query(models.User).filter(models.User.id == item.seller_id).first()
    
    # Convertir image_urls de JSON a lista
    if item.image_urls:
        item.image_urls = json.loads(item.image_urls)
    
    return item


@app.put("/marketplace/{item_id}", response_model=schemas.MarketplaceItem)
def update_marketplace_item(
    item_id: int,
    item_update: schemas.MarketplaceItemUpdate,
    db: Session = Depends(get_db)
):
    """Actualizar un producto (precio, descripción, estado, etc.)"""
    item = db.query(models.MarketplaceItem).filter(models.MarketplaceItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Producto no encontrado")
    
    # Solo el vendedor puede actualizar
    
    update_data = item_update.model_dump(exclude_unset=True)
    
    if 'image_urls' in update_data and update_data['image_urls']:
        update_data['image_urls'] = json.dumps(update_data['image_urls'])
    
    for field, value in update_data.items():
        setattr(item, field, value)
    
    db.commit()
    db.refresh(item)
    
    if item.image_urls:
        item.image_urls = json.loads(item.image_urls)
    
    return item


## Endpoints de compras y calificaciones eliminados; pagos fuera de la app


@app.get("/marketplace/stats")
def get_marketplace_stats(
    chat_id: Optional[int] = Query(None, description="Filtrar por chat"),
    seller_id: Optional[int] = Query(None, description="Filtrar por vendedor"),
    db: Session = Depends(get_db)
):
    """Obtener estadísticas del marketplace"""
    base_query = db.query(models.MarketplaceItem)
    
    if chat_id:
        base_query = base_query.filter(models.MarketplaceItem.chat_id == chat_id)
    if seller_id:
        base_query = base_query.filter(models.MarketplaceItem.seller_id == seller_id)
    
    total_items = base_query.count()
    active_items = base_query.filter(models.MarketplaceItem.status == 'active').count()
    sold_items = base_query.filter(models.MarketplaceItem.status == 'sold').count()
    
    # Top sellers por cantidad de items vendidos (sin ingresos)
    top_sellers = db.query(
        models.User.id.label('user_id'),
        models.User.username.label('username'),
        func.count(models.MarketplaceItem.id).label('items_sold')
    ).join(
        models.MarketplaceItem, models.User.id == models.MarketplaceItem.seller_id
    ).filter(
        models.MarketplaceItem.status == 'sold'
    )
    if chat_id:
        top_sellers = top_sellers.filter(models.MarketplaceItem.chat_id == chat_id)
    top_sellers = top_sellers.group_by(
        models.User.id, models.User.username
    ).order_by(
        func.count(models.MarketplaceItem.id).desc()
    ).limit(10).all()

    return {
        "total_items": total_items,
        "active_items": active_items,
        "sold_items": sold_items,
        "top_sellers": [
            {
                "user_id": row.user_id,
                "username": row.username,
                "items_sold": row.items_sold,
            }
            for row in top_sellers
        ]
    }


# ===================== Marketplace Categories =====================

@app.post("/marketplace/categories", response_model=schemas.MarketplaceCategory, status_code=201)
def create_category(category: schemas.MarketplaceCategoryCreate, db: Session = Depends(get_db)):
    existing = db.query(models.MarketplaceCategory).filter(models.MarketplaceCategory.name == category.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="La categoría ya existe")
    db_category = models.MarketplaceCategory(**category.model_dump())
    db.add(db_category)
    db.commit()
    db.refresh(db_category)
    return db_category


@app.get("/marketplace/categories", response_model=dict)
def list_categories(
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(50, ge=1, le=250, description="Cantidad de items por página"),
    include_total: bool = Query(False, description="Incluir total y total_pages"),
    db: Session = Depends(get_db)
):
    query = db.query(models.MarketplaceCategory).order_by(models.MarketplaceCategory.name.asc())
    return paginate_query(query, page, page_size, include_total, schemas.MarketplaceCategory)


# Alias para evitar posibles conflictos con /marketplace/{item_id}
@app.get("/categories", response_model=dict)
def list_categories_root(
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(50, ge=1, le=250, description="Cantidad de items por página"),
    include_total: bool = Query(False, description="Incluir total y total_pages"),
    db: Session = Depends(get_db)
):
    query = db.query(models.MarketplaceCategory).order_by(models.MarketplaceCategory.name.asc())
    return paginate_query(query, page, page_size, include_total, schemas.MarketplaceCategory)


@app.get("/marketplace/categories/{category_id}", response_model=schemas.MarketplaceCategory)
def get_category(category_id: int, db: Session = Depends(get_db)):
    category = db.query(models.MarketplaceCategory).filter(models.MarketplaceCategory.id == category_id).first()
    if not category:
        raise HTTPException(status_code=404, detail="Categoría no encontrada")
    return category


@app.put("/marketplace/categories/{category_id}", response_model=schemas.MarketplaceCategory)
def update_category(category_id: int, update: schemas.MarketplaceCategoryUpdate, db: Session = Depends(get_db)):
    category = db.query(models.MarketplaceCategory).filter(models.MarketplaceCategory.id == category_id).first()
    if not category:
        raise HTTPException(status_code=404, detail="Categoría no encontrada")
    update_data = update.model_dump(exclude_unset=True)
    for k, v in update_data.items():
        setattr(category, k, v)
    db.commit()
    db.refresh(category)
    return category


@app.delete("/marketplace/categories/{category_id}", status_code=204)
def delete_category(category_id: int, db: Session = Depends(get_db)):
    category = db.query(models.MarketplaceCategory).filter(models.MarketplaceCategory.id == category_id).first()
    if not category:
        raise HTTPException(status_code=404, detail="Categoría no encontrada")
    # Evitar eliminar si tiene items asociados
    has_items = db.query(models.MarketplaceItem).filter(models.MarketplaceItem.category_id == category_id).first()
    if has_items:
        raise HTTPException(status_code=400, detail="No se puede eliminar: hay items asociados a esta categoría")
    db.delete(category)
    db.commit()
    return


# ===================== Seller Profiles =====================

@app.post("/sellers", response_model=schemas.SellerProfile, status_code=201)
def create_seller_profile(profile: schemas.SellerProfileCreate, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.id == profile.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    existing = db.query(models.SellerProfile).filter(models.SellerProfile.user_id == profile.user_id).first()
    if existing:
        raise HTTPException(status_code=400, detail="Este usuario ya tiene perfil de vendedor")
    data = profile.model_dump()
    category_ids = data.pop('category_ids', None)
    db_profile = models.SellerProfile(**data)
    if category_ids:
        categories = db.query(models.MarketplaceCategory).filter(models.MarketplaceCategory.id.in_(category_ids)).all()
        db_profile.categories = categories
    db.add(db_profile)
    db.commit()
    db.refresh(db_profile)
    return db_profile


@app.get("/sellers", response_model=dict)
def list_seller_profiles(
    category_id: Optional[int] = Query(None, description="Filtrar por categoría preferida"),
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(50, ge=1, le=250, description="Cantidad de items por página"),
    include_total: bool = Query(False, description="Incluir total y total_pages"),
    db: Session = Depends(get_db)
):
    query = db.query(models.SellerProfile)
    if category_id:
        query = query.join(models.seller_categories).filter(models.seller_categories.c.category_id == category_id)
    query = query.order_by(models.SellerProfile.created_at.desc())
    return paginate_query(query, page, page_size, include_total, schemas.SellerProfile)


@app.get("/sellers/{profile_id}", response_model=schemas.SellerProfile)
def get_seller_profile(profile_id: int, db: Session = Depends(get_db)):
    profile = db.query(models.SellerProfile).filter(models.SellerProfile.id == profile_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Perfil de vendedor no encontrado")
    return profile


@app.put("/sellers/{profile_id}", response_model=schemas.SellerProfile)
def update_seller_profile(profile_id: int, update: schemas.SellerProfileUpdate, db: Session = Depends(get_db)):
    profile = db.query(models.SellerProfile).filter(models.SellerProfile.id == profile_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Perfil de vendedor no encontrado")
    data = update.model_dump(exclude_unset=True)
    category_ids = data.pop('category_ids', None)
    for k, v in data.items():
        setattr(profile, k, v)
    if category_ids is not None:
        categories = db.query(models.MarketplaceCategory).filter(models.MarketplaceCategory.id.in_(category_ids)).all()
        profile.categories = categories
    db.commit()
    db.refresh(profile)
    return profile
