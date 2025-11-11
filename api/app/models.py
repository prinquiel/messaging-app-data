from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, Boolean, Table, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


chat_members = Table(
    'chat_members',
    Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True),
    Column('chat_id', Integer, ForeignKey('chats.id', ondelete='CASCADE'), primary_key=True),
    Column('joined_at', DateTime(timezone=True), server_default=func.now())
)


class User(Base):
    """Modelo de Usuario - Representa a cada usuario de la plataforma"""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), unique=True, nullable=False, index=True)
    full_name = Column(String(100), nullable=False)
    phone_number = Column(String(20))
    bio = Column(Text)
    avatar_url = Column(String(500))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_seen = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    sent_messages = relationship('Message', back_populates='sender', cascade='all, delete-orphan')
    chats = relationship('Chat', secondary=chat_members, back_populates='members')
    items_sold = relationship('MarketplaceItem', back_populates='seller', foreign_keys='MarketplaceItem.seller_id')
    seller_profile = relationship('SellerProfile', back_populates='user', uselist=False, cascade='all, delete-orphan')


class Chat(Base):
    """Modelo de Chat - Puede ser conversación privada o grupal"""
    __tablename__ = 'chats'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100))  # Nombre del grupo (null para chats privados)
    chat_type = Column(String(20), nullable=False)  # 'private' o 'group'
    description = Column(Text)
    avatar_url = Column(String(500))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(Integer, ForeignKey('users.id', ondelete='SET NULL'))
    
    # Relationships
    messages = relationship('Message', back_populates='chat', cascade='all, delete-orphan')
    members = relationship('User', secondary=chat_members, back_populates='chats')
    marketplace_items = relationship('MarketplaceItem', back_populates='chat', cascade='all, delete-orphan')


class Message(Base):
    """Modelo de Mensaje - Cada mensaje enviado en un chat"""
    __tablename__ = 'messages'
    
    id = Column(Integer, primary_key=True, index=True)
    content = Column(Text, nullable=False)
    sender_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)
    chat_id = Column(Integer, ForeignKey('chats.id', ondelete='CASCADE'), nullable=False, index=True)
    sent_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    edited_at = Column(DateTime(timezone=True))
    is_deleted = Column(Boolean, default=False)
    message_type = Column(String(20), default='text')  # text, image, video, file, etc.
    
    # Relationships
    sender = relationship('User', back_populates='sent_messages')
    chat = relationship('Chat', back_populates='messages')
    marketplace_item = relationship('MarketplaceItem', back_populates='message', uselist=False)


class MarketplaceCategory(Base):
    __tablename__ = 'marketplace_categories'

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    # Relationships
    items = relationship('MarketplaceItem', back_populates='category')


# Asociación muchos-a-muchos entre SellerProfile y MarketplaceCategory
seller_categories = Table(
    'seller_categories',
    Base.metadata,
    Column('seller_profile_id', Integer, ForeignKey('seller_profiles.id', ondelete='CASCADE'), primary_key=True),
    Column('category_id', Integer, ForeignKey('marketplace_categories.id', ondelete='CASCADE'), primary_key=True),
    Column('linked_at', DateTime(timezone=True), server_default=func.now())
)


class SellerProfile(Base):
    """Perfil de Vendedor - información del usuario como vendedor"""
    __tablename__ = 'seller_profiles'

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), unique=True, nullable=False, index=True)
    display_name = Column(String(150))
    bio = Column(Text)
    location = Column(String(150))
    contact_info = Column(String(300))  # enlace/whatsapp/instagram, etc.
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    user = relationship('User', back_populates='seller_profile')
    categories = relationship('MarketplaceCategory', secondary=seller_categories, backref='sellers')


class MarketplaceItem(Base):
    """Modelo de Item de Marketplace - Convierte un mensaje en un producto en venta"""
    __tablename__ = 'marketplace_items'
    
    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, ForeignKey('messages.id', ondelete='CASCADE'), nullable=False, unique=True, index=True)
    seller_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)
    chat_id = Column(Integer, ForeignKey('chats.id', ondelete='CASCADE'), nullable=False, index=True)
    category_id = Column(Integer, ForeignKey('marketplace_categories.id', ondelete='SET NULL'))
    
    title = Column(String(200), nullable=False)
    description = Column(Text)
    price = Column(Numeric(10, 2), nullable=False)  # Decimal with 2 decimal places
    currency = Column(String(3), default='USD')  # USD, EUR, etc.
    
    # Image URLs (stored as JSON array or comma-separated)
    image_urls = Column(Text)  # JSON array of image URLs
    
    status = Column(String(20), default='active')  # active, sold, cancelled, pending
    
    # Negotiation
    is_negotiable = Column(Boolean, default=True)
    current_price = Column(Numeric(10, 2))  # For price negotiations
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    sold_at = Column(DateTime(timezone=True))
    
    # Relationships
    message = relationship('Message', back_populates='marketplace_item')
    seller = relationship('User', foreign_keys=[seller_id], back_populates='items_sold')
    chat = relationship('Chat', back_populates='marketplace_items')
    category = relationship('MarketplaceCategory', back_populates='items')




