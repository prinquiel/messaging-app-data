from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, Boolean, Table
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

