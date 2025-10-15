from pydantic import BaseModel, EmailStr, Field
from datetime import datetime
from typing import Optional, List


# User Schemas
class UserBase(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    full_name: str = Field(..., min_length=1, max_length=100)
    phone_number: Optional[str] = None
    bio: Optional[str] = None
    avatar_url: Optional[str] = None


class UserCreate(UserBase):
    pass


class User(UserBase):
    id: int
    is_active: bool
    created_at: datetime
    last_seen: Optional[datetime] = None
    
    class Config:
        from_attributes = True


# Chat Schemas
class ChatBase(BaseModel):
    name: Optional[str] = None
    chat_type: str = Field(..., pattern='^(private|group)$')
    description: Optional[str] = None
    avatar_url: Optional[str] = None


class ChatCreate(ChatBase):
    member_ids: List[int] = Field(..., min_items=2)


class Chat(ChatBase):
    id: int
    created_at: datetime
    created_by: Optional[int] = None
    
    class Config:
        from_attributes = True


class ChatWithMembers(Chat):
    members: List[User]


# Message Schemas
class MessageBase(BaseModel):
    content: str = Field(..., min_length=1)
    message_type: str = Field(default='text')


class MessageCreate(MessageBase):
    chat_id: int
    sender_id: int


class Message(MessageBase):
    id: int
    sender_id: int
    chat_id: int
    sent_at: datetime
    edited_at: Optional[datetime] = None
    is_deleted: bool
    
    class Config:
        from_attributes = True


class MessageWithSender(Message):
    sender: User


# Pagination Schema
class PaginatedResponse(BaseModel):
    items: List
    total: int
    page: int
    page_size: int
    total_pages: int

