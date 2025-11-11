from pydantic import BaseModel, EmailStr, Field, field_validator
from datetime import datetime
from typing import Optional, List
import json


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


# Marketplace Schemas
class MarketplaceItemBase(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    price: float = Field(..., gt=0, description="Price must be greater than 0")
    currency: str = Field(default='USD', max_length=3)
    image_urls: Optional[List[str]] = None
    is_negotiable: bool = Field(default=True)
    category_id: Optional[int] = None

    @field_validator('image_urls', mode='before')
    @classmethod
    def _parse_image_urls(cls, v):
        if v is None or isinstance(v, list):
            return v
        if isinstance(v, (bytes, bytearray)):
            try:
                v = v.decode('utf-8')
            except Exception:
                return None
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None
            try:
                parsed = json.loads(v)
                return parsed if isinstance(parsed, list) else None
            except Exception:
                return None
        return None


class MarketplaceItemCreate(MarketplaceItemBase):
    message_id: int
    chat_id: int


class MarketplaceItemUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=200)
    description: Optional[str] = None
    price: Optional[float] = Field(None, gt=0)
    current_price: Optional[float] = Field(None, gt=0, description="Negotiated price")
    is_negotiable: Optional[bool] = None
    status: Optional[str] = Field(None, pattern='^(active|sold|cancelled|pending)$')
    image_urls: Optional[List[str]] = None


class MarketplaceItem(MarketplaceItemBase):
    id: int
    message_id: int
    seller_id: int
    chat_id: int
    status: str
    current_price: Optional[float] = None
    created_at: datetime
    sold_at: Optional[datetime] = None
    image_urls: Optional[List[str]] = None  # Override to handle JSON conversion
    
    class Config:
        from_attributes = True


class MarketplaceItemWithSeller(MarketplaceItem):
    seller: User


class MarketplaceItemWithMessage(MarketplaceItem):
    message: Message


# Marketplace Category Schemas
class MarketplaceCategoryBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None


class MarketplaceCategoryCreate(MarketplaceCategoryBase):
    pass


class MarketplaceCategoryUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None


class MarketplaceCategory(MarketplaceCategoryBase):
    id: int
    created_at: datetime
    
    class Config:
        from_attributes = True


# Seller Profile Schemas
class SellerProfileBase(BaseModel):
    display_name: Optional[str] = None
    bio: Optional[str] = None
    location: Optional[str] = None
    contact_info: Optional[str] = None
    category_ids: Optional[List[int]] = None


class SellerProfileCreate(SellerProfileBase):
    user_id: int


class SellerProfileUpdate(BaseModel):
    display_name: Optional[str] = None
    bio: Optional[str] = None
    location: Optional[str] = None
    contact_info: Optional[str] = None
    category_ids: Optional[List[int]] = None


class SellerProfile(SellerProfileBase):
    id: int
    user_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    categories: Optional[List[MarketplaceCategory]] = None
    
    class Config:
        from_attributes = True


# Pagination Schema
class PaginatedResponse(BaseModel):
    items: List
    total: int
    page: int
    page_size: int
    total_pages: int

