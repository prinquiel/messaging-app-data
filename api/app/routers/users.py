from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app import models, schemas
from app.database import get_db

router = APIRouter(prefix="", tags=["users"])


@router.get("/users/search", response_model=List[schemas.User])
def search_users(
    query: str = Query(..., min_length=2, description="Fragmento de username o email"),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    search = f"%{query}%"
    users = (
        db.query(models.User)
        .filter(
            (models.User.username.ilike(search))
            | (models.User.email.ilike(search))
            | (models.User.full_name.ilike(search))
        )
        .order_by(models.User.username.asc())
        .limit(limit)
        .all()
    )
    return users


