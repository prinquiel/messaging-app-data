from __future__ import annotations

from typing import List, Dict

from sqlalchemy.orm import Session

from app import models

DEFAULT_MARKETPLACE_CATEGORIES: List[Dict[str, str]] = [
    {
        "name": "Tecnología",
        "description": "Computadoras, celulares, accesorios y electrónica en general.",
    },
    {
        "name": "Hogar",
        "description": "Muebles, decoración, electrodomésticos y artículos para el hogar.",
    },
    {
        "name": "Vehículos",
        "description": "Autos, motos, bicicletas y repuestos.",
    },
    {
        "name": "Moda",
        "description": "Ropa, zapatos, bolsos y accesorios personales.",
    },
    {
        "name": "Deportes",
        "description": "Equipamiento deportivo, ropa atlética y artículos de recreación.",
    },
    {
        "name": "Servicios",
        "description": "Oficios, consultorías y servicios profesionales.",
    },
    {
        "name": "Mascotas",
        "description": "Accesorios, alimentos y servicios para mascotas.",
    },
    {
        "name": "Coleccionables",
        "description": "Arte, juguetes, figuras y artículos de colección.",
    },
]


def seed_default_categories(db: Session) -> None:
    """
    Ensure the predefined marketplace categories exist once at startup.

    This keeps the dropdowns consistent between entornos y evita que el usuario
    tenga que crear categorías manualmente antes de publicar productos.
    """

    existing = {
        row.name
        for row in db.query(models.MarketplaceCategory.name).all()
    }
    new_categories = [
        models.MarketplaceCategory(**category)
        for category in DEFAULT_MARKETPLACE_CATEGORIES
        if category["name"] not in existing
    ]
    if new_categories:
        db.add_all(new_categories)
        db.commit()

