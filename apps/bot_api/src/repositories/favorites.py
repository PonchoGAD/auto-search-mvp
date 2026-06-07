from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db.enums import FavoriteSource
from src.db.models import Favorite
from src.schemas.favorites import FavoriteCreateRequest
from src.schemas.search import SearchResultItem


class FavoritesRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def list_by_user(self, user_id: int) -> list[Favorite]:
        stmt = (
            select(Favorite)
            .where(Favorite.user_id == user_id)
            .order_by(Favorite.created_at.desc(), Favorite.id.desc())
        )
        return list(self.db.execute(stmt).scalars().all())

    def get_by_id(self, favorite_id: int) -> Optional[Favorite]:
        stmt = select(Favorite).where(Favorite.id == favorite_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_by_user_and_id(self, user_id: int, favorite_id: int) -> Optional[Favorite]:
        stmt = select(Favorite).where(
            Favorite.user_id == user_id,
            Favorite.id == favorite_id,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def get_by_user_and_listing(self, user_id: int, listing_id: str) -> Optional[Favorite]:
        clean_listing_id = str(listing_id or "").strip()

        if not clean_listing_id:
            return None

        stmt = select(Favorite).where(
            Favorite.user_id == user_id,
            Favorite.listing_id == clean_listing_id,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def create(self, user_id: int, payload: FavoriteCreateRequest) -> Favorite:
        listing_id = str(payload.listing_id or "").strip()
        if not listing_id:
            raise ValueError("listing_id is required")

        existing = self.get_by_user_and_listing(user_id, listing_id)
        if existing:
            return existing

        favorite = Favorite(
            user_id=user_id,
            listing_id=listing_id,
            source_url=payload.source_url,
            source_name=payload.source_name,
            title=payload.title,
            brand=payload.brand,
            model=payload.model,
            year=payload.year,
            mileage=payload.mileage,
            price=payload.price,
            currency=payload.currency,
            fuel=payload.fuel,
            region=payload.region,
            city=getattr(payload, "city", None),
            color=getattr(payload, "color", None),
            condition=getattr(payload, "condition", None),
            paint_condition=payload.paint_condition,
            image_url=payload.image_url,
            photos=payload.photos,
            created_at_ts=getattr(payload, "created_at_ts", None),
            payload=payload.payload,
            source_type=payload.source_type or FavoriteSource.SEARCH.value,
        )

        try:
            self.db.add(favorite)
            self.db.commit()
            self.db.refresh(favorite)
            return favorite
        except IntegrityError:
            self.db.rollback()
            existing = self.get_by_user_and_listing(user_id, listing_id)
            if existing:
                return existing
            raise

    def create_from_search_item(self, user_id: int, item: SearchResultItem) -> Favorite:
        item = item.ensure_listing_id()

        listing_id = str(item.listing_id or "").strip()
        if not listing_id:
            raise ValueError("listing_id is required")

        existing = self.get_by_user_and_listing(user_id, listing_id)
        if existing:
            return existing

        favorite = Favorite(
            user_id=user_id,
            listing_id=listing_id,
            source_url=item.source_url,
            source_name=item.source_name,
            title=item.title,
            brand=item.brand,
            model=item.model,
            year=item.year,
            mileage=item.mileage,
            price=item.price,
            currency=item.currency,
            fuel=item.fuel,
            region=item.region,
            city=getattr(item, "city", None),
            color=getattr(item, "color", None),
            condition=getattr(item, "condition", None),
            paint_condition=item.paint_condition,
            image_url=item.image_url,
            photos=item.photos,
            created_at_ts=getattr(item, "created_at_ts", None),
            payload=item.raw_payload,
            source_type=FavoriteSource.SEARCH.value,
        )

        try:
            self.db.add(favorite)
            self.db.commit()
            self.db.refresh(favorite)
            return favorite
        except IntegrityError:
            self.db.rollback()
            existing = self.get_by_user_and_listing(user_id, listing_id)
            if existing:
                return existing
            raise

    def delete_by_user_and_listing(self, user_id: int, listing_id: str) -> bool:
        existing = self.get_by_user_and_listing(user_id, listing_id)
        if not existing:
            return False

        self.db.delete(existing)
        self.db.commit()
        return True

    def delete_by_user_and_id(self, user_id: int, favorite_id: int) -> bool:
        stmt = delete(Favorite).where(
            Favorite.user_id == user_id,
            Favorite.id == favorite_id,
        )
        result = self.db.execute(stmt)
        self.db.commit()
        return bool(result.rowcount)

    def total_favorites(self) -> int:
        stmt = select(func.count(Favorite.id))
        return int(self.db.execute(stmt).scalar() or 0)

    def favorites_today(self) -> int:
        now = datetime.now(timezone.utc)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        stmt = select(func.count(Favorite.id)).where(Favorite.created_at >= start)
        return int(self.db.execute(stmt).scalar() or 0)

    def favorites_last_24h(self) -> int:
        since = datetime.now(timezone.utc) - timedelta(hours=24)

        stmt = select(func.count(Favorite.id)).where(Favorite.created_at >= since)
        return int(self.db.execute(stmt).scalar() or 0)

    def top_brands(self, limit: int = 20) -> list[dict]:
        stmt = (
            select(Favorite.brand, func.count(Favorite.id).label("count"))
            .where(Favorite.brand.is_not(None))
            .group_by(Favorite.brand)
            .order_by(func.count(Favorite.id).desc())
            .limit(limit)
        )

        rows = self.db.execute(stmt).all()

        return [
            {
                "brand": row.brand,
                "count": int(row.count or 0),
            }
            for row in rows
        ]

    def top_regions(self, limit: int = 20) -> list[dict]:
        stmt = (
            select(Favorite.region, func.count(Favorite.id).label("count"))
            .where(Favorite.region.is_not(None))
            .group_by(Favorite.region)
            .order_by(func.count(Favorite.id).desc())
            .limit(limit)
        )

        rows = self.db.execute(stmt).all()

        return [
            {
                "region": row.region,
                "count": int(row.count or 0),
            }
            for row in rows
        ]

    def top_cities(self, limit: int = 20) -> list[dict]:
        stmt = (
            select(Favorite.city, func.count(Favorite.id).label("count"))
            .where(Favorite.city.is_not(None))
            .group_by(Favorite.city)
            .order_by(func.count(Favorite.id).desc())
            .limit(limit)
        )

        rows = self.db.execute(stmt).all()

        return [
            {
                "city": row.city,
                "count": int(row.count or 0),
            }
            for row in rows
        ]

    def favorites_stats(self) -> dict:
        return {
            "total_favorites": self.total_favorites(),
            "favorites_today": self.favorites_today(),
            "favorites_last_24h": self.favorites_last_24h(),
            "top_brands": self.top_brands(limit=20),
            "top_regions": self.top_regions(limit=20),
            "top_cities": self.top_cities(limit=20),
        }