from __future__ import annotations

from sqlalchemy.orm import Session

from src.clients.search_api import SearchApiClient
from src.repositories.favorites import FavoritesRepository
from src.repositories.payments import PaymentsRepository
from src.repositories.saved_searches import SavedSearchesRepository
from src.repositories.search_history import SearchHistoryRepository
from src.repositories.subscriptions import SubscriptionsRepository
from src.repositories.users import UsersRepository


def get_search_api_client() -> SearchApiClient:
    return SearchApiClient()


def get_users_repository(db: Session) -> UsersRepository:
    return UsersRepository(db)


def get_favorites_repository(db: Session) -> FavoritesRepository:
    return FavoritesRepository(db)


def get_saved_searches_repository(db: Session) -> SavedSearchesRepository:
    return SavedSearchesRepository(db)


def get_search_history_repository(db: Session) -> SearchHistoryRepository:
    return SearchHistoryRepository(db)


def get_subscriptions_repository(db: Session) -> SubscriptionsRepository:
    return SubscriptionsRepository(db)


def get_payments_repository(db: Session) -> PaymentsRepository:
    return PaymentsRepository(db)