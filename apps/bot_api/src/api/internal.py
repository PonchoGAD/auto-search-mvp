from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.dependencies.auth import require_internal_api_key
from src.db.session import get_db
from src.repositories.notifications import NotificationsRepository
from src.repositories.payments import PaymentsRepository
from src.repositories.saved_searches import SavedSearchesRepository
from src.repositories.search_history import SearchHistoryRepository
from src.repositories.subscriptions import SubscriptionsRepository
from src.repositories.usage_limits import UsageLimitsRepository
from src.repositories.users import UsersRepository
from src.schemas.common import MessageResponse


router = APIRouter(prefix="/internal", tags=["internal"])


@router.get("/users/by-telegram-id")
def get_user_by_telegram_id(
    telegram_user_id: int = Query(...),
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> dict:
    repo = UsersRepository(db)
    user = repo.get_by_telegram_user_id(telegram_user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return {
        "id": user.id,
        "telegram_user_id": user.telegram_user_id,
        "telegram_chat_id": user.telegram_chat_id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "language_code": user.language_code,
        "role": user.role,
        "status": user.status,
        "is_premium": user.is_premium,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
    }


@router.get("/usage-limits")
def get_usage_limits(
    telegram_user_id: int = Query(...),
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> dict:
    user_repo = UsersRepository(db)
    user = user_repo.get_by_telegram_user_id(telegram_user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    limits_repo = UsageLimitsRepository(db)
    return limits_repo.get_limits_snapshot(user.id)


@router.get("/search-history")
def get_search_history(
    telegram_user_id: int = Query(...),
    limit: int = Query(default=50, ge=1, le=200),
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> list[dict]:
    user_repo = UsersRepository(db)
    user = user_repo.get_by_telegram_user_id(telegram_user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    repo = SearchHistoryRepository(db)
    rows = repo.list_by_user(user.id, limit=limit)

    return [
        {
            "id": row.id,
            "raw_query": row.raw_query,
            "query_payload": row.query_payload,
            "results_count": row.results_count,
            "latency_ms": row.latency_ms,
            "empty_result": row.empty_result,
            "created_at": row.created_at,
        }
        for row in rows
    ]


@router.post("/search-history", response_model=MessageResponse)
def create_search_history(
    payload: dict,
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> MessageResponse:
    telegram_user_id = payload.get("telegram_user_id")
    raw_query = payload.get("raw_query")

    if not telegram_user_id or not raw_query:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="telegram_user_id and raw_query are required",
        )

    user_repo = UsersRepository(db)
    user = user_repo.get_by_telegram_user_id(int(telegram_user_id))

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    repo = SearchHistoryRepository(db)
    repo.create(
        user_id=user.id,
        raw_query=str(raw_query),
        query_payload=payload.get("query_payload") or {},
        results_count=int(payload.get("results_count", 0) or 0),
        latency_ms=payload.get("latency_ms"),
        empty_result=bool(payload.get("empty_result", False)),
    )

    return MessageResponse(message="Search history created")


@router.get("/saved-searches/active")
def list_active_saved_searches(
    limit: int = Query(default=100, ge=1, le=1000),
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> list[dict]:
    from src.db.models import SavedSearch, User

    rows = list(
        db.execute(
            select(SavedSearch, User)
            .join(User, User.id == SavedSearch.user_id)
            .where(
                SavedSearch.status == "active",
                SavedSearch.is_alert_enabled.is_(True),
                User.status == "active",
            )
            .order_by(
                SavedSearch.last_checked_at.asc().nullsfirst(),
                SavedSearch.id.asc(),
            )
            .limit(limit)
        ).all()
    )

    result: list[dict] = []

    for saved_search, user in rows:
        result.append(
            {
                "id": saved_search.id,
                "user_id": saved_search.user_id,
                "telegram_user_id": user.telegram_user_id,
                "telegram_chat_id": user.telegram_chat_id,
                "name": saved_search.name,
                "raw_query": saved_search.raw_query,
                "query_payload": saved_search.query_payload,
                "status": saved_search.status,
                "is_alert_enabled": saved_search.is_alert_enabled,
                "last_seen_listing_id": saved_search.last_seen_listing_id,
                "last_checked_at": saved_search.last_checked_at,
                "created_at": saved_search.created_at,
                "updated_at": saved_search.updated_at,
            }
        )

    return result


@router.post("/saved-searches/{saved_search_id}/mark-checked", response_model=MessageResponse)
def mark_saved_search_checked(
    saved_search_id: int,
    payload: dict,
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> MessageResponse:
    repo = SavedSearchesRepository(db)
    entity = repo.get_by_id(saved_search_id)

    if not entity:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Saved search not found",
        )

    repo.mark_checked(
        entity=entity,
        last_seen_listing_id=payload.get("last_seen_listing_id"),
    )

    return MessageResponse(message="Saved search marked as checked")


@router.get("/notifications/pending")
def list_pending_notifications(
    limit: int = Query(default=100, ge=1, le=500),
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> list[dict]:
    repo = NotificationsRepository(db)
    rows = repo.list_pending(limit=limit)

    return [
        {
            "id": row.id,
            "user_id": row.user_id,
            "type": row.type,
            "status": row.status,
            "dedup_key": row.dedup_key,
            "payload": row.payload,
            "error_message": row.error_message,
            "sent_at": row.sent_at,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
        for row in rows
    ]


@router.post("/notifications", response_model=MessageResponse)
def create_notification(
    payload: dict,
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> MessageResponse:
    user_id = payload.get("user_id")
    notification_type = payload.get("type")

    if not user_id or not notification_type:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="user_id and type are required",
        )

    repo = NotificationsRepository(db)
    dedup_key = payload.get("dedup_key")

    notification, created = repo.safe_create(
        user_id=int(user_id),
        type=str(notification_type),
        payload=payload.get("payload") or {},
        dedup_key=dedup_key,
        status=payload.get("status") or "pending",
    )

    if not created:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Notification already exists",
        )

    return MessageResponse(message=f"Notification created: {notification.id}")


@router.post("/notifications/{notification_id}/mark-sent", response_model=MessageResponse)
def mark_notification_sent(
    notification_id: int,
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> MessageResponse:
    repo = NotificationsRepository(db)
    notification = repo.mark_sent_by_id(notification_id)

    if not notification:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Notification not found",
        )

    return MessageResponse(message="Notification marked as sent")


@router.post("/notifications/{notification_id}/mark-failed", response_model=MessageResponse)
def mark_notification_failed(
    notification_id: int,
    payload: dict | None = None,
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> MessageResponse:
    repo = NotificationsRepository(db)
    notification = repo.mark_failed_by_id(
        notification_id=notification_id,
        error_message=(payload or {}).get("error_message"),
    )

    if not notification:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Notification not found",
        )

    return MessageResponse(message="Notification marked as failed")


@router.post("/subscriptions/expire-overdue", response_model=MessageResponse)
def expire_overdue_subscriptions(
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> MessageResponse:
    repo = SubscriptionsRepository(db)
    affected = repo.expire_overdue()
    return MessageResponse(message=f"Expired subscriptions: {affected}")


@router.get("/admin/user-stats")
def admin_user_stats(
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> dict:
    repo = UsersRepository(db)

    return {
        "total_users": repo.total_users(),
        "active_users": repo.active_users(),
        "premium_users": repo.premium_users(),
        "admin_users": repo.admin_users(),
        "new_users_24h": repo.new_users_last_24h(),
    }


@router.get("/admin/search-stats")
def admin_search_stats(
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> dict:
    repo = SearchHistoryRepository(db)

    return {
        "searches_today": repo.searches_today(),
        "searches_24h": repo.searches_last_24h(),
        "total_searches": repo.total_searches(),
        "empty_results_today": repo.empty_results_today(),
        "avg_latency_today_ms": repo.avg_latency_today(),
        "top_queries_today": repo.top_queries_today(limit=20),
    }


@router.get("/admin/revenue-stats")
def admin_revenue_stats(
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> dict:
    repo = PaymentsRepository(db)

    return {
        "total_revenue": str(repo.total_revenue()),
        "revenue_30d": str(repo.revenue_last_30d()),
        "payments_total": repo.total_payments(),
        "payments_successful": repo.successful_payments(),
        "payments_failed": repo.failed_payments(),
        "payments_pending": repo.pending_payments(),
    }


@router.get("/admin/subscription-stats")
def admin_subscription_stats(
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> dict:
    repo = SubscriptionsRepository(db)

    return {
        "active_total": repo.active_total(),
        "free_active": repo.active_by_plan("free"),
        "premium_active": repo.active_by_plan("premium"),
        "pro_active": repo.active_by_plan("pro"),
        "expired_total": repo.expired_total(),
        "canceled_total": repo.canceled_total(),
    }


@router.get("/admin/payment-logs")
def admin_payment_logs(
    limit: int = Query(default=50, ge=1, le=500),
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> list[dict]:
    repo = PaymentsRepository(db)
    return repo.latest_payments(limit=limit)


@router.get("/admin/notification-logs")
def admin_notification_logs(
    limit: int = Query(default=50, ge=1, le=500),
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> list[dict]:
    repo = NotificationsRepository(db)
    return repo.latest_logs(limit=limit)


@router.get("/admin/error-logs")
def admin_error_logs(
    limit: int = Query(default=50, ge=1, le=500),
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> list[dict]:
    repo = NotificationsRepository(db)
    return repo.failed_logs(limit=limit)


@router.post("/admin/run-alerts", response_model=MessageResponse)
def admin_run_alerts(
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> MessageResponse:
    repo = SavedSearchesRepository(db)
    count = repo.count_alert_ready()

    return MessageResponse(
        message=f"Eligible saved searches for alerts: {count}. Worker scheduler should execute alerts.",
    )

