from __future__ import annotations

from typing import Any

from src.clients.bot_api import BotApiClient
from src.clients.telegram_bot import TelegramBotClient
from src.config import settings
from src.formatters.notifications import (
    format_saved_search_alert,
    format_subscription_expiry_notice,
)
from src.logging import get_logger
from src.schemas.listing import ListingResult
from src.schemas.notification_schema import SavedSearchAlertPayload
from src.services.deduplication import (
    build_saved_search_dedup_key,
    listing_ids,
    pick_last_seen_listing_id,
)


logger = get_logger(__name__)


class AlertDispatcherService:
    def __init__(self) -> None:
        self.bot_api = BotApiClient()
        self.telegram_bot = TelegramBotClient()

    async def dispatch_saved_search_alert(
        self,
        user: dict[str, Any],
        saved_search: dict[str, Any],
        new_items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        chat_id = user.get("telegram_chat_id") or user.get("telegram_user_id")

        if not chat_id:
            logger.warning(
                "saved_search_alert_skipped_no_chat_id saved_search_id=%s user_id=%s",
                saved_search.get("id"),
                user.get("id"),
            )
            return {"status": "skipped", "reason": "no_chat_id"}

        if not new_items:
            return {"status": "skipped", "reason": "no_new_items"}

        max_items = int(getattr(settings, "ALERTS_MAX_ITEMS_PER_MESSAGE", 5))
        limited_items = new_items[:max_items]

        item_ids = listing_ids(limited_items)
        first_listing_id = item_ids[0] if item_ids else "unknown"

        dedup_key = build_saved_search_dedup_key(
            saved_search_id=saved_search.get("id"),
            listing_id=first_listing_id,
        )

        payload_model = SavedSearchAlertPayload(
            saved_search_id=int(saved_search.get("id")),
            saved_search_name=str(saved_search.get("name") or "saved_search"),
            items=[
                ListingResult.model_validate(item).ensure_listing_id()
                for item in limited_items
            ],
        )

        notification = await self.bot_api.create_notification(
            user_id=int(user["id"]),
            type="saved_search_alert",
            payload=payload_model.model_dump(exclude_none=True),
            dedup_key=dedup_key,
            status="pending",
        )

        if notification.get("status") == "duplicate":
            logger.info(
                "saved_search_alert_duplicate_skipped saved_search_id=%s dedup_key=%s",
                saved_search.get("id"),
                dedup_key,
            )
            return {
                "status": "skipped",
                "reason": "duplicate",
                "dedup_key": dedup_key,
            }

        notification_id = notification.get("notification_id")

        text = format_saved_search_alert(
            saved_search_name=str(saved_search.get("name") or "Поиск"),
            items=limited_items,
        )

        try:
            await self.telegram_bot.send_message(
                chat_id=int(chat_id),
                text=text,
                disable_web_page_preview=True,
            )
        except Exception as exc:
            logger.exception(
                "saved_search_alert_send_failed saved_search_id=%s chat_id=%s error=%s",
                saved_search.get("id"),
                chat_id,
                repr(exc),
            )

            if notification_id:
                try:
                    await self.bot_api.mark_notification_failed(
                        notification_id=int(notification_id),
                        error_message=str(exc),
                    )
                except Exception as mark_exc:
                    logger.exception(
                        "saved_search_alert_mark_failed_failed notification_id=%s error=%s",
                        notification_id,
                        repr(mark_exc),
                    )

            return {
                "status": "failed",
                "reason": "telegram_send_failed",
                "error": str(exc),
                "dedup_key": dedup_key,
                "notification_id": notification_id,
            }

        if notification_id:
            try:
                await self.bot_api.mark_notification_sent(int(notification_id))
            except Exception as exc:
                logger.exception(
                    "saved_search_alert_mark_sent_failed notification_id=%s error=%s",
                    notification_id,
                    repr(exc),
                )

        last_seen_listing_id = pick_last_seen_listing_id(new_items)

        if last_seen_listing_id:
            await self.bot_api.mark_saved_search_checked(
                saved_search_id=int(saved_search["id"]),
                last_seen_listing_id=last_seen_listing_id,
            )

        logger.info(
            "saved_search_alert_sent saved_search_id=%s user_id=%s items_sent=%s last_seen=%s notification_id=%s",
            saved_search.get("id"),
            user.get("id"),
            len(limited_items),
            last_seen_listing_id,
            notification_id,
        )

        return {
            "status": "sent",
            "saved_search_id": saved_search.get("id"),
            "items_sent": len(limited_items),
            "last_seen_listing_id": last_seen_listing_id,
            "dedup_key": dedup_key,
            "notification_id": notification_id,
        }

    async def dispatch_subscription_expiry_notice(
        self,
        user: dict[str, Any],
        expires_at: str | None,
    ) -> dict[str, Any]:
        chat_id = user.get("telegram_chat_id") or user.get("telegram_user_id")

        if not chat_id:
            return {"status": "skipped", "reason": "no_chat_id"}

        dedup_key = f"subscription_expiry:{user.get('id')}:{expires_at or 'unknown'}"

        notification = await self.bot_api.create_notification(
            user_id=int(user["id"]),
            type="subscription_expiry",
            payload={
                "expires_at": expires_at,
            },
            dedup_key=dedup_key,
            status="pending",
        )

        if notification.get("status") == "duplicate":
            return {
                "status": "skipped",
                "reason": "duplicate",
                "dedup_key": dedup_key,
            }

        notification_id = notification.get("notification_id")
        text = format_subscription_expiry_notice(expires_at=expires_at)

        try:
            await self.telegram_bot.send_message(
                chat_id=int(chat_id),
                text=text,
                disable_web_page_preview=True,
            )
        except Exception as exc:
            logger.exception(
                "subscription_expiry_notice_failed user_id=%s error=%s",
                user.get("id"),
                repr(exc),
            )

            if notification_id:
                try:
                    await self.bot_api.mark_notification_failed(
                        notification_id=int(notification_id),
                        error_message=str(exc),
                    )
                except Exception as mark_exc:
                    logger.exception(
                        "subscription_expiry_mark_failed_failed notification_id=%s error=%s",
                        notification_id,
                        repr(mark_exc),
                    )

            return {
                "status": "failed",
                "reason": "telegram_send_failed",
                "error": str(exc),
                "dedup_key": dedup_key,
                "notification_id": notification_id,
            }

        if notification_id:
            try:
                await self.bot_api.mark_notification_sent(int(notification_id))
            except Exception as exc:
                logger.exception(
                    "subscription_expiry_mark_sent_failed notification_id=%s error=%s",
                    notification_id,
                    repr(exc),
                )

        return {
            "status": "sent",
            "user_id": user.get("id"),
            "expires_at": expires_at,
            "dedup_key": dedup_key,
            "notification_id": notification_id,
        }