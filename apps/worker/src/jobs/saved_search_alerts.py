from __future__ import annotations

from src.clients.bot_api import BotApiClient
from src.config import settings
from src.logging import get_logger
from src.services.alert_dispatcher import AlertDispatcherService
from src.services.search_matcher import SearchMatcherService


logger = get_logger(__name__)


class SavedSearchAlertsJob:
    def __init__(self) -> None:
        self.bot_api = BotApiClient()
        self.matcher = SearchMatcherService()
        self.dispatcher = AlertDispatcherService()

    async def run(self) -> dict:
        saved_searches = await self.bot_api.list_active_saved_searches()

        processed = 0
        sent = 0
        skipped = 0
        failed = 0
        bootstrapped = 0
        limited = 0

        max_per_run = int(getattr(settings, "ALERTS_MAX_SAVED_SEARCHES_PER_RUN", 100))
        max_sent_per_run = int(getattr(settings, "ALERTS_MAX_SENT_PER_RUN", 50))
        max_failed_per_run = int(getattr(settings, "ALERTS_MAX_FAILED_PER_RUN", 50))

        total_loaded = len(saved_searches)

        if max_per_run > 0 and len(saved_searches) > max_per_run:
            limited += len(saved_searches) - max_per_run
            saved_searches = saved_searches[:max_per_run]

        logger.info(
            "saved_search_alerts_job_started total_loaded=%s selected=%s max_per_run=%s max_sent_per_run=%s max_failed_per_run=%s",
            total_loaded,
            len(saved_searches),
            max_per_run,
            max_sent_per_run,
            max_failed_per_run,
        )

        for saved_search in saved_searches:
            if max_sent_per_run > 0 and sent >= max_sent_per_run:
                limited += 1
                logger.warning(
                    "saved_search_alerts_sent_limit_reached sent=%s max_sent_per_run=%s",
                    sent,
                    max_sent_per_run,
                )
                break

            if max_failed_per_run > 0 and failed >= max_failed_per_run:
                limited += 1
                logger.warning(
                    "saved_search_alerts_failed_limit_reached failed=%s max_failed_per_run=%s",
                    failed,
                    max_failed_per_run,
                )
                break

            processed += 1
            saved_search_id = saved_search.get("id")

            if not saved_search_id:
                skipped += 1
                logger.warning("saved_search_skipped_no_id payload=%s", saved_search)
                continue

            try:
                match_result = await self.matcher.run_saved_search(saved_search)
                new_items = list(match_result.get("new_results") or [])

                if match_result.get("is_first_run"):
                    bootstrap_id = match_result.get("bootstrap_last_seen_listing_id")

                    await self.bot_api.mark_saved_search_checked(
                        saved_search_id=int(saved_search_id),
                        last_seen_listing_id=bootstrap_id,
                    )

                    bootstrapped += 1

                    logger.info(
                        "saved_search_first_run_bootstrapped saved_search_id=%s bootstrap_last_seen_listing_id=%s",
                        saved_search_id,
                        bootstrap_id,
                    )
                    continue

                if not new_items:
                    await self.bot_api.mark_saved_search_checked(
                        saved_search_id=int(saved_search_id),
                        last_seen_listing_id=match_result.get("selected_last_seen_listing_id")
                        or match_result.get("last_seen_listing_id"),
                    )

                    skipped += 1

                    logger.info(
                        "saved_search_no_new_items saved_search_id=%s",
                        saved_search_id,
                    )
                    continue

                user_payload = {
                    "id": saved_search.get("user_id"),
                    "telegram_user_id": saved_search.get("telegram_user_id"),
                    "telegram_chat_id": saved_search.get("telegram_chat_id"),
                }

                if not user_payload.get("id"):
                    skipped += 1
                    logger.warning(
                        "saved_search_skipped_no_user saved_search_id=%s",
                        saved_search_id,
                    )
                    continue

                result = await self.dispatcher.dispatch_saved_search_alert(
                    user=user_payload,
                    saved_search=saved_search,
                    new_items=new_items,
                )

                if result.get("status") == "sent":
                    sent += 1
                elif result.get("status") == "failed":
                    failed += 1
                else:
                    skipped += 1

                logger.info(
                    "saved_search_alert_result saved_search_id=%s result=%s",
                    saved_search_id,
                    result,
                )

            except Exception as exc:
                logger.exception(
                    "saved_search_alert_failed saved_search_id=%s error=%s",
                    saved_search_id,
                    repr(exc),
                )
                failed += 1
                continue

        summary = {
            "status": "ok",
            "total_loaded": total_loaded,
            "processed": processed,
            "sent": sent,
            "skipped": skipped,
            "bootstrapped": bootstrapped,
            "failed": failed,
            "limited": limited,
        }

        logger.info("saved_search_alerts_job_finished summary=%s", summary)
        return summary