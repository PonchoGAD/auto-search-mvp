from __future__ import annotations

import hashlib

import httpx
from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from src.config import settings
from src.formatters.listing_card import format_listings_page
from src.keyboards.search import search_results_keyboard
from src.logging import get_logger
from src.states.search_filters import SearchFiltersState
from src.utils.callback_data import SearchCallback
from src.utils.internal_api import bot_api_headers
from src.utils.pagination import paginate_items


logger = get_logger(__name__)
router = Router()


MIN_QUERY_LEN = 2
MAX_QUERY_LEN = 500


def _extract_api_error(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            detail = exc.response.json().get("detail")

            if isinstance(detail, dict):
                message = detail.get("message")
                if message:
                    return str(message)

            if isinstance(detail, str):
                return detail
        except Exception:
            pass

        if exc.response.status_code == 400:
            return "Некорректный запрос. Попробуйте написать проще."

        if exc.response.status_code == 401:
            return "Ошибка доступа к API. INTERNAL_API_KEY в tg-bot и bot_api не совпадает или не передается."

        if exc.response.status_code == 403:
            return "Доступ запрещен. Проверь INTERNAL_API_KEY и права доступа."

        if exc.response.status_code == 429:
            return "Лимит бесплатного тарифа исчерпан. Откройте /subscription."

        if exc.response.status_code == 502:
            return "Поисковое ядро сейчас недоступно. Проверь VPS search core, nginx и /api/v1/health."

    if isinstance(exc, httpx.ConnectError):
        return "Не удалось подключиться к bot_api. Проверь контейнер auto-search-bot-api."

    if isinstance(exc, httpx.ReadTimeout):
        return "Поиск занял слишком много времени. Попробуйте короче запрос или проверьте search core."

    if isinstance(exc, httpx.TimeoutException):
        return "Превышено время ожидания ответа. Проверь bot_api и search core."

    return "Не удалось выполнить поиск. Проверь доступность search core и bot_api."


def _validate_query(query: str) -> str | None:
    if not query:
        return "Пустой запрос. Напишите, какое авто нужно найти."

    if len(query) < MIN_QUERY_LEN:
        return "Запрос слишком короткий. Напишите марку, модель или параметры авто."

    if len(query) > MAX_QUERY_LEN:
        return f"Запрос слишком длинный. Максимум {MAX_QUERY_LEN} символов."

    return None


def _make_callback_id(item: dict, index: int) -> str:
    listing_id = str(item.get("listing_id") or "").strip()
    source_url = str(item.get("source_url") or "").strip()
    seed = listing_id or source_url or f"item-{index}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"i{index}_{digest}"[:32]


def _prepare_items_for_callbacks(items: list[dict]) -> tuple[list[dict], dict[str, dict]]:
    prepared: list[dict] = []
    mapping: dict[str, dict] = {}

    for index, raw_item in enumerate(items, start=1):
        item = dict(raw_item)
        callback_id = str(item.get("callback_id") or "").strip()

        if not callback_id:
            callback_id = _make_callback_id(item, index)

        item["callback_id"] = callback_id
        prepared.append(item)
        mapping[callback_id] = item

    return prepared, mapping




async def _run_search(query: str, telegram_user_id: int, page: int = 1) -> dict:
    payload = {
        "query": query,
        "page": page,
        "limit": settings.MAX_PAGE_SIZE,
        "include_answer": False,
    }

    url = f"{settings.BOT_API_BASE_URL.rstrip('/')}{settings.BOT_API_PREFIX}/search-proxy/search"

    async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
        response = await client.post(
            url,
            params={"telegram_user_id": telegram_user_id},
            json=payload,
            headers=bot_api_headers(),
        )
        response.raise_for_status()
        return response.json()


def _extract_items(search_response: dict) -> list[dict]:
    return list(search_response.get("results") or [])


def _selected_callback_id(items: list[dict]) -> str | None:
    if not items:
        return None

    value = items[0].get("callback_id")
    return str(value) if value else None


@router.message(F.text & ~F.text.startswith("/"))
async def search_text(message: Message, state: FSMContext) -> None:
    query = (message.text or "").strip()
    validation_error = _validate_query(query)

    if validation_error:
        await message.answer(validation_error)
        return

    if not message.from_user:
        await message.answer("Не удалось определить пользователя Telegram.")
        return

    loading_message = await message.answer("Ищу подходящие объявления...")

    try:
        data = await _run_search(query, message.from_user.id, page=1)
    except Exception as exc:
        logger.exception(
            "telegram_search_failed user_id=%s query=%s error=%s",
            message.from_user.id,
            query,
            repr(exc),
        )
        await loading_message.edit_text(_extract_api_error(exc))
        return

    raw_items = _extract_items(data)
    items, callback_map = _prepare_items_for_callbacks(raw_items)

    await state.update_data(
        search_query=query,
        search_results=items,
        search_callback_map=callback_map,
        search_page=1,
        last_search_response=data,
    )

    page_slice = paginate_items(
        items=items,
        page=1,
        per_page=settings.SEARCH_RESULTS_PER_PAGE,
    )

    text = format_listings_page(
        items=page_slice.items,
        page=page_slice.page,
        total_pages=page_slice.total_pages,
        total_items=page_slice.total_items,
    )

    await loading_message.edit_text(
        text,
        reply_markup=search_results_keyboard(
            page=page_slice.page,
            has_prev=page_slice.has_prev,
            has_next=page_slice.has_next,
            selected_callback_id=_selected_callback_id(page_slice.items),
        ),
        disable_web_page_preview=True,
    )


@router.callback_query(F.data.startswith("search:"))
async def search_callbacks(callback: CallbackQuery, state: FSMContext) -> None:
    parsed = SearchCallback.unpack(callback.data or "")
    if not parsed:
        await callback.answer("Некорректная кнопка", show_alert=True)
        return

    data = await state.get_data()
    items = list(data.get("search_results") or [])
    query = str(data.get("search_query") or "").strip()

    if parsed.action == "new":
        await state.set_state(SearchFiltersState.waiting_for_query)
        await callback.message.answer("Отправьте новый поисковый запрос.")
        await callback.answer()
        return

    if parsed.action == "save":
        if not query:
            await callback.answer("Нет поискового запроса для сохранения", show_alert=True)
            return

        await state.set_state(SearchFiltersState.waiting_for_saved_search_name)
        await callback.message.answer("Введите название для сохраненного поиска.")
        await callback.answer()
        return

    if parsed.action == "page":
        if not items:
            await callback.answer("Результаты поиска устарели. Выполните поиск заново.", show_alert=True)
            return

        page_slice = paginate_items(
            items=items,
            page=parsed.page,
            per_page=settings.SEARCH_RESULTS_PER_PAGE,
        )

        await state.update_data(search_page=page_slice.page)

        text = format_listings_page(
            items=page_slice.items,
            page=page_slice.page,
            total_pages=page_slice.total_pages,
            total_items=page_slice.total_items,
        )

        await callback.message.edit_text(
            text,
            reply_markup=search_results_keyboard(
                page=page_slice.page,
                has_prev=page_slice.has_prev,
                has_next=page_slice.has_next,
                selected_callback_id=_selected_callback_id(page_slice.items),
            ),
            disable_web_page_preview=True,
        )
        await callback.answer()
        return

    await callback.answer()