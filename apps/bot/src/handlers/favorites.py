from __future__ import annotations

import hashlib

import httpx
from aiogram import Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from src.config import settings
from src.formatters.listing_card import format_listings_page
from src.keyboards.favorites import favorites_list_keyboard
from src.logging import get_logger
from src.utils.callback_data import FavoriteCallback
from src.utils.internal_api import bot_api_headers
from src.utils.pagination import paginate_items


logger = get_logger(__name__)
router = Router()


def _api_error_message(exc: Exception, fallback: str) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            detail = exc.response.json().get("detail")
            if isinstance(detail, dict):
                return str(detail.get("message") or fallback)
            if isinstance(detail, str):
                return detail
        except Exception:
            pass

        if exc.response.status_code == 401:
            return "Ошибка доступа к bot_api. INTERNAL_API_KEY не совпадает или не передается."

        if exc.response.status_code == 403:
            return "Доступ запрещен. Проверь INTERNAL_API_KEY."

        if exc.response.status_code == 404:
            return "Пользователь или объявление не найдено. Нажмите /start и попробуйте снова."

        if exc.response.status_code == 429:
            return "Лимит бесплатного тарифа исчерпан. Откройте /subscription."

    if isinstance(exc, httpx.ConnectError):
        return "Не удалось подключиться к bot_api."

    if isinstance(exc, httpx.TimeoutException):
        return "bot_api долго не отвечает. Попробуйте позже."

    return fallback


def _favorites_url(telegram_user_id: int) -> str:
    return f"{settings.favorites_url}?telegram_user_id={telegram_user_id}"


def _make_callback_id(item: dict, index: int) -> str:
    listing_id = str(item.get("listing_id") or "").strip()
    source_url = str(item.get("source_url") or "").strip()
    seed = listing_id or source_url or f"favorite-{index}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"f{index}_{digest}"[:32]


def _prepare_favorites_for_callbacks(items: list[dict]) -> tuple[list[dict], dict[str, dict]]:
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


async def _list_favorites(telegram_user_id: int) -> list[dict]:
    async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
        response = await client.get(
            _favorites_url(telegram_user_id),
            headers=bot_api_headers(),
        )
        response.raise_for_status()
        payload = response.json()
        return list(payload.get("items") or [])


def _find_search_item_by_callback_id(state_data: dict, callback_id: str) -> dict | None:
    callback_map = dict(state_data.get("search_callback_map") or {})
    item = callback_map.get(callback_id)
    if isinstance(item, dict):
        return item

    search_results = list(state_data.get("search_results") or [])
    for result in search_results:
        if str(result.get("callback_id") or "").strip() == callback_id:
            return result

    return None


def _find_favorite_by_callback_id(state_data: dict, callback_id: str) -> dict | None:
    callback_map = dict(state_data.get("favorites_callback_map") or {})
    item = callback_map.get(callback_id)
    if isinstance(item, dict):
        return item

    favorite_results = list(state_data.get("favorites_results") or [])
    for result in favorite_results:
        if str(result.get("callback_id") or "").strip() == callback_id:
            return result

    return None


def _selected_callback_id(items: list[dict]) -> str | None:
    if not items:
        return None

    value = items[0].get("callback_id")
    return str(value) if value else None


async def _send_favorites_page(
    message: Message,
    state: FSMContext,
    telegram_user_id: int,
    page: int = 1,
) -> None:
    raw_items = await _list_favorites(telegram_user_id)
    items, callback_map = _prepare_favorites_for_callbacks(raw_items)

    await state.update_data(
        favorites_results=items,
        favorites_callback_map=callback_map,
        favorites_page=page,
    )

    page_slice = paginate_items(
        items=items,
        page=page,
        per_page=settings.FAVORITES_PER_PAGE,
    )

    text = format_listings_page(
        items=page_slice.items,
        page=page_slice.page,
        total_pages=page_slice.total_pages,
        total_items=page_slice.total_items,
    )

    await message.answer(
        text,
        reply_markup=favorites_list_keyboard(
            page=page_slice.page,
            has_prev=page_slice.has_prev,
            has_next=page_slice.has_next,
            selected_callback_id=_selected_callback_id(page_slice.items),
        ),
        disable_web_page_preview=True,
    )


@router.message(Command("favorites"))
async def cmd_favorites(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        await message.answer("Не удалось определить пользователя Telegram.")
        return

    try:
        await _send_favorites_page(message, state, message.from_user.id, page=1)
    except Exception as exc:
        logger.exception("favorites_load_failed error=%s", repr(exc))
        await message.answer(_api_error_message(exc, "Ошибка загрузки избранного."))


@router.callback_query(lambda c: c.data and c.data.startswith("fav:list:"))
async def favorites_list_callback(callback: CallbackQuery, state: FSMContext) -> None:
    telegram_user_id = callback.from_user.id

    try:
        page = int((callback.data or "").split(":")[2])
    except Exception:
        page = 1

    try:
        raw_items = await _list_favorites(telegram_user_id)
        items, callback_map = _prepare_favorites_for_callbacks(raw_items)

        await state.update_data(
            favorites_results=items,
            favorites_callback_map=callback_map,
            favorites_page=page,
        )
    except Exception as exc:
        logger.exception("favorites_pagination_failed error=%s", repr(exc))
        await callback.answer(_api_error_message(exc, "Ошибка загрузки"), show_alert=True)
        return

    page_slice = paginate_items(
        items=items,
        page=page,
        per_page=settings.FAVORITES_PER_PAGE,
    )

    text = format_listings_page(
        items=page_slice.items,
        page=page_slice.page,
        total_pages=page_slice.total_pages,
        total_items=page_slice.total_items,
    )

    await callback.message.edit_text(
        text,
        reply_markup=favorites_list_keyboard(
            page=page_slice.page,
            has_prev=page_slice.has_prev,
            has_next=page_slice.has_next,
            selected_callback_id=_selected_callback_id(page_slice.items),
        ),
        disable_web_page_preview=True,
    )
    await callback.answer()


@router.callback_query(lambda c: c.data and c.data.startswith("fav:add:"))
async def add_favorite_callback(callback: CallbackQuery, state: FSMContext) -> None:
    parsed = FavoriteCallback.unpack(callback.data or "")
    if not parsed:
        await callback.answer("Некорректная кнопка", show_alert=True)
        return

    telegram_user_id = callback.from_user.id
    callback_id = parsed.item_id

    state_data = await state.get_data()
    item = _find_search_item_by_callback_id(state_data, callback_id)

    if not item:
        await callback.answer("Карточка не найдена в текущем поиске", show_alert=True)
        return

    try:
        async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
            response = await client.post(
                f"{settings.favorites_url}/from-search?telegram_user_id={telegram_user_id}",
                json=item,
                headers=bot_api_headers(),
            )
            response.raise_for_status()
    except Exception as exc:
        logger.exception("favorite_add_failed error=%s", repr(exc))
        await callback.answer(
            _api_error_message(exc, "Ошибка добавления в избранное"),
            show_alert=True,
        )
        return

    await callback.answer("Добавлено в избранное")


@router.callback_query(lambda c: c.data and c.data.startswith("fav:remove:"))
async def remove_favorite_callback(callback: CallbackQuery, state: FSMContext) -> None:
    parsed = FavoriteCallback.unpack(callback.data or "")
    if not parsed:
        await callback.answer("Некорректная кнопка", show_alert=True)
        return

    telegram_user_id = callback.from_user.id
    callback_id = parsed.item_id

    state_data = await state.get_data()

    item = _find_favorite_by_callback_id(state_data, callback_id)
    if item is None:
        item = _find_search_item_by_callback_id(state_data, callback_id)

    listing_id = str((item or {}).get("listing_id") or "").strip()

    if not listing_id:
        await callback.answer("Не найден ID объявления для удаления", show_alert=True)
        return

    try:
        async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
            response = await client.delete(
                f"{settings.favorites_url}/{listing_id}?telegram_user_id={telegram_user_id}",
                headers=bot_api_headers(),
            )
            if response.status_code not in (200, 404):
                response.raise_for_status()
    except Exception as exc:
        logger.exception("favorite_remove_failed error=%s", repr(exc))
        await callback.answer(_api_error_message(exc, "Ошибка удаления"), show_alert=True)
        return

    await callback.answer("Удалено из избранного")