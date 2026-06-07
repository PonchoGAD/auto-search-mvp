from __future__ import annotations

import httpx
from aiogram import Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from src.config import settings
from src.keyboards.main import main_menu_keyboard
from src.logging import get_logger
from src.states.search_filters import SearchFiltersState
from src.utils.callback_data import SavedSearchCallback
from src.utils.internal_api import bot_api_headers


logger = get_logger(__name__)
router = Router()


MAX_SAVED_SEARCH_NAME_LEN = 80




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
            return "Пользователь или сохраненный поиск не найден. Нажмите /start и попробуйте снова."

        if exc.response.status_code == 409:
            return "Поиск с таким названием уже есть."

        if exc.response.status_code == 429:
            return "Лимит бесплатного тарифа исчерпан. Откройте /subscription."

    if isinstance(exc, httpx.ConnectError):
        return "Не удалось подключиться к bot_api."

    if isinstance(exc, httpx.TimeoutException):
        return "bot_api долго не отвечает. Попробуйте позже."

    return fallback


def _saved_url(telegram_user_id: int) -> str:
    return f"{settings.saved_searches_url}?telegram_user_id={telegram_user_id}"


async def _list_saved_searches(telegram_user_id: int) -> list[dict]:
    async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
        response = await client.get(
            _saved_url(telegram_user_id),
            headers=bot_api_headers(),
        )
        response.raise_for_status()
        return list(response.json().get("items") or [])


def _format_saved_searches(items: list[dict]) -> str:
    if not items:
        return (
            "<b>Сохраненных поисков нет.</b>\n\n"
            "Сначала выполните поиск, затем нажмите кнопку сохранения."
        )

    lines = ["<b>Сохраненные поиски</b>", ""]

    for idx, item in enumerate(items, start=1):
        name = item.get("name") or "Без названия"
        raw_query = item.get("raw_query") or ""
        enabled = "включены" if item.get("is_alert_enabled") else "выключены"
        status = item.get("status") or "active"
        saved_search_id = item.get("id")

        lines.append(f"<b>{idx}. {name}</b>")
        lines.append(f"ID: <code>{saved_search_id}</code>")
        lines.append(f"Запрос: <code>{raw_query}</code>")
        lines.append(f"Статус: <code>{status}</code>")
        lines.append(f"Уведомления: <code>{enabled}</code>")
        lines.append("")

    return "\n".join(lines).strip()


def _saved_keyboard(items: list[dict]):
    builder = InlineKeyboardBuilder()

    for item in items[: settings.SAVED_SEARCHES_PER_PAGE]:
        saved_search_id = item.get("id")
        if not saved_search_id:
            continue

        if item.get("is_alert_enabled"):
            builder.button(
                text=f"Пауза #{saved_search_id}",
                callback_data=SavedSearchCallback(
                    action="pause",
                    saved_search_id=int(saved_search_id),
                ).pack(),
            )
        else:
            builder.button(
                text=f"Возобновить #{saved_search_id}",
                callback_data=SavedSearchCallback(
                    action="resume",
                    saved_search_id=int(saved_search_id),
                ).pack(),
            )

        builder.button(
            text=f"Удалить #{saved_search_id}",
            callback_data=SavedSearchCallback(
                action="delete",
                saved_search_id=int(saved_search_id),
            ).pack(),
        )

    builder.button(text="В меню", callback_data="menu:main")
    builder.adjust(2, 1)
    return builder.as_markup()


@router.message(Command("saved"))
async def cmd_saved(message: Message) -> None:
    if not message.from_user:
        await message.answer("Не удалось определить пользователя Telegram.")
        return

    telegram_user_id = message.from_user.id

    try:
        items = await _list_saved_searches(telegram_user_id)
    except Exception as exc:
        logger.exception("saved_searches_load_failed error=%s", repr(exc))
        await message.answer(_api_error_message(exc, "Ошибка загрузки сохраненных поисков."))
        return

    await message.answer(
        _format_saved_searches(items),
        reply_markup=_saved_keyboard(items) if items else main_menu_keyboard(
            is_admin=message.from_user.id in settings.admin_telegram_ids
        ),
    )


@router.message(Command("save_search"))
async def cmd_save_search(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    query = data.get("search_query")

    if not query:
        await message.answer("Сначала выполните поиск, потом сохраните его.")
        return

    await state.set_state(SearchFiltersState.waiting_for_saved_search_name)
    await message.answer("Отправьте название для сохраненного поиска.")


@router.message(SearchFiltersState.waiting_for_saved_search_name)
async def save_search_name(message: Message, state: FSMContext) -> None:
    name = (message.text or "").strip()

    if not name:
        await message.answer("Название пустое. Введите короткое название.")
        return

    if len(name) > MAX_SAVED_SEARCH_NAME_LEN:
        await message.answer(f"Название слишком длинное. Максимум {MAX_SAVED_SEARCH_NAME_LEN} символов.")
        return

    data = await state.get_data()
    query = str(data.get("search_query") or "").strip()

    if not query:
        await state.clear()
        await message.answer("Нет последнего поискового запроса для сохранения.")
        return

    if not message.from_user:
        await message.answer("Не удалось определить пользователя Telegram.")
        return

    telegram_user_id = message.from_user.id

    payload = {
        "name": name,
        "raw_query": query,
        "query_payload": {
            "query": query,
            "page": 1,
            "limit": settings.MAX_PAGE_SIZE,
            "include_answer": False,
        },
        "is_alert_enabled": True,
    }

    try:
        async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
            response = await client.post(
                _saved_url(telegram_user_id),
                json=payload,
                headers=bot_api_headers(),
            )
            response.raise_for_status()
    except Exception as exc:
        logger.exception("saved_search_create_failed error=%s", repr(exc))
        await message.answer(_api_error_message(exc, "Ошибка сохранения поиска."))
        return

    await state.clear()
    await message.answer(
        "Поиск сохранен. Уведомления включены.",
        reply_markup=main_menu_keyboard(
            is_admin=message.from_user.id in settings.admin_telegram_ids
        ),
    )


@router.callback_query(lambda c: c.data and c.data.startswith("saved:"))
async def saved_search_callback(callback: CallbackQuery) -> None:
    parsed = SavedSearchCallback.unpack(callback.data or "")
    if not parsed:
        await callback.answer("Некорректная кнопка", show_alert=True)
        return

    telegram_user_id = callback.from_user.id

    try:
        async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
            if parsed.action == "delete":
                response = await client.delete(
                    f"{settings.saved_searches_url}/{parsed.saved_search_id}?telegram_user_id={telegram_user_id}",
                    headers=bot_api_headers(),
                )
            elif parsed.action == "pause":
                response = await client.patch(
                    f"{settings.saved_searches_url}/{parsed.saved_search_id}?telegram_user_id={telegram_user_id}",
                    json={"is_alert_enabled": False, "status": "paused"},
                    headers=bot_api_headers(),
                )
            elif parsed.action == "resume":
                response = await client.patch(
                    f"{settings.saved_searches_url}/{parsed.saved_search_id}?telegram_user_id={telegram_user_id}",
                    json={"is_alert_enabled": True, "status": "active"},
                    headers=bot_api_headers(),
                )
            else:
                await callback.answer("Неизвестное действие", show_alert=True)
                return

            response.raise_for_status()

        items = await _list_saved_searches(telegram_user_id)

        await callback.message.edit_text(
            _format_saved_searches(items),
            reply_markup=_saved_keyboard(items) if items else main_menu_keyboard(
                is_admin=callback.from_user.id in settings.admin_telegram_ids
            ),
        )
        await callback.answer("Готово")

    except Exception as exc:
        logger.exception("saved_search_action_failed error=%s", repr(exc))
        await callback.answer(
            _api_error_message(exc, "Ошибка выполнения"),
            show_alert=True,
        )