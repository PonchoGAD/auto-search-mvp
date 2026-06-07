from __future__ import annotations

from typing import Any

from src.utils.text import escape_html, format_bool, join_nonempty, safe_text


def format_profile_card(profile: dict[str, Any]) -> str:
    first_name = safe_text(profile.get("first_name"), "")
    last_name = safe_text(profile.get("last_name"), "")
    username = safe_text(profile.get("username"), "")
    role = safe_text(profile.get("role"), "user")
    status = safe_text(profile.get("status"), "active")

    full_name = " ".join(x for x in [first_name, last_name] if x).strip() or "Пользователь"

    lines: list[str] = [
        "<b>Профиль</b>",
        f"Имя: {escape_html(full_name)}",
    ]

    if username:
        lines.append(f"Username: @{escape_html(username.lstrip('@'))}")

    lines.extend(
        [
            f"Telegram ID: <code>{profile.get('telegram_user_id')}</code>",
            f"Роль: {escape_html(role)}",
            f"Статус: {escape_html(status)}",
            f"Premium: {escape_html(format_bool(bool(profile.get('is_premium'))))}",
            f"Избранное: {profile.get('favorites_count', 0)}",
            f"Сохраненные поиски: {profile.get('saved_searches_count', 0)}",
        ]
    )

    active_plan = profile.get("active_subscription_plan")
    active_status = profile.get("active_subscription_status")
    expires_at = profile.get("subscription_expires_at")

    if active_plan:
        lines.append(f"Подписка: {escape_html(str(active_plan))}")
    if active_status:
        lines.append(f"Статус подписки: {escape_html(str(active_status))}")
    if expires_at:
        lines.append(f"Действует до: {escape_html(str(expires_at))}")

    return join_nonempty(lines, sep="\n")