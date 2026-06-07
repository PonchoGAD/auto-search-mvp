from __future__ import annotations

from dataclasses import dataclass


CALLBACK_SEPARATOR = ":"
MAX_CALLBACK_LEN = 64


class CallbackNamespaces:
    SEARCH = "search"
    FAVORITES = "fav"
    SAVED = "saved"
    PROFILE = "profile"
    SUBSCRIPTION = "sub"
    ADMIN = "admin"


def _safe_part(value: object) -> str:
    text = str(value).strip()
    text = text.replace(CALLBACK_SEPARATOR, "_")
    return text


def build_callback(*parts: object) -> str:
    safe_parts = [_safe_part(part) for part in parts if _safe_part(part)]
    value = CALLBACK_SEPARATOR.join(safe_parts)
    return value[:MAX_CALLBACK_LEN]


def parse_callback(value: str) -> list[str]:
    if not value:
        return []
    return [part for part in value.split(CALLBACK_SEPARATOR) if part]


@dataclass(slots=True)
class SearchCallback:
    action: str
    page: int = 1
    item_id: str | None = None

    def pack(self) -> str:
        parts: list[object] = [CallbackNamespaces.SEARCH, self.action, self.page]
        if self.item_id:
            parts.append(self.item_id)
        return build_callback(*parts)

    @classmethod
    def unpack(cls, value: str) -> "SearchCallback | None":
        parts = parse_callback(value)
        if len(parts) < 3:
            return None
        if parts[0] != CallbackNamespaces.SEARCH:
            return None

        try:
            page = int(parts[2])
        except ValueError:
            page = 1

        item_id = parts[3] if len(parts) > 3 else None

        return cls(action=parts[1], page=page, item_id=item_id)


@dataclass(slots=True)
class FavoriteCallback:
    action: str
    item_id: str

    def pack(self) -> str:
        return build_callback(CallbackNamespaces.FAVORITES, self.action, self.item_id)

    @classmethod
    def unpack(cls, value: str) -> "FavoriteCallback | None":
        parts = parse_callback(value)
        if len(parts) < 3:
            return None
        if parts[0] != CallbackNamespaces.FAVORITES:
            return None

        return cls(action=parts[1], item_id=parts[2])


@dataclass(slots=True)
class SavedSearchCallback:
    action: str
    saved_search_id: int

    def pack(self) -> str:
        return build_callback(CallbackNamespaces.SAVED, self.action, self.saved_search_id)

    @classmethod
    def unpack(cls, value: str) -> "SavedSearchCallback | None":
        parts = parse_callback(value)
        if len(parts) < 3:
            return None
        if parts[0] != CallbackNamespaces.SAVED:
            return None

        try:
            saved_search_id = int(parts[2])
        except ValueError:
            return None

        return cls(action=parts[1], saved_search_id=saved_search_id)


@dataclass(slots=True)
class ProfileCallback:
    action: str

    def pack(self) -> str:
        return build_callback(CallbackNamespaces.PROFILE, self.action)

    @classmethod
    def unpack(cls, value: str) -> "ProfileCallback | None":
        parts = parse_callback(value)
        if len(parts) < 2:
            return None
        if parts[0] != CallbackNamespaces.PROFILE:
            return None

        return cls(action=parts[1])


@dataclass(slots=True)
class SubscriptionCallback:
    action: str
    plan: str | None = None

    def pack(self) -> str:
        parts: list[object] = [CallbackNamespaces.SUBSCRIPTION, self.action]
        if self.plan:
            parts.append(self.plan)
        return build_callback(*parts)

    @classmethod
    def unpack(cls, value: str) -> "SubscriptionCallback | None":
        parts = parse_callback(value)
        if len(parts) < 2:
            return None
        if parts[0] != CallbackNamespaces.SUBSCRIPTION:
            return None

        return cls(
            action=parts[1],
            plan=parts[2] if len(parts) > 2 else None,
        )