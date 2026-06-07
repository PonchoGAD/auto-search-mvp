from __future__ import annotations

from aiogram.fsm.state import State, StatesGroup


class SearchFiltersState(StatesGroup):
    waiting_for_query = State()
    waiting_for_saved_search_name = State()