from typing import Literal

from domain.query_schema import StructuredQuery


RouteType = Literal["structured", "brand_only", "semantic"]


def route_query(structured: StructuredQuery) -> RouteType:
    has_brand = bool(getattr(structured, "brand", None))
    has_model = bool(getattr(structured, "model", None))

    # 🔥 ФИКС: Проверяем наличие любых числовых фильтров (цена, пробег, год)
    has_numeric = any([
        getattr(structured, "price_max", None) is not None,
        getattr(structured, "mileage_max", None) is not None,
        getattr(structured, "year_min", None) is not None,
    ])

    has_fuel = bool(getattr(structured, "fuel", None))
    has_city = bool(getattr(structured, "city", None))
    has_paint = bool(getattr(structured, "paint_condition", None))

    keywords = getattr(structured, "keywords", []) or []
    exclusions = getattr(structured, "exclusions",[]) or[]

    has_keywords = bool(keywords)
    has_exclusions = bool(exclusions)

    generation_hints = ("xv", "e", "f", "g", "w", "xa", "lc", "j", "t", "mq", "ql", "nq")
    keywords_only_generation = False

    if keywords:
        keywords_only_generation = all(
            any(k.lower().startswith(h) or h in k.lower() for h in generation_hints)
            for k in keywords
        )

    # 🔥 ФИКС: Теперь наличие пробега, года или цены (has_numeric) 
    # ГАРАНТИРОВАННО отправляет запрос в строгий структурированный поиск
    if has_brand and (has_fuel or has_city or has_paint or has_numeric):
        return "structured"

    if has_brand and has_model:
        return "brand_only"

    if has_brand and not has_numeric and not has_fuel and not has_city and not has_paint and not has_exclusions:
        if not has_keywords or keywords_only_generation:
            return "brand_only"

    return "semantic"