# apps/api/src/services/answer_builder.py

from typing import List, Dict, Any
from services.query_parser import StructuredQuery


class AnswerBuilder:
    """
    Формирует текстовый ответ на основе найденных результатов.
    НИЧЕГО не выдумывает.
    """

    def build(
        self,
        structured: StructuredQuery,
        results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:

        # 🔒 FINAL RESULT GUARD (brand mismatch protection)
        if structured.brand:
            filtered =[]
            for r in results:
                brand = r.get("brand")
                if not brand:
                    continue
                if brand.lower() == structured.brand.lower():
                    filtered.append(r)

            # применяем только если сохраняется >=1 результата
            if len(filtered) >= 1:
                results = filtered

        # 🔒 MODEL GUARD
        if structured.model:
            filtered =[]
            for r in results:
                model = r.get("model")
                if not model:
                    continue
                if model.lower() == structured.model.lower():
                    filtered.append(r)

            # model фильтр более строгий
            if len(filtered) >= 2:
                results = filtered

        if not results:
            return {
                "summary": "По вашему запросу подходящих вариантов не найдено.",
                "highlights": [],
                "sources":[],
            }

        # Краткое резюме
        summary = self._build_summary(structured, results)

        # Лучшие варианты (top 3–5)
        highlights = self._build_highlights(results[:5])

        # Источники
        sources = list(
            {
                r["source_name"]: r["source_url"]
                for r in results
            }.items()
        )

        return {
            "summary": summary,
            "highlights": highlights,
            "sources":[
                {"name": name, "url": url}
                for name, url in sources
            ],
        }

    # -------------------------
    # INTERNAL
    # -------------------------

    def _build_summary(
        self,
        structured: StructuredQuery,
        results: List[Dict[str, Any]],
    ) -> str:
        brand = structured.brand or "автомобилей"
        count = len(results)

        return (
            f"Найдено {count} вариантов {brand}, "
            f"наиболее соответствующих вашему запросу."
        )

    def _build_highlights(
        self,
        results: List[Dict[str, Any]],
    ) -> List[str]:
        bullets =