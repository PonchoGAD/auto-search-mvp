import os
import hashlib
import re
from typing import List, Tuple, Optional, Any, Dict
from datetime import datetime, timezone

from qdrant_client.models import PointStruct

from db.session import SessionLocal, engine
from db.models import Base, NormalizedDocument, DocumentChunk
from integrations.vector_db.qdrant import QdrantStore
from shared.embeddings.provider import embed_text


def _clean_text(text: str) -> str:
    if not text:
        return ""

    text = text.replace("\u00A0", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# =====================================================
# VECTOR CONFIG
# =====================================================

VECTOR_SIZE = 768


# =====================================================
# ⚠️ CRITICAL ADAPTER (DO NOT REMOVE)
# =====================================================

def deterministic_embedding(text: str) -> List[float]:
    """
    ADAPTER FOR SearchService.

    SearchService imports this function.
    Therefore it must return the same embedding
    that was used during indexing.
    """
    return embed_text(text)


# =====================================================
# PAYLOAD NORMALIZATION / VALIDATION HELPERS
# =====================================================

ALLOWED_FUELS = {"petrol", "diesel", "hybrid", "electric", "gas", "gas_petrol"}
MARKETPLACE_SOURCES = {"avito", "drom", "auto_ru", "autoru", "cars", "dealer", "marketplace"}
SEARCH_MIN_DOC_QUALITY = int(os.getenv("SEARCH_MIN_DOC_QUALITY", "0"))
ALLOW_ZERO_QUALITY_INDEX = os.getenv("ALLOW_ZERO_QUALITY_INDEX", "0").strip() == "1"


def _norm_str(v: object) -> str | None:
    if isinstance(v, str):
        v = v.strip().lower()
        return v or None
    return None


def _norm_int(v: object) -> int | None:
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.replace(" ", "")
        return int(v)
    except Exception:
        return None


def _norm_float(v: object) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except Exception:
        return 0.0


def _norm_sale_intent(v: object) -> int:
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, (int, float)):
        return 1 if int(v) > 0 else 0
    if isinstance(v, str):
        return 1 if v.strip().lower() in {"1", "true", "yes", "y"} else 0
    return 0


def _count_vehicle_signals(
    brand: str | None,
    model: str | None,
    price: int | None,
    mileage: int | None,
    year: int | None,
    fuel: str | None,
) -> int:
    return sum([
            1 if brand else 0,
            1 if model else 0,
            1 if price else 0,
            1 if mileage else 0,
            1 if year else 0,
            1 if fuel else 0,
        ]
    )


def _is_probable_search_or_category_url(source: str | None, source_url: str | None) -> bool:
    if not source_url:
        return True

    url = (source_url or "").strip().lower()
    source = (source or "").strip().lower()

    if not url.startswith(("http://", "https://")):
        return True

    generic_bad_parts =[
        "/search",
        "/search/",
        "/catalog",
        "/catalog/",
        "/all/",
        "/listing",
        "/listings",
        "/cars/",
        "/cars?",
        "/avtomobili",
        "/avtomobili/",
        "/auto/",
        "/legkovye/",
        "?p=",
        "&p=",
        "?page=",
        "&page=",
        "?q=",
        "&q=",
        "?query=",
        "&query=",
        "?text=",
        "&text=",
        "?search",
        "&search",
        "?filter",
        "&filter",
        "?sort=",
        "&sort=",
    ]

    if any(part in url for part in generic_bad_parts):
        return True

    if source == "avito":
        avito_garbage =[
            "/all/avtomobili",
            "/rossiya/avtomobili",
            "/avtomobili?s=",
            "/avtomobili?",
            "/cars?",
            "/cars/",
        ]
        if any(part in url for part in avito_garbage):
            return True

        if "/items/" not in url and re.search(r"/[a-zа-я0-9_-]+$", url) is None:
            return True

    return False


def _validate_canonical_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Index layer validation only.
    No extraction, no recovery, no taxonomy logic.
    """

    brand = payload.get("brand")
    payload["brand"] = brand if isinstance(brand, str) and brand.strip() else None

    model = payload.get("model")
    payload["model"] = model if isinstance(model, str) and model.strip() else None

    fuel = payload.get("fuel")
    fuel = fuel if isinstance(fuel, str) and fuel.strip() else None

    # 🔥 нормализация топлива (RU → EN)
    FUEL_MAP = {
        "бензин": "petrol",
        "дизель": "diesel",
        "электро": "electric",
        "электр": "electric",
        "электромобиль": "electric",
        "ev": "electric",
        "гибрид": "hybrid",
        "газ": "gas",
        "газ бензин": "gas_petrol",
    }

    if isinstance(fuel, str):
        fuel_norm = fuel.strip().lower()

        if fuel_norm in FUEL_MAP:
            fuel = FUEL_MAP[fuel_norm]

    if fuel not in ALLOWED_FUELS:
        fuel = None

    payload["fuel"] = fuel

    payload["price"] = _norm_int(payload.get("price"))
    if payload["price"] is not None and payload["price"] <= 0:
        payload["price"] = None

    payload["mileage"] = _norm_int(payload.get("mileage"))
    payload["year"] = _norm_int(payload.get("year"))

    payload["sale_intent"] = _norm_sale_intent(payload.get("sale_intent"))
    payload["quality_score"] = _norm_float(payload.get("quality_score"))

    payload["brand_model"] = f"{payload.get('brand') or ''} {payload.get('model') or ''}".strip() or None
    return payload


def _should_index_listing_doc(doc: NormalizedDocument, chunk: DocumentChunk) -> Tuple[bool, str]:
    source = _norm_str(getattr(doc, "source", None))
    source_url = getattr(doc, "source_url", None)
    title = _clean_text(getattr(doc, "title", "") or "")
    chunk_text = _clean_text(getattr(chunk, "chunk_text", "") or "")

    if not source_url or not title:
        return False, "missing_url_or_title"

    if _is_probable_search_or_category_url(source, source_url):
        return False, "non_listing_url"

    brand = _norm_str(getattr(doc, "brand", None))
    model = _norm_str(getattr(doc, "model", None))

    fuel = _norm_str(getattr(doc, "fuel", None))
    if fuel not in ALLOWED_FUELS:
        fuel = None

    price = _norm_int(getattr(doc, "price", None))
    if price is not None and price <= 0:
        price = None

    mileage = _norm_int(getattr(doc, "mileage", None))
    year = _norm_int(getattr(doc, "year", None))
    sale_intent = _norm_sale_intent(getattr(doc, "sale_intent", None))
    quality_score = _norm_float(getattr(doc, "quality_score", None))

    vehicle_signals = _count_vehicle_signals(
        brand=brand,
        model=model,
        price=price,
        mileage=mileage,
        year=year,
        fuel=fuel,
    )

    doc_quality = (
        (1 if price else 0)
        + (1 if mileage else 0)
        + (1 if year else 0)
    )

    # ❗ ослабляем фильтр (иначе пустая выдача)
    if doc_quality < 0:
        return False, "low_quality"

    if source == "telegram":
        enough_sale_signals = (
            sale_intent == 1
            or price is not None
            or vehicle_signals >= 4
            or (vehicle_signals >= 3 and year is not None)
        )

        suspicious_chat_markers =[
            "кто что думает",
            "подскажите",
            "это норм",
            "реальная цена",
            "как вам",
            "км/ч",
            "скорость",
            "масло",
            "редуктор",
            "шины",
            "резина",
            "диски",
            "запчаст",
            "?",
        ]

        text_for_check = f"{title} {chunk_text}".lower()
        looks_like_chat = any(marker in text_for_check for marker in suspicious_chat_markers)

        if not enough_sale_signals or looks_like_chat:
            return False, "non_sale_telegram"

    # ❗ разрешаем индекс даже слабых документов
    if not brand and not model and price is None and mileage is None and year is None:
        return False, "low_quality"

    return True, "primary"


# =====================================================
# STRUCTURED TEXT BUILDER (MULTI VECTOR)
# =====================================================

def build_structured_text(doc: NormalizedDocument) -> str:
    parts =[]

    brand = _norm_str(getattr(doc, "brand", None))
    model = _norm_str(getattr(doc, "model", None))
    fuel = _norm_str(getattr(doc, "fuel", None))
    year = _norm_int(getattr(doc, "year", None))
    price = _norm_int(getattr(doc, "price", None))
    mileage = _norm_int(getattr(doc, "mileage", None))
    paint_condition = getattr(doc, "paint_condition", None)

    if brand and model:
        parts.append(f"{brand} {model}")

    if brand:
        parts.append(f"brand {brand}")

    if model:
        parts.append(f"model {model}")

    if fuel in ALLOWED_FUELS:
        parts.append(f"fuel {fuel}")

    if year:
        parts.append(f"year {year}")

    if price:
        parts.append(f"price {price}")

    if mileage:
        parts.append(f"mileage {mileage}")

    if isinstance(paint_condition, str) and paint_condition.strip():
        parts.append(f"paint {paint_condition.strip().lower()}")

    return " ".join(parts).strip()


# =====================================================
# INDEX DOCUMENT CHUNKS
# =====================================================

def index_document_chunks(
    limit: int = 2000,
    force_rebuild: bool = False
) -> int:

    Base.metadata.create_all(bind=engine)
    session = SessionLocal()

    try:
        store = QdrantStore()

        if force_rebuild:
            try:
                print("[INDEX] force rebuild: clearing collection", flush=True)
                store.client.delete_collection("auto_search_chunks")
            except Exception:
                pass

            store.create_collection(VECTOR_SIZE)

        store.create_collection(VECTOR_SIZE)

        chunks = (
            session.query(DocumentChunk, NormalizedDocument)
            .join(NormalizedDocument, DocumentChunk.normalized_id == NormalizedDocument.id)
            .order_by(DocumentChunk.id.desc())
            .limit(limit)
            .all()
        )

        if not chunks:
            print("[INDEX][WARN] no document chunks found", flush=True)
            return 0

        points: List[PointStruct] =[]
        now = datetime.now(tz=timezone.utc)

        skipped_empty_text = 0
        skipped_missing_url = 0
        skipped_non_listing_url = 0
        skipped_low_quality = 0
        skipped_non_sale_telegram = 0
        indexed_primary = 0
        indexed_auxiliary = 0

        for ch, doc in chunks:
            # 🔥 КРИТИЧЕСКИЙ ФИКС — используем normalized_text
            chunk_text = _clean_text(
                (doc.normalized_text or "") + " " + (ch.chunk_text or "")
            )[:800]
            if not chunk_text:
                skipped_empty_text += 1
                continue

            source_url = getattr(doc, "source_url", None)
            title_text = _clean_text(getattr(doc, "title", "") or "")

            if not source_url or not title_text:
                skipped_missing_url += 1
                continue

            should_index, reason = _should_index_listing_doc(doc, ch)
            if not should_index:
                if reason == "non_listing_url":
                    skipped_non_listing_url += 1
                elif reason == "low_quality":
                    skipped_low_quality += 1
                elif reason == "non_sale_telegram":
                    skipped_non_sale_telegram += 1
                else:
                    skipped_low_quality += 1
                continue

            structured_text = build_structured_text(doc)
            vectors =[]

            brand = _norm_str(getattr(doc, "brand", None))
            model = _norm_str(getattr(doc, "model", None))

            if title_text:
                title_embedding_text = f"""
title {title_text}
brand {brand or ''}
model {model or ''}
""".strip()

                title_vec = deterministic_embedding(title_embedding_text)
                if len(title_vec) == VECTOR_SIZE:
                    vectors.append(("title", title_vec))
                    vectors.append(("title_boost", title_vec))

            content_embedding_text = f"""
title {title_text}
brand {brand or ''}
model {model or ''}
content {chunk_text}
""".strip()

            content_vec = deterministic_embedding(content_embedding_text)
            if len(content_vec) == VECTOR_SIZE:
                vectors.append(("content", content_vec))

            if structured_text:
                structured_embedding_text = f"""
structured {structured_text}
title {title_text}
""".strip()

                structured_vec = deterministic_embedding(structured_embedding_text)
                if len(structured_vec) == VECTOR_SIZE:
                    vectors.append(("structured", structured_vec))

            if not vectors:
                continue

            created_at_ts = getattr(doc, "created_at_ts", None)
            if created_at_ts is None:
                created_at_ts = int(now.timestamp())

            created_at = getattr(doc, "created_at", None)
            if not created_at:
                created_at = now.isoformat()

            currency = getattr(doc, "currency", None)
            if not currency:
                currency = "RUB"

            print("[DEBUG BEFORE VALIDATE]", {
                "brand": getattr(doc, "brand", None),
                "model": getattr(doc, "model", None),
                "fuel": getattr(doc, "fuel", None),
                "mileage": getattr(doc, "mileage", None),
            })

            payload = _validate_canonical_payload({
                "source": getattr(doc, "source", None),
                "source_url": source_url,
                "title": getattr(doc, "title", None),

                "brand": getattr(doc, "brand", None),
                "model": getattr(doc, "model", None),
                "price": getattr(doc, "price", None),
                "currency": currency,
                "mileage": getattr(doc, "mileage", None),
                "year": getattr(doc, "year", None),
                "fuel": getattr(doc, "fuel", None),
                "region": _norm_str(getattr(doc, "region", None)),
                "paint_condition": getattr(doc, "paint_condition", None),

                "sale_intent": getattr(doc, "sale_intent", None),
                "quality_score": getattr(doc, "quality_score", None),

                "doc_id": getattr(doc, "id", None),
                "normalized_id": getattr(doc, "id", None),
                "chunk_id": getattr(ch, "id", None),
                "chunk_index": getattr(ch, "chunk_index", None),
                "content": chunk_text,
                "title_text": title_text,

                "created_at": created_at if isinstance(created_at, str) else now.isoformat(),
                "created_at_ts": _norm_int(created_at_ts) or int(now.timestamp()),
                "created_at_source": "normalized",
            })

            doc_quality = (
                (1 if payload.get("price") else 0)
                + (1 if payload.get("year") else 0)
            )
            payload["doc_quality"] = doc_quality

            for vec_type, vec in vectors:
                if not vec or len(vec) != VECTOR_SIZE:
                    continue

                point_hash = hashlib.sha1(f"{ch.id}_{vec_type}".encode()).hexdigest()
                point_id = int(point_hash[:16], 16)

                points.append(
                    PointStruct(
                        id=point_id,
                        vector=vec,
                        payload={
                            **payload,
                            "vector_type": vec_type,
                        },
                    )
                )

                indexed_primary += 1

        if not points:
            print("[INDEX][WARN] no valid points generated from chunks", flush=True)
            print(
                f"[INDEX][DEBUG] skipped_empty_text={skipped_empty_text} "
                f"skipped_missing_url={skipped_missing_url} "
                f"skipped_non_listing_url={skipped_non_listing_url} "
                f"skipped_low_quality={skipped_low_quality} "
                f"skipped_non_sale_telegram={skipped_non_sale_telegram} "
                f"indexed_primary={indexed_primary} "
                f"indexed_auxiliary={indexed_auxiliary}",
                flush=True,
            )
            return 0

        store.upsert(points)

        print(f"[INDEX] indexed chunks: {len(points)}", flush=True)
        print(
            f"[INDEX][DEBUG] skipped_empty_text={skipped_empty_text} "
            f"skipped_missing_url={skipped_missing_url} "
            f"skipped_non_listing_url={skipped_non_listing_url} "
            f"skipped_low_quality={skipped_low_quality} "
            f"skipped_non_sale_telegram={skipped_non_sale_telegram} "
            f"indexed_primary={indexed_primary} "
            f"indexed_auxiliary={indexed_auxiliary}",
            flush=True,
        )

        return len(points)

    finally:
        try:
            session.close()
        except Exception:
            pass


# =====================================================
# RUN INDEX
# =====================================================

def run_index(limit: int = 2000, force_rebuild: bool = False):
    print("[INDEX] run_index -> index_document_chunks", flush=True)
    return index_document_chunks(
        limit=limit,
        force_rebuild=force_rebuild,
    )