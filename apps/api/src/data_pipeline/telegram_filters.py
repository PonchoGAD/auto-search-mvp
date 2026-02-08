import re
import yaml
from typing import Dict, Tuple, Optional

# =========================
# CONSTANTS / THRESHOLDS
# =========================

MIN_TEXT_LEN = 80

# =========================
# LOAD BRANDS WHITELIST
# =========================

def load_brands() -> Dict[str, dict]:
 """
 Загружаем brands.yaml.
 Используется ТОЛЬКО для быстрого pre-filter Telegram.
 Основная логика брендов — в ingest_quality.
 """
 try:
     with open("apps/api/src/config/brands.yaml", "r", encoding="utf-8") as f:
         data = yaml.safe_load(f) or {}
         return data.get("brands", {})
 except Exception:
     return {}


BRANDS_WHITELIST = load_brands()


# =========================
# REGEX / KEYWORDS
# =========================

STOP_WORDS = [
 "подписывайтесь",
 "вакансия",
 "работа",
 "скидк",
 "акция",
 "обсуждение",
 "опрос",
 "новости",
 "ремонт",
 "диагностика",
 "ошибка",
 "проблема",
 "запчасти",
 "разбор",
 "ищу",
 "куплю",
]

SALE_POSITIVE_WORDS = [
 # RU
 "продам",
 "продаю",
 "продается",
 "продаётся",
 "продажа",
 "срочно продам",
 "торг",
 "обмен",
 "рассмотрю обмен",
 # EN
 "for sale",
 "selling",
 "sell",
]

SALE_NEGATIVE_WORDS = [
 # RU
 "ищу",
 "куплю",
 "нужен",
 "подскажите",
 "помогите",
 "вопрос",
 # EN
 "looking for",
 "help",
 "question",
 "repair",
]

RE_PRICE = re.compile(r"(\d[\d\s]{1,10})\s*(₽|руб|р\.|тыс|к|k|\$|€)")
RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")
RE_MILEAGE = re.compile(r"\d[\d\s]{1,8}\s*км")


# =========================
# HELPERS
# =========================

def contains_car_entity(text: str) -> bool:
 """
 Проверка на наличие бренда или модели.
 Быстро, без confidence.
 """
 if not text:
     return False

 t = text.lower()

 for brand_data in BRANDS_WHITELIST.values():
     for group in brand_data.values():
         if not isinstance(group, list):
             continue
         for alias in group:
             if alias.lower() in t:
                 return True

 return False


def has_price(text: str) -> bool:
 """
 Цена — ОБЯЗАТЕЛЬНА для продажи.
 """
 if not text:
     return False
 return bool(RE_PRICE.search(text))


# =========================
# SALE INTENT (FAST)
# =========================

def is_sale_intent(text: str, min_score: int = 2) -> bool:
 """
 Упрощённый intent-фильтр для Telegram:

 +2 за позитивные слова
 +1 за цену
 -2 за негативные слова
 """
 if not text:
     return False

 t = text.lower()
 score = 0

 for w in SALE_POSITIVE_WORDS:
     if w in t:
         score += 2

 if has_price(t):
     score += 1

 for w in SALE_NEGATIVE_WORDS:
     if w in t:
         score -= 2

 return score >= min_score


# =========================
# MAIN FILTER (PRE-INGEST)
# =========================

def is_valid_telegram_post(text: str) -> Tuple[bool, Optional[str]]:
 """
 Жёсткий Telegram pre-filter.
 Используется ДО ingest и ДО RawDocument.

 Возвращает:
   (ok, reason)

 reason используется для аналитики.
 """

 if not text:
     return False, "spam"

 t = text.lower().strip()

 # 1️⃣ минимальная длина
 if len(t) < MIN_TEXT_LEN:
     return False, "spam"

 # 2️⃣ стоп-слова → обсуждения / сервисы
 for w in STOP_WORDS:
     if w in t:
         return False, "discussion"

 # 3️⃣ intent продажи
 if not is_sale_intent(t):
     return False, "discussion"

 # 4️⃣ обязательна цена
 if not has_price(t):
     return False, "no_price"

 # 5️⃣ обязательна марка или модель
 if not contains_car_entity(t):
     return False, "no_car_entity"

 return True, "ok"