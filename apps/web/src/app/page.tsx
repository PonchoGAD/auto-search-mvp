"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import styles from "./page.module.css";
import ResultsBySource from "../components/ResultsBySource";

type SearchResult = {
  brand?: string;
  model?: string;
  year?: number;
  mileage?: number;
  price?: number;
  currency?: string;
  fuel?: string;
  region?: string;
  paint_condition?: string;
  score: number;
  why_match: string;
  source_url: string;
  source_name?: string;
};

type Option = { label: string; value: string };

const BRAND_OPTIONS: Option[] = [
  { label: "Любая", value: "" },
  { label: "BMW", value: "BMW" },
  { label: "Mercedes-Benz", value: "Mercedes" },
  { label: "Toyota", value: "Toyota" },
  { label: "Audi", value: "Audi" },
  { label: "Volkswagen", value: "Volkswagen" },
  { label: "Lexus", value: "Lexus" },
  { label: "Porsche", value: "Porsche" },
  { label: "Honda", value: "Honda" },
  { label: "Mazda", value: "Mazda" },
  { label: "Nissan", value: "Nissan" },
  { label: "Hyundai", value: "Hyundai" },
  { label: "Kia", value: "Kia" },
  { label: "Tesla", value: "Tesla" },
  { label: "Geely", value: "Geely" },
  { label: "Chery", value: "Chery" },
  { label: "Exeed", value: "Exeed" },
  { label: "Haval", value: "Haval" },
  { label: "Li Auto (Lixiang)", value: "Li Auto" },
  { label: "Zeekr", value: "Zeekr" },
];

const PRICE_OPTIONS: Option[] = [
  { label: "Любая", value: "" },
  { label: "до 1.5 млн", value: "до 1.5 млн" },
  { label: "до 2 млн", value: "до 2 млн" },
  { label: "до 2.5 млн", value: "до 2.5 млн" },
  { label: "до 3 млн", value: "до 3 млн" },
  { label: "до 4 млн", value: "до 4 млн" },
  { label: "до 5 млн", value: "до 5 млн" },
  { label: "до 7 млн", value: "до 7 млн" },
  { label: "до 10 млн", value: "до 10 млн" },
];

const MILEAGE_OPTIONS: Option[] = [
  { label: "Любой", value: "" },
  { label: "до 30 тыс", value: "пробег до 30 тыс" },
  { label: "до 50 тыс", value: "пробег до 50 тыс" },
  { label: "до 80 тыс", value: "пробег до 80 тыс" },
  { label: "до 100 тыс", value: "пробег до 100 тыс" },
  { label: "до 150 тыс", value: "пробег до 150 тыс" },
  { label: "до 200 тыс", value: "пробег до 200 тыс" },
];

const CONDITION_OPTIONS: Option[] = [
  { label: "Любое", value: "" },
  { label: "без окраса", value: "без окраса" },
  { label: "1–2 окраса", value: "1-2 окраса" },
  { label: "не бит", value: "не бит" },
  { label: "без ДТП", value: "без ДТП" },
  { label: "1 владелец", value: "1 владелец" },
  { label: "сервисная история", value: "сервисная история" },
  { label: "идеальное состояние", value: "идеальное состояние" },
  { label: "требует вложений", value: "требует вложений" },
];

const FUEL_OPTIONS: Option[] = [
  { label: "Любое", value: "" },
  { label: "бензин", value: "бензин" },
  { label: "дизель", value: "дизель" },
  { label: "гибрид", value: "гибрид" },
  { label: "электро", value: "электро" },
  { label: "газ/бензин", value: "газ/бензин" },
];

export default function HomePage() {
  const [query, setQuery] = useState("");
  const [brand, setBrand] = useState("");
  const [price, setPrice] = useState("");
  const [mileage, setMileage] = useState("");
  const [condition, setCondition] = useState("");
  const [fuel, setFuel] = useState("");

  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<SearchResult[]>([]);
  const [error, setError] = useState<string | null>(null);

  const debounceRef = useRef<NodeJS.Timeout | null>(null);
  const lastQueryRef = useRef<string>("");
  const abortRef = useRef<AbortController | null>(null);

  const composedQuery = useMemo(() => {
    const parts: string[] = [];
    if (query.trim()) parts.push(query.trim());
    if (brand) parts.push(brand);
    if (price) parts.push(price);
    if (mileage) parts.push(mileage);
    if (condition) parts.push(condition);
    if (fuel) parts.push(fuel);
    return parts.join(", ");
  }, [query, brand, price, mileage, condition, fuel]);

  const handleSearch = useCallback(
    async (overrideQuery?: string) => {
      const q = (overrideQuery ?? composedQuery).trim();
      if (!q) return;
      if (q === lastQueryRef.current) return;

      lastQueryRef.current = q;

      if (abortRef.current) abortRef.current.abort();
      abortRef.current = new AbortController();

      setLoading(true);
      setError(null);

      try {
        const res = await fetch("/api/v1/search", {
          method: "POST",
          headers: { "Content-Type": "application/json"},
          body: JSON.stringify({ query: q }),
          signal: abortRef.current.signal,
        });

        if (!res.ok) {
          throw new Error(`Search request failed: ${res.status}`);
        }

        const data = await res.json();
        setResults(data.results || []);
      } catch (e: unknown) {
        const error = e instanceof Error ? e : new Error("Unknown error");
        if (error.name === "AbortError") return;
        setError(error.message || "Ошибка поиска");
      } finally {
        setLoading(false);
      }
    },
    [composedQuery]
  );

  useEffect(() => {
    if (!composedQuery.trim()) return;
    if (debounceRef.current) clearTimeout(debounceRef.current);

    debounceRef.current = setTimeout(() => {
      handleSearch(composedQuery);
    }, 450);

    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [composedQuery, handleSearch]);

  const clear = () => {
    setQuery("");
    setBrand("");
    setPrice("");
    setMileage("");
    setCondition("");
    setFuel("");
    setResults([]);
    setError(null);
    lastQueryRef.current = "";
  };

  return (
    <div className={styles.page}>
      <div className={styles.bg} />
      <div className={styles.overlay} />

      <div className={styles.container}>
        <header className={styles.header}>
          <div className={styles.headerTop}>
            <div className={styles.brandBlock}>
              <h1 className={styles.title}>
                <span className={styles.titleStrongItalic}>
                  Поисковая платформа авто-объявлений
                </span>
              </h1>
              <p className={styles.subtitle}>
                Семантический поиск автомобилей по форумам и маркетплейсам
              </p>
            </div>

            <nav className={styles.nav}>
              <Link className={styles.navLink} href="/search">
                Поиск
              </Link>
              <Link className={styles.navLink} href="/analytics">
                Аналитика
              </Link>
            </nav>
          </div>
        </header>

        <section className={styles.panel}>
          <div className={styles.searchRow}>
            <input
              className={styles.searchInput}
              placeholder="BMW до 2 млн, пробег до 50 тыс, без окраса"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />

            <button
              className={styles.primaryBtn}
              onClick={() => handleSearch()}
              disabled={loading}
            >
              {loading ? "Ищем…" : "Найти"}
            </button>

            <button className={styles.secondaryBtn} onClick={clear}>
              Очистить
            </button>
          </div>

          <div className={styles.filtersRow}>
            <FilterSelect label="Марка" value={brand} onChange={setBrand} options={BRAND_OPTIONS} />
            <FilterSelect label="Цена" value={price} onChange={setPrice} options={PRICE_OPTIONS} />
            <FilterSelect label="Пробег" value={mileage} onChange={setMileage} options={MILEAGE_OPTIONS} />
            <FilterSelect label="Состояние" value={condition} onChange={setCondition} options={CONDITION_OPTIONS} />
            <FilterSelect label="Топливо" value={fuel} onChange={setFuel} options={FUEL_OPTIONS} />
          </div>

          <div className={styles.hintRow}>
            <div className={styles.hintLabel}>Запрос:</div>
            <div className={styles.hintValue}>{composedQuery || "—"}</div>
          </div>
        </section>

        {error && <div className={styles.error}>❌ {error}</div>}

        {loading && (
          <section className={styles.loadingBlock}>
            <div className={styles.skeletonGrid}>
              {[1, 2, 3].map((i) => (
                <div key={i} className={styles.skeletonCard} />
              ))}
            </div>
          </section>
        )}

        {!loading && results.length > 0 && (
          <section className={styles.results}>
            <ResultsBySource results={results} />
          </section>
        )}

        <footer className={styles.footer}>
          SaaS Semantic Auto Search © 2026
        </footer>
      </div>
    </div>
  );
}

function FilterSelect({
  label,
  value,
  onChange,
  options,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  options: Option[];
}) {
  return (
    <div className={styles.filter}>
      <div className={styles.filterLabel}>{label}</div>
      <div className={styles.selectWrap}>
        <select
          className={styles.select}
          value={value}
          onChange={(e) => onChange(e.target.value)}
        >
          {options.map((o) => (
            <option key={`${label}-${o.label}`} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
        <span className={styles.selectChevron}>▾</span>
      </div>
    </div>
  );
}