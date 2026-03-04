// apps/web/src/app/search/page.tsx
"use client";

import {
  useEffect,
  useMemo,
  useRef,
  useState,
  useCallback,
} from "react";
import QuickFilters from "../../components/QuickFilters";
import RepeatSearch from "../../components/RepeatSearch";
import ResultsBySource from "../../components/ResultsBySource";

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

export default function SearchPage() {
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<SearchResult[]>([]);
  const [error, setError] = useState<string | null>(null);

  const debounceRef = useRef<NodeJS.Timeout | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const API_URL = useMemo(
    () => process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000",
    []
  );

  const performSearch = useCallback(
    async (q: string) => {
      if (!q.trim()) return;

      // cancel previous in-flight request
      if (abortRef.current) {
        abortRef.current.abort();
      }
      abortRef.current = new AbortController();

      setLoading(true);
      setError(null);

      try {
        const res = await fetch("/api/v1/search", {
          method: "POST",
          headers: { "Content-Type": "application/json",
                     "X-API-Key": process.env.NEXT_PUBLIC_API_KEY as string
           },
          body: JSON.stringify({ query: q }),
          signal: abortRef.current.signal,
        });

        if (!res.ok) {
          throw new Error("Search request failed");
        }

        const data = await res.json();
        setResults(data.results || []);
      } catch (e: unknown) {
        // ignore abort errors
        if (e instanceof DOMException && e.name === "AbortError") return;

        const msg = e instanceof Error ? e.message : "Ошибка поиска";
        setError(msg);
      } finally {
        setLoading(false);
      }
    },
    [API_URL]
  );

  // Debounce 400ms
  useEffect(() => {
    if (!query.trim()) {
      setResults([]);
      return;
    }

    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }

    debounceRef.current = setTimeout(() => {
      performSearch(query);
    }, 400);

    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
    };
  }, [query, performSearch]);

  const handleManualSearch = () => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }
    performSearch(query);
  };

  const onRepeat = (q: string) => {
    setQuery(q);
    performSearch(q);
  };

  const clear = () => {
    setQuery("");
    setResults([]);
    setError(null);
  };

  const exampleQueries = [
    "BMW до 2 млн, пробег до 50 тыс, без окраса",
    "Mercedes E-класс, дизель, до 120 000 км",
    "Toyota Camry, не бит, до 1.8 млн",
    "Lexus RX, гибрид, до 3.5 млн",
  ];

  return (
    <div
      style={{
        minHeight: "100vh",
        background: "linear-gradient(180deg, #0f172a 0%, #0b0f1a 100%)",
        color: "#f8fafc",
        padding: "48px 20px",
        fontFamily: "Inter, system-ui, sans-serif",
      }}
    >
      <div style={{ maxWidth: 1100, margin: "0 auto" }}>
        <h1
          style={{
            fontSize: 34,
            marginBottom: 24,
            background: "linear-gradient(90deg,#6366f1,#8b5cf6)",
            WebkitBackgroundClip: "text",
            WebkitTextFillColor: "transparent",
          }}
        >
          🔎 Поиск автомобилей
        </h1>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "1fr auto auto auto",
            gap: 12,
            marginBottom: 24,
          }}
        >
          <input
            type="text"
            placeholder="BMW до 2 млн, пробег до 50 тыс, без окраса"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleManualSearch()}
            style={{
              padding: "16px 18px",
              fontSize: 16,
              borderRadius: 14,
              border: "1px solid #1e293b",
              background: "#111827",
              color: "#fff",
              outline: "none",
            }}
          />

          <QuickFilters
            query={query}
            onApply={(q) => {
              setQuery(q);
              performSearch(q);
            }}
          />

          <button
            onClick={handleManualSearch}
            disabled={loading}
            style={{
              padding: "16px 22px",
              fontSize: 16,
              borderRadius: 14,
              background: "linear-gradient(90deg,#6366f1,#8b5cf6)",
              color: "#fff",
              border: "none",
              cursor: loading ? "not-allowed" : "pointer",
              opacity: loading ? 0.7 : 1,
              fontWeight: 600,
            }}
          >
            {loading ? "Поиск…" : "Найти"}
          </button>

          <button
            onClick={clear}
            style={{
              padding: "16px 16px",
              fontSize: 14,
              borderRadius: 14,
              background: "transparent",
              color: "#94a3b8",
              border: "1px solid #1e293b",
              cursor: "pointer",
            }}
          >
            Очистить
          </button>
        </div>

        {!query && (
          <div style={{ marginBottom: 32 }}>
            <div
              style={{
                display: "flex",
                gap: 10,
                flexWrap: "wrap",
                marginBottom: 16,
              }}
            >
              {exampleQueries.map((q) => (
                <button
                  key={q}
                  onClick={() => onRepeat(q)}
                  style={{
                    padding: "8px 12px",
                    borderRadius: 999,
                    border: "1px solid #1e293b",
                    background: "#0f172a",
                    color: "#cbd5e1",
                    fontSize: 13,
                    cursor: "pointer",
                  }}
                >
                  {q}
                </button>
              ))}
            </div>

            <RepeatSearch onRepeatAction={onRepeat} />
          </div>
        )}

        {error && (
          <div style={{ color: "#ef4444", marginBottom: 14 }}>
            ❌ {error}
          </div>
        )}

        {loading && (
          <div style={{ color: "#94a3b8", marginBottom: 14 }}>
            🔍 Анализируем источники…
          </div>
        )}

        {!loading && results.length > 0 && (
          <ResultsBySource results={results} />
        )}

        {!loading && query && results.length === 0 && (
          <div
            style={{
              marginTop: 24,
              padding: 24,
              borderRadius: 16,
              background: "#0f172a",
              border: "1px dashed #334155",
              color: "#94a3b8",
            }}
          >
            ⚠️ Пока ничего не найдено.
            Попробуйте изменить параметры поиска.
          </div>
        )}
      </div>
    </div>
  );
}