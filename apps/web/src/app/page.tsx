"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import QuickFilters from "../components/QuickFilters";
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

export default function HomePage() {
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<SearchResult[]>([]);
  const [error, setError] = useState<string | null>(null);

  const debounceRef = useRef<NodeJS.Timeout | null>(null);

  const API_URL = useMemo(
    () => process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000",
    []
  );

  const handleSearch = useCallback(
    async (overrideQuery?: string) => {
      const q = (overrideQuery ?? query).trim();
      if (!q || loading) return;

      setLoading(true);
      setError(null);
      setResults([]);

      try {
        const res = await fetch(`${API_URL}/api/v1/search`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query: q }),
        });

        if (!res.ok) throw new Error("Search request failed");

        const data = await res.json();
        setResults(data.results || []);
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : "Ошибка поиска";
        setError(msg);
      } finally {
        setLoading(false);
      }
    },
    [query, loading, API_URL]
  );

  // =========================
  // 🔥 DEBOUNCE 400ms
  // =========================
  useEffect(() => {
    if (!query.trim()) return;

    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }

    debounceRef.current = setTimeout(() => {
      handleSearch(query);
    }, 400);

    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
    };
  }, [query, handleSearch]);

  const clear = () => {
    setQuery("");
    setResults([]);
    setError(null);
  };

  useEffect(() => {
    const el = document.getElementById("search-input") as HTMLInputElement | null;
    el?.focus();
  }, []);

  // Example queries removed as they were unused

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
      <style>
        {`
          @keyframes pulse {
            0% { opacity: 0.6; }
            50% { opacity: 1; }
            100% { opacity: 0.6; }
          }
          .skeleton {
            animation: pulse 1.4s ease-in-out infinite;
          }
        `}
      </style>

      <div style={{ maxWidth: 1150, margin: "0 auto" }}>
        <header style={{ marginBottom: 36 }}>
          <h1
            style={{
              fontSize: 36,
              marginBottom: 10,
              background: "linear-gradient(90deg,#6366f1,#8b5cf6)",
              WebkitBackgroundClip: "text",
              WebkitTextFillColor: "transparent",
            }}
          >
            🚗 Auto Search Platform
          </h1>
          <p style={{ color: "#94a3b8" }}>
            Семантический поиск автомобилей по форумам и маркетплейсам
          </p>
        </header>

        {/* SEARCH */}
        <section style={{ marginBottom: 32 }}>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1fr auto auto auto",
              gap: 12,
            }}
          >
            <input
              id="search-input"
              type="text"
              placeholder="BMW до 2 млн, пробег до 50 тыс, без окраса"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              style={{
                padding: "16px 18px",
                fontSize: 16,
                borderRadius: 14,
                border: "1px solid #1e293b",
                background: "#111827",
                color: "#fff",
              }}
            />

            <QuickFilters
              query={query}
              onApply={(q) => {
                setQuery(q);
                handleSearch(q);
              }}
            />

            <button
              onClick={() => handleSearch()}
              disabled={loading}
              style={{
                padding: "16px 22px",
                borderRadius: 14,
                background: "linear-gradient(90deg,#6366f1,#8b5cf6)",
                color: "#fff",
                border: "none",
                cursor: "pointer",
              }}
            >
              Найти
            </button>

            <button
              onClick={clear}
              style={{
                padding: "16px 16px",
                borderRadius: 14,
                background: "transparent",
                color: "#94a3b8",
                border: "1px solid #1e293b",
              }}
            >
              Очистить
            </button>
          </div>
        </section>

        {error && <div style={{ color: "#ef4444" }}>❌ {error}</div>}

        {loading && (
          <>
            <div style={{ color: "#94a3b8", marginBottom: 14 }}>
              🔍 Анализируем источники...
            </div>

            {/* 🔥 SKELETON */}
            <div style={{ display: "grid", gap: 16 }}>
              {[1, 2, 3, 4].map((i) => (
                <div
                  key={i}
                  className="skeleton"
                  style={{
                    height: 120,
                    borderRadius: 16,
                    background: "#1e293b",
                  }}
                />
              ))}
            </div>
          </>
        )}

        {!loading && results.length > 0 && (
          <section style={{ marginTop: 24 }}>
            <ResultsBySource results={results} />
          </section>
        )}
      </div>
    </div>
  );
}
