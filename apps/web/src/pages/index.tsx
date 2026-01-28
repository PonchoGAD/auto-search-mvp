import { useEffect, useMemo, useState } from "react";
import RepeatSearch from "../components/RepeatSearch";
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

  // sticky search bar state
  const [isSticky, setIsSticky] = useState(false);

  const API_URL = useMemo(
    () => process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000",
    []
  );

  const handleSearch = async (overrideQuery?: string) => {
    const q = (overrideQuery ?? query).trim();
    if (!q) return;
    if (loading) return;

    setLoading(true);
    setError(null);
    setResults([]);

    try {
      const res = await fetch(`${API_URL}/search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: q }),
      });

      if (!res.ok) {
        throw new Error("Search request failed");
      }

      const data = await res.json();
      setResults(data.results || []);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Ошибка поиска";
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  const onRepeat = (q: string) => {
    setQuery(q);
    handleSearch(q);
  };

  const clear = () => {
    setQuery("");
    setResults([]);
    setError(null);
  };

  // фокус на поле
  useEffect(() => {
    const el = document.getElementById("search-input") as HTMLInputElement | null;
    el?.focus();
  }, []);

  // sticky logic
  useEffect(() => {
    const onScroll = () => {
      setIsSticky(window.scrollY > 120);
    };
    window.addEventListener("scroll", onScroll);
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

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
        background: "#0e0e11",
        color: "#f1f1f1",
        padding: "40px 16px",
        fontFamily: "Inter, system-ui, sans-serif",
      }}
    >
      <div style={{ maxWidth: 1100, margin: "0 auto" }}>
        {/* ================= HEADER ================= */}
        <header
          style={{
            display: "flex",
            justifyContent: "space-between",
            gap: 16,
            alignItems: "flex-start",
            marginBottom: 28,
          }}
        >
          <div>
            <h1 style={{ fontSize: 34, marginBottom: 8 }}>
              🚗 Auto Search Platform
            </h1>
            <p style={{ color: "#9aa0a6", margin: 0 }}>
              Семантический поиск автомобилей по форумам и маркетплейсам
            </p>
          </div>

          <div
            style={{
              padding: "10px 12px",
              borderRadius: 10,
              border: "1px solid #2a2a2e",
              background: "#15151a",
              color: "#9aa0a6",
              fontSize: 12,
              lineHeight: 1.35,
              minWidth: 240,
            }}
          >
            <div style={{ color: "#e5e7eb", fontWeight: 600, marginBottom: 6 }}>
              MVP статус
            </div>
            <div>• Ingest + Normalize + Vector Search</div>
            <div>• Ranking (source / brand / sale)</div>
            <div>• Analytics (top / empty / signals)</div>
          </div>
        </header>

        {/* ================= SEARCH ZONE ================= */}
        <section
          style={{
            position: isSticky ? "sticky" : "relative",
            top: isSticky ? 0 : "auto",
            zIndex: 20,
            background: isSticky ? "rgba(14,14,17,0.85)" : "transparent",
            backdropFilter: isSticky ? "blur(6px)" : "none",
            paddingTop: isSticky ? 12 : 0,
            paddingBottom: isSticky ? 12 : 0,
            boxShadow: isSticky ? "0 8px 24px rgba(0,0,0,0.4)" : "none",
            marginBottom: 24,
          }}
        >
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1fr auto auto auto",
              gap: 10,
            }}
          >
            <input
              id="search-input"
              type="text"
              placeholder="BMW до 2 млн, пробег до 50 тыс, без окраса"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleSearch()}
              style={{
                width: "100%",
                padding: "14px 16px",
                fontSize: 16,
                borderRadius: 10,
                border: "1px solid #2a2a2e",
                background: "#15151a",
                color: "#fff",
                outline: "none",
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
                padding: "14px 18px",
                fontSize: 16,
                borderRadius: 10,
                background: "#4f46e5",
                color: "#fff",
                border: "none",
                cursor: loading ? "not-allowed" : "pointer",
                opacity: loading ? 0.7 : 1,
                minWidth: 120,
              }}
            >
              {loading ? "Поиск…" : "Найти"}
            </button>

            <button
              onClick={clear}
              style={{
                padding: "14px 14px",
                fontSize: 14,
                borderRadius: 10,
                background: "transparent",
                color: "#9aa0a6",
                border: "1px solid #2a2a2e",
                cursor: "pointer",
                minWidth: 110,
              }}
            >
              Очистить
            </button>
          </div>
        </section>

        {/* ================= DISCOVERY ================= */}
        <section style={{ marginBottom: 28 }}>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
            {exampleQueries.map((q) => (
              <button
                key={q}
                onClick={() => onRepeat(q)}
                style={{
                  padding: "7px 10px",
                  borderRadius: 999,
                  border: "1px solid #2a2a2e",
                  background: "#111827",
                  color: "#cbd5e1",
                  fontSize: 13,
                  cursor: "pointer",
                }}
              >
                {q}
              </button>
            ))}
          </div>

          <RepeatSearch onRepeat={onRepeat} />
        </section>

        {/* ================= SYSTEM STATES ================= */}
        {error && (
          <div style={{ color: "#ef4444", marginBottom: 12 }}>
            ❌ {error}
          </div>
        )}

        {loading && (
          <div style={{ color: "#9aa0a6", marginBottom: 12 }}>
            🔍 Анализируем источники, форумы и маркетплейсы…
          </div>
        )}

        {/* ================= RESULTS ================= */}
        {!loading && results.length > 0 && (
          <section style={{ marginTop: 24 }}>
            <ResultsBySource results={results} />
          </section>
        )}

        {/* ================= EMPTY STATE ================= */}
        {!loading && results.length === 0 && query && (
          <section
            style={{
              marginTop: 24,
              padding: 20,
              borderRadius: 12,
              background: "#111827",
              border: "1px dashed #374151",
              color: "#9aa0a6",
            }}
          >
            ⚠️ Пока ничего не найдено
            <div style={{ fontSize: 13, marginTop: 8 }}>
              Попробуйте:
              <ul>
                <li>убрать “без окраса”</li>
                <li>увеличить пробег или бюджет</li>
                <li>поискать без бренда</li>
              </ul>
            </div>
          </section>
        )}
      </div>
    </div>
  );
}

const chipStyle: React.CSSProperties = {
  padding: "6px 12px",
  borderRadius: 999,
  background: "#1f2933",
  fontSize: 13,
};
