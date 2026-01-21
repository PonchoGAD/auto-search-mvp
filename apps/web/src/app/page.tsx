"use client";

import { useMemo, useState } from "react";

/* =======================
   TYPES
======================= */

type StructuredQuery = {
  brand?: string | null;
  model?: string | null;
  year_min?: number | null;
  year_max?: number | null;
  mileage_max?: number | null;
  price_min?: number | null;
  price_max?: number | null;
  fuel?: string | null;
  color?: string | null;
  paint_condition?: string | null;
  condition?: string | null;
  region?: string | null;
  keywords?: string[];
  exclusions?: string[];
};

type SearchResult = {
  brand?: string | null;
  model?: string | null;
  year?: number | null;
  mileage?: number | null;
  price?: number | null;
  currency?: string;
  fuel?: string | null;
  color?: string | null;
  region?: string | null;
  condition?: string | null;
  paint_condition?: string | null;
  score: number;
  why_match: string;
  source_url: string;
  source_name: string;
};

type SourceStat = {
  name: string;
  result_count: number;
};

type DebugInfo = {
  latency_ms: number;
  vector_hits: number;
  final_results: number;
  query_language: string;
  empty_result: boolean;
};

type AnswerBlock = {
  summary: string;
  highlights: string[];
  sources: { name: string; url: string }[];
};

type SearchResponse = {
  structuredQuery: StructuredQuery;
  results: SearchResult[];
  sources: SourceStat[];
  debug: DebugInfo;
  answer?: AnswerBlock;
};

/* =======================
   HELPERS
======================= */

function fmtNum(n?: number | null): string {
  if (n === null || n === undefined) return "—";
  return new Intl.NumberFormat("ru-RU").format(n);
}

/* =======================
   PAGE
======================= */

export default function Page() {
  const [query, setQuery] = useState(
    "BMW до 50 000 км, без окрасов, бензин"
  );
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<SearchResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showDebug, setShowDebug] = useState(false);

  const canSearch = useMemo(
    () => query.trim().length >= 3,
    [query]
  );

  async function runSearch(): Promise<void> {
    if (!canSearch) return;

    setLoading(true);
    setError(null);

    try {
      const resp = await fetch("/api/search", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query,
          include_answer: true,
        }),
      });

      const json: SearchResponse | { error?: string } =
        await resp.json();

      if (!resp.ok || "error" in json) {
        setError(
          "error" in json && json.error
            ? json.error
            : "Ошибка API"
        );
        setData(null);
      } else {
        setData(json);
      }
    } catch {
      setError("Network error");
      setData(null);
    } finally {
      setLoading(false);
    }
  }

  function onKeyDown(
    e: React.KeyboardEvent<HTMLInputElement>
  ): void {
    if (e.key === "Enter") runSearch();
  }

  return (
    <main
      style={{
        maxWidth: 980,
        margin: "0 auto",
        padding: 24,
        fontFamily:
          "system-ui, -apple-system, Segoe UI, Roboto",
      }}
    >
      {/* HEADER */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "baseline",
          gap: 12,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 22 }}>
          Auto Search MVP
        </h1>

        <button
          onClick={() => setShowDebug((v) => !v)}
          style={{
            border: "1px solid #ddd",
            background: "white",
            padding: "6px 10px",
            borderRadius: 8,
            cursor: "pointer",
            fontSize: 12,
          }}
        >
          Debug
        </button>
      </div>

      {/* SEARCH BAR */}
      <div style={{ marginTop: 16, display: "flex", gap: 10 }}>
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={onKeyDown}
          placeholder="Например: BMW до 50 000 км, бензин"
          style={{
            flex: 1,
            padding: "12px 14px",
            borderRadius: 10,
            border: "1px solid #ddd",
            fontSize: 14,
          }}
        />

        <button
          onClick={runSearch}
          disabled={!canSearch || loading}
          style={{
            padding: "12px 16px",
            borderRadius: 10,
            border: "1px solid #111",
            background: loading ? "#eee" : "#111",
            color: loading ? "#555" : "white",
            cursor:
              !canSearch || loading
                ? "not-allowed"
                : "pointer",
            minWidth: 120,
          }}
        >
          {loading ? "Ищу..." : "Поиск"}
        </button>
      </div>

      {/* LOADER */}
      {loading && (
        <div style={{ marginTop: 14, fontSize: 13 }}>
          Загрузка… анализирую рынок
        </div>
      )}

      {/* ERROR */}
      {error && (
        <div
          style={{
            marginTop: 14,
            padding: 12,
            background: "#fff5f5",
            border: "1px solid #ffd1d1",
            borderRadius: 10,
          }}
        >
          <strong>Ошибка</strong>
          <div style={{ fontSize: 13 }}>{error}</div>
        </div>
      )}

      {/* ANSWER */}
      {data?.answer && (
        <div
          style={{
            marginTop: 16,
            padding: 14,
            border: "1px solid #e5e5e5",
            borderRadius: 12,
          }}
        >
          <div style={{ fontWeight: 700, marginBottom: 6 }}>
            Сводка
          </div>
          <div>{data.answer.summary}</div>
        </div>
      )}

      {/* RESULTS */}
      {data?.results?.length ? (
        <div
          style={{
            marginTop: 16,
            display: "grid",
            gridTemplateColumns:
              "repeat(2, minmax(0, 1fr))",
            gap: 12,
          }}
        >
          {data.results.map((r, idx) => (
            <a
              key={idx}
              href={r.source_url}
              target="_blank"
              rel="noreferrer"
              style={{
                border: "1px solid #e5e5e5",
                borderRadius: 12,
                padding: 14,
                textDecoration: "none",
                color: "inherit",
              }}
            >
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                }}
              >
                <strong>
                  {r.brand ?? "—"} {r.model ?? ""}
                </strong>
                <span style={{ fontSize: 12 }}>
                  score: {r.score.toFixed(3)}
                </span>
              </div>

              <div style={{ marginTop: 6, fontSize: 13 }}>
                {r.year ?? "—"} г ·{" "}
                {fmtNum(r.mileage)} км ·{" "}
                {fmtNum(r.price)} {r.currency ?? "RUB"}
              </div>

              <div
                style={{
                  marginTop: 8,
                  fontSize: 12,
                  color: "#444",
                }}
              >
                {r.why_match}
              </div>

              <div
                style={{
                  marginTop: 10,
                  fontSize: 12,
                  color: "#666",
                  display: "flex",
                  justifyContent: "space-between",
                }}
              >
                <span>{r.region ?? "—"}</span>
                <span>{r.source_name}</span>
              </div>
            </a>
          ))}
        </div>
      ) : data && !loading && !error ? (
        <div
          style={{
            marginTop: 16,
            padding: 12,
            border: "1px solid #eee",
            borderRadius: 12,
          }}
        >
          Ничего не найдено — уточни запрос
        </div>
      ) : null}

      {/* DEBUG */}
      {showDebug && (
        <div
          style={{
            marginTop: 18,
            padding: 12,
            border: "1px dashed #ccc",
            borderRadius: 12,
          }}
        >
          <strong>Debug</strong>
          <pre style={{ fontSize: 12, marginTop: 8 }}>
{JSON.stringify(
  {
    structuredQuery: data?.structuredQuery ?? null,
    debug: data?.debug ?? null,
  },
  null,
  2
)}
          </pre>
        </div>
      )}
    </main>
  );
}
