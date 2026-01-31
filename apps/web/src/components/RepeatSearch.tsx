"use client";

import { useEffect, useState } from "react";

type RecentSearch = {
  raw_query: string;
  structured_query?: Record<string, unknown>;
  created_at?: string;
};

type TopQuery = {
  raw_query: string;
  count: number;
};

type RepeatSearchProps = {
  /**
   * Повтор поиска:
   * можно передать raw_query
   * или structured_query (если UI станет умнее)
   */
  onRepeatAction: (query: string, structured?: Record<string, unknown>) => void;
};

export default function RepeatSearch({ onRepeatAction }: RepeatSearchProps) {
  const [recent, setRecent] = useState<RecentSearch[]>([]);
  const [topQueries, setTopQueries] = useState<TopQuery[]>([]);
  const [loading, setLoading] = useState(true);

  const API_URL =
    process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

  // =========================
  // LOAD DATA
  // =========================
  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const [recentRes, topRes] = await Promise.all([
          fetch(`${API_URL}/analytics/recent-searches?limit=5`),
          fetch(`${API_URL}/analytics/top-queries?limit=6`),
        ]);

        if (!cancelled) {
          if (recentRes.ok) {
            const r = await recentRes.json();
            setRecent(Array.isArray(r) ? r : []);
          }

          if (topRes.ok) {
            const t = await topRes.json();
            setTopQueries(Array.isArray(t) ? t : []);
          }
        }
      } catch {
        // silent fail — UX не должен ломаться
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, [API_URL]);

  if (loading) return null;
  if (recent.length === 0 && topQueries.length === 0) return null;

  // =========================
  // UI
  // =========================
  return (
    <div
      style={{
        marginTop: 32,
        padding: 20,
        borderRadius: 14,
        border: "1px solid #e5e7eb",
        background: "linear-gradient(180deg, #fafafa, #f9fafb)",
      }}
    >
      {/* =========================
          LAST SEARCHES
         ========================= */}
      {recent.length > 0 && (
        <div style={{ marginBottom: 24 }}>
          <div
            style={{
              fontSize: 13,
              fontWeight: 600,
              marginBottom: 10,
              color: "#111827",
              textTransform: "uppercase",
              letterSpacing: "0.04em",
            }}
          >
            🔁 Последние поиски
          </div>

          <div
            style={{
              display: "flex",
              flexDirection: "column",
              gap: 8,
            }}
          >
            {recent.map((r, idx) => (
              <div
                key={idx}
                style={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  gap: 12,
                  padding: "10px 12px",
                  background: "#fff",
                  border: "1px solid #e5e7eb",
                  borderRadius: 8,
                }}
              >
                <div
                  style={{
                    fontSize: 14,
                    color: "#111827",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                    flex: 1,
                  }}
                >
                  {r.raw_query}
                </div>

                <button
                  onClick={() =>
                    onRepeatAction(r.raw_query, r.structured_query)
                  }
                  style={{
                    padding: "6px 12px",
                    fontSize: 13,
                    borderRadius: 6,
                    border: "1px solid #2563eb",
                    background: "#2563eb",
                    color: "#fff",
                    cursor: "pointer",
                    whiteSpace: "nowrap",
                  }}
                >
                  Повторить
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* =========================
          TOP QUERIES
         ========================= */}
      {topQueries.length > 0 && (
        <div>
          <div
            style={{
              fontSize: 13,
              fontWeight: 600,
              marginBottom: 10,
              color: "#111827",
              textTransform: "uppercase",
              letterSpacing: "0.04em",
            }}
          >
            🔥 Популярное
          </div>

          <div
            style={{
              display: "flex",
              gap: 8,
              flexWrap: "wrap",
            }}
          >
            {topQueries.map((q, idx) => (
              <button
                key={idx}
                onClick={() => onRepeatAction(q.raw_query)}
                title={`Искали ${q.count} раз`}
                style={{
                  padding: "6px 12px",
                  fontSize: 13,
                  borderRadius: 999,
                  border: "1px solid #d1d5db",
                  background: "#fff",
                  cursor: "pointer",
                  color: "#111827",
                }}
              >
                {q.raw_query}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
