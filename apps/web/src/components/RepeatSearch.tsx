"use client";

import { useEffect, useState } from "react";

type RecentSearch = {
  id: number;
  raw_query: string;
  created_at: string;
};

type TopQuery = {
  query: string;
  count: number;
};

type RepeatSearchProps = {
  onRepeat: (query: string) => void;
};

export default function RepeatSearch({ onRepeat }: RepeatSearchProps) {
  const [recent, setRecent] = useState<RecentSearch[]>([]);
  const [topQueries, setTopQueries] = useState<TopQuery[]>([]);
  const [loading, setLoading] = useState(true);

  const API_URL =
    process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const [recentRes, topRes] = await Promise.all([
          fetch(`${API_URL}/analytics/recent-searches?limit=1`),
          fetch(`${API_URL}/analytics/top-queries?limit=5`),
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
      } catch (e) {
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

  if (loading) {
    return null;
  }

  if (recent.length === 0 && topQueries.length === 0) {
    return null;
  }

  return (
    <div
      style={{
        marginTop: 32,
        padding: 16,
        borderRadius: 12,
        border: "1px solid #e5e7eb",
        background: "#fafafa",
      }}
    >
      {/* LAST SEARCH */}
      {recent.length > 0 && (
        <div style={{ marginBottom: 20 }}>
          <div
            style={{
              fontSize: 14,
              fontWeight: 600,
              marginBottom: 8,
              color: "#111827",
            }}
          >
            🔁 Последний поиск
          </div>

          <div
            style={{
              display: "flex",
              gap: 8,
              alignItems: "center",
              flexWrap: "wrap",
            }}
          >
            <div
              style={{
                padding: "6px 10px",
                background: "#fff",
                border: "1px solid #d1d5db",
                borderRadius: 6,
                fontSize: 14,
              }}
            >
              {recent[0].raw_query}
            </div>

            <button
              onClick={() => onRepeat(recent[0].raw_query)}
              style={{
                padding: "6px 12px",
                fontSize: 14,
                borderRadius: 6,
                border: "1px solid #2563eb",
                background: "#2563eb",
                color: "#fff",
                cursor: "pointer",
              }}
            >
              Повторить
            </button>
          </div>
        </div>
      )}

      {/* TOP QUERIES */}
      {topQueries.length > 0 && (
        <div>
          <div
            style={{
              fontSize: 14,
              fontWeight: 600,
              marginBottom: 8,
              color: "#111827",
            }}
          >
            🔥 Популярные запросы
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
                onClick={() => onRepeat(q.query)}
                title={`Искали ${q.count} раз`}
                style={{
                  padding: "6px 10px",
                  fontSize: 13,
                  borderRadius: 999,
                  border: "1px solid #d1d5db",
                  background: "#fff",
                  cursor: "pointer",
                }}
              >
                {q.query}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
