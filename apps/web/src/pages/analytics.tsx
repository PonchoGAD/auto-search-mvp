"use client";

import { useEffect, useState } from "react";

/* =====================================================
 * Types
 * ===================================================== */

type TopQuery = {
  query: string;
  count: number;
};

type EmptyQuery = {
  query: string;
  count: number;
};

type TopBrand = {
  brand: string;
  count: number;
};

type NoisySource = {
  source: string;
  raw_documents: number;
  normalized_documents: number;
  quality_ratio: number;
  signal?: string;
};

type DataSignals = {
  no_results_rate?: {
    total_searches: number;
    empty_searches: number;
    no_results_rate: number;
  };
  brand_gap?: {
    brand: string;
    search_count: number;
    documents: number;
    signal: string;
  }[];
  noisy_source?: NoisySource[];
};

/* =====================================================
 * Page
 * ===================================================== */

export default function AnalyticsPage() {
  const API_URL =
    process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [topQueries, setTopQueries] = useState<TopQuery[]>([]);
  const [emptyQueries, setEmptyQueries] = useState<EmptyQuery[]>([]);
  const [topBrands, setTopBrands] = useState<TopBrand[]>([]);
  const [noisySources, setNoisySources] = useState<NoisySource[]>([]);
  const [signals, setSignals] = useState<DataSignals | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      setError(null);

      try {
        const [
          topQ,
          emptyQ,
          brands,
          sources,
          signalsRes,
        ] = await Promise.all([
          fetch(`${API_URL}/analytics/top-queries?limit=10`),
          fetch(`${API_URL}/analytics/empty-queries?limit=10`),
          fetch(`${API_URL}/analytics/top-brands?limit=10`),
          fetch(`${API_URL}/analytics/source-noise`),
          fetch(`${API_URL}/analytics/data-signals`),
        ]);

        if (cancelled) return;

        if (
          !topQ.ok ||
          !emptyQ.ok ||
          !brands.ok ||
          !sources.ok ||
          !signalsRes.ok
        ) {
          throw new Error("Failed to load analytics");
        }

        setTopQueries(await topQ.json());
        setEmptyQueries(await emptyQ.json());
        setTopBrands(await brands.json());
        setNoisySources(await sources.json());
        setSignals(await signalsRes.json());
      } catch {
        if (!cancelled) {
          setError("Не удалось загрузить аналитику. Проверьте API или соединение.");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, [API_URL]);

  /* =========================
   * STATES
   * ========================= */

  if (loading) {
    return (
      <div style={{ padding: 40, color: "#6b7280" }}>
        ⏳ Загрузка аналитики…
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: 40, color: "#b91c1c" }}>
        {error}
      </div>
    );
  }

  return (
    <div style={{ padding: 40, maxWidth: 1200 }}>
      <h1 style={{ fontSize: 28, fontWeight: 700, marginBottom: 8 }}>
        📊 Analytics Overview
      </h1>
      <p style={{ color: "#6b7280", marginBottom: 28 }}>
        Аналитика пользовательского спроса, качества источников и точек роста продукта
      </p>

      {/* ==============================
          CARDS GRID
         ============================== */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))",
          gap: 20,
          marginBottom: 32,
        }}
      >
        <Card title="🔥 Top Queries" hint="Что пользователи ищут чаще всего">
          <List
            items={topQueries}
            emptyHint="Запросов пока нет — система только начинает собирать данные."
            render={(q) => (
              <span>
                {q.query} <Muted>({q.count})</Muted>
              </span>
            )}
          />
        </Card>

        <Card title="❌ Empty Queries" hint="Спрос без покрытия">
          <List
            items={emptyQueries}
            emptyHint="Пустых запросов нет — это хороший знак."
            render={(q) => (
              <span>
                {q.query} <Muted>({q.count})</Muted>
              </span>
            )}
          />
        </Card>

        <Card title="🧠 Top Brands" hint="Самые популярные бренды">
          <List
            items={topBrands}
            emptyHint="Бренды ещё не выделены — нужно больше данных."
            render={(b) => (
              <span>
                {b.brand} <Muted>({b.count})</Muted>
              </span>
            )}
          />
        </Card>

        <Card title="🗑 Noisy Sources" hint="Источники с низким качеством данных">
          <List
            items={noisySources}
            emptyHint="Все источники выглядят качественными."
            render={(s) => (
              <span>
                {s.source}{" "}
                <Muted>
                  ({Math.round(s.quality_ratio * 100)}% качества)
                </Muted>
              </span>
            )}
          />
        </Card>
      </div>

      {/* ==============================
          GROWTH INSIGHTS
         ============================== */}
      <Card title="🚀 Growth Insights" accent>
        <Insights signals={signals} />
        <NextSteps />
      </Card>
    </div>
  );
}

/* =====================================================
 * UI Blocks
 * ===================================================== */

function Card({
  title,
  children,
  hint,
  accent,
}: {
  title: string;
  children: React.ReactNode;
  hint?: string;
  accent?: boolean;
}) {
  return (
    <div
      style={{
        padding: 20,
        borderRadius: 12,
        border: "1px solid #e5e7eb",
        background: accent ? "#f8fafc" : "#ffffff",
      }}
    >
      <div style={{ marginBottom: 12 }}>
        <div
          style={{
            fontSize: 15,
            fontWeight: 600,
            color: "#111827",
          }}
        >
          {title}
        </div>
        {hint && (
          <div style={{ fontSize: 12, color: "#6b7280" }}>
            {hint}
          </div>
        )}
      </div>
      {children}
    </div>
  );
}

function List<T>({
  items,
  render,
  emptyHint,
}: {
  items: T[];
  render: (item: T) => React.ReactNode;
  emptyHint?: string;
}) {
  if (!items || items.length === 0) {
    return <Muted>{emptyHint || "Нет данных"}</Muted>;
  }

  return (
    <ul style={{ listStyle: "none", padding: 0, margin: 0 }}>
      {items.map((item, idx) => (
        <li key={idx} style={{ marginBottom: 6 }}>
          {render(item)}
        </li>
      ))}
    </ul>
  );
}

function Muted({ children }: { children: React.ReactNode }) {
  return <span style={{ color: "#6b7280" }}>{children}</span>;
}

/* =====================================================
 * Growth Insights Generator
 * ===================================================== */

function Insights({ signals }: { signals: DataSignals | null }) {
  if (!signals) {
    return <Muted>Сигналы пока не сформированы.</Muted>;
  }

  const insights: string[] = [];

  if (signals.no_results_rate) {
    const rate = signals.no_results_rate.no_results_rate;
    if (rate > 0.3) {
      insights.push(
        `❗ ${Math.round(
          rate * 100
        )}% запросов не находят результатов — спрос превышает текущее покрытие данных.`
      );
    }
  }

  if (signals.brand_gap && signals.brand_gap.length > 0) {
    const b = signals.brand_gap[0];
    insights.push(
      `🧠 Высокий интерес к бренду "${b.brand}", но объявлений нет — приоритет для подключения источников.`
    );
  }

  if (signals.noisy_source && signals.noisy_source.length > 0) {
    const s = signals.noisy_source[0];
    insights.push(
      `🗑 Источник "${s.source}" даёт много шума — стоит усилить фильтрацию или снизить вес.`
    );
  }

  if (insights.length === 0) {
    insights.push(
      "✅ Система выглядит сбалансированной: спрос покрывается данными, критических проблем не выявлено."
    );
  }

  return (
    <ul style={{ paddingLeft: 16, marginBottom: 16 }}>
      {insights.map((i, idx) => (
        <li key={idx} style={{ marginBottom: 8 }}>
          {i}
        </li>
      ))}
    </ul>
  );
}

/* =====================================================
 * CTA — What to do next
 * ===================================================== */

function NextSteps() {
  return (
    <div
      style={{
        marginTop: 16,
        padding: 14,
        borderRadius: 10,
        background: "#eef2ff",
        border: "1px solid #c7d2fe",
        fontSize: 14,
      }}
    >
      <strong>Рекомендуемые действия:</strong>
      <ul style={{ marginTop: 8, paddingLeft: 18 }}>
        <li>Добавить источники под самые частые и пустые запросы</li>
        <li>Оптимизировать или отключить шумные источники</li>
        <li>Использовать аналитику для планирования масштабирования</li>
      </ul>
    </div>
  );
}
