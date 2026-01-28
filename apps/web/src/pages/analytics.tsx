import { useEffect, useMemo, useState } from "react";

type TopItem = {
  query?: string;
  brand?: string;
  count: number;
};

type SourceNoise = {
  source: string;
  total_queries: number;
  empty_results: number;
  noise_ratio: number;
};

type DataSignals = {
  no_results_rate: number;
  brand_gap: {
    brand: string;
    searches: number;
    documents: number;
  }[];
  noisy_source: {
    source: string;
    raw: number;
    normalized: number;
    ratio: number;
  }[];
};

export default function AnalyticsPage() {
  const API_URL = useMemo(
    () => process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000",
    []
  );

  const [topQueries, setTopQueries] = useState<TopItem[]>([]);
  const [emptyQueries, setEmptyQueries] = useState<TopItem[]>([]);
  const [topBrands, setTopBrands] = useState<TopItem[]>([]);
  const [sourceNoise, setSourceNoise] = useState<SourceNoise[]>([]);
  const [signals, setSignals] = useState<DataSignals | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const load = async () => {
      try {
        const [
          tq,
          eq,
          tb,
          sn,
          ds,
        ] = await Promise.all([
          fetch(`${API_URL}/analytics/top-queries`).then((r) => r.json()),
          fetch(`${API_URL}/analytics/empty-queries`).then((r) => r.json()),
          fetch(`${API_URL}/analytics/top-brands`).then((r) => r.json()),
          fetch(`${API_URL}/analytics/source-noise`).then((r) => r.json()),
          fetch(`${API_URL}/analytics/data-signals`).then((r) => r.json()),
        ]);

        setTopQueries(tq || []);
        setEmptyQueries(eq || []);
        setTopBrands(tb || []);
        setSourceNoise(sn || []);
        setSignals(ds || null);
      } catch (e) {
        console.error("Failed to load analytics", e);
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [API_URL]);

  if (loading) {
    return (
      <div style={pageStyle}>
        <div style={{ color: "#9aa0a6" }}>📊 Загружаем аналитику…</div>
      </div>
    );
  }

  return (
    <div style={pageStyle}>
      <div style={{ maxWidth: 1200, margin: "0 auto" }}>
        {/* HEADER */}
        <div style={{ marginBottom: 32 }}>
          <h1 style={{ fontSize: 32, marginBottom: 8 }}>
            📊 Product Analytics
          </h1>
          <p style={{ color: "#9aa0a6", margin: 0 }}>
            Реальные поисковые данные. Сигналы роста. Качество источников.
          </p>
        </div>

        {/* TOP BLOCKS */}
        <Grid>
          <Block title="🔥 Топ запросов">
            {topQueries.map((q, i) => (
              <Row key={i}>
                <span>{q.query}</span>
                <strong>{q.count}</strong>
              </Row>
            ))}
          </Block>

          <Block title="❌ Пустые запросы">
            {emptyQueries.map((q, i) => (
              <Row key={i} danger>
                <span>{q.query}</span>
                <strong>{q.count}</strong>
              </Row>
            ))}
          </Block>

          <Block title="🚗 Топ брендов">
            {topBrands.map((b, i) => (
              <Row key={i}>
                <span>{b.brand}</span>
                <strong>{b.count}</strong>
              </Row>
            ))}
          </Block>
        </Grid>

        {/* SOURCE QUALITY */}
        <Section title="🧪 Качество источников">
          {sourceNoise.map((s, i) => (
            <Row key={i}>
              <span>{s.source}</span>
              <span>
                {s.empty_results} / {s.total_queries}
              </span>
              <strong
                style={{
                  color:
                    s.noise_ratio > 0.4 ? "#ef4444" : "#22c55e",
                }}
              >
                {s.noise_ratio}
              </strong>
            </Row>
          ))}
        </Section>

        {/* DATA SIGNALS */}
        {signals && (
          <>
            <Section title="🚨 Сигналы роста">
              <Signal>
                <strong>No results rate:</strong>{" "}
                {(signals.no_results_rate * 100).toFixed(1)}%
              </Signal>
            </Section>

            <Grid>
              <Block title="⚠️ Brand gap">
                {signals.brand_gap.map((b, i) => (
                  <Row key={i} danger>
                    <span>{b.brand}</span>
                    <span>
                      запросов: {b.searches}
                    </span>
                    <strong>
                      документов: {b.documents}
                    </strong>
                  </Row>
                ))}
              </Block>

              <Block title="🗑 Noisy sources">
                {signals.noisy_source.map((s, i) => (
                  <Row key={i} danger>
                    <span>{s.source}</span>
                    <span>
                      raw: {s.raw} / norm: {s.normalized}
                    </span>
                    <strong>{s.ratio}</strong>
                  </Row>
                ))}
              </Block>
            </Grid>
          </>
        )}
      </div>
    </div>
  );
}

/* ---------------- UI HELPERS ---------------- */

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div style={{ marginBottom: 32 }}>
      <h3 style={{ marginBottom: 12 }}>{title}</h3>
      <div style={blockStyle}>{children}</div>
    </div>
  );
}

function Block({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div style={blockStyle}>
      <div
        style={{
          fontWeight: 600,
          marginBottom: 12,
        }}
      >
        {title}
      </div>
      {children}
    </div>
  );
}

function Row({
  children,
  danger,
}: {
  children: React.ReactNode;
  danger?: boolean;
}) {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        gap: 12,
        padding: "6px 0",
        color: danger ? "#fca5a5" : "#e5e7eb",
        fontSize: 14,
      }}
    >
      {children}
    </div>
  );
}

function Grid({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))",
        gap: 20,
        marginBottom: 32,
      }}
    >
      {children}
    </div>
  );
}

function Signal({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        padding: 12,
        borderRadius: 8,
        background: "#111827",
        border: "1px solid #374151",
        fontSize: 14,
        color: "#f1f5f9",
      }}
    >
      {children}
    </div>
  );
}

/* ---------------- STYLES ---------------- */

const pageStyle: React.CSSProperties = {
  minHeight: "100vh",
  background: "#0e0e11",
  color: "#f1f1f1",
  padding: "40px 16px",
  fontFamily: "Inter, system-ui, sans-serif",
};

const blockStyle: React.CSSProperties = {
  background: "#15151a",
  border: "1px solid #2a2a2e",
  borderRadius: 12,
  padding: 16,
};
