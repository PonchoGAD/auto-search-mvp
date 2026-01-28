import React from "react";

/**
 * Тип результата — совпадает с тем, что приходит из backend /search
 */
export type SearchResult = {
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

/**
 * Определяем тип источника
 * Это нужно для бейджей, цветов и будущей аналитики
 */
function getSourceType(source?: string) {
  const s = (source || "").toLowerCase();

  if (s.includes("forum") || s.includes("club")) return "forum";
  if (s.includes("telegram")) return "telegram";
  if (s.includes("avito") || s.includes("auto") || s.includes("drom"))
    return "marketplace";

  return "other";
}

function SourceBadge({ source }: { source?: string }) {
  const type = getSourceType(source);

  const styles: Record<
    string,
    { label: string; bg: string; color: string }
  > = {
    forum: {
      label: "Forum",
      bg: "#312e81",
      color: "#c7d2fe",
    },
    marketplace: {
      label: "Marketplace",
      bg: "#064e3b",
      color: "#6ee7b7",
    },
    telegram: {
      label: "Telegram",
      bg: "#0c4a6e",
      color: "#7dd3fc",
    },
    other: {
      label: "Source",
      bg: "#374151",
      color: "#e5e7eb",
    },
  };

  const cfg = styles[type];

  return (
    <span
      style={{
        padding: "4px 10px",
        borderRadius: 999,
        fontSize: 12,
        fontWeight: 500,
        background: cfg.bg,
        color: cfg.color,
      }}
    >
      {cfg.label}
    </span>
  );
}

function MatchBadge({ label }: { label: string }) {
  return (
    <span
      style={{
        padding: "4px 8px",
        borderRadius: 6,
        fontSize: 11,
        background: "#1f2937",
        color: "#93c5fd",
        border: "1px solid #374151",
      }}
    >
      {label}
    </span>
  );
}

type Props = {
  result: SearchResult;
};

export default function ResultCard({ result }: Props) {
  const {
    brand,
    model,
    year,
    mileage,
    price,
    currency,
    fuel,
    paint_condition,
    score,
    why_match,
    source_url,
    source_name,
  } = result;

  return (
    <div
      style={{
        background: "#111827",
        border: "1px solid #1f2937",
        borderRadius: 12,
        padding: 16,
        marginBottom: 12,
      }}
    >
      {/* HEADER */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
          gap: 12,
        }}
      >
        <div>
          <div style={{ fontSize: 18, fontWeight: 600 }}>
            {brand ?? "—"} {model ?? ""} {year ?? ""}
          </div>

          <div style={{ marginTop: 4, color: "#9ca3af", fontSize: 14 }}>
            💰 {price ?? "—"} {currency ?? ""} · 🛣 {mileage ?? "—"} км · ⛽{" "}
            {fuel ?? "—"}
          </div>

          <div style={{ marginTop: 4, fontSize: 13, color: "#9ca3af" }}>
            🎨 Окрас: {paint_condition ?? "—"}
          </div>
        </div>

        <SourceBadge source={source_name} />
      </div>

      {/* MATCH BADGES */}
      <div
        style={{
          display: "flex",
          gap: 8,
          marginTop: 10,
          flexWrap: "wrap",
        }}
      >
        {why_match.includes("brand") && <MatchBadge label="Brand match" />}
        {why_match.includes("price") && <MatchBadge label="Price OK" />}
        {why_match.includes("mileage") && <MatchBadge label="Mileage OK" />}
        {why_match.includes("sale") && <MatchBadge label="Sale intent" />}
      </div>

      {/* WHY */}
      <div
        style={{
          marginTop: 8,
          fontSize: 12,
          color: "#9ca3af",
        }}
      >
        🧠 {why_match}
      </div>

      {/* FOOTER */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginTop: 12,
        }}
      >
        <a
          href={source_url}
          target="_blank"
          rel="noreferrer"
          style={{
            fontSize: 13,
            color: "#60a5fa",
            textDecoration: "none",
          }}
        >
          🔗 Открыть источник
        </a>

        <div style={{ fontSize: 12, color: "#6b7280" }}>
          score: {score.toFixed(3)}
        </div>
      </div>
    </div>
  );
}
