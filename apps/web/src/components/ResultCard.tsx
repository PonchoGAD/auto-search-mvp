import React from "react";

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

function getSourceType(source?: string) {
  const s = (source || "").toLowerCase();
  if (s.includes("forum") || s.includes("club")) return "forum";
  if (s.includes("telegram")) return "telegram";
  if (s.includes("avito") || s.includes("auto") || s.includes("drom"))
    return "marketplace";
  return "other";
}

const SOURCE_CFG: Record<string, { label: string; color: string }> = {
  forum:       { label: "Форум",       color: "#9C8A5A" },
  marketplace: { label: "Маркетплейс", color: "#7A9C6A" },
  telegram:    { label: "Telegram",    color: "#6A8A9C" },
  other:       { label: "Источник",    color: "#7A7A7A" },
};

function SourceBadge({ source }: { source?: string }) {
  const type = getSourceType(source);
  const cfg = SOURCE_CFG[type];

  return (
    <span
      style={{
        padding: "3px 10px",
        borderRadius: 2,
        fontSize: 10,
        fontWeight: 600,
        letterSpacing: "0.08em",
        textTransform: "uppercase",
        border: `1px solid ${cfg.color}44`,
        color: cfg.color,
        background: `${cfg.color}12`,
        flexShrink: 0,
      }}
    >
      {cfg.label}
    </span>
  );
}

function MatchTag({ label }: { label: string }) {
  return (
    <span
      style={{
        padding: "3px 8px",
        borderRadius: 2,
        fontSize: 10,
        fontWeight: 500,
        letterSpacing: "0.06em",
        textTransform: "uppercase",
        border: "1px solid rgba(201, 168, 76, 0.2)",
        color: "rgba(201, 168, 76, 0.65)",
        background: "rgba(201, 168, 76, 0.06)",
      }}
    >
      {label}
    </span>
  );
}

function ScoreBar({ score }: { score: number }) {
  const pct = Math.round(Math.min(1, Math.max(0, score)) * 100);
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        flexShrink: 0,
      }}
    >
      <div
        style={{
          width: 48,
          height: 3,
          borderRadius: 2,
          background: "rgba(201, 168, 76, 0.12)",
          overflow: "hidden",
        }}
      >
        <div
          style={{
            height: "100%",
            width: `${pct}%`,
            background: "linear-gradient(90deg, #9E7A2A, #C9A84C)",
            borderRadius: 2,
          }}
        />
      </div>
      <span
        style={{
          fontSize: 10,
          color: "rgba(201, 168, 76, 0.5)",
          fontWeight: 600,
          letterSpacing: "0.04em",
          width: 28,
        }}
      >
        {pct}%
      </span>
    </div>
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
    why_match,
    source_url,
    source_name,
    score,
  } = result;

  const title = [brand, model, year].filter(Boolean).join(" ") || "Объявление";

  const specs: string[] = [];
  if (price != null) specs.push(`${price.toLocaleString("ru-RU")} ${currency ?? "₽"}`);
  if (mileage != null) specs.push(`${mileage.toLocaleString("ru-RU")} км`);
  if (fuel) specs.push(fuel);

  return (
    <div
      style={{
        background: "#141414",
        border: "1px solid rgba(201, 168, 76, 0.14)",
        borderRadius: 6,
        padding: "18px 20px",
        transition: "border-color 0.2s ease",
        cursor: "default",
      }}
      onMouseEnter={(e) =>
        ((e.currentTarget as HTMLDivElement).style.borderColor =
          "rgba(201, 168, 76, 0.35)")
      }
      onMouseLeave={(e) =>
        ((e.currentTarget as HTMLDivElement).style.borderColor =
          "rgba(201, 168, 76, 0.14)")
      }
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
        <div style={{ flex: 1, minWidth: 0 }}>
          <div
            style={{
              fontSize: 17,
              fontWeight: 600,
              color: "#F5F0E8",
              letterSpacing: "0.01em",
              marginBottom: 6,
            }}
          >
            {title}
          </div>

          {specs.length > 0 && (
            <div
              style={{
                fontSize: 13,
                color: "rgba(245, 240, 232, 0.55)",
                letterSpacing: "0.02em",
              }}
            >
              {specs.join(" · ")}
            </div>
          )}

          {paint_condition && (
            <div
              style={{
                marginTop: 4,
                fontSize: 12,
                color: "rgba(245, 240, 232, 0.38)",
                letterSpacing: "0.02em",
              }}
            >
              Состояние: {paint_condition}
            </div>
          )}
        </div>

        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "flex-end",
            gap: 8,
            flexShrink: 0,
          }}
        >
          <SourceBadge source={source_name} />
          <ScoreBar score={score} />
        </div>
      </div>

      {/* MATCH TAGS */}
      {(why_match.includes("brand") ||
        why_match.includes("price") ||
        why_match.includes("mileage") ||
        why_match.includes("sale")) && (
        <div
          style={{
            display: "flex",
            gap: 6,
            marginTop: 12,
            flexWrap: "wrap",
          }}
        >
          {why_match.includes("brand") && <MatchTag label="Марка" />}
          {why_match.includes("price") && <MatchTag label="Цена" />}
          {why_match.includes("mileage") && <MatchTag label="Пробег" />}
          {why_match.includes("sale") && <MatchTag label="Продажа" />}
        </div>
      )}

      {/* WHY MATCH */}
      {why_match && (
        <div
          style={{
            marginTop: 10,
            fontSize: 11,
            color: "rgba(245, 240, 232, 0.3)",
            fontStyle: "italic",
            letterSpacing: "0.02em",
            lineHeight: 1.5,
          }}
        >
          {why_match}
        </div>
      )}

      {/* FOOTER */}
      <div style={{ marginTop: 14 }}>
        <a
          href={source_url}
          target="_blank"
          rel="noreferrer"
          style={{
            fontSize: 11,
            fontWeight: 600,
            letterSpacing: "0.08em",
            textTransform: "uppercase",
            color: "rgba(201, 168, 76, 0.6)",
            textDecoration: "none",
            transition: "color 0.2s ease",
          }}
          onMouseEnter={(e) =>
            ((e.currentTarget as HTMLAnchorElement).style.color = "#C9A84C")
          }
          onMouseLeave={(e) =>
            ((e.currentTarget as HTMLAnchorElement).style.color =
              "rgba(201, 168, 76, 0.6)")
          }
        >
          Открыть объявление &rarr;
        </a>
      </div>
    </div>
  );
}
