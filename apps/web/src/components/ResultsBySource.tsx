import React, { useMemo, useState } from "react";
import ResultCard, { SearchResult } from "./ResultCard";

function normalizeSourceName(source?: string): string {
  if (!source) return "Other";
  const s = source.toLowerCase();
  if (s.includes("benzclub")) return "BenzClub Forum";
  if (s.includes("bmwclub")) return "BMW Club Forum";
  if (s.includes("forum")) return "Forum";
  if (s.includes("telegram")) return "Telegram";
  if (s.includes("avito")) return "Avito";
  if (s.includes("auto")) return "Auto.ru";
  if (s.includes("drom")) return "Drom.ru";
  return source;
}

function detectSourceType(name: string): "forum" | "marketplace" | "other" {
  const s = name.toLowerCase();
  if (s.includes("forum") || s.includes("club")) return "forum";
  if (s.includes("avito") || s.includes("auto") || s.includes("drom"))
    return "marketplace";
  return "other";
}

const SOURCE_TYPE_LABEL: Record<string, string> = {
  forum: "Форум",
  marketplace: "Маркетплейс",
  other: "Источник",
};

type SourceGroup = { source: string; items: SearchResult[] };
type Props = { results: SearchResult[] };

const INITIAL_VISIBLE = 3;
const STEP_VISIBLE = 5;
const DOMINANCE_THRESHOLD = 10;

export default function ResultsBySource({ results }: Props) {
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({});
  const [visibleCount, setVisibleCount] = useState<Record<string, number>>({});

  const grouped: SourceGroup[] = useMemo(() => {
    const map: Record<string, SearchResult[]> = {};
    for (const r of results) {
      const key = normalizeSourceName(r.source_name);
      if (!map[key]) map[key] = [];
      map[key].push(r);
    }
    return Object.entries(map).map(([source, items]) => ({ source, items }));
  }, [results]);

  if (!results.length) return null;

  const toggleCollapse = (source: string) => {
    setCollapsed((prev) => ({ ...prev, [source]: !prev[source] }));
  };

  const showMore = (source: string) => {
    setVisibleCount((prev) => ({
      ...prev,
      [source]: (prev[source] ?? INITIAL_VISIBLE) + STEP_VISIBLE,
    }));
  };

  return (
    <div>
      {grouped.map((group) => {
        const type = detectSourceType(group.source);
        const isDominant = group.items.length >= DOMINANCE_THRESHOLD;
        const isCollapsed =
          collapsed[group.source] ?? (type === "marketplace" || isDominant);
        const visible = visibleCount[group.source] ?? INITIAL_VISIBLE;
        const canShowMore = visible < group.items.length;

        return (
          <div
            key={group.source}
            style={{ marginBottom: 40 }}
          >
            {/* SOURCE HEADER */}
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                marginBottom: 16,
                paddingBottom: 14,
                borderBottom: "1px solid rgba(201, 168, 76, 0.12)",
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                <h3
                  style={{
                    fontFamily: "var(--font-cormorant), 'Cormorant Garamond', Georgia, serif",
                    fontSize: 22,
                    fontWeight: 500,
                    fontStyle: "italic",
                    color: "#F5F0E8",
                    margin: 0,
                    letterSpacing: "0.02em",
                  }}
                >
                  {group.source}
                </h3>

                <span
                  style={{
                    padding: "3px 9px",
                    borderRadius: 2,
                    fontSize: 10,
                    fontWeight: 600,
                    letterSpacing: "0.08em",
                    textTransform: "uppercase",
                    border: "1px solid rgba(201, 168, 76, 0.2)",
                    color: "rgba(201, 168, 76, 0.55)",
                    background: "rgba(201, 168, 76, 0.06)",
                  }}
                >
                  {SOURCE_TYPE_LABEL[type]}
                </span>

                {isDominant && (
                  <span
                    style={{
                      padding: "3px 9px",
                      borderRadius: 2,
                      fontSize: 10,
                      fontWeight: 600,
                      letterSpacing: "0.08em",
                      textTransform: "uppercase",
                      border: "1px solid rgba(180, 80, 80, 0.3)",
                      color: "rgba(220, 120, 120, 0.7)",
                      background: "rgba(180, 60, 60, 0.08)",
                    }}
                    title="Источник доминирует в выдаче"
                  >
                    Доминирует
                  </span>
                )}
              </div>

              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 16,
                }}
              >
                <span
                  style={{
                    fontSize: 11,
                    color: "rgba(245, 240, 232, 0.35)",
                    letterSpacing: "0.06em",
                    textTransform: "uppercase",
                  }}
                >
                  {group.items.length} объявлений
                </span>

                <button
                  onClick={() => toggleCollapse(group.source)}
                  style={{
                    fontSize: 11,
                    fontWeight: 600,
                    letterSpacing: "0.08em",
                    textTransform: "uppercase",
                    background: "transparent",
                    border: "none",
                    color: isCollapsed
                      ? "rgba(201, 168, 76, 0.65)"
                      : "rgba(245, 240, 232, 0.4)",
                    cursor: "pointer",
                    padding: 0,
                    transition: "color 0.2s ease",
                  }}
                >
                  {isCollapsed ? "Показать" : "Свернуть"}
                </button>
              </div>
            </div>

            {/* RESULTS */}
            {!isCollapsed && (
              <>
                <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                  {group.items.slice(0, visible).map((item, idx) => (
                    <ResultCard key={idx} result={item} />
                  ))}
                </div>

                {canShowMore && (
                  <div style={{ marginTop: 14 }}>
                    <button
                      onClick={() => showMore(group.source)}
                      style={{
                        padding: "10px 20px",
                        borderRadius: 4,
                        border: "1px solid rgba(201, 168, 76, 0.2)",
                        background: "transparent",
                        color: "rgba(201, 168, 76, 0.6)",
                        fontSize: 11,
                        fontWeight: 600,
                        letterSpacing: "0.08em",
                        textTransform: "uppercase",
                        cursor: "pointer",
                        transition: "border-color 0.2s ease, color 0.2s ease",
                      }}
                      onMouseEnter={(e) => {
                        (e.currentTarget as HTMLButtonElement).style.borderColor =
                          "rgba(201, 168, 76, 0.45)";
                        (e.currentTarget as HTMLButtonElement).style.color =
                          "#C9A84C";
                      }}
                      onMouseLeave={(e) => {
                        (e.currentTarget as HTMLButtonElement).style.borderColor =
                          "rgba(201, 168, 76, 0.2)";
                        (e.currentTarget as HTMLButtonElement).style.color =
                          "rgba(201, 168, 76, 0.6)";
                      }}
                    >
                      Показать ещё {group.items.length - visible}
                    </button>
                  </div>
                )}
              </>
            )}
          </div>
        );
      })}
    </div>
  );
}
