import React, { useMemo, useState } from "react";
import ResultCard, { SearchResult } from "./ResultCard";

/**
 * Человекочитаемое имя источника
 * (можно расширять без изменения логики)
 */
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

/**
 * Определяем тип источника
 * (нужно для дефолтного поведения)
 */
function detectSourceType(
  sourceName: string
): "forum" | "marketplace" | "other" {
  const s = sourceName.toLowerCase();

  if (s.includes("forum")) return "forum";
  if (s.includes("avito") || s.includes("auto") || s.includes("drom"))
    return "marketplace";

  return "other";
}

type SourceGroup = {
  source: string;
  items: SearchResult[];
};

type Props = {
  results: SearchResult[];
};

const INITIAL_VISIBLE = 3;
const STEP_VISIBLE = 5;

/**
 * 🔒 UX-ограничение доминирования источника
 * Если источник даёт слишком много результатов —
 * сворачиваем и помечаем визуально
 */
const DOMINANCE_THRESHOLD = 10;

export default function ResultsBySource({ results }: Props) {
  /**
   * collapse state по источникам
   */
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({});

  /**
   * сколько элементов показано по источнику
   */
  const [visibleCount, setVisibleCount] = useState<Record<string, number>>({});

  /**
   * Группировка по source_name
   */
  const grouped: SourceGroup[] = useMemo(() => {
    const map: Record<string, SearchResult[]> = {};

    for (const r of results) {
      const key = normalizeSourceName(r.source_name);

      if (!map[key]) {
        map[key] = [];
      }

      map[key].push(r);
    }

    return Object.entries(map).map(([source, items]) => ({
      source,
      items,
    }));
  }, [results]);

  if (!results.length) {
    return null;
  }

  const toggleCollapse = (source: string) => {
    setCollapsed((prev) => ({
      ...prev,
      [source]: !prev[source],
    }));
  };

  const showMore = (source: string) => {
    setVisibleCount((prev) => ({
      ...prev,
      [source]: (prev[source] ?? INITIAL_VISIBLE) + STEP_VISIBLE,
    }));
  };

  return (
    <div style={{ marginTop: 24 }}>
      {grouped.map((group) => {
        const type = detectSourceType(group.source);
        const isDominant = group.items.length >= DOMINANCE_THRESHOLD;

        /**
         * дефолтный collapse:
         * - marketplace → collapsed
         * - доминирующий источник → collapsed
         */
        const isCollapsed =
          collapsed[group.source] ??
          (type === "marketplace" || isDominant);

        const visible =
          visibleCount[group.source] ?? INITIAL_VISIBLE;

        const canShowMore = visible < group.items.length;

        return (
          <div
            key={group.source}
            style={{
              marginBottom: 36,
              opacity: isDominant && isCollapsed ? 0.85 : 1,
            }}
          >
            {/* ================= HEADER ================= */}
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                marginBottom: 10,
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <h3
                  style={{
                    fontSize: 18,
                    fontWeight: 600,
                    margin: 0,
                  }}
                >
                  {group.source}
                </h3>

                {/* SOURCE TYPE BADGE */}
                <span
                  style={{
                    padding: "4px 10px",
                    borderRadius: 999,
                    fontSize: 12,
                    background:
                      type === "forum"
                        ? "#064e3b"
                        : type === "marketplace"
                        ? "#1e293b"
                        : "#1f2933",
                    color:
                      type === "forum"
                        ? "#6ee7b7"
                        : "#cbd5e1",
                  }}
                >
                  {type}
                </span>

                {/* DOMINANCE BADGE */}
                {isDominant && (
                  <span
                    style={{
                      padding: "4px 10px",
                      borderRadius: 999,
                      fontSize: 12,
                      background: "#3f1d1d",
                      color: "#fca5a5",
                    }}
                    title="Источник даёт слишком много объявлений"
                  >
                    dominant
                  </span>
                )}
              </div>

              <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                <span
                  style={{
                    fontSize: 13,
                    color: "#9ca3af",
                  }}
                >
                  {group.items.length} объявлений
                </span>

                <button
                  onClick={() => toggleCollapse(group.source)}
                  style={{
                    fontSize: 13,
                    background: "transparent",
                    border: "none",
                    color: "#60a5fa",
                    cursor: "pointer",
                  }}
                >
                  {isCollapsed ? "Показать" : "Свернуть"}
                </button>
              </div>
            </div>

            {/* ================= RESULTS ================= */}
            {!isCollapsed && (
              <>
                <div
                  style={{
                    display: "flex",
                    flexDirection: "column",
                    gap: 12,
                  }}
                >
                  {group.items
                    .slice(0, visible)
                    .map((item, idx) => (
                      <ResultCard key={idx} result={item} />
                    ))}
                </div>

                {canShowMore && (
                  <div style={{ marginTop: 12 }}>
                    <button
                      onClick={() => showMore(group.source)}
                      style={{
                        padding: "8px 14px",
                        borderRadius: 8,
                        border: "1px solid #2a2a2e",
                        background: "#111827",
                        color: "#cbd5e1",
                        fontSize: 13,
                        cursor: "pointer",
                      }}
                    >
                      Показать ещё ({group.items.length - visible})
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
