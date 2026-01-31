"use client";

import React from "react";

type QuickFiltersProps = {
  /**
   * Текущий поисковый запрос
   */
  query: string;

  /**
   * Колбэк:
   *  - принимает новый query
   *  - родитель решает: искать сразу или нет
   */
  onApply: (query: string) => void;
};

/**
 * QuickFilters — UX-блок быстрых фильтров
 *
 * Принцип:
 * - Никакой логики StructuredQuery
 * - Только текст
 * - Полная совместимость с backend parser
 * - Быстрый first value
 */
export default function QuickFilters({
  query,
  onApply,
}: QuickFiltersProps) {
  const normalizedQuery = query.toLowerCase();

  const apply = (fragment: string) => {
    const base = query.trim();
    const next = base
      ? `${base} ${fragment}`.replace(/\s+/g, " ").trim()
      : fragment;

    onApply(next);
  };

  const reset = () => {
    onApply("");
  };

  const isActive = (fragment: string) => {
    return normalizedQuery.includes(fragment.toLowerCase());
  };

  return (
    <div
      style={{
        marginTop: 16,
        marginBottom: 24,
        padding: 16,
        borderRadius: 14,
        border: "1px solid #e5e7eb",
        background: "linear-gradient(180deg, #fafafa, #f9fafb)",
      }}
    >
      {/* HEADER */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 12,
        }}
      >
        <div
          style={{
            fontSize: 13,
            fontWeight: 600,
            color: "#111827",
            textTransform: "uppercase",
            letterSpacing: "0.04em",
          }}
        >
          ⚡ Быстрые фильтры
        </div>

        {query && (
          <button
            onClick={reset}
            style={{
              fontSize: 12,
              color: "#6b7280",
              background: "transparent",
              border: "none",
              cursor: "pointer",
            }}
          >
            Сбросить
          </button>
        )}
      </div>

      {/* FILTER GROUPS */}
      <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
        {/* BRAND */}
        <FilterRow label="Марка">
          <FilterButton
            label="BMW"
            active={isActive("bmw")}
            onClick={() => apply("bmw")}
          />
          <FilterButton
            label="Mercedes"
            active={isActive("mercedes")}
            onClick={() => apply("mercedes")}
          />
          <FilterButton
            label="Toyota"
            active={isActive("toyota")}
            onClick={() => apply("toyota")}
          />
        </FilterRow>

        {/* PRICE */}
        <FilterRow label="Цена">
          <FilterButton
            label="до 2 млн"
            active={isActive("2 млн")}
            onClick={() => apply("до 2 млн")}
          />
          <FilterButton
            label="до 3 млн"
            active={isActive("3 млн")}
            onClick={() => apply("до 3 млн")}
          />
        </FilterRow>

        {/* MILEAGE */}
        <FilterRow label="Пробег">
          <FilterButton
            label="до 50 тыс"
            active={isActive("50 тыс")}
            onClick={() => apply("пробег до 50 тыс")}
          />
          <FilterButton
            label="до 100 тыс"
            active={isActive("100 тыс")}
            onClick={() => apply("пробег до 100 тыс")}
          />
        </FilterRow>

        {/* CONDITION */}
        <FilterRow label="Состояние">
          <FilterButton
            label="без окраса"
            active={isActive("без окраса")}
            onClick={() => apply("без окраса")}
          />
          <FilterButton
            label="не бит"
            active={isActive("не бит")}
            onClick={() => apply("не бит")}
          />
        </FilterRow>

        {/* FUEL */}
        <FilterRow label="Топливо">
          <FilterButton
            label="бензин"
            active={isActive("бензин")}
            onClick={() => apply("бензин")}
          />
          <FilterButton
            label="дизель"
            active={isActive("дизель")}
            onClick={() => apply("дизель")}
          />
          <FilterButton
            label="гибрид"
            active={isActive("гибрид")}
            onClick={() => apply("гибрид")}
          />
        </FilterRow>
      </div>
    </div>
  );
}

/* =====================================================
 * UI helpers
 * ===================================================== */

function FilterRow({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <div
        style={{
          fontSize: 12,
          color: "#6b7280",
          marginBottom: 6,
        }}
      >
        {label}
      </div>
      <div
        style={{
          display: "flex",
          gap: 8,
          flexWrap: "wrap",
        }}
      >
        {children}
      </div>
    </div>
  );
}

function FilterButton({
  label,
  onClick,
  active,
}: {
  label: string;
  onClick: () => void;
  active?: boolean;
}) {
  return (
    <button
      onClick={onClick}
      style={{
        padding: "6px 14px",
        fontSize: 13,
        borderRadius: 999,
        background: active ? "#2563eb" : "#111827",
        border: active ? "1px solid #2563eb" : "1px solid #2a2a2e",
        color: "#e5e7eb",
        cursor: "pointer",
        transition: "all 0.15s ease",
      }}
      title={`Добавить: ${label}`}
    >
      {label}
    </button>
  );
}
