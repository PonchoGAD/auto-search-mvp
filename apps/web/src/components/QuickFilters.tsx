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
 * QuickFilters — UX-блок быстрых фильтров (PROMPT 24.3)
 *
 * Принцип:
 * - Никакой сложной логики
 * - Только дописываем текст к query
 * - Полная совместимость с backend parser
 *
 * Примеры:
 *  BMW → "bmw"
 *  до 2м → "до 2 млн"
 *  до 50к → "пробег до 50 тыс"
 *  без окраса → "без окраса"
 */
export default function QuickFilters({
  query,
  onApply,
}: QuickFiltersProps) {
  const apply = (fragment: string) => {
    const base = query.trim();
    const next = base
      ? `${base} ${fragment}`.replace(/\s+/g, " ").trim()
      : fragment;

    onApply(next);
  };

  return (
    <div
      style={{
        marginTop: 12,
        marginBottom: 20,
      }}
    >
      <div
        style={{
          fontSize: 13,
          color: "#9aa0a6",
          marginBottom: 8,
        }}
      >
        Быстрые фильтры
      </div>

      <div
        style={{
          display: "flex",
          gap: 8,
          flexWrap: "wrap",
        }}
      >
        {/* BRAND */}
        <FilterButton label="BMW" onClick={() => apply("bmw")} />
        <FilterButton label="Mercedes" onClick={() => apply("mercedes")} />
        <FilterButton label="Toyota" onClick={() => apply("toyota")} />

        {/* PRICE */}
        <Divider />
        <FilterButton label="до 2 млн" onClick={() => apply("до 2 млн")} />
        <FilterButton label="до 3 млн" onClick={() => apply("до 3 млн")} />

        {/* MILEAGE */}
        <Divider />
        <FilterButton
          label="пробег до 50 тыс"
          onClick={() => apply("пробег до 50 тыс")}
        />
        <FilterButton
          label="пробег до 100 тыс"
          onClick={() => apply("пробег до 100 тыс")}
        />

        {/* CONDITION */}
        <Divider />
        <FilterButton
          label="без окраса"
          onClick={() => apply("без окраса")}
        />
        <FilterButton label="не бит" onClick={() => apply("не бит")} />

        {/* FUEL */}
        <Divider />
        <FilterButton label="бензин" onClick={() => apply("бензин")} />
        <FilterButton label="дизель" onClick={() => apply("дизель")} />
        <FilterButton label="гибрид" onClick={() => apply("гибрид")} />
      </div>
    </div>
  );
}

/* =====================================================
 * UI helpers
 * ===================================================== */

function FilterButton({
  label,
  onClick,
}: {
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      style={{
        padding: "6px 12px",
        fontSize: 13,
        borderRadius: 999,
        background: "#111827",
        border: "1px solid #2a2a2e",
        color: "#e5e7eb",
        cursor: "pointer",
        transition: "all 0.15s ease",
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.background = "#1f2933";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = "#111827";
      }}
      title={`Добавить: ${label}`}
    >
      {label}
    </button>
  );
}

function Divider() {
  return (
    <span
      style={{
        width: 1,
        height: 24,
        background: "#2a2a2e",
        alignSelf: "center",
        margin: "0 4px",
      }}
    />
  );
}
