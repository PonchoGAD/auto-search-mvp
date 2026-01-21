import { NextRequest, NextResponse } from "next/server";

/**
 * Тип тела запроса, который прокидываем в backend
 * Можно расширять позже (include_answer и т.д.)
 */
type SearchRequestBody = {
  query: string;
  include_answer?: boolean;
};

type ErrorResponse = {
  error: string;
  details?: unknown;
  status?: number;
};

const API_BASE =
  process.env.API_BASE_URL ?? "http://localhost:8000";

// ⏱ таймаут запроса к backend
const FETCH_TIMEOUT_MS = 15_000;

export async function POST(req: NextRequest) {
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    FETCH_TIMEOUT_MS
  );

  let body: SearchRequestBody;

  // -------------------------
  // 1️⃣ Читаем body
  // -------------------------
  try {
    body = (await req.json()) as SearchRequestBody;

    if (!body?.query || typeof body.query !== "string") {
      clearTimeout(timeout);
      return NextResponse.json(
        { error: "Field `query` is required" },
        { status: 400 }
      );
    }
  } catch {
    clearTimeout(timeout);
    return NextResponse.json(
      { error: "Invalid JSON body" },
      { status: 400 }
    );
  }

  // -------------------------
  // 2️⃣ Запрос в backend
  // -------------------------
  try {
    const resp = await fetch(`${API_BASE}/api/v1/search`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal,
    });

    clearTimeout(timeout);

    // ❌ Backend ответил, но статус не OK
    if (!resp.ok) {
      let details: unknown;

      try {
        details = await resp.json();
      } catch {
        details = await resp.text();
      }

      const errorPayload: ErrorResponse = {
        error: "Search API error",
        status: resp.status,
        details,
      };

      return NextResponse.json(errorPayload, { status: 500 });
    }

    // ✅ Успешный ответ
    const data = (await resp.json()) as unknown;
    return NextResponse.json(data);

  } catch (err: unknown) {
    clearTimeout(timeout);

    // ⛔ Таймаут
    if (
      typeof err === "object" &&
      err !== null &&
      "name" in err &&
      err.name === "AbortError"
    ) {
      return NextResponse.json(
        {
          error: "Search API timeout",
          details: `Backend did not respond within ${FETCH_TIMEOUT_MS}ms`,
        },
        { status: 504 }
      );
    }

    // ⛔ Любая другая ошибка
    const message =
      err instanceof Error ? err.message : "Unknown error";

    return NextResponse.json(
      {
        error: "Search request failed",
        details: message,
      },
      { status: 500 }
    );
  }
}
