// apps/web/src/app/api/v1/search/route.ts
import { NextResponse } from "next/server";

export const runtime = "nodejs"; // важно: не edge (на edge часто проблемы с сетью/таймаутами)

const UPSTREAM = process.env.API_UPSTREAM_URL; // например: http://YOUR_VPS_IP:8000

export async function POST(req: Request) {
  try {
    if (!UPSTREAM) {
      return NextResponse.json(
        { error: "API_UPSTREAM_URL is not set" },
        { status: 500 }
      );
    }

    const body = await req.json();

    const r = await fetch(`${UPSTREAM}/api/v1/search`, {
      method: "POST",
      headers: { "Content-Type": "application/json",
                 "X-API-Key": process.env.NEXT_PUBLIC_API_KEY as string
       },
      // можно пробросить user-agent / x-forwarded-for при желании
      body: JSON.stringify(body),
      // таймауты в fetch в Node 18+ — через AbortController (если нужно)
    });

    const text = await r.text();

    // вернуть как есть (FastAPI отдаёт JSON)
    return new NextResponse(text, {
      status: r.status,
      headers: { "Content-Type": "application/json",
                 "X-API-Key": process.env.NEXT_PUBLIC_API_KEY as string
       },
    });
  } catch (e: unknown) {
    const err = e instanceof Error ? e : new Error(String(e));
    return NextResponse.json(
      { error: "Proxy failed", detail: err.message },
      { status: 502 }
    );
  }
}

// (опционально) чтобы 405 не было от GET
export async function GET() {
  return NextResponse.json(
    { error: "Method Not Allowed. Use POST." },
    { status: 405 }
  );
}
