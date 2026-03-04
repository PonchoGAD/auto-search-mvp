import { NextResponse } from "next/server";

export const runtime = "nodejs";

const UPSTREAM = process.env.API_UPSTREAM_URL; // http://VPS_IP
const API_KEY = process.env.INTERNAL_API_KEY; // НЕ NEXT_PUBLIC

export async function POST(req: Request) {
  try {
    if (!UPSTREAM) {
      return NextResponse.json(
        { error: "API_UPSTREAM_URL is not set" },
        { status: 500 }
      );
    }

    if (!API_KEY) {
      return NextResponse.json(
        { error: "INTERNAL_API_KEY is not set" },
        { status: 500 }
      );
    }

    const body = await req.json();

    const r = await fetch(`${UPSTREAM}/api/v1/search`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": API_KEY,
      },
      body: JSON.stringify(body),
    });

    const text = await r.text();

    return new NextResponse(text, {
      status: r.status,
      headers: { "Content-Type": "application/json" },
    });

  } catch (e: unknown) {
    const err = e instanceof Error ? e : new Error(String(e));

    return NextResponse.json(
      { error: "Proxy failed", detail: err.message },
      { status: 502 }
    );
  }
}

export async function GET() {
  return NextResponse.json(
    { error: "Method Not Allowed. Use POST." },
    { status: 405 }
  );
}