import { NextResponse } from "next/server";

const baseUrl = process.env.PYTHON_EXECUTION_URL ?? "http://127.0.0.1:8090";

export const runtime = "nodejs";

export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => ({}));

    const response = await fetch(`${baseUrl}/v1/cortex-terminal/spawn`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      cache: "no-store",
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "Spawn failed");
      return NextResponse.json({ error: text }, { status: response.status });
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to spawn cortex terminal";
    return NextResponse.json({ error: message }, { status: 503 });
  }
}
