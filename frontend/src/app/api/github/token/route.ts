import { NextResponse } from "next/server";
import {
  setGitHubTokenCookie,
  clearGitHubTokenCookie,
} from "@/lib/github-token";

export const runtime = "nodejs";

export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => null);
    const token = typeof body?.token === "string" ? body.token.trim() : "";
    const org = typeof body?.org === "string" ? body.org.trim() : "";

    if (!token) {
      return NextResponse.json(
        { error: "token is required" },
        { status: 400 },
      );
    }

    const response = NextResponse.json({ ok: true });
    setGitHubTokenCookie(response, token, org);
    return response;
  } catch {
    return NextResponse.json(
      { error: "Failed to store token" },
      { status: 500 },
    );
  }
}

export async function DELETE() {
  const response = NextResponse.json({ ok: true });
  clearGitHubTokenCookie(response);
  return response;
}
