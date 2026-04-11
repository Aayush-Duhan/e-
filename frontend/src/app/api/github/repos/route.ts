import { NextResponse } from "next/server";
import { listOrgRepositories, isGitHubApiError } from "@/lib/github-client";
import { getGitHubTokenFromRequest } from "@/lib/github-token";
import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger("api/github/repos");

export const runtime = "nodejs";

export async function POST(request: Request) {
  try {
    const credentials = await getGitHubTokenFromRequest();
    if (!credentials?.token) {
      return NextResponse.json(
        { error: "GitHub token not found. Please reconnect." },
        { status: 401 },
      );
    }

    const body = await request.json().catch(() => null);
    const org =
      (typeof body?.org === "string" && body.org.trim()) ||
      credentials.org;

    if (!org) {
      return NextResponse.json(
        { error: "org is required" },
        { status: 400 },
      );
    }

    const result = await listOrgRepositories({ token: credentials.token, org });

    logger.info(`Loaded ${result.repositories.length} repos for org ${org}`);

    return NextResponse.json({
      repositories: result.repositories,
      hasMore: result.hasMore,
    });
  } catch (error) {
    if (isGitHubApiError(error)) {
      const status = error.status >= 500 ? 503 : error.status;
      logger.warn(`Repo lookup failed (${status}): ${error.message}`);
      return NextResponse.json(
        { error: error.message, ssoUrl: error.ssoUrl ?? undefined },
        { status },
      );
    }

    logger.error("Unexpected repo lookup error:", error);
    return NextResponse.json(
      { error: "Failed to load repositories." },
      { status: 503 },
    );
  }
}
