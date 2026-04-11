import { NextResponse } from "next/server";
import { getTree, isGitHubApiError } from "@/lib/github-client";
import { getGitHubTokenFromRequest } from "@/lib/github-token";
import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger("api/github/tree");

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
    const repositoryName =
      typeof body?.repositoryName === "string" ? body.repositoryName.trim() : "";
    const branch =
      typeof body?.branch === "string" && body.branch.trim().length > 0
        ? body.branch.trim()
        : undefined;

    if (!org || !repositoryName) {
      return NextResponse.json(
        { error: "org and repositoryName are required" },
        { status: 400 },
      );
    }

    const result = await getTree({
      token: credentials.token,
      org,
      repositoryName,
      branch,
    });

    logger.info(
      `Loaded tree for ${org}/${repositoryName}@${result.defaultBranch || branch || "default"}: ${result.tree.length} files`,
    );

    return NextResponse.json(result);
  } catch (error) {
    if (isGitHubApiError(error)) {
      const status = error.status >= 500 ? 503 : error.status;
      logger.warn(`Tree lookup failed (${status}): ${error.message}`);
      return NextResponse.json(
        { error: error.message, ssoUrl: error.ssoUrl ?? undefined },
        { status },
      );
    }

    logger.error("Unexpected tree lookup error:", error);
    return NextResponse.json(
      { error: "Failed to load repository tree." },
      { status: 503 },
    );
  }
}
