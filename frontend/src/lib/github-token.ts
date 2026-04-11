import crypto from "node:crypto";
import { cookies } from "next/headers";
import { NextResponse } from "next/server";

const COOKIE_NAME = "gh_token";
const ALGORITHM = "aes-256-gcm";
const IV_LENGTH = 12;
const TAG_LENGTH = 16;

function getEncryptionKey(): Buffer {
  const secret = process.env.GITHUB_TOKEN_SECRET;
  if (!secret) {
    if (process.env.NODE_ENV === "production") {
      throw new Error(
        "GITHUB_TOKEN_SECRET is required in production. Set a 32+ character secret.",
      );
    }
    console.warn(
      "[github-token] GITHUB_TOKEN_SECRET not set -- using deterministic dev-only key. Do NOT use in production.",
    );
    return crypto.scryptSync("dev-only-insecure-key", "salt", 32);
  }
  return crypto.scryptSync(secret, "gh-token-salt", 32);
}

function encrypt(plaintext: string): string {
  const key = getEncryptionKey();
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);
  const encrypted = Buffer.concat([
    cipher.update(plaintext, "utf-8"),
    cipher.final(),
  ]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, tag, encrypted]).toString("base64");
}

function decrypt(ciphertext: string): string {
  const key = getEncryptionKey();
  const data = Buffer.from(ciphertext, "base64");
  const iv = data.subarray(0, IV_LENGTH);
  const tag = data.subarray(IV_LENGTH, IV_LENGTH + TAG_LENGTH);
  const encrypted = data.subarray(IV_LENGTH + TAG_LENGTH);
  const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([
    decipher.update(encrypted),
    decipher.final(),
  ]).toString("utf-8");
}

export interface GitHubCredentials {
  token: string;
  org: string;
}

export async function getGitHubTokenFromRequest(): Promise<GitHubCredentials | null> {
  const cookieStore = await cookies();
  const cookie = cookieStore.get(COOKIE_NAME);
  if (!cookie?.value) return null;
  try {
    const decrypted = decrypt(cookie.value);
    const parsed = JSON.parse(decrypted) as Partial<GitHubCredentials>;
    const token = typeof parsed.token === "string" ? parsed.token : "";
    const org = typeof parsed.org === "string" ? parsed.org : "";
    if (!token) return null;
    return { token, org };
  } catch {
    return null;
  }
}

export function setGitHubTokenCookie(
  response: NextResponse,
  token: string,
  org: string,
): void {
  const payload = JSON.stringify({ token, org });
  const encrypted = encrypt(payload);
  const isSecure = process.env.NODE_ENV === "production";
  response.cookies.set(COOKIE_NAME, encrypted, {
    httpOnly: true,
    sameSite: "strict",
    secure: isSecure,
    path: "/api/github",
    maxAge: 60 * 60 * 8, // 8 hours
  });
}

export function clearGitHubTokenCookie(response: NextResponse): void {
  response.cookies.set(COOKIE_NAME, "", {
    httpOnly: true,
    sameSite: "strict",
    path: "/api/github",
    maxAge: 0,
  });
}
