/**
 * Google OAuth2 access tokens for BigQuery, with no Google SDK: `jose` signs the
 * service-account assertion, the token endpoint is one form POST, and on GKE/GCE the
 * metadata server hands out the workload's token.
 */

import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { importPKCS8, SignJWT } from "jose";
import { AuthenticationError, DatabaseConfigError } from "@/lib/db/errors";
import { logger } from "@/lib/logger";
import { type NodeTransport, TransportError } from "@/lib/db/http/node-transport";

const PROVIDER = "bigquery" as const;
const TOKEN_URL = "https://oauth2.googleapis.com/token";
const SCOPE = "https://www.googleapis.com/auth/bigquery";
const REFRESH_EARLY_MS = 60_000;
/** The shared refresh runs on its own deadline, so no single caller's abort fails the others waiting on it. */
const REFRESH_TIMEOUT_MS = 30_000;
const TOKEN_RESPONSE_BYTES = 64 * 1024;
/** Opt-in for the server's own identity (ADC file or metadata server) on connections that carry no key. */
const SERVER_CREDENTIALS_ENV = "BIGQUERY_SERVER_CREDENTIALS";
const METADATA_TOKEN_PATH = "/computeMetadata/v1/instance/service-accounts/default/token";

export type GoogleCredentials =
  | { readonly type: "service_account"; readonly client_email: string; readonly private_key: string }
  | {
      readonly type: "authorized_user";
      readonly client_id: string;
      readonly client_secret: string;
      readonly refresh_token: string;
    }
  /** GKE Workload Identity or a GCE/Cloud Run service account, via the metadata server. */
  | { readonly type: "metadata"; readonly host: string }
  /** A bearer token pasted as-is (`gcloud auth print-access-token`): about an hour of life and no refresh. */
  | { readonly type: "access_token"; readonly token: string };

/** What a Google access token looks like: `ya29.…` and its relatives, one unbroken run of URL-safe characters. */
const ACCESS_TOKEN = /^[\w.~+/=-]{20,}$/;

/**
 * The key JSON, or an access token, pasted into the password field. Without one, and only when the operator set
 * BIGQUERY_SERVER_CREDENTIALS=true, the server's own identity: GOOGLE_APPLICATION_CREDENTIALS, then
 * gcloud's ADC file, then the metadata server (Workload Identity). The opt-in exists because every
 * user who can add a connection would otherwise query as the server.
 */
export function readCredentials(
  secret: string | undefined,
  env: Readonly<Record<string, string | undefined>> = process.env,
): GoogleCredentials {
  let text = secret?.trim();
  if (!text) {
    if (env[SERVER_CREDENTIALS_ENV] !== "true") {
      throw new DatabaseConfigError(
        `BigQuery needs a service-account key in the password field. To use the server's own credentials (ADC or Workload Identity) instead, set ${SERVER_CREDENTIALS_ENV}=true on the server.`,
        PROVIDER,
      );
    }
    const explicit = env.GOOGLE_APPLICATION_CREDENTIALS;
    const file = explicit ?? join(env.HOME ?? homedir(), ".config", "gcloud", "application_default_credentials.json");
    try {
      text = readFileSync(file, "utf8");
    } catch {
      // A named file that is missing is a mistake; a missing default file means "use the metadata server".
      if (explicit) {
        logger.warn("GOOGLE_APPLICATION_CREDENTIALS names a file that cannot be read", { route: "bigquery", file });
        throw new DatabaseConfigError("GOOGLE_APPLICATION_CREDENTIALS names a file that cannot be read", PROVIDER);
      }
      return { type: "metadata", host: env.GCE_METADATA_HOST ?? "metadata.google.internal" };
    }
  }

  if (!text.startsWith("{")) {
    const token = text.replace(/^Bearer\s+/i, "");
    if (ACCESS_TOKEN.test(token)) return { type: "access_token", token };
    throw new DatabaseConfigError(
      "BigQuery credentials are neither a service-account key JSON nor an access token",
      PROVIDER,
    );
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new DatabaseConfigError("BigQuery credentials are not valid JSON", PROVIDER);
  }
  const creds = parsed as Partial<Record<string, unknown>>;
  if (
    creds.type === "service_account" &&
    typeof creds.client_email === "string" &&
    typeof creds.private_key === "string"
  ) {
    return creds as GoogleCredentials;
  }
  if (
    creds.type === "authorized_user" &&
    typeof creds.client_id === "string" &&
    typeof creds.client_secret === "string" &&
    typeof creds.refresh_token === "string"
  ) {
    return creds as GoogleCredentials;
  }
  throw new DatabaseConfigError(
    `BigQuery credentials of type "${String(creds.type)}" are not supported: use a service_account key or authorized_user ADC`,
    PROVIDER,
  );
}

async function grantForm(
  creds: Exclude<GoogleCredentials, { type: "metadata" | "access_token" }>,
  now: number,
): Promise<Record<string, string>> {
  if (creds.type === "authorized_user") {
    return {
      grant_type: "refresh_token",
      client_id: creds.client_id,
      client_secret: creds.client_secret,
      refresh_token: creds.refresh_token,
    };
  }
  const iat = Math.floor(now / 1000);
  const assertion = await new SignJWT({ scope: SCOPE })
    .setProtectedHeader({ alg: "RS256", typ: "JWT" })
    .setIssuer(creds.client_email)
    .setAudience(TOKEN_URL)
    .setIssuedAt(iat)
    .setExpirationTime(iat + 3600)
    .sign(await importPKCS8(creds.private_key, "RS256"));
  return { grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion };
}

/** Where tokens for these credentials come from: Google's token endpoint, or the metadata server. */
export function tokenOrigin(creds: GoogleCredentials): { scheme: "http" | "https"; host: string; port: number } {
  if (creds.type !== "metadata") return { scheme: "https", host: new URL(TOKEN_URL).hostname, port: 443 };
  const [host, port] = creds.host.split(":");
  return { scheme: "http", host, port: port ? Number(port) : 80 };
}

/** A cached bearer token, refreshed a minute before Google says it expires. */
export function createTokenSource(
  creds: GoogleCredentials,
  tokens: NodeTransport,
  now: () => number = Date.now,
): (signal: AbortSignal) => Promise<string> {
  if (creds.type === "access_token") {
    const { token } = creds;
    return async () => token;
  }

  let cached: { token: string; expiresAt: number } | null = null;
  // Concurrent callers share one refresh; a 401 does not invalidate the cache (expiry only).
  let pending: Promise<string> | null = null;

  const refresh = async (signal: AbortSignal): Promise<string> => {
    const response =
      creds.type === "metadata"
        ? await tokens.request({
            method: "GET",
            url: `http://${creds.host}${METADATA_TOKEN_PATH}?${new URLSearchParams({ scopes: SCOPE })}`,
            signal,
            maxResponseBytes: TOKEN_RESPONSE_BYTES,
          })
        : await tokens.request({
            method: "POST",
            url: TOKEN_URL,
            form: await grantForm(creds, now()),
            signal,
            maxResponseBytes: TOKEN_RESPONSE_BYTES,
          });
    let body: { access_token?: unknown; expires_in?: unknown; error_description?: unknown; error?: unknown } = {};
    try {
      body = JSON.parse(response.text);
    } catch {
      // Falls through to the status check with an empty body.
    }
    if (response.status !== 200 || typeof body.access_token !== "string") {
      const reason = body.error_description ?? body.error ?? `HTTP ${response.status}`;
      throw new AuthenticationError(`Google token request failed: ${String(reason)}`, PROVIDER);
    }
    const ttlSeconds = typeof body.expires_in === "number" ? body.expires_in : 3600;
    cached = { token: body.access_token, expiresAt: now() + ttlSeconds * 1000 };
    return cached.token;
  };

  return async (signal) => {
    if (cached && now() < cached.expiresAt - REFRESH_EARLY_MS) return cached.token;
    pending ??= refresh(AbortSignal.timeout(REFRESH_TIMEOUT_MS)).finally(() => {
      pending = null;
    });
    return untilAborted(pending, signal);
  };
}

/** The shared work, or this caller's own abort as the node transport reports one, whichever comes first. */
function untilAborted<T>(work: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      reject(
        signal.reason instanceof DOMException && signal.reason.name === "TimeoutError"
          ? new TransportError("timeout", "The request did not finish within its time limit")
          : new TransportError("aborted", "The request was cancelled"),
      );
    };
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
    work.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
  });
}
