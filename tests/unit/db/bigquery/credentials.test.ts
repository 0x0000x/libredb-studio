import { describe, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { readCredentials, tokenOrigin } from "@/lib/db/providers/sql/bigquery/auth";

const key = JSON.stringify({ type: "service_account", client_email: "sa@p.iam", private_key: "k" });

describe("bigquery readCredentials", () => {
  test("a pasted key wins and needs no opt-in", () => {
    expect(readCredentials(key, {}).type).toBe("service_account");
  });

  test("a pasted access token is taken as-is, with or without a Bearer prefix, and needs no opt-in", () => {
    const token = "ya29.a0AfH6SMBx-example_token~value";
    expect(readCredentials(token, {})).toEqual({ type: "access_token", token });
    expect(readCredentials(`Bearer ${token}`, {})).toEqual({ type: "access_token", token });
    expect(readCredentials(`  ${token}\n`, {})).toEqual({ type: "access_token", token });
  });

  test("without a key the server's identity is refused unless opted in", () => {
    expect(() => readCredentials("", {})).toThrow("BIGQUERY_SERVER_CREDENTIALS=true");
  });

  test("GOOGLE_APPLICATION_CREDENTIALS is read when opted in", () => {
    const file = join(mkdtempSync(join(tmpdir(), "bq-")), "sa.json");
    writeFileSync(file, key);
    const env = { BIGQUERY_SERVER_CREDENTIALS: "true", GOOGLE_APPLICATION_CREDENTIALS: file };
    expect(readCredentials(undefined, env).type).toBe("service_account");
  });

  test("a named but missing file is an error, not a silent fallback", () => {
    const env = { BIGQUERY_SERVER_CREDENTIALS: "true", GOOGLE_APPLICATION_CREDENTIALS: "/nope/sa.json" };
    expect(() => readCredentials(undefined, env)).toThrow("names a file that cannot be read");
    expect(() => readCredentials(undefined, env)).not.toThrow("/nope");
  });

  test("no file at all falls back to the metadata server (Workload Identity)", () => {
    const env = {
      BIGQUERY_SERVER_CREDENTIALS: "true",
      HOME: mkdtempSync(join(tmpdir(), "home-")),
      GCE_METADATA_HOST: "169.254.169.254:8080",
    };
    const creds = readCredentials(undefined, env);
    expect(creds).toEqual({ type: "metadata", host: "169.254.169.254:8080" });
    expect(tokenOrigin(creds)).toEqual({ scheme: "http", host: "169.254.169.254", port: 8080 });
  });
});
