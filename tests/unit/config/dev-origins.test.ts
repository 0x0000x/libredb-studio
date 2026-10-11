import { describe, expect, test } from "bun:test";
import { readDevAllowedOrigins } from "@/lib/config/dev-origins";

describe("DEV_ALLOWED_ORIGINS", () => {
  test("unset or empty keeps Next's localhost-only default", () => {
    expect(readDevAllowedOrigins()).toEqual([]);
    expect(readDevAllowedOrigins("")).toEqual([]);
  });

  test("entries are trimmed and empty ones dropped, since Next matches the hostname exactly", () => {
    expect(readDevAllowedOrigins("a.local, 192.0.2.10,")).toEqual(["a.local", "192.0.2.10"]);
  });

  test("a wildcard subdomain is kept as written", () => {
    expect(readDevAllowedOrigins("*.devbox.local")).toEqual(["*.devbox.local"]);
  });

  test("a scheme, port or path is refused, because such an entry never matches", () => {
    expect(() => readDevAllowedOrigins("http://devbox.local")).toThrow("DEV_ALLOWED_ORIGINS");
    expect(() => readDevAllowedOrigins("devbox.local:3000")).toThrow("DEV_ALLOWED_ORIGINS");
    expect(() => readDevAllowedOrigins("devbox.local/x")).toThrow("DEV_ALLOWED_ORIGINS");
  });
});
