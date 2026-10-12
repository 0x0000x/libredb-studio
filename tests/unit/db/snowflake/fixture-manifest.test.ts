/**
 * Every Snowflake fixture set is a capture: each `manifest.json` under tests/fixtures/snowflake/ names the cloud, the
 * region, `CURRENT_VERSION()`, the edition, the capture date and the Studio commit the harness ran from, and none is
 * provisional.
 *
 * A set written from documentation rather than captured says so with `"provisional": true`, and this test fails on
 * it: the provider is not done while its integration test replays answers no Snowflake account gave. The set the
 * integration test replays, `trial-2026-10-12-v10.37.100/`, is the capture tests/live/snowflake-evidence.ts recorded
 * on the trial account on 2026-10-12.
 */
import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";

const FIXTURES = join(import.meta.dir, "..", "..", "..", "fixtures", "snowflake");
const REQUIRED = ["cloud", "region", "currentVersion", "edition", "captured", "commit"] as const;

function manifests(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) return manifests(path);
    return entry.name === "manifest.json" ? [path] : [];
  });
}

describe("the Snowflake fixture manifests", () => {
  const files = manifests(FIXTURES);

  test("there is at least one fixture set", () => {
    expect(files.length).toBeGreaterThan(0);
  });

  test.each(files.map((file) => [relative(FIXTURES, file), file]))(
    "%s is a capture, not provisional",
    (_name, file) => {
      const manifest = JSON.parse(readFileSync(file, "utf8")) as Record<string, unknown>;
      expect({ provisional: manifest.provisional ?? false }).toEqual({ provisional: false });
      const missing = REQUIRED.filter((field) => typeof manifest[field] !== "string" || manifest[field] === "");
      expect(missing).toEqual([]);
    },
  );
});
