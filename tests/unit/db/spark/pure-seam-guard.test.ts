import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { basename, join } from "node:path";

/**
 * Lane C's guard over the Spark provider's pure modules (spec 4.1 and 10.2; PLAN-00 gap f). The sc:// reader is
 * imported by the connection-string parser, which the browser bundle carries, so it imports types only; every pure
 * module imports no I/O, no logger and no transport, and a relative import names only another pure module, the
 * directory's constants or its errors. T6.1, T6.2 and T6.7 extend the lists as the decoder modules land, and T6.1
 * adds the apache-arrow importer rule.
 */
const ROOT = join(import.meta.dir, "..", "..", "..", "..");
const SPARK = "src/lib/db/providers/sql/spark";

/** Files whose every import is `import type`. */
const TYPE_ONLY: readonly string[] = [`${SPARK}/sc-uri.ts`];
/** Files that import no I/O, no logger and no transport. */
const PURE: readonly string[] = [`${SPARK}/sc-uri.ts`, `${SPARK}/ipc-guard.ts`, `${SPARK}/arrow.ts`];
const FORBIDDEN: readonly RegExp[] = [
  /^(?:node:)?(?:fs|fs\/promises|net|tls|http|https|http2|dgram|dns|child_process|worker_threads)$/,
  /^@grpc\//,
  /\/grpc(?:\/|$)/,
  /(?:^|\/)logger$/,
];
const PURE_SIBLINGS: ReadonlySet<string> = new Set([
  "./constants",
  "./errors",
  ...PURE.map((path) => `./${basename(path, ".ts")}`),
]);

interface ImportLine {
  readonly specifier: string;
  readonly typeOnly: boolean;
}

/** Every static import, re-export and side-effect import of a file, the `import type` ones marked, and every dynamic import. */
function importLines(path: string): ImportLine[] {
  const text = readFileSync(join(ROOT, path), "utf8");
  const lines: ImportLine[] = [];
  for (const match of text.matchAll(/^\s*(?:import|export)\s+(type\s+)?(?:[^'"`;]*?\s+from\s+)?["']([^"']+)["']/gm)) {
    lines.push({ specifier: match[2], typeOnly: match[1] !== undefined });
  }
  for (const match of text.matchAll(/\bimport\(\s*["']([^"']+)["']\s*\)/g)) {
    lines.push({ specifier: match[1], typeOnly: false });
  }
  return lines;
}

/** The files that import apache-arrow, held exactly: under src/ only arrow.ts, and the test stream writer. */
const ARROW_IMPORTERS: readonly string[] = ["src/lib/db/providers/sql/spark/arrow.ts", "tests/helpers/spark-ipc.ts"];
const SCANNED = /\.(?:ts|tsx|mts|js|mjs)$/;

function filesUnder(dir: string): string[] {
  return (readdirSync(join(ROOT, dir), { recursive: true }) as string[])
    .filter((path) => SCANNED.test(path) && !path.split(/[\\/]/).includes("node_modules"))
    .map((path) => `${dir}/${path.split("\\").join("/")}`);
}

describe("lane C's Spark seam guard", () => {
  test("the reader finds imports, so an empty answer below is not a reader that reads nothing", () => {
    const parser = importLines("src/lib/connection-string-parser.ts").map((line) => line.specifier);
    expect(parser).toContain("@/lib/db/providers/sql/spark/sc-uri");
    expect(importLines(`${SPARK}/sc-uri.ts`).length).toBeGreaterThan(0);
  });

  test("the type-only modules import types only", () => {
    for (const file of TYPE_ONLY) {
      const runtime = importLines(file).filter((line) => !line.typeOnly);
      expect({ file, runtime }).toEqual({ file, runtime: [] });
    }
  });

  test("the pure modules import no I/O, no logger, no transport and no impure sibling", () => {
    for (const file of PURE) {
      const refused = importLines(file)
        .map((line) => line.specifier)
        .filter(
          (specifier) =>
            FORBIDDEN.some((pattern) => pattern.test(specifier)) ||
            (specifier.startsWith(".") && !PURE_SIBLINGS.has(specifier)),
        );
      expect({ file, refused }).toEqual({ file, refused: [] });
    }
  });

  test("only the listed files import apache-arrow (spec 4.1, 8.7 row 7.2)", () => {
    const found = ["src", "tests", "scripts", "e2e"]
      .flatMap(filesUnder)
      .filter((file) =>
        importLines(file).some(
          (line) => line.specifier === "apache-arrow" || line.specifier.startsWith("apache-arrow/"),
        ),
      );
    expect(found.sort()).toStrictEqual([...ARROW_IMPORTERS].sort());
  });
});
