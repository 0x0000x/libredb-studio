import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SPARK_ERROR_SENTENCES } from "@/lib/db/providers/sql/spark/errors";
import { runSparkRuntimeCases, type SparkRuntimeOutcome } from "../../../helpers/spark-runtime-cases";

const HELPER = join(import.meta.dir, "../../../helpers/spark-runtime-cases.ts");
const ROOT = join(import.meta.dir, "../../../..");

/** What both runtimes must show: the class and the sentence of each row, never a runtime's own text. */
const EXPECTED_SENTENCES: Readonly<Record<string, string>> = {
  "8 oversized command": `QueryError: ${SPARK_ERROR_SENTENCES.receiveCapCommand}`,
  "9 large var types": `QueryError: ${SPARK_ERROR_SENTENCES.largeVarTypes}`,
  "12 keepalive cut": `QueryError: ${SPARK_ERROR_SENTENCES.pingGoaway}`,
  "13 no token": `AuthenticationError: ${SPARK_ERROR_SENTENCES.tokenMissing}`,
  "13 wrong token": `AuthenticationError: ${SPARK_ERROR_SENTENCES.tokenWrong}`,
  "14 old version": `ConnectionError: ${SPARK_ERROR_SENTENCES.versionTooOld("3.5.9")}`,
  "14 vendor suffix": "connected",
  "16 reset": `ConnectionError: ${SPARK_ERROR_SENTENCES.connectionDropped}`,
  "6.5 status details": "undefined",
  "6.5 status details size": '65536 reason="R" errorClass="X" | 65537 undefined',
  "6.5 status details count": '1056 reason="R" errorClass="X" | 1122 undefined',
  "6.5 status details value":
    '8285 reason="R" errorClass="X" messageParameters=8192 chars | 8286 reason="R" errorClass="X"',
  "6.5 status details bom": '69 reason="﻿R" errorClass="X"',
  "6.5 wkb depth": "undefined",
};

function holdsExpectations(outcomes: readonly SparkRuntimeOutcome[]): void {
  const byName = Object.fromEntries(outcomes.map((item) => [item.name, item.outcome]));
  for (const [name, outcome] of Object.entries(EXPECTED_SENTENCES)) {
    expect({ name, outcome: byName[name] }).toStrictEqual({ name, outcome });
  }
  expect(byName["6 interrupt"]).toStartWith("true QueryCancelledError");
  expect(byName["7 error info"]).toStartWith("QueryError: ");
  expect(byName["7 error info"]).toContain("TABLE_OR_VIEW_NOT_FOUND");
  expect(byName["10 over-deep schema"]).toStartWith("QueryError: ");
  expect(byName["11 heartbeat"]).toBe("true true");
  const [plainPort, ...plainOutcome] = byName["15 tls to plaintext"].split(" ");
  expect(plainOutcome.join(" ")).toBe(
    `ConnectionError: ${SPARK_ERROR_SENTENCES.tlsPlaintextServer("127.0.0.1", Number(plainPort))}`,
  );
  const [tlsPort, ...tlsOutcome] = byName["15 plaintext to tls"].split(" ");
  expect(tlsOutcome.join(" ")).toBe(
    `ConnectionError: ${SPARK_ERROR_SENTENCES.tlsPortExpected("127.0.0.1", Number(tlsPort))}`,
  );
  expect(byName["15 untrusted ca"]).toBe(`ConnectionError: ${SPARK_ERROR_SENTENCES.tlsUntrusted}`);
  const ipcBounds = byName["6.5 ipc bounds"].split(" | ");
  expect(ipcBounds).toHaveLength(5);
  // Each refusal's sentence says "could not read", so the check is on the whole outcome, never a substring.
  expect(ipcBounds).not.toContain("read");
  for (const outcome of ipcBounds) expect(outcome).toStartWith("SparkError: ");
  expect(byName["6.5 variant depth"]).toStartWith("SparkError: ");
  expect(byName["6.5 schema depth"]).toStartWith("SparkError: ");
}

let bun: readonly SparkRuntimeOutcome[] = [];
let node: { version: string; outcomes: readonly SparkRuntimeOutcome[] } = { version: "", outcomes: [] };
let work = "";

beforeAll(async () => {
  bun = await runSparkRuntimeCases();
  work = mkdtempSync(join(tmpdir(), "spark-runtimes-"));
  const binary = Bun.which("node");
  if (binary === null)
    throw new Error("No node on PATH: this file runs the cases under Node; install Node 24 or later");
  writeFileSync(
    join(work, "child.ts"),
    [
      `import { runSparkRuntimeCases } from ${JSON.stringify(HELPER)};`,
      "const outcomes = await runSparkRuntimeCases();",
      'process.stdout.write(JSON.stringify({ version: process.version, outcomes }) + "\\n");',
      "process.exit(0);",
      "",
    ].join("\n"),
  );
  const build = await Bun.build({ entrypoints: [join(work, "child.ts")], target: "node", format: "esm", outdir: work });
  if (!build.success) throw new Error(`Bun.build could not bundle the child: ${build.logs.join("\n")}`);
  const child = Bun.spawn([binary, join(work, "child.js")], { cwd: ROOT, stdout: "pipe", stderr: "pipe" });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode !== 0) throw new Error(`The Node child exited ${exitCode}: ${stderr}`);
  node = JSON.parse(stdout.trim()) as typeof node;
}, 150_000);

afterAll(() => {
  if (work !== "") rmSync(work, { recursive: true, force: true });
});

describe("Spark Connect under both runtimes (spec 10.1, 6.5)", () => {
  test("under Bun every row shows its class and sentence", () => {
    holdsExpectations(bun);
  });

  test("the child is Node and says which version ran the cases", () => {
    expect(node.version).toMatch(/^v\d+\.\d+\.\d+/);
  });

  test("under Node every row shows the same class and sentence as under Bun", () => {
    holdsExpectations(node.outcomes);
    expect(node.outcomes.map((item) => item.name)).toEqual(bun.map((item) => item.name));
  });
});
