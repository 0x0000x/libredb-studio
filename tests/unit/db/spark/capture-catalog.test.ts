/**
 * The Spark capture catalog (spec 10.3, D16c): every capture the catalog names exists for every version it lists, in
 * the D16c shape, from its service's pinned image and the server version of its directory; no capture holds the
 * token, a client-chosen id or a configured catalog's value; each stays under 2 MB; the session settings the harness
 * sends are the provider's; and README.md is what the harness renders.
 */
import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { captureBytes, readSparkCapture, type SparkCaptureVersion } from "../../../helpers/spark-captures";
import { renderSparkFixturesReadme } from "../../../live/spark-evidence";
import {
  capturesOf,
  SPARK_CAPTURE_CATALOG,
  SPARK_CAPTURE_METADATA_SESSION_CONF,
  SPARK_CAPTURE_NAMES,
  SPARK_CAPTURE_SESSION_CONF,
  SPARK_EVERY_TYPE_SELECT,
  SPARK_SERVICE_IMAGES,
  type SparkCaptureName,
} from "../../../live/spark-evidence-plan";

const ROOT = path.resolve(import.meta.dir, "../../../..");
const FIXTURES = path.join(ROOT, "tests", "fixtures", "spark");
const VERSIONS: readonly SparkCaptureVersion[] = ["4.2.0", "4.1.3"];

function expected(
  version: SparkCaptureVersion,
): Map<SparkCaptureName, { readonly entry: (typeof SPARK_CAPTURE_CATALOG)[number]; readonly primary: boolean }> {
  const names = new Map<
    SparkCaptureName,
    { readonly entry: (typeof SPARK_CAPTURE_CATALOG)[number]; readonly primary: boolean }
  >();
  for (const entry of SPARK_CAPTURE_CATALOG.filter((candidate) => candidate.versions.includes(version))) {
    for (const name of capturesOf(entry)) names.set(name, { entry, primary: name === entry.name });
  }
  return names;
}

describe("the catalog", () => {
  test("the session settings are the provider's, in its order (PLAN-00 Global Constraints)", () => {
    expect(SPARK_CAPTURE_SESSION_CONF).toEqual([
      ["spark.sql.variable.substitute", "false"],
      ["spark.sql.execution.arrow.compression.codec", "none"],
      ["spark.sql.execution.arrow.useLargeVarTypes", "false"],
      ["spark.connect.session.resultChunking.maxChunkSize", "4194304"],
      ["spark.sql.execution.arrow.maxRecordsPerBatch", "1000"],
      ["spark.connect.progress.reportInterval", "2s"],
      ["spark.sql.connect.enrichError.enabled", "true"],
    ]);
    expect(SPARK_CAPTURE_METADATA_SESSION_CONF).toEqual([
      ["spark.sql.legacy.keepCommandOutputSchema", "false"],
      ["spark.sql.debug.maxToStringFields", "10000"],
    ]);
  });

  test("every name is written by exactly one entry, and every written name is in SPARK_CAPTURE_NAMES", () => {
    const written = SPARK_CAPTURE_CATALOG.flatMap(capturesOf);
    expect(new Set(written).size).toBe(written.length);
    expect([...written].sort()).toEqual([...SPARK_CAPTURE_NAMES].sort());
  });

  test("the every-type SELECT reads the stored table and adds the two types no table stores", () => {
    expect(SPARK_EVERY_TYPE_SELECT).toContain("FROM spark_catalog.studio.every_type");
    expect(SPARK_EVERY_TYPE_SELECT).not.toMatch(/\bTIME\b/);
  });
});

describe.each([...VERSIONS])("the captures of %s", (version) => {
  const names = expected(version);

  test("the directory holds exactly the catalogued captures", () => {
    const files = readdirSync(path.join(FIXTURES, version)).sort();
    expect(files).toEqual([...names.keys()].map((name) => `${name}.json`).sort());
  });

  test.each([...names.keys()])("%s is a D16c capture from its pinned image", (name) => {
    const { entry, primary } = names.get(name) as {
      readonly entry: (typeof SPARK_CAPTURE_CATALOG)[number];
      readonly primary: boolean;
    };
    const capture = readSparkCapture(version, name);
    const service = version === "4.1.3" ? "spark-4-1" : entry.service;
    expect(capture.$captured.service).toBe(service);
    expect(capture.$captured.image).toBe(SPARK_SERVICE_IMAGES[service].image);
    expect(capture.$captured.digest).toBe(SPARK_SERVICE_IMAGES[service].digest);
    expect(capture.$captured.sparkVersion).toBe(version);
    expect(capture.$captured.runtime).toMatch(/^bun \d+\.\d+\.\d+$/);
    expect(capture.$captured.rpc).toMatch(
      /^SparkConnectService\/(ExecutePlan|AnalyzePlan|Config|Interrupt|ReleaseSession|FetchErrorDetails)$/,
    );
    expect(Number.isNaN(Date.parse(capture.$captured.date))).toBe(false);
    if (primary && entry.action.kind !== "interrupt") expect(capture.outcome).toBe(entry.expect);
    if (capture.outcome === "error") {
      expect(Number.isInteger(capture.code)).toBe(true);
      expect(typeof capture.details).toBe("string");
    }
    const request = capture.$captured.request;
    if ("session_id" in request) expect(request.session_id).toBe("<session>");
    if ("tags" in request) for (const tag of request.tags as unknown[]) expect(tag).toBe("<tag>");
    if (typeof request.operation_id === "string") expect(request.operation_id).toBe("<operation>");
  });

  test("no capture is larger than 2 MB, holds a bearer token or a client-chosen id", () => {
    for (const name of names.keys()) {
      const file = path.join(FIXTURES, version, `${name}.json`);
      expect({ name, small: statSync(file).size <= 2_000_000 }).toEqual({ name, small: true });
      const text = readFileSync(file, "utf8");
      expect({ name, bearer: /Bearer (?!<token>)/.test(text) }).toEqual({ name, bearer: false });
      expect({ name, ids: /libredb-studio-[0-9a-f]{8}-/.test(text) }).toEqual({ name, ids: false });
    }
  });
});

describe("the answers the captures hold", () => {
  test("Config get_all of spark.sql.catalog. keeps the key names and writes every value <value>", () => {
    const capture = readSparkCapture("4.2.0", "config-catalog-keys");
    if (capture.outcome !== "pass") throw new Error("config-catalog-keys is an error capture");
    const pairs = (capture.payload as { readonly pairs: readonly { readonly key: string; readonly value: string }[] })
      .pairs;
    expect(pairs.map((pair) => pair.key)).toContain("studio_unloaded");
    for (const pair of pairs) expect(pair.value).toBe("<value>");
  });

  test("a failure carries Spark's status details byte for byte", () => {
    const capture = readSparkCapture("4.2.0", "error-syntax");
    if (capture.outcome !== "error") throw new Error("error-syntax is a pass capture");
    expect(captureBytes(capture.metadata["grpc-status-details-bin"]).length).toBeGreaterThan(0);
  });
});

describe("the reader", () => {
  test("captureBytes reads a $bytes value and refuses anything else", () => {
    expect([...captureBytes({ $bytes: Buffer.from([1, 2, 255]).toString("base64") })]).toEqual([1, 2, 255]);
    for (const value of [null, "AQI=", { $bytes: 1 }, { $bytes: "AQI=", extra: true }]) {
      expect(() => captureBytes(value)).toThrow('captureBytes reads a {"$bytes": "<base64>"} value only');
    }
  });

  test("a capture that is absent is refused naming its file", () => {
    expect(() => readSparkCapture("4.1.3", "auth-no-token")).toThrow(
      "tests/fixtures/spark/4.1.3/auth-no-token.json is missing: capture it with bun tests/live/spark-evidence.ts --capture <service>",
    );
  });
});

test("README.md is what the harness renders from the captures", () => {
  expect(readFileSync(path.join(FIXTURES, "README.md"), "utf8")).toBe(renderSparkFixturesReadme());
});
