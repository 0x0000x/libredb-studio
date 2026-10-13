/**
 * Spec 10.1 rows 6 to 16 and the pure bounds of 6.5, run once under Bun by grpc-runtimes.test.ts and once in a Node
 * child it bundles, because every bound must hold under both and the TLS failure texts differ by runtime while the
 * sentences Studio shows must not. No bun:test import and no Bun global.
 */
import { readFileSync } from "node:fs";
import { SparkHeartbeat } from "@/lib/db/providers/sql/spark/heartbeat";
import { SparkProvider } from "@/lib/db/providers/sql/spark/index";
import { checkIpcStream } from "@/lib/db/providers/sql/spark/ipc-guard";
import { columnsFromDataType } from "@/lib/db/providers/sql/spark/schema";
import { readStatusDetails } from "@/lib/db/providers/sql/spark/status-details";
import { variantJson } from "@/lib/db/providers/sql/spark/variant";
import { wkbToWkt } from "@/lib/db/providers/sql/spark/wkb";
import type { DatabaseConnection } from "@/lib/types";
import { type SparkFakeOptions, startSparkFake } from "./spark-connect-server";
import { columnsStream, largeUtf8Stream, malformedStream, nestedSchemaStream } from "./spark-ipc";

export interface SparkRuntimeOutcome {
  readonly name: string;
  readonly outcome: string;
}

/** The SSL / TLS panel for verify-full, in the field names readGrpcTlsPanel reads, with a pasted CA when one is given. */
const TLS_VERIFY_FULL = (ca: string | undefined) => ({
  mode: "verify-full",
  ...(ca === undefined ? {} : { caCert: ca }),
});

const CURRENT = columnsStream([
  { name: "catalog", kind: "utf8", values: ["spark_catalog"] },
  { name: "namespace", kind: "utf8", values: ["default"] },
]);

const failed = (error: unknown) => `${(error as Error).constructor.name}: ${(error as Error).message}`;

/**
 * `google.rpc.Status` bytes written by hand, so the cases need no protobufjs: Status.details is field 3, Any.type_url
 * 1 and Any.value 2, ErrorInfo.reason 1 and ErrorInfo.metadata 3, a map entry's key 1 and value 2.
 */
const utf8 = (value: string) => new TextEncoder().encode(value);
function varint(value: number): number[] {
  const out: number[] = [];
  let rest = value;
  while (rest >= 0x80) {
    out.push((rest & 0x7f) | 0x80);
    rest = Math.floor(rest / 128);
  }
  out.push(rest);
  return out;
}
const concat = (...parts: Uint8Array[]) => {
  const out = new Uint8Array(parts.reduce((total, part) => total + part.length, 0));
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.length;
  }
  return out;
};
const field = (number: number, bytes: Uint8Array) =>
  concat(Uint8Array.from([...varint((number << 3) | 2), ...varint(bytes.length)]), bytes);
const entry = (key: string, value: string) => field(3, concat(field(1, utf8(key)), field(2, utf8(value))));
/** One Status detail: an Any holding an ErrorInfo with this reason and these metadata entries. */
const detail = (reason: string, ...entries: Uint8Array[]) =>
  field(
    3,
    concat(
      field(1, utf8("type.googleapis.com/google.rpc.ErrorInfo")),
      field(2, concat(field(1, utf8(reason)), ...entries)),
    ),
  );
const CLASS_X = () => detail("R", entry("errorClass", "X"));
/** A Status of exactly `total` bytes (65,000 and more): one detail, then Status.message (field 2) as padding, whose length takes three varint bytes. */
const paddedTo = (total: number) => concat(CLASS_X(), field(2, utf8("m".repeat(total - CLASS_X().length - 4))));
/** The decoder's answer as a short text: its byte count first, each kept key in order, a value over 64 characters as its length. */
const details = (bytes: Uint8Array) => {
  const read = readStatusDetails(bytes);
  const shown =
    read === undefined
      ? "undefined"
      : Object.entries(read)
          .map(([key, value]) => `${key}=${value.length > 64 ? `${value.length} chars` : JSON.stringify(value)}`)
          .join(" ");
  return `${bytes.length} ${shown}`;
};

/** A CA that did not sign the fake's certificate, read from the working directory (the repository root, PLAN-00 gap m). */
const OTHER_CA = () => readFileSync("tests/fixtures/tls/other-ca.crt", "utf8");

/**
 * Connects to a fresh fake, runs `body`, and answers its outcome text or the failure's class and sentence, led by the
 * fake's port and a space when `portFirst` is set (the TLS sentences name the port).
 */
async function withFake(
  options: Omit<SparkFakeOptions, "statements"> & { statements?: SparkFakeOptions["statements"] },
  config: Partial<DatabaseConnection>,
  body: (provider: SparkProvider) => Promise<string>,
  portFirst = false,
): Promise<string> {
  const fake = await startSparkFake({
    progressEveryMs: 0,
    ...options,
    statements: { "SHOW CURRENT NAMESPACE": { kind: "command", ipc: CURRENT }, ...options.statements },
  });
  const port = Number(fake.target.split(":").at(-1));
  const provider = new SparkProvider({
    id: `rt-${port}`,
    name: "rt",
    type: "spark",
    host: "127.0.0.1",
    port,
    ...config,
  } as DatabaseConnection);
  const lead = portFirst ? `${port} ` : "";
  try {
    await provider.connect();
    return lead + (await body(provider));
  } catch (error) {
    return lead + failed(error);
  } finally {
    await provider.disconnect();
    fake.close();
  }
}

const statementFails = (answer: SparkFakeOptions["statements"][string]) =>
  withFake({ statements: { "SELECT x": answer } }, {}, async (provider) => {
    await provider.query("SELECT x");
    return "answered";
  });

export async function runSparkRuntimeCases(): Promise<readonly SparkRuntimeOutcome[]> {
  const cases: [string, () => Promise<string>][] = [
    [
      "6 interrupt",
      () =>
        withFake({ statements: { "SELECT x": { kind: "silent" } } }, {}, async (provider) => {
          const run = provider.query("SELECT x", undefined, "q-6").then(() => "answered", failed);
          await new Promise((resolve) => setTimeout(resolve, 50));
          const cancelled = await provider.cancelQuery("q-6");
          return `${cancelled} ${await run}`;
        }),
    ],
    [
      "7 error info",
      () =>
        statementFails({
          kind: "error",
          code: 13,
          message: "[TABLE_OR_VIEW_NOT_FOUND] The table cannot be found.",
          errorInfo: { errorClass: "TABLE_OR_VIEW_NOT_FOUND", sqlState: "42P01" },
        }),
    ],
    ["8 oversized command", () => statementFails({ kind: "oversized", bytes: 17_000_000, path: "command" })],
    ["9 large var types", () => statementFails({ kind: "command", ipc: largeUtf8Stream() })],
    ["10 over-deep schema", () => statementFails({ kind: "command", ipc: nestedSchemaStream(65) })],
    [
      "11 heartbeat",
      async () => {
        let beats = 0;
        const heartbeat = new SparkHeartbeat(async () => {
          beats += 1;
        }, 50);
        const release = heartbeat.hold({ sessionId: "00000000-0000-4000-8000-000000000000" });
        await new Promise((resolve) => setTimeout(resolve, 180));
        release();
        const atRelease = beats;
        await new Promise((resolve) => setTimeout(resolve, 120));
        return `${atRelease >= 3} ${beats === atRelease}`;
      },
    ],
    [
      "12 keepalive cut",
      () => statementFails({ kind: "status", code: 8, message: "Bandwidth exhausted or memory limit exceeded" }),
    ],
    ["13 no token", () => withFake({ token: "t0ken" }, {}, async () => "connected")],
    ["13 wrong token", () => withFake({ token: "t0ken" }, { password: "wrong" }, async () => "connected")],
    ["14 old version", () => withFake({ sparkVersion: "3.5.9" }, {}, async () => "connected")],
    ["14 vendor suffix", () => withFake({ sparkVersion: "4.1.3-vendor.1" }, {}, async () => "connected")],
    [
      "15 tls to plaintext",
      () =>
        withFake({}, { ssl: TLS_VERIFY_FULL(undefined) } as Partial<DatabaseConnection>, async () => "connected", true),
    ],
    ["15 plaintext to tls", () => withFake({ tls: true }, {}, async () => "connected", true)],
    [
      "15 untrusted ca",
      () =>
        withFake(
          { tls: true },
          { ssl: TLS_VERIFY_FULL(OTHER_CA()) } as Partial<DatabaseConnection>,
          async () => "connected",
        ),
    ],
    ["16 reset", () => statementFails({ kind: "reset" })],
    [
      "6.5 ipc bounds",
      async () =>
        (["no-continuation", "metadata-too-large", "body-past-end", "no-end-marker", "dictionary-batch"] as const)
          .map((kind) => {
            try {
              checkIpcStream(malformedStream(kind));
              return "read";
            } catch (error) {
              return failed(error);
            }
          })
          .join(" | "),
    ],
    ["6.5 status details", async () => String(readStatusDetails(Uint8Array.of(0x0a, 0xff)))],
    ["6.5 status details size", async () => `${details(paddedTo(65_536))} | ${details(paddedTo(65_537))}`],
    [
      "6.5 status details count",
      async () =>
        `${details(concat(...Array.from({ length: 16 }, CLASS_X)))} | ${details(concat(...Array.from({ length: 17 }, CLASS_X)))}`,
    ],
    [
      "6.5 status details value",
      async () =>
        [8_192, 8_193]
          .map((length) =>
            details(detail("R", entry("errorClass", "X"), entry("messageParameters", "p".repeat(length)))),
          )
          .join(" | "),
    ],
    ["6.5 status details bom", async () => details(detail("﻿R", entry("errorClass", "X")))],
    [
      "6.5 variant depth",
      async () => {
        let value = Uint8Array.of(0x00);
        for (let level = 0; level < 65; level += 1) value = Uint8Array.from([0x03, 1, 0, value.length, ...value]);
        try {
          return String(variantJson(value, Uint8Array.of(0x01, 0x00, 0x00), 1_000_000));
        } catch (error) {
          return failed(error);
        }
      },
    ],
    [
      "6.5 wkb depth",
      async () => {
        let nested = Uint8Array.from([1, 1, 0, 0, 0, ...new Uint8Array(Float64Array.of(1, 2).buffer)]);
        for (let level = 0; level < 65; level += 1) nested = Uint8Array.from([1, 7, 0, 0, 0, 1, 0, 0, 0, ...nested]);
        return String(wkbToWkt(nested));
      },
    ],
    [
      "6.5 schema depth",
      async () => {
        let type: object = { kind: "integer", integer: {} };
        for (let level = 0; level < 65; level += 1) type = { kind: "array", array: { element_type: type } };
        try {
          columnsFromDataType({ kind: "struct", struct: { fields: [{ name: "c", data_type: type, nullable: true }] } });
          return "read";
        } catch (error) {
          return failed(error);
        }
      },
    ],
  ];
  const outcomes: SparkRuntimeOutcome[] = [];
  for (const [name, run] of cases) {
    // oxlint-disable-next-line no-await-in-loop -- one case at a time, so no case's timers or fake share the loop with another's.
    outcomes.push({ name, outcome: await run() });
  }
  return outcomes;
}
