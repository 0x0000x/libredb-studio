/**
 * Apache Spark provider, end to end over answers Spark gave (spec 10.3, D16c).
 *
 * The real SparkProvider runs: connection options, sessions, the two-step statement run, the result stream and its
 * budgets, the Arrow decoder, the error table, the object surface and the monitoring reads. Only the wire is
 * replaced, by the recorded transport of `tests/helpers/spark-fixtures.ts`, handed through the constructor's third
 * argument; `grpc-client.ts` itself is proven against the fake server (10.1, 10.2). A call is matched by its RPC and
 * its decoded request, and a request no capture matches fails the test by name: no answer is invented.
 *
 * The captures were taken by `tests/live/spark-evidence.ts` on 2026-10-13, under bun, from two servers:
 * - `apache/spark:4.2.0@sha256:fc64959c04bd87b0ac686be9aaa9008b69cdb1afc695400528279c8b01f43d89`, whose AnalyzePlan
 *   answers the version "4.2.0" (services `spark` and `spark-auth`, into `tests/fixtures/spark/4.2.0/`);
 * - `apache/spark:4.1.3@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211`, whose AnalyzePlan
 *   answers the version "4.1.3" (service `spark-4-1`, into `tests/fixtures/spark/4.1.3/`).
 * `tests/fixtures/spark/README.md` lists every capture with its digest and date, and a test below holds every capture
 * to it.
 *
 * What is BUILT rather than read from a capture, each labelled where it is built:
 * - the ids: the replay writes the request's own session, operation and tag ids into the echo fields of the captured
 *   answer, because the client chooses them at random, and answers every call of one session with one server-side
 *   session id, because each capture comes from its own run and the provider reads a changed id as a restarted
 *   server; every case shares this step;
 * - the version refusal: the 4.2.0 version answer rewritten to "3.5.9" and "4.0.4", since no capture is of a server
 *   below the floor;
 * - the nested namespace walk: the session catalog's `SHOW NAMESPACES` answer rewritten to multi-part spellings,
 *   because no catalog of the open-source fixture nests.
 */
import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { AuthenticationError, ConnectionError, QueryError } from "@/lib/db/errors";
import { SPARK_ERROR_SENTENCES } from "@/lib/db/providers/sql/spark/errors";
import type { SparkWireTransport } from "@/lib/db/providers/sql/spark/grpc-client";
import { SparkProvider } from "@/lib/db/providers/sql/spark/index";
import { SPARK_CUT_WARNINGS } from "@/lib/db/providers/sql/spark/result-stream";
import type { DatabaseConnection } from "@/lib/types";
import { readSparkCapture, type SparkCaptureVersion } from "../../helpers/spark-captures";
import { recordedSparkTransport } from "../../helpers/spark-fixtures";
import { columnsStream } from "../../helpers/spark-ipc";
import {
  SPARK_CAPTURE_CATALOG,
  SPARK_CAPTURE_NAMES,
  SPARK_EVERY_TYPE_SELECT,
  SPARK_SAMPLE_NAMESPACE,
  SPARK_SERVICE_IMAGES,
  type SparkCaptureEntry,
  type SparkCaptureName,
} from "../../live/spark-evidence-plan";

function entry(name: SparkCaptureName): SparkCaptureEntry {
  const found = SPARK_CAPTURE_CATALOG.find((candidate) => candidate.name === name);
  if (found === undefined) throw new Error(`the capture plan has no entry ${name}`);
  return found;
}

/** The statement a capture ran, read from the plan, because the matcher compares statement texts. */
function actionSql(name: SparkCaptureName): string {
  const { action } = entry(name);
  if (!("sql" in action) || action.sql === undefined) throw new Error(`the capture plan's ${name} runs no statement`);
  return action.sql;
}

/** The first statement a capture ran before its own, read from the plan. */
function beforeSql(name: SparkCaptureName): string {
  const sql = entry(name).before?.[0]?.sql;
  if (sql === undefined) throw new Error(`the capture plan's ${name} runs no statement before its own`);
  return sql;
}

const FIXTURES = join(import.meta.dir, "..", "..", "fixtures", "spark");
const EVERY_TYPE_SELECT = SPARK_EVERY_TYPE_SELECT;
const EXPLAIN_STATEMENT = actionSql("explain-formatted");
const ROW_BUDGET_STATEMENT = actionSql("row-budget-cut");
const TIME_STATEMENT = actionSql("time-type-refused");
const PERMISSION_STATEMENT = actionSql("error-permission-denied");
const MAX_RESULT_STATEMENT = actionSql("error-max-result-size");
const SHOW_TABLES_STATEMENT = actionSql("show-tables-inline");
const SYNTAX_STATEMENT = actionSql("error-syntax");
const MISSING_TABLE_STATEMENT = actionSql("error-missing-table");
const LARGE_TYPES_SET = beforeSql("large-var-types-refused");
const LARGE_TYPES_SELECT = actionSql("large-var-types-refused");
const SUBSTITUTION_SET = beforeSql("substitution-off");
const SUBSTITUTION_SELECT = actionSql("substitution-off");
const ZSTD_SET = beforeSql("compressed-zstd");
const ZSTD_SELECT = actionSql("compressed-zstd");
const TEMP_VIEW_SCRIPT = [beforeSql("temp-view-script"), actionSql("temp-view-script")];

const CONNECT: readonly SparkCaptureName[] = [
  "connect-version",
  "release-session",
  "connect-session-setup",
  "connect-current-namespace",
];
/** The capture that answers a refusal's Interrupt by tag, recorded on 4.2.0 only. */
const INTERRUPT: Readonly<Record<SparkCaptureVersion, readonly SparkCaptureName[]>> = {
  "4.2.0": ["cancel-interrupt"],
  "4.1.3": [],
};
/** A statement's own session: its Config set of the seven pairs, the setup every request session sends first (4.5). */
const STATEMENT_SESSION: readonly SparkCaptureName[] = ["request-session-setup"];

const config = (extra: Partial<DatabaseConnection> = {}): DatabaseConnection =>
  ({
    id: "spark-replay",
    name: "spark",
    type: "spark",
    host: "127.0.0.1",
    port: 15002,
    ...extra,
  }) as DatabaseConnection;

async function connected(
  version: SparkCaptureVersion,
  names: readonly SparkCaptureName[],
  extra: Partial<DatabaseConnection> = {},
) {
  const provider = new SparkProvider(
    config(extra),
    {},
    { transport: recordedSparkTransport(version, [...CONNECT, ...STATEMENT_SESSION, ...names]) },
  );
  await provider.connect();
  return provider;
}

/** A recorded transport whose AnalyzePlan version answer is rewritten (BUILT): the only way to replay an old server. */
function withVersion(base: SparkWireTransport, version: string): SparkWireTransport {
  return (options) => {
    const channel = base(options);
    return {
      ...channel,
      unary: async (rpc, request, call) => {
        const answer = await channel.unary(rpc, request, call);
        return rpc === "AnalyzePlan" ? { ...answer, spark_version: { version } } : answer;
      },
    };
  };
}

/**
 * A recorded transport whose `SHOW NAMESPACES IN \`spark_catalog\`` answer holds `spellings` (BUILT): no catalog of the
 * open-source fixture nests, so the captured answer's inline rows are replaced by one Utf8 `namespace` column.
 */
function withNamespaces(base: SparkWireTransport, spellings: readonly string[]): SparkWireTransport {
  return (options) => {
    const channel = base(options);
    return {
      ...channel,
      executePlan: (request, call, limits) => {
        const stream = channel.executePlan(request, call, limits);
        const query = (request as { plan?: { command?: { sql_command?: { input?: { sql?: { query?: string } } } } } })
          .plan?.command?.sql_command?.input?.sql?.query;
        if (query !== "SHOW NAMESPACES IN `spark_catalog`") return stream;
        const data = Buffer.from(columnsStream([{ name: "namespace", kind: "utf8", values: spellings }]));
        return {
          read: async () => {
            const response = (await stream.read()) as
              | { readonly sql_command_result?: { readonly relation: { readonly local_relation: object } } }
              | undefined;
            if (response?.sql_command_result === undefined) return response;
            const { relation } = response.sql_command_result;
            return {
              ...response,
              sql_command_result: { relation: { ...relation, local_relation: { ...relation.local_relation, data } } },
            };
          },
          get receivedBytes() {
            return stream.receivedBytes;
          },
          get truncated() {
            return stream.truncated;
          },
          cancel: () => stream.cancel(),
        };
      },
    };
  };
}

describe("provenance", () => {
  test("every capture carries its service's pinned image and digest, and tests/fixtures/spark/README.md lists it", () => {
    const readme = readFileSync(join(FIXTURES, "README.md"), "utf8");
    let read = 0;
    for (const version of ["4.2.0", "4.1.3"] as const) {
      const section = readme.slice(readme.indexOf(`## ${version}`));
      for (const file of readdirSync(join(FIXTURES, version))) {
        const name = file.replace(/\.json$/, "") as SparkCaptureName;
        expect(SPARK_CAPTURE_NAMES).toContain(name);
        const { $captured } = readSparkCapture(version, name);
        expect($captured.sparkVersion).toBe(version);
        expect($captured.image).toBe(SPARK_SERVICE_IMAGES[$captured.service].image);
        expect($captured.digest).toBe(SPARK_SERVICE_IMAGES[$captured.service].digest);
        expect(section).toContain(`| \`${name}\` | ${$captured.service} | ${$captured.rpc} |`);
        expect(section).toContain(`| ${$captured.date} | ${$captured.runtime} |`);
        read += 1;
      }
    }
    expect(read).toBeGreaterThan(0);
  });
});

for (const version of ["4.2.0", "4.1.3"] as const) {
  describe(`replay on ${version}`, () => {
    test("connect: version, session setup, current catalog and namespace, the tokenless caution", async () => {
      const provider = await connected(version, []);
      expect(provider.isConnected()).toBe(true);
      expect(provider.connectWarnings()).toEqual([
        { message: SPARK_ERROR_SENTENCES.tokenlessCaution("127.0.0.1", 15002) },
      ]);
      await provider.disconnect();
    });

    test("a statement that turns large types on, then a SELECT of a string: both refused with the large-types sentence", async () => {
      const provider = await connected(version, [
        "large-var-types-set",
        "large-var-types-refused-command",
        "large-var-types-refused",
        // The Interrupt each refusal sends reads, masked, as the one cancel-interrupt captured (4.2.0 only); on 4.1.3
        // it finds no capture and the provider swallows that failure, as it swallows any failed Interrupt (T7.3).
        ...INTERRUPT[version],
      ]);
      // The SET's own inline answer arrives as LargeUtf8 (large-var-types-set, both versions; D03-C19).
      await expect(provider.query(LARGE_TYPES_SET, undefined, undefined, "s-large")).rejects.toThrow(
        SPARK_ERROR_SENTENCES.largeVarTypes,
      );
      await expect(provider.query(LARGE_TYPES_SELECT, undefined, undefined, "s-large")).rejects.toThrow(
        SPARK_ERROR_SENTENCES.largeVarTypes,
      );
      await provider.disconnect();
    });

    test("the every-type SELECT through both steps: 34 columns decoded and JSON-safe", async () => {
      const provider = await connected(version, ["every-type-command", "every-type-relation"]);
      const captured = readSparkCapture(version, "every-type-command").$captured.request as {
        readonly plan: {
          readonly command: { readonly sql_command: { readonly input: { readonly sql: { readonly query: string } } } };
        };
      };
      expect(captured.plan.command.sql_command.input.sql.query).toBe(EVERY_TYPE_SELECT);
      const result = await provider.query(EVERY_TYPE_SELECT);
      expect(result.fields).toHaveLength(34);
      expect(() => JSON.stringify(result.rows)).not.toThrow();
      await provider.disconnect();
    });

    test("TIME is refused with Studio's sentence after Spark's text", async () => {
      const provider = await connected(version, ["time-type-refused-command", "time-type-refused"]);
      await expect(provider.query(TIME_STATEMENT)).rejects.toThrow(SPARK_ERROR_SENTENCES.timeTypeOff);
      await provider.disconnect();
    });

    test("SHOW TABLES answers inline with no Spark job", async () => {
      const provider = await connected(version, ["show-tables-inline"]);
      const result = await provider.query(SHOW_TABLES_STATEMENT);
      expect(result.fields).toEqual(["namespace", "tableName", "isTemporary"]);
      await provider.disconnect();
    });

    test("Explain: EXPLAIN FORMATTED text becomes the plan tree", async () => {
      const provider = await connected(version, ["explain-formatted"]);
      const result = await provider.query(EXPLAIN_STATEMENT);
      expect(String(Object.values(result.rows[0])[0])).toContain("== Physical Plan ==");
      await provider.disconnect();
    });
  });
}

describe("replay on 4.2.0 only", () => {
  test("no token and a wrong token are refused with the two sentences of 3.3", async () => {
    const none = new SparkProvider(config(), {}, { transport: recordedSparkTransport("4.2.0", ["auth-no-token"]) });
    await expect(none.connect()).rejects.toBeInstanceOf(AuthenticationError);
    const wrong = new SparkProvider(
      config({ password: "not-the-token" }),
      {},
      { transport: recordedSparkTransport("4.2.0", ["auth-wrong-token"]) },
    );
    await expect(wrong.connect()).rejects.toThrow(SPARK_ERROR_SENTENCES.tokenWrong);
  });

  test("a FROM-led result of 100,001 rows is cut by the row budget, with its warning and one Interrupt", async () => {
    const transport = recordedSparkTransport("4.2.0", [
      ...CONNECT,
      ...STATEMENT_SESSION,
      "row-budget-cut-command",
      "row-budget-cut",
      "cancel-interrupt",
    ]);
    const provider = new SparkProvider(config(), {}, { transport });
    await provider.connect();
    const result = await provider.query(ROW_BUDGET_STATEMENT);
    expect(result.rows).toHaveLength(100_000);
    expect(result.pagination?.wasLimited).toBe(true);
    expect(result.warnings).toEqual([{ message: SPARK_CUT_WARNINGS.rows }]);
    // interrupt() swallows a failed call (T7.3), so only `served` shows the Interrupt matched its capture.
    expect(transport.served.filter((name) => name === "cancel-interrupt")).toEqual(["cancel-interrupt"]);
    await provider.disconnect();
  });

  test("a script in one scope: a temp view one statement makes, the next reads", async () => {
    const provider = await connected("4.2.0", ["temp-view-create", "temp-view-script-command", "temp-view-script"]);
    // oxlint-disable-next-line no-await-in-loop -- the script's statements run in order in one scope.
    for (const statement of TEMP_VIEW_SCRIPT) await provider.query(statement, undefined, undefined, "scope-script");
    await provider.endOpenQueryTransaction("scope-script");
    await provider.disconnect();
  });

  test("errors: a syntax error with ErrorInfo and one FetchErrorDetails, a missing table, a permission refusal's cause, maxResultSize", async () => {
    const transport = recordedSparkTransport("4.2.0", [
      ...CONNECT,
      ...STATEMENT_SESSION,
      "error-syntax",
      "error-syntax-details",
      "error-missing-table",
      "error-permission-denied",
      "error-permission-denied-details",
      "error-max-result-size-command",
      "error-max-result-size",
    ]);
    const provider = new SparkProvider(config(), {}, { transport });
    await provider.connect();
    await expect(provider.query(SYNTAX_STATEMENT)).rejects.toBeInstanceOf(QueryError);
    await expect(provider.query(MISSING_TABLE_STATEMENT)).rejects.toBeInstanceOf(QueryError);
    await expect(provider.query(PERMISSION_STATEMENT)).rejects.toThrow("Permission denied");
    await expect(provider.query(MAX_RESULT_STATEMENT)).rejects.toThrow(SPARK_ERROR_SENTENCES.maxResultSize);
    // Each FetchErrorDetails carried the errorId of the failure it details, so its capture answered it.
    expect(transport.served.filter((name) => name.endsWith("-details"))).toEqual([
      "error-syntax-details",
      "error-permission-denied-details",
    ]);
    await provider.disconnect();
  });

  test("substitution off: SELECT '${x}' after SET x=1 returns the text as written (D7)", async () => {
    const provider = await connected("4.2.0", ["substitution-set", "substitution-off-command", "substitution-off"]);
    await provider.query(SUBSTITUTION_SET, undefined, undefined, "scope-subst");
    const result = await provider.query(SUBSTITUTION_SELECT, undefined, undefined, "scope-subst");
    expect(result.rows).toEqual([{ v: "${x}" }]);
    await provider.disconnect();
  });

  test("a session SET of the codec to zstd, then the next result refused naming the codec (5.4)", async () => {
    const provider = await connected("4.2.0", [
      "compressed-zstd-set",
      "compressed-zstd-command",
      "compressed-zstd",
      // The Interrupt each refusal sends: a masked Interrupt by tag reads as the one cancel-interrupt captured.
      "cancel-interrupt",
    ]);
    // The SET's own inline answer is already a ZSTD batch (compressed-zstd-set), so it is refused as the SELECT is.
    await expect(provider.query(ZSTD_SET, undefined, undefined, "scope-zstd")).rejects.toThrow(
      SPARK_ERROR_SENTENCES.compressed("zstd"),
    );
    await expect(provider.query(ZSTD_SELECT, undefined, undefined, "scope-zstd")).rejects.toThrow(
      SPARK_ERROR_SENTENCES.compressed("zstd"),
    );
    await provider.disconnect();
  });

  test("the tree over the studio namespace's captures: catalogs, namespaces, objects, DESCRIBE and SHOW CREATE", async () => {
    const provider = await connected("4.2.0", [
      "show-catalogs",
      "config-catalog-keys",
      "show-namespaces",
      "show-tables",
      "show-views",
      "show-user-functions",
      "describe-table",
      "describe-table-struct30",
      "describe-view",
      "show-create-table",
      "show-create-view",
    ]);
    // The session catalog and the configured catalog key Spark has not loaded (config-catalog-keys).
    const catalogs = await provider.listContainers([]);
    expect(catalogs.map((container) => container.path)).toEqual([["spark_catalog"], ["studio_unloaded"]]);
    const namespaces = await provider.listContainers(["spark_catalog"]);
    expect(namespaces.map((container) => container.name)).toContain(SPARK_SAMPLE_NAMESPACE);
    const studio = ["spark_catalog", SPARK_SAMPLE_NAMESPACE];
    expect(await provider.countObjects(studio)).toEqual(STUDIO_COUNTS);
    const tables = await provider.listObjects(studio, "table");
    expect(tables.map((object) => object.name).sort()).toEqual(["every_type", "wide_struct"]);
    const views = await provider.listObjects(studio, "view");
    expect(views.map((object) => object.name)).toEqual(["every_type_view"]);
    const functions = await provider.listObjects(studio, "function");
    expect(functions.map((object) => object.name)).toEqual(["f1"]);
    for (const [name, kind] of [
      ["every_type", "table"],
      ["wide_struct", "table"],
      ["every_type_view", "view"],
    ] as const) {
      // oxlint-disable-next-line no-await-in-loop -- one read at a time on one provider.
      expect((await provider.describeObject([...studio, name], kind)).path).toEqual([...studio, name]);
    }
    for (const [name, kind] of [
      ["every_type", "table"],
      ["every_type_view", "view"],
    ] as const) {
      // oxlint-disable-next-line no-await-in-loop -- one read at a time on one provider.
      const [ddl] = (await provider.readObjectSource([...studio, name], kind)).parts;
      expect((ddl as { readonly text: string }).text).toContain(name);
    }
    await provider.disconnect();
  });

  test("BUILT: a nested namespace listing, the session catalog's SHOW NAMESPACES answer rewritten to multi-part spellings", async () => {
    const spellings = ["studio", "lake.raw", "lake.`x.y`"];
    const transport = withNamespaces(recordedSparkTransport("4.2.0", [...CONNECT, "show-namespaces"]), spellings);
    const provider = new SparkProvider(config(), {}, { transport });
    await provider.connect();
    const namespaces = await provider.listContainers(["spark_catalog"]);
    expect(namespaces.map((container) => container.path)).toEqual(
      spellings.map((spelling) => ["spark_catalog", spelling]),
    );
    await provider.disconnect();
  });

  test("health: one AnalyzePlan version read on the metadata session", async () => {
    const transport = recordedSparkTransport("4.2.0", CONNECT);
    const provider = new SparkProvider(config(), {}, { transport });
    await provider.connect();
    const before = transport.served.length;
    expect((await provider.getHealth()).slowQueries).toEqual([]);
    expect(transport.served.slice(before)).toEqual(["connect-version"]);
    await provider.disconnect();
  });

  test("a released session answers SESSION_CLOSED and the metadata session is re-created", async () => {
    const provider = await connected("4.2.0", ["session-closed", "show-catalogs", "config-catalog-keys"]);
    await expect(provider.listContainers([])).resolves.toBeDefined();
    await provider.disconnect();
  });

  test("BUILT: the version refusal, the 4.2.0 version answer rewritten to 3.5.9 and 4.0.4", async () => {
    for (const old of ["3.5.9", "4.0.4"]) {
      const provider = new SparkProvider(
        config(),
        {},
        { transport: withVersion(recordedSparkTransport("4.2.0", CONNECT), old) },
      );
      // oxlint-disable-next-line no-await-in-loop -- one connect at a time on one provider.
      await expect(provider.connect()).rejects.toThrow(SPARK_ERROR_SENTENCES.versionTooOld(old));
      // oxlint-disable-next-line no-await-in-loop -- one connect at a time on one provider.
      await expect(provider.connect()).rejects.toBeInstanceOf(ConnectionError);
    }
  });
});

/** What the studio namespace holds (SPARK_SAMPLE_STATEMENTS): two tables, one view over one of them, one function. */
const STUDIO_COUNTS = { table: { count: 2 }, view: { count: 1 }, function: { count: 1 } };
