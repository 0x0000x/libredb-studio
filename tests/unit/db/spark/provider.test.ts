/**
 * The Apache Spark provider through its public surface. Two seams reach the wire through the constructor's third
 * argument: the real adapter over the fake Spark Connect server, for wire behaviour, and a scripted channel, for the
 * edges a fake cannot hold open (a Config call that waits on the test, a ReleaseSession that answers UNIMPLEMENTED, an
 * ExecutePlan held until the test lets it answer). The scripted channel answers in the shapes the descriptor decodes:
 * field names as the protos spell them, and bytes as a Buffer.
 */
import { afterAll, beforeAll, describe, expect, spyOn, test } from "bun:test";
import { ConnectionError, DatabaseConfigError, QueryCancelledError, QueryError } from "@/lib/db/errors";
import { buildSparkConnectionOptions } from "@/lib/db/providers/sql/spark/connection-options";
import {
  SPARK_CONTROL_DEADLINE_MS,
  SPARK_HEARTBEAT_DEADLINE_MS,
  SPARK_HEARTBEAT_INTERVAL_MS,
  SPARK_REFUSED_SESSION_MAP_MAX,
  SPARK_VERSION_TEXT_MAX_CHARS,
} from "@/lib/db/providers/sql/spark/constants";
import { SPARK_ERROR_SENTENCES, SparkError } from "@/lib/db/providers/sql/spark/errors";
import {
  grpcWireTransport,
  type SparkPlanStream,
  type SparkWireCall,
  type SparkWireChannel,
  type SparkWireTransport,
} from "@/lib/db/providers/sql/spark/grpc-client";
import { SPARK_OBJECT_KINDS, SPARK_PROVIDER_SENTENCES, SparkProvider } from "@/lib/db/providers/sql/spark/index";
import { SPARK_INTROSPECT_SENTENCES } from "@/lib/db/providers/sql/spark/introspect";
import { SPARK_MONITORING_LABELS } from "@/lib/db/providers/sql/spark/monitoring";
import { releaseSessionRequest } from "@/lib/db/providers/sql/spark/requests";
import { SPARK_CUT_WARNINGS } from "@/lib/db/providers/sql/spark/result-stream";
import { SPARK_STATEMENT_SENTENCES } from "@/lib/db/providers/sql/spark/statement-shape";
import { type DatabaseProvider, DEFAULT_QUERY_TIMEOUT } from "@/lib/db/types";
import { MAX_UNLIMITED_ROWS } from "@/lib/db/utils/query-limiter";
import { logger } from "@/lib/logger";
import { type DatabaseConnection, TUNNEL_FAR_END, type TunnelFarEnd, type WithTunnelFarEnd } from "@/lib/types";
import { type SparkFakeAnswer, type SparkFakeServer, startSparkFake } from "../../../helpers/spark-connect-server";
import { columnsStream, int64Batch } from "../../../helpers/spark-ipc";

const CURRENT = columnsStream([
  { name: "catalog", kind: "utf8", values: ["spark_catalog"] },
  { name: "namespace", kind: "utf8", values: ["default"] },
]);
const ONE = columnsStream([{ name: "1", kind: "int32", values: [1] }]);
/** What a SET CATALOG or a USE NAMESPACE answers here; a setup statement's answer is read past, never decoded. */
const SET_ANSWER = columnsStream([{ name: "key", kind: "utf8", values: [] }]);
const DEFAULT = "`spark_catalog`.default";
const CATALOGS = columnsStream([{ name: "catalog", kind: "utf8", values: ["spark_catalog"] }]);
const T_DDL = "CREATE TABLE spark_catalog.default.t (\n  id BIGINT,\n  `#tag` STRING)\nUSING parquet\n";
const T_COLUMNS = [
  { name: "id", type: "bigint", nullable: true, isPrimary: false },
  { name: "#tag", type: "string", nullable: true, isPrimary: false },
];

/** SHOW TABLES or SHOW VIEWS in `spark_catalog`.default: every name in `default`, none temporary. */
const listing = (column: "tableName" | "viewName", names: readonly string[]) =>
  columnsStream([
    { name: "namespace", kind: "utf8", values: names.map(() => "default") },
    { name: column, kind: "utf8", values: [...names] },
    { name: "isTemporary", kind: "bool", values: names.map(() => false) },
  ]);

const FAKE_STATEMENTS: Record<string, SparkFakeAnswer> = {
  "SHOW CURRENT NAMESPACE": { kind: "command", ipc: CURRENT },
  "SELECT 1": { kind: "command", ipc: ONE },
  "CREATE TABLE x (id INT)": { kind: "command", ipc: new Uint8Array(0) },
  "BIG INLINE": { kind: "command", ipc: int64Batch(0, MAX_UNLIMITED_ROWS + 1) },
  "SHOW CATALOGS": { kind: "command", ipc: CATALOGS },
  "SHOW NAMESPACES IN `spark_catalog`": {
    kind: "command",
    ipc: columnsStream([{ name: "namespace", kind: "utf8", values: ["default", "studio"] }]),
  },
  [`SHOW VIEWS IN ${DEFAULT}`]: { kind: "command", ipc: listing("viewName", ["v"]) },
  // On the session catalog SHOW TABLES lists persistent views with the tables.
  [`SHOW TABLES IN ${DEFAULT}`]: { kind: "command", ipc: listing("tableName", ["t", "v"]) },
  [`SHOW USER FUNCTIONS IN ${DEFAULT}`]: {
    kind: "command",
    ipc: columnsStream([{ name: "function", kind: "utf8", values: ["spark_catalog.default.f1"] }]),
  },
  [`DESCRIBE TABLE ${DEFAULT}.\`t\``]: {
    kind: "command",
    ipc: columnsStream([
      { name: "col_name", kind: "utf8", values: ["id", "#tag", "", "# Detailed Table Information"] },
      { name: "data_type", kind: "utf8", values: ["bigint", "string", "", ""] },
      { name: "comment", kind: "utf8", values: [null, null, "", ""] },
    ]),
  },
  [`SHOW CREATE TABLE ${DEFAULT}.\`t\``]: {
    kind: "command",
    ipc: columnsStream([{ name: "createtab_stmt", kind: "utf8", values: [T_DDL] }]),
  },
  "SET CATALOG 'spark_catalog'": { kind: "command", ipc: SET_ANSWER },
  "USE NAMESPACE `default`": { kind: "command", ipc: SET_ANSWER },
  "USE NAMESPACE `missing`": {
    kind: "error",
    code: 13,
    message: "[SCHEMA_NOT_FOUND] The schema `missing` cannot be found.",
    errorInfo: { errorClass: "SCHEMA_NOT_FOUND", sqlState: "42704" },
  },
};

function connection(port: number, extra: Partial<DatabaseConnection> = {}): DatabaseConnection {
  return { id: `spark-${port}`, name: "spark", type: "spark", host: "127.0.0.1", port, ...extra } as DatabaseConnection;
}

/** A connection as the factory hands it over through an SSH tunnel: Host and Port name the tunnel's local forward. */
function tunnelled(id: string, localPort: number, farEnd: TunnelFarEnd): DatabaseConnection & WithTunnelFarEnd {
  const sshTunnel = { enabled: true, host: "bastion", port: 22, username: "u", authMethod: "password" } as const;
  return { ...connection(localPort, { id, sshTunnel }), [TUNNEL_FAR_END]: farEnd };
}

/** The SQL text of an ExecutePlan request: plan.command.sql_command.input.sql.query. */
const statementOf = (request: object): string | undefined =>
  (request as { plan?: { command?: { sql_command?: { input?: { sql?: { query?: string } } } } } }).plan?.command
    ?.sql_command?.input?.sql?.query;

/** Whether a Config request sets values; a decoded request carries every unset oneof member as null. */
const setsConfig = (request: object): boolean => {
  const set = (request as { operation?: { set?: unknown } }).operation?.set;
  return set !== null && set !== undefined;
};

const sessionOf = (request: object): string => String((request as { session_id?: unknown }).session_id);

interface ScriptedCall {
  readonly rpc: string;
  readonly request: object;
  readonly sessionId: string;
  readonly metadata: Readonly<Record<string, string>>;
  /** Date.now() when the channel received the call. */
  readonly at: number;
  /** The call's deadline, in epoch milliseconds. */
  readonly deadline: number;
}

interface Scripted {
  readonly transport: SparkWireTransport;
  readonly calls: readonly ScriptedCall[];
  readonly closed: () => number;
}

interface ScriptOptions {
  readonly version?: string;
  /** An answer or a failure for one unary call; undefined takes the default answer. */
  readonly unary?: (rpc: string, request: object, call: SparkWireCall) => Promise<object> | undefined;
  /** The stream of one ExecutePlan; undefined answers from `commands`, and a statement with no command ends empty. */
  readonly stream?: (sql: string, request: object, call: SparkWireCall) => SparkPlanStream | undefined;
  readonly commands?: Readonly<Record<string, Uint8Array>>;
  /** Run when the transport is asked for a channel, before it answers one. */
  readonly opening?: () => void;
}

/** A command's inline answer, as Spark sends it. */
const commandAnswer = (request: object, ipc: Uint8Array) => ({
  session_id: sessionOf(request),
  server_side_session_id: "server-1",
  sql_command_result: { relation: { local_relation: { data: Buffer.from(ipc) } } },
});

function streamOf(responses: readonly object[]): SparkPlanStream {
  let next = 0;
  return {
    read: async () => responses[next++],
    receivedBytes: 0,
    truncated: false,
    cancel: () => undefined,
  };
}

const failingStream = (error: unknown): SparkPlanStream => ({
  read: () => Promise.reject(error),
  receivedBytes: 0,
  truncated: false,
  cancel: () => undefined,
});

/** A channel that answers each unary RPC from `unary` (default: an empty answer) and each command from `commands`. */
function scripted(options: ScriptOptions = {}): Scripted {
  const calls: ScriptedCall[] = [];
  let closed = 0;
  const record = (rpc: string, request: object, call: SparkWireCall) => {
    calls.push({
      rpc,
      request,
      sessionId: sessionOf(request),
      metadata: call.metadata,
      at: Date.now(),
      deadline: call.deadline.getTime(),
    });
  };
  const channel: SparkWireChannel = {
    unary: async (rpc, request, call) => {
      record(rpc, request, call);
      const scriptedAnswer = options.unary?.(rpc, request, call);
      if (scriptedAnswer !== undefined) return scriptedAnswer;
      const base = { session_id: sessionOf(request), server_side_session_id: "server-1" };
      if (rpc === "AnalyzePlan") return { ...base, spark_version: { version: options.version ?? "4.2.0" } };
      if (rpc === "Config") return { ...base, pairs: [], warnings: [] };
      if (rpc === "Interrupt") return { ...base, interrupted_ids: [] };
      return base;
    },
    executePlan: (request, call) => {
      record("ExecutePlan", request, call);
      const sql = statementOf(request) ?? "";
      const stream = options.stream?.(sql, request, call);
      if (stream !== undefined) return stream;
      const answer = options.commands?.[sql];
      return streamOf(answer === undefined ? [] : [commandAnswer(request, answer)]);
    },
    close: () => {
      closed += 1;
    },
  };
  return {
    transport: () => {
      options.opening?.();
      return channel;
    },
    calls,
    closed: () => closed,
  };
}

/** The scripted transport with the channels it opens told apart: `closed()` answers, per open in order, whether it closed. */
function channelsOf(script: Scripted): { readonly transport: SparkWireTransport; readonly closed: () => boolean[] } {
  const opens: { closed: boolean }[] = [];
  return {
    transport: (options) => {
      const channel = script.transport(options);
      const open = { closed: false };
      opens.push(open);
      return {
        ...channel,
        close: () => {
          open.closed = true;
          channel.close();
        },
      };
    },
    closed: () => opens.map((open) => open.closed),
  };
}

/** A promise the test settles. */
function gate() {
  let open: () => void = () => undefined;
  let fail: (error: unknown) => void = () => undefined;
  const promise = new Promise<void>((resolve, reject) => {
    open = resolve;
    fail = reject;
  });
  promise.catch(() => undefined);
  return { promise, open: () => open(), fail: (error: unknown) => fail(error) };
}

/** grpc-js's own failure for a call its client cancelled. */
const clientCancel = () =>
  Object.assign(new Error("1 CANCELLED: Cancelled on client"), { code: 1, details: "Cancelled on client" });
const deadlineStatus = () =>
  Object.assign(new Error("4 DEADLINE_EXCEEDED: Deadline exceeded"), { code: 4, details: "Deadline exceeded" });
const unavailable = () =>
  Object.assign(new Error("14 UNAVAILABLE: No connection established"), {
    code: 14,
    details: "No connection established",
  });
const unimplemented = () =>
  Object.assign(new Error("12 UNIMPLEMENTED: Method not found"), { code: 12, details: "Method not found" });

/** A command answer held until `until` settles; the call's signal ends it as grpc-js ends a cancelled call. */
function heldStream(request: object, call: SparkWireCall, ipc: Uint8Array, until: Promise<void>): SparkPlanStream {
  const aborted = new Promise<never>((_, reject) => {
    if (call.signal.aborted) reject(clientCancel());
    else call.signal.addEventListener("abort", () => reject(clientCancel()), { once: true });
  });
  aborted.catch(() => undefined);
  let reads = 0;
  return {
    read: async () => {
      reads += 1;
      if (reads > 1) return undefined;
      await Promise.race([until, aborted]);
      return commandAnswer(request, ipc);
    },
    receivedBytes: 0,
    truncated: false,
    cancel: () => undefined,
  };
}

/** Waits, one macrotask at a time, until `check` holds. */
async function eventually(check: () => boolean): Promise<void> {
  for (let turn = 0; turn < 500; turn += 1) {
    if (check()) return;
    // oxlint-disable-next-line no-await-in-loop -- one macrotask per turn lets the calls the test waits for arrive.
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("the condition the test waited for never held");
}

const failureOf = (pending: Promise<unknown>): Promise<unknown> =>
  pending.then(
    () => undefined,
    (error: unknown) => error,
  );

let fake: SparkFakeServer;
let port: number;
beforeAll(async () => {
  fake = await startSparkFake({ progressEveryMs: 0, statements: FAKE_STATEMENTS });
  port = Number(fake.target.split(":").at(-1));
});
afterAll(() => fake.close());

describe("SparkProvider: construction and declarations", () => {
  test("the constructor validates nothing and opens nothing", () => {
    const script = scripted();
    const provider = new SparkProvider({ id: "x", name: "x", type: "spark" } as DatabaseConnection, {}, script);
    expect(script.calls).toStrictEqual([]);
    expect(provider.isConnected()).toBe(false);
    expect(provider.hasRunsInFlight()).toBe(false);
    expect(provider.connectWarnings()).toStrictEqual([]);
  });

  test("the capabilities literal, field by field", () => {
    const capabilities = new SparkProvider(connection(1)).getCapabilities();
    expect(capabilities).toStrictEqual({
      queryLanguage: "sql",
      supportsExplain: true,
      explainFormat: "spark-text",
      supportsExternalQueryLimiting: true,
      supportsCreateTable: false,
      supportsInlineRowEdit: false,
      supportsTestDataGeneration: false,
      supportsResultPagination: false,
      supportsColumnProfiling: false,
      supportsTransactions: false,
      supportsSqlFormat: false,
      declaresForeignKeys: false,
      supportsMaintenance: false,
      maintenanceOperations: [],
      supportsConnectionString: false,
      defaultPort: 15002,
      identifierQuoting: "backtick-always",
      containerNameSpelling: "engine",
      schemaRefreshPattern: "^\\s*(CREATE|DROP|ALTER|REPLACE|REPAIR|MSCK)\\b",
      containerLevels: [
        { id: "catalog", label: "Catalog", labelPlural: "Catalogs" },
        { id: "schema", label: "Namespace", labelPlural: "Namespaces" },
      ],
      containerPathShapes: "exact",
      objectKinds: SPARK_OBJECT_KINDS,
      agentWalkScope: { mode: "session-default", whenNoDefault: { walk: "parent-default-children" } },
    });
    expect(SPARK_OBJECT_KINDS).toStrictEqual([
      {
        id: "table",
        role: "relation",
        label: "Table",
        labelPlural: "Tables",
        hasColumns: true,
        hasSource: true,
        sourceLanguage: "sql",
      },
      {
        id: "view",
        role: "relation",
        label: "View",
        labelPlural: "Views",
        hasColumns: true,
        hasSource: true,
        sourceLanguage: "sql",
      },
      {
        id: "function",
        role: "routine",
        label: "Function",
        labelPlural: "Functions",
        hasColumns: false,
        hasSource: false,
      },
    ]);
  });

  test("the labels: Spark SQL, the two cards worded true, the monitoring empty states", () => {
    const labels = new SparkProvider(connection(1)).getLabels();
    // Every other label is the base wording for tables and rows.
    expect(labels).toMatchObject({
      entityName: "Table",
      rowName: "row",
      statementLanguage: "Spark SQL",
      analyzeGlobalTitle: "Not Run from Studio",
      vacuumGlobalTitle: "Not Run from Studio",
      analyzeGlobalDesc: SPARK_PROVIDER_SENTENCES.analyzeGlobalDesc,
      vacuumGlobalDesc: SPARK_PROVIDER_SENTENCES.vacuumGlobalDesc,
      slowQueriesEmptyState: SPARK_MONITORING_LABELS.slowQueriesEmptyState,
      sessionsEmptyState: SPARK_MONITORING_LABELS.sessionsEmptyState,
    });
    expect(SPARK_PROVIDER_SENTENCES).toStrictEqual({
      maintenanceRefused: "Apache Spark has no maintenance operations in Studio.",
      analyzeGlobalDesc:
        "Spark computes table statistics with ANALYZE TABLE, which runs a job on the cluster; Studio does not run it in this version.",
      vacuumGlobalDesc:
        "Spark has no VACUUM of its own; table formats such as Delta and Iceberg run their own maintenance.",
    });
  });
});

describe("SparkProvider: connect, disconnect and warnings over the fake", () => {
  test("connect reads the version on a probe session it releases, then sets up the metadata session and reads the current namespace", async () => {
    fake.log.clear();
    const provider = new SparkProvider(connection(port));
    await provider.connect();
    expect(provider.isConnected()).toBe(true);
    expect(provider.connectWarnings()).toStrictEqual([
      { message: SPARK_ERROR_SENTENCES.tokenlessCaution("127.0.0.1", port) },
    ]);
    await provider.disconnect();
    await provider.disconnect();
    expect(provider.isConnected()).toBe(false);
    expect(provider.hasRunsInFlight()).toBe(false);
    expect(provider.connectWarnings()).toStrictEqual([]);
    const entries = fake.log.entries;
    const [probe, metadata] = [entries[0]?.sessionId, entries[2]?.sessionId];
    expect(entries.map((entry) => [entry.rpc, entry.sessionId])).toStrictEqual([
      ["AnalyzePlan", probe],
      ["ReleaseSession", probe],
      ["Config", metadata],
      ["ExecutePlan", metadata],
      ["ReleaseSession", metadata],
    ]);
    expect(probe).not.toBe(metadata);
    expect(statementOf(entries[3]?.request ?? {})).toBe("SHOW CURRENT NAMESPACE");
  });

  test("Catalog and Namespace set every session to them before its first use, the metadata session included", async () => {
    fake.log.clear();
    const provider = new SparkProvider(connection(port, { database: "spark_catalog", schema: "default" }));
    try {
      await provider.connect();
      expect((await provider.query("SELECT 1", undefined, "q-place", "scope-place")).rows).toStrictEqual([{ "1": 1 }]);
      await provider.endOpenQueryTransaction("scope-place");
    } finally {
      await provider.disconnect();
    }
    const sent = fake.log.entries
      .filter((entry) => entry.rpc === "Config" || entry.rpc === "ExecutePlan")
      .map((entry) => [entry.sessionId, entry.rpc === "Config" ? "Config" : statementOf(entry.request)]);
    const metadata = sent[0]?.[0];
    const scope = sent[4]?.[0];
    expect(sent).toStrictEqual([
      [metadata, "Config"],
      [metadata, "SET CATALOG 'spark_catalog'"],
      [metadata, "USE NAMESPACE `default`"],
      [metadata, "SHOW CURRENT NAMESPACE"],
      [scope, "Config"],
      [scope, "SET CATALOG 'spark_catalog'"],
      [scope, "USE NAMESPACE `default`"],
      [scope, "SELECT 1"],
    ]);
    expect(scope).not.toBe(metadata);
  });

  test("a setup answer from another server-side session fails connect as a restarted server", async () => {
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      stream: (sql, request) =>
        sql === "SET CATALOG 'spark_catalog'"
          ? streamOf([{ ...commandAnswer(request, SET_ANSWER), server_side_session_id: "server-2" }])
          : undefined,
    });
    const provider = new SparkProvider(connection(1, { database: "spark_catalog" }), {}, script);
    const failure = await failureOf(provider.connect());
    expect(failure).toBeInstanceOf(QueryError);
    expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.sessionChanged);
    expect(script.calls.some((call) => statementOf(call.request) === "SHOW CURRENT NAMESPACE")).toBe(false);
  });

  test("a setup statement Spark refuses fails connect with Spark's text and releases the session it opened", async () => {
    fake.log.clear();
    const provider = new SparkProvider(connection(port, { schema: "missing" }));
    const failure = await failureOf(provider.connect());
    expect(failure).toBeInstanceOf(QueryError);
    expect((failure as Error).message).toBe("[SCHEMA_NOT_FOUND] The schema `missing` cannot be found.");
    expect(provider.isConnected()).toBe(false);
    const metadata = fake.log.entries.find((entry) => entry.rpc === "Config")?.sessionId;
    expect(
      fake.log.entries.filter((entry) => entry.rpc === "ReleaseSession").map((entry) => entry.sessionId),
    ).toContain(metadata as string);
  });

  test("a query runs in its scope's session, and endOpenQueryTransaction releases that session and answers none", async () => {
    const provider = new SparkProvider(connection(port));
    await provider.connect();
    try {
      fake.log.clear();
      const result = await provider.query("SELECT 1", undefined, "q1", "scope-1");
      expect(result.rows).toStrictEqual([{ "1": 1 }]);
      expect(result.fields).toStrictEqual(["1"]);
      expect(result.rowCount).toBe(1);
      expect(result.columnTypes).toStrictEqual({ "1": "int" });
      expect(result.pagination).toBeUndefined();
      expect(result.warnings).toBeUndefined();
      const scope = fake.log.entries.find((entry) => entry.rpc === "ExecutePlan")?.sessionId;
      expect(await provider.endOpenQueryTransaction("scope-1")).toBe("none");
      expect(
        fake.log.entries.filter((entry) => entry.rpc === "ReleaseSession").map((entry) => entry.sessionId),
      ).toStrictEqual([scope as string]);
      expect(await provider.endOpenQueryTransaction(undefined)).toBe("none");
      expect(await provider.endOpenQueryTransaction("scope-never-used")).toBe("none");
      expect(fake.log.entries.filter((entry) => entry.rpc === "ReleaseSession")).toHaveLength(1);
    } finally {
      await provider.disconnect();
    }
    expect(await provider.endOpenQueryTransaction("scope-1")).toBe("none");
  });

  test("a statement that answers no column carries no column types", async () => {
    const provider = new SparkProvider(connection(port));
    await provider.connect();
    try {
      const result = await provider.query("CREATE TABLE x (id INT)");
      expect({ ...result, executionTime: 0 }).toStrictEqual({ fields: [], rows: [], rowCount: 0, executionTime: 0 });
    } finally {
      await provider.disconnect();
    }
  });

  test("a cut result carries its warning and pagination.wasLimited, with the rows before the cut", async () => {
    const provider = new SparkProvider(connection(port));
    await provider.connect();
    try {
      fake.log.clear();
      const result = await provider.query("BIG INLINE", undefined, "q-cut");
      expect(result.rowCount).toBe(MAX_UNLIMITED_ROWS);
      expect(result.rows).toHaveLength(MAX_UNLIMITED_ROWS);
      expect(result.rows[MAX_UNLIMITED_ROWS - 1]).toStrictEqual({ id: MAX_UNLIMITED_ROWS - 1 });
      expect(result.columnTypes).toStrictEqual({ id: "bigint" });
      expect(result.warnings).toStrictEqual([{ message: SPARK_CUT_WARNINGS.rows }]);
      expect(result.pagination).toStrictEqual({
        limit: MAX_UNLIMITED_ROWS,
        offset: 0,
        hasMore: false,
        totalReturned: MAX_UNLIMITED_ROWS,
        wasLimited: true,
      });
      // A cut asks Spark to stop the statement, once, by the run's tag.
      const [execute] = fake.log.entries.filter((entry) => entry.rpc === "ExecutePlan");
      const interrupts = fake.log.entries.filter((entry) => entry.rpc === "Interrupt");
      expect(interrupts.map((entry) => (entry.request as { operation_tag?: string }).operation_tag)).toStrictEqual(
        execute?.tags as string[],
      );
    } finally {
      await provider.disconnect();
    }
  });

  test("testConnectionWarnings: nothing sent without a token; with one, an unchecked call on a fresh session, released", async () => {
    const plain = new SparkProvider(connection(port));
    await plain.connect();
    fake.log.clear();
    expect(await plain.testConnectionWarnings()).toStrictEqual([]);
    expect(fake.log.entries).toStrictEqual([]);
    await plain.disconnect();

    const tokened = new SparkProvider(connection(port, { password: "t0ken" }));
    await tokened.connect();
    try {
      expect(tokened.connectWarnings()).toStrictEqual([]);
      fake.log.clear();
      expect(await tokened.testConnectionWarnings()).toStrictEqual([
        { message: SPARK_ERROR_SENTENCES.tokenUnchecked("127.0.0.1", port) },
      ]);
      const sent = fake.log.entries.map((entry) => ({
        rpc: entry.rpc,
        sessionId: entry.sessionId,
        authorized: "authorization" in entry.metadata,
      }));
      const probe = sent[0]?.sessionId as string;
      expect(sent).toStrictEqual([
        { rpc: "AnalyzePlan", sessionId: probe, authorized: false },
        { rpc: "ReleaseSession", sessionId: probe, authorized: false },
      ]);
      // The connection's own calls carry the token, on its metadata session, never the unchecked call's.
      fake.log.clear();
      await tokened.getHealth();
      expect(fake.log.entries).toHaveLength(1);
      const [pulse] = fake.log.entries;
      expect(pulse?.metadata.authorization).toBe("Bearer t0ken");
      expect(pulse?.sessionId).not.toBe(probe);
    } finally {
      await tokened.disconnect();
    }
  });

  test("testConnectionWarnings answers [] when the server refuses a call without the token", async () => {
    const guarded = await startSparkFake({
      token: "t0ken",
      progressEveryMs: 0,
      statements: { "SHOW CURRENT NAMESPACE": { kind: "command", ipc: CURRENT } },
    });
    try {
      const provider = new SparkProvider(connection(Number(guarded.target.split(":").at(-1)), { password: "t0ken" }));
      await provider.connect();
      expect(await provider.testConnectionWarnings()).toStrictEqual([]);
      await provider.disconnect();
    } finally {
      guarded.close();
    }
  });

  test("testConnectionWarnings keeps its caution when the release of the unchecked call's session fails", async () => {
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      unary: (rpc, _request, call) =>
        rpc === "ReleaseSession" && call.metadata.authorization === undefined
          ? Promise.reject(unavailable())
          : undefined,
    });
    const provider = new SparkProvider(connection(1, { password: "t0ken" }), {}, script);
    await provider.connect();
    expect(await provider.testConnectionWarnings()).toStrictEqual([
      { message: SPARK_ERROR_SENTENCES.tokenUnchecked("127.0.0.1", 1) },
    ]);
    await provider.disconnect();
  });
});

describe("SparkProvider: the object surface on the metadata session", () => {
  test("every object method reads through statements sent on the metadata session only", async () => {
    const provider = new SparkProvider(connection(port));
    fake.log.clear();
    await provider.connect();
    const metadata = fake.log.entries.find((entry) => entry.rpc === "Config")?.sessionId;
    try {
      expect(await provider.listContainers()).toStrictEqual([
        { path: ["spark_catalog"], name: "spark_catalog", level: 0, isSessionDefault: true },
      ]);
      expect(await provider.listContainers(["spark_catalog"])).toStrictEqual([
        { path: ["spark_catalog", "default"], name: "default", level: 1, isSessionDefault: true },
        { path: ["spark_catalog", "studio"], name: "studio", level: 1 },
      ]);
      expect(await provider.listContainers(["spark_catalog", "default"])).toStrictEqual([]);
      expect(await provider.countObjects(["spark_catalog", "default"])).toStrictEqual({
        table: { count: 1 },
        view: { count: 1 },
        function: { count: 1 },
      });
      expect(await provider.listObjects(["spark_catalog", "default"], "table")).toStrictEqual([
        { path: ["spark_catalog", "default", "t"], name: "t", kind: "table" },
      ]);
      const detail = { path: ["spark_catalog", "default", "t"], columns: T_COLUMNS, indexes: [], foreignKeys: [] };
      expect(await provider.describeObject(["spark_catalog", "default", "t"], "table")).toStrictEqual(detail);
      expect(await provider.describeObjects(["spark_catalog", "default"], "table", 5)).toStrictEqual({
        details: [detail],
      });
      expect(await provider.readObjectSource(["spark_catalog", "default", "t"], "table")).toStrictEqual({
        path: ["spark_catalog", "default", "t"],
        kind: "table",
        parts: [
          { id: "ddl", label: "Definition", text: T_DDL, language: "sql", form: "complete", origin: "regenerated" },
        ],
      });
      expect(await provider.getOverview()).toStrictEqual({
        version: "4.2.0",
        uptime: "N/A",
        maxConnections: 0,
        databaseSize: "N/A",
        tableCount: 1,
        tableCountSampledFrom: "the tables of spark_catalog.default, the connection's current namespace",
        indexCount: 0,
      });
    } finally {
      await provider.disconnect();
    }
    const reads = fake.log.entries.filter(
      (entry) => entry.rpc === "ExecutePlan" || entry.rpc === "Config" || entry.rpc === "AnalyzePlan",
    );
    // The probe's version read is the one call before the metadata session exists.
    expect(reads.slice(1).every((entry) => entry.sessionId === metadata)).toBe(true);
  });
});

describe("SparkProvider: refusals before any call", () => {
  test("bound parameters, a script block, a Unicode space and an address Studio reads differently are refused, and nothing is sent", async () => {
    const script = scripted({ commands: { "SHOW CURRENT NAMESPACE": CURRENT, "SELECT 1": ONE } });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    const sent = script.calls.length;
    await expect(provider.query("SELECT ?", [1])).rejects.toThrow(SPARK_STATEMENT_SENTENCES.boundParameters);
    await expect(provider.query("BEGIN SELECT 1; END")).rejects.toThrow(SPARK_STATEMENT_SENTENCES.scriptBlock);
    await expect(provider.query("SELECT 1")).rejects.toThrow(SPARK_STATEMENT_SENTENCES.unicodeSpace);
    await expect(provider.query("ADD JAR hdfs://nn/a--b.jar")).rejects.toThrow(SPARK_STATEMENT_SENTENCES.uriForm);
    const bound = await failureOf(provider.query("SELECT ?", [1]));
    expect(bound).toBeInstanceOf(QueryError);
    expect([(bound as QueryError).provider, (bound as QueryError).query]).toStrictEqual(["spark", "SELECT ?"]);
    const block = await failureOf(provider.query("BEGIN SELECT 1; END"));
    expect(block).toBeInstanceOf(QueryError);
    expect([(block as QueryError).provider, (block as QueryError).query]).toStrictEqual([
      "spark",
      "BEGIN SELECT 1; END",
    ]);
    expect(script.calls.length).toBe(sent);
    // An empty parameter list carries no bound parameter, so the statement runs.
    expect((await provider.query("SELECT 1", [])).rows).toStrictEqual([{ "1": 1 }]);
    await provider.disconnect();
  });

  test("runMaintenance is refused, and each object method refuses a wrong-depth path or an unknown kind before any statement", async () => {
    const script = scripted({ commands: { "SHOW CURRENT NAMESPACE": CURRENT } });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    const sent = script.calls.length;
    // Called as the maintenance route calls any provider, with an operation.
    await expect((provider as DatabaseProvider).runMaintenance("analyze")).rejects.toThrow(
      SPARK_PROVIDER_SENTENCES.maintenanceRefused,
    );
    const containerShape = "An Apache Spark container path is [catalog, namespace]";
    await expect(provider.listObjects(["spark_catalog"], "table")).rejects.toThrow(containerShape);
    await expect(provider.countObjects(["spark_catalog", "default", "x"])).rejects.toThrow(containerShape);
    await expect(provider.describeObjects(["spark_catalog"], "table")).rejects.toThrow(containerShape);
    const objectShape = 'An Apache Spark "table" path is [catalog, namespace, name]';
    await expect(provider.describeObject(["spark_catalog", "t"], "table")).rejects.toThrow(objectShape);
    await expect(provider.readObjectSource(["spark_catalog", "default"], "view")).rejects.toThrow(
      'An Apache Spark "view" path is [catalog, namespace, name]',
    );
    await expect(provider.describeObject(["spark_catalog", "default", "t"], "index")).rejects.toThrow(
      SPARK_INTROSPECT_SENTENCES.noKind("index"),
    );
    await expect(provider.readObjectSource(["spark_catalog", "default", "t"], "index")).rejects.toBeInstanceOf(
      QueryError,
    );
    expect(script.calls.length).toBe(sent);
    expect(await provider.listContainers(["spark_catalog", "default"])).toStrictEqual([]);
    await provider.disconnect();
  });

  test("a config the option checks refuse fails connect as a DatabaseConfigError with nothing dialled", async () => {
    let dialled = 0;
    const script = scripted({
      opening: () => {
        dialled += 1;
      },
    });
    const provider = new SparkProvider(connection(1, { user: "alice" }), {}, script);
    await expect(provider.connect()).rejects.toBeInstanceOf(DatabaseConfigError);
    expect(dialled).toBe(0);
    expect(script.calls).toStrictEqual([]);
    expect(provider.isConnected()).toBe(false);
  });

  test("a transport that cannot open fails connect with the client's sentence and repeats nothing of its error", async () => {
    const script = scripted({
      opening: () => {
        throw new Error("the channel could not open with authorization Bearer t0ken");
      },
    });
    const provider = new SparkProvider(connection(1, { password: "t0ken" }), {}, script);
    const failure = await failureOf(provider.connect());
    expect(failure).toBeInstanceOf(QueryError);
    expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.localClientError);
    expect(script.calls).toStrictEqual([]);
    expect(provider.isConnected()).toBe(false);
  });
});

describe("SparkProvider: the version floor and the probe session", () => {
  test("two refused connects of two instances for one connection send one probe session id to a server without ReleaseSession", async () => {
    const script = scripted({
      version: "3.5.9",
      unary: (rpc) => (rpc === "ReleaseSession" ? Promise.reject(unimplemented()) : undefined),
    });
    const config = connection(1, { id: "probe-35" });
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const provider = new SparkProvider(config, {}, script);
      // oxlint-disable-next-line no-await-in-loop -- the connects are sequential on purpose.
      await expect(provider.connect()).rejects.toThrow(SPARK_ERROR_SENTENCES.versionTooOld("3.5.9"));
      // oxlint-disable-next-line no-await-in-loop -- the connects are sequential on purpose.
      await expect(provider.connect()).rejects.toBeInstanceOf(ConnectionError);
      expect(provider.isConnected()).toBe(false);
    }
    const probes = new Set(script.calls.filter((call) => call.rpc === "AnalyzePlan").map((call) => call.sessionId));
    expect(probes.size).toBe(1);
    expect(script.calls.filter((call) => call.rpc === "AnalyzePlan")).toHaveLength(4);
    expect(script.calls.some((call) => call.rpc === "Config" || call.rpc === "ExecutePlan")).toBe(false);
    expect(script.closed()).toBe(4);
  });

  test("a refused connect against a server that offers ReleaseSession releases its probe session", async () => {
    const script = scripted({ version: "4.0.4" });
    const provider = new SparkProvider(connection(1, { id: "probe-40" }), {}, script);
    await expect(provider.connect()).rejects.toThrow(SPARK_ERROR_SENTENCES.versionTooOld("4.0.4"));
    await expect(provider.connect()).rejects.toThrow(SPARK_ERROR_SENTENCES.versionTooOld("4.0.4"));
    const probes = script.calls.filter((call) => call.rpc === "AnalyzePlan").map((call) => call.sessionId);
    expect(probes).toHaveLength(2);
    // A released id is closed on the server, so the next connect reads the version on a new one.
    expect(probes[1]).not.toBe(probes[0]);
    expect(script.calls.filter((call) => call.rpc === "ReleaseSession").map((call) => call.sessionId)).toStrictEqual(
      probes,
    );
  });

  test("a remembered probe that a later refused connect releases is forgotten, so no connect reads on a released id", async () => {
    let offersRelease = false;
    let version = "3.5.9";
    const script = scripted({
      unary: (rpc) => {
        if (rpc === "AnalyzePlan") return Promise.resolve({ spark_version: { version } });
        return rpc === "ReleaseSession" && !offersRelease ? Promise.reject(unimplemented()) : undefined;
      },
    });
    const config = connection(1, { id: "probe-upgraded" });
    await expect(new SparkProvider(config, {}, script).connect()).rejects.toThrow(
      SPARK_ERROR_SENTENCES.versionTooOld("3.5.9"),
    );
    // The server now runs 4.0.4, which offers ReleaseSession and is still below the floor.
    offersRelease = true;
    version = "4.0.4";
    await expect(new SparkProvider(config, {}, script).connect()).rejects.toThrow(
      SPARK_ERROR_SENTENCES.versionTooOld("4.0.4"),
    );
    await expect(new SparkProvider(config, {}, script).connect()).rejects.toThrow(
      SPARK_ERROR_SENTENCES.versionTooOld("4.0.4"),
    );
    const probes = script.calls.filter((call) => call.rpc === "AnalyzePlan").map((call) => call.sessionId);
    expect(probes).toHaveLength(3);
    expect(probes[1]).toBe(probes[0] as string);
    expect(probes[2]).not.toBe(probes[0] as string);
  });

  test("a connect that meets the floor forgets the remembered probe, even when the probe's release fails", async () => {
    let version = "3.5.9";
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      unary: (rpc, request) => {
        if (rpc === "AnalyzePlan") return Promise.resolve({ spark_version: { version } });
        if (rpc !== "ReleaseSession") return undefined;
        if (version === "3.5.9") return Promise.reject(unimplemented());
        // On the upgraded server only the probe's release fails.
        return sessionOf(request) === firstProbe() ? Promise.reject(unavailable()) : undefined;
      },
    });
    const probes = () => script.calls.filter((call) => call.rpc === "AnalyzePlan").map((call) => call.sessionId);
    const firstProbe = () => probes()[0];
    const config = connection(1, { id: "probe-floor-met" });
    await expect(new SparkProvider(config, {}, script).connect()).rejects.toBeInstanceOf(ConnectionError);
    version = "4.2.0";
    const upgraded = new SparkProvider(config, {}, script);
    await upgraded.connect();
    await upgraded.disconnect();
    version = "3.5.9";
    await expect(new SparkProvider(config, {}, script).connect()).rejects.toBeInstanceOf(ConnectionError);
    const [first, second, third] = probes();
    expect(second).toBe(first as string);
    expect(third).not.toBe(first as string);
  });

  test("the remembered probe sessions are bounded: past the bound the oldest is dropped", async () => {
    const script = scripted({
      version: "3.5.9",
      unary: (rpc) => (rpc === "ReleaseSession" ? Promise.reject(unimplemented()) : undefined),
    });
    const refuse = async (id: string): Promise<string> => {
      const before = script.calls.length;
      await expect(new SparkProvider(connection(1, { id }), {}, script).connect()).rejects.toBeInstanceOf(
        ConnectionError,
      );
      return script.calls.slice(before).find((call) => call.rpc === "AnalyzePlan")?.sessionId as string;
    };
    const first: string[] = [];
    for (let index = 0; index <= SPARK_REFUSED_SESSION_MAP_MAX; index += 1) {
      // oxlint-disable-next-line no-await-in-loop -- each connect is remembered in order, so they run one at a time.
      first.push(await refuse(`bound-${index}`));
    }
    expect(new Set(first).size).toBe(SPARK_REFUSED_SESSION_MAP_MAX + 1);
    // The second oldest is still remembered; the oldest was dropped, so its next connect uses a new session id.
    expect(await refuse("bound-1")).toBe(first[1] as string);
    const again = await refuse("bound-0");
    expect(first.includes(again)).toBe(false);
  });

  test("refused connects through a tunnel whose local port changes on every attempt read the version on one probe session id", async () => {
    const script = scripted({
      version: "3.5.9",
      unary: (rpc) => (rpc === "ReleaseSession" ? Promise.reject(unimplemented()) : undefined),
    });
    const farEnd = { host: "spark.remote", port: 15002 };
    // Each attempt's tunnel listens on a new local port; the far end stays the same.
    for (const localPort of [40001, 40002]) {
      const provider = new SparkProvider(tunnelled("probe-tunnel", localPort, farEnd), {}, script);
      // oxlint-disable-next-line no-await-in-loop -- the connects are sequential on purpose.
      await expect(provider.connect()).rejects.toThrow(SPARK_ERROR_SENTENCES.versionTooOld("3.5.9"));
    }
    const probes = script.calls.filter((call) => call.rpc === "AnalyzePlan").map((call) => call.sessionId);
    expect(probes).toHaveLength(2);
    expect(probes[1]).toBe(probes[0] as string);
  });

  test("refused connects of one connection to different far ends through one local port use different probe session ids", async () => {
    const script = scripted({
      version: "3.5.9",
      unary: (rpc) => (rpc === "ReleaseSession" ? Promise.reject(unimplemented()) : undefined),
    });
    const probeOf = async (farEnd: TunnelFarEnd): Promise<string> => {
      const before = script.calls.length;
      const provider = new SparkProvider(tunnelled("probe-far-ends", 40001, farEnd), {}, script);
      await expect(provider.connect()).rejects.toBeInstanceOf(ConnectionError);
      return script.calls.slice(before).find((call) => call.rpc === "AnalyzePlan")?.sessionId as string;
    };
    const probes = [
      await probeOf({ host: "spark.remote", port: 15002 }),
      await probeOf({ host: "spark.other", port: 15002 }),
      await probeOf({ host: "spark.remote", port: 15003 }),
      // Run together, this host and port would spell the first far end's host and port.
      await probeOf({ host: "spark.remote1", port: 5002 }),
    ];
    expect(new Set(probes).size).toBe(4);
  });

  test("refused connects of two connections whose id and host would run together use different probe session ids", async () => {
    const script = scripted({
      version: "3.5.9",
      unary: (rpc) => (rpc === "ReleaseSession" ? Promise.reject(unimplemented()) : undefined),
    });
    const probeOf = async (id: string, host: string): Promise<string> => {
      const before = script.calls.length;
      const provider = new SparkProvider(connection(15002, { id, host }), {}, script);
      await expect(provider.connect()).rejects.toThrow(SPARK_ERROR_SENTENCES.versionTooOld("3.5.9"));
      return script.calls.slice(before).find((call) => call.rpc === "AnalyzePlan")?.sessionId as string;
    };
    const probes = [
      await probeOf("probe-x", "1a.remote"),
      // Run together, this id and host would spell the first connection's id and host.
      await probeOf("probe-x1", "a.remote"),
    ];
    expect(new Set(probes).size).toBe(2);
  });

  test("a failed release of the metadata session logs one warning holding only its category, never the server's text or the token", async () => {
    let failRelease = false;
    const script = scripted({
      unary: (rpc) =>
        failRelease && rpc === "ReleaseSession"
          ? Promise.reject(
              Object.assign(new Error("13 INTERNAL: release-server-detail"), {
                code: 13,
                details: "release-server-detail",
              }),
            )
          : undefined,
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
    });
    const provider = new SparkProvider(connection(1, { password: "t0ken" }), {}, script);
    await provider.connect();
    const warn = spyOn(logger, "warn").mockImplementation(() => {});
    try {
      failRelease = true;
      await provider.disconnect();
      expect(warn).toHaveBeenCalledTimes(1);
      const [message, fields] = warn.mock.calls[0] ?? [];
      expect(message).toBe("A Spark session release failed");
      expect(fields).toStrictEqual({ category: "spark" });
      const logged = JSON.stringify(warn.mock.calls);
      expect(logged).not.toContain("release-server-detail");
      expect(logged).not.toContain("t0ken");
      expect(provider.isConnected()).toBe(false);
    } finally {
      warn.mockRestore();
    }
  });

  test("a probe release that fails does not fail a connect whose version meets the floor", async () => {
    let probeReleased = false;
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      unary: (rpc) => {
        if (rpc !== "ReleaseSession" || probeReleased) return undefined;
        probeReleased = true;
        return Promise.reject(unavailable());
      },
    });
    const provider = new SparkProvider(connection(1, { id: "probe-release-fails" }), {}, script);
    await provider.connect();
    expect(provider.isConnected()).toBe(true);
    await provider.disconnect();
  });

  test("unreadable version text is refused naming it", async () => {
    const script = scripted({ version: "spark-unknown" });
    const provider = new SparkProvider(connection(1), {}, script);
    const failure = await failureOf(provider.connect());
    expect(failure).toBeInstanceOf(ConnectionError);
    expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.versionUnreadable("spark-unknown"));
  });

  test("a second connect replaces and closes the first, and a failure at each step closes what it opened", async () => {
    const script = scripted({ commands: { "SHOW CURRENT NAMESPACE": CURRENT } });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    const firstMetadata = script.calls.find((call) => call.rpc === "Config")?.sessionId;
    await provider.connect();
    expect(script.closed()).toBe(1);
    expect(provider.isConnected()).toBe(true);
    // Closing the first connection released its metadata session.
    expect(script.calls.filter((call) => call.rpc === "ReleaseSession").map((call) => call.sessionId)).toContain(
      firstMetadata as string,
    );
    await provider.disconnect();
    for (const failing of ["AnalyzePlan", "Config", "ExecutePlan"]) {
      const broken = scripted({
        unary: (rpc) => (rpc === failing ? Promise.reject(unavailable()) : undefined),
        stream: (sql) =>
          failing === "ExecutePlan" && sql === "SHOW CURRENT NAMESPACE" ? failingStream(unavailable()) : undefined,
      });
      const failed = new SparkProvider(connection(1), {}, broken);
      // oxlint-disable-next-line no-await-in-loop -- one broken step per connect.
      const failure = await failureOf(failed.connect());
      expect(failure).toBeInstanceOf(ConnectionError);
      expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.notConnected("127.0.0.1", 1));
      expect(broken.closed()).toBe(1);
      expect(failed.isConnected()).toBe(false);
      expect(failed.hasRunsInFlight()).toBe(false);
    }
  });

  test("a connect a second connect displaced ends cancelled and leaves the second connected", async () => {
    const first = gate();
    let shows = 0;
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      stream: (sql, request, call) => {
        if (sql !== "SHOW CURRENT NAMESPACE") return undefined;
        shows += 1;
        return shows === 1 ? heldStream(request, call, CURRENT, first.promise) : undefined;
      },
    });
    const provider = new SparkProvider(connection(1), {}, script);
    const displaced = failureOf(provider.connect());
    await eventually(() => shows === 1);
    await provider.connect();
    first.open();
    expect(await displaced).toBeInstanceOf(QueryCancelledError);
    expect(provider.isConnected()).toBe(true);
    expect(script.closed()).toBe(1);
    await provider.disconnect();
    expect(script.closed()).toBe(2);
  });

  test("a second connect the version floor refuses closes the first connection too, so nothing stays open behind a disconnected provider", async () => {
    let version = "4.2.0";
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT, "SELECT 1": ONE },
      unary: (rpc) => (rpc === "AnalyzePlan" ? Promise.resolve({ spark_version: { version } }) : undefined),
    });
    const channels = channelsOf(script);
    const provider = new SparkProvider(connection(1), {}, channels);
    await provider.connect();
    await provider.query("SELECT 1", undefined, "q-first", "scope-first");
    // The first connection's metadata session and the scope's session, each set up by one Config call.
    const sessions = script.calls.filter((call) => call.rpc === "Config").map((call) => call.sessionId);
    expect(sessions).toHaveLength(2);
    // The server behind the same address now runs a version below the floor.
    version = "3.5.9";
    const before = script.calls.length;
    const failure = await failureOf(provider.connect());
    expect(failure).toBeInstanceOf(ConnectionError);
    expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.versionTooOld("3.5.9"));
    const sent = script.calls.slice(before);
    const probe = sent.find((call) => call.rpc === "AnalyzePlan")?.sessionId as string;
    expect(sent.filter((call) => call.rpc === "ReleaseSession").map((call) => call.sessionId)).toStrictEqual([
      probe,
      ...sessions,
    ]);
    // The first connection's channel and the refused connect's own.
    expect(channels.closed()).toStrictEqual([true, true]);
    expect(provider.isConnected()).toBe(false);
    expect(provider.hasRunsInFlight()).toBe(false);
    expect(provider.connectWarnings()).toStrictEqual([]);
    const settled = script.calls.length;
    const refused = await failureOf(provider.query("SELECT 1"));
    expect(refused).toBeInstanceOf(DatabaseConfigError);
    expect((refused as Error).message).toBe("Provider is not connected. Call connect() first.");
    expect(await provider.endOpenQueryTransaction("scope-first")).toBe("none");
    expect(script.calls.length).toBe(settled);
  });

  test("a failed release of the first connection's session leaves the refusal of the connect that closed it as its failure", async () => {
    let version = "4.2.0";
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      unary: (rpc, request) => {
        if (rpc === "AnalyzePlan") return Promise.resolve({ spark_version: { version } });
        return rpc === "ReleaseSession" && sessionOf(request) === metadata()
          ? Promise.reject(unavailable())
          : undefined;
      },
    });
    const metadata = () => script.calls.find((call) => call.rpc === "Config")?.sessionId;
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    version = "3.5.9";
    const warn = spyOn(logger, "warn").mockImplementation(() => {});
    try {
      const failure = await failureOf(provider.connect());
      expect(failure).toBeInstanceOf(ConnectionError);
      expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.versionTooOld("3.5.9"));
      // The release was sent and failed, and its warning is the one trace it leaves.
      expect(warn.mock.calls).toStrictEqual([["A Spark session release failed", { category: "not-connected" }]]);
      expect(provider.isConnected()).toBe(false);
    } finally {
      warn.mockRestore();
    }
  });

  test("a third connect that completes while a failed second connect is still closing keeps its connection and its state", async () => {
    const release = gate();
    let shows = 0;
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT, "SELECT 1": ONE },
      stream: (sql) => {
        if (sql !== "SHOW CURRENT NAMESPACE") return undefined;
        shows += 1;
        return shows === 2 ? failingStream(unavailable()) : undefined;
      },
      unary: (rpc, request) =>
        rpc === "ReleaseSession" && sessionOf(request) === secondMetadata()
          ? release.promise.then(() => ({}))
          : undefined,
    });
    const secondMetadata = () => script.calls.filter((call) => call.rpc === "Config")[1]?.sessionId;
    const channels = channelsOf(script);
    const provider = new SparkProvider(connection(1), {}, channels);
    await provider.connect();
    const failed = failureOf(provider.connect());
    // The failed connect is closing what it opened: the release of its metadata session waits on the test.
    await eventually(() =>
      script.calls.some((call) => call.rpc === "ReleaseSession" && call.sessionId === secondMetadata()),
    );
    expect(provider.isConnected()).toBe(false);
    await provider.connect();
    release.open();
    const failure = await failed;
    expect(failure).toBeInstanceOf(ConnectionError);
    expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.notConnected("127.0.0.1", 1));
    // The first and the second closed by the failed connect, the third open.
    expect(channels.closed()).toStrictEqual([true, true, false]);
    expect(provider.isConnected()).toBe(true);
    expect((await provider.query("SELECT 1")).rows).toStrictEqual([{ "1": 1 }]);
    await provider.disconnect();
  });

  test("a disconnect while a failed second connect still closes its own connection finds the first connection closed and its sessions released", async () => {
    const release = gate();
    let shows = 0;
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT, "SELECT 1": ONE },
      stream: (sql) => {
        if (sql !== "SHOW CURRENT NAMESPACE") return undefined;
        shows += 1;
        return shows === 2 ? failingStream(unavailable()) : undefined;
      },
      unary: (rpc, request) =>
        rpc === "ReleaseSession" && sessionOf(request) === secondMetadata()
          ? release.promise.then(() => ({}))
          : undefined,
    });
    // The first connection's metadata session, the scope's session, then the second connect's metadata session.
    const sessions = () => script.calls.filter((call) => call.rpc === "Config").map((call) => call.sessionId);
    const secondMetadata = () => sessions()[2];
    const released = () => script.calls.filter((call) => call.rpc === "ReleaseSession").map((call) => call.sessionId);
    const channels = channelsOf(script);
    const provider = new SparkProvider(connection(1), {}, channels);
    await provider.connect();
    await provider.query("SELECT 1", undefined, "q-first", "scope-first");
    const first = sessions();
    expect(first).toHaveLength(2);
    const failed = failureOf(provider.connect());
    // The failed connect is closing what it opened: the release of its metadata session waits on the test.
    await eventually(() =>
      script.calls.some((call) => call.rpc === "ReleaseSession" && call.sessionId === secondMetadata()),
    );
    await provider.disconnect();
    expect(channels.closed()).toStrictEqual([true, false]);
    expect(released().filter((id) => first.includes(id))).toStrictEqual(first);
    release.open();
    const failure = await failed;
    expect(failure).toBeInstanceOf(ConnectionError);
    expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.notConnected("127.0.0.1", 1));
    expect(channels.closed()).toStrictEqual([true, true]);
    expect(provider.isConnected()).toBe(false);
    expect(provider.hasRunsInFlight()).toBe(false);
  });

  test("a connect displaced while it replaced a live connection leaves the connect that displaced it connected", async () => {
    const held = gate();
    let shows = 0;
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT, "SELECT 1": ONE },
      stream: (sql, request, call) => {
        if (sql !== "SHOW CURRENT NAMESPACE") return undefined;
        shows += 1;
        // The second connect's namespace read waits on the test.
        return shows === 2 ? heldStream(request, call, CURRENT, held.promise) : undefined;
      },
    });
    const channels = channelsOf(script);
    const provider = new SparkProvider(connection(1), {}, channels);
    await provider.connect();
    const displaced = failureOf(provider.connect());
    await eventually(() => shows === 2);
    await provider.connect();
    held.open();
    expect(await displaced).toBeInstanceOf(QueryCancelledError);
    // The first was closed by the third connect, which replaced it, and the second by its own failure.
    expect(channels.closed()).toStrictEqual([true, true, false]);
    expect(provider.isConnected()).toBe(true);
    expect((await provider.query("SELECT 1")).rows).toStrictEqual([{ "1": 1 }]);
    await provider.disconnect();
    expect(channels.closed()).toStrictEqual([true, true, true]);
  });

  test("a disconnect during connect ends the connect cancelled, its namespace read stopped once", async () => {
    const never = gate();
    const script = scripted({
      stream: (sql, request, call) =>
        sql === "SHOW CURRENT NAMESPACE" ? heldStream(request, call, CURRENT, never.promise) : undefined,
    });
    const provider = new SparkProvider(connection(1), {}, script);
    const pending = failureOf(provider.connect());
    await eventually(() => script.calls.some((call) => call.rpc === "ExecutePlan"));
    await provider.disconnect();
    expect(await pending).toBeInstanceOf(QueryCancelledError);
    expect(provider.isConnected()).toBe(false);
    expect(provider.hasRunsInFlight()).toBe(false);
    expect(script.calls.filter((call) => call.rpc === "Interrupt")).toHaveLength(1);
    expect(script.closed()).toBe(1);
  });
});

describe("SparkProvider: Stop at the edges of a run", () => {
  test("Stop during the scope's session setup: the setup is shared, and neither an ExecutePlan nor an Interrupt follows", async () => {
    let releaseConfig: () => void = () => undefined;
    let configCalls = 0;
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT, "SELECT 1": ONE },
      unary: (rpc) => {
        if (rpc !== "Config") return undefined;
        configCalls += 1;
        // The first is the metadata session's setup at connect.
        if (configCalls === 1) return undefined;
        return new Promise((resolve) => {
          releaseConfig = () => resolve({ pairs: [], warnings: [] });
        });
      },
    });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    const before = script.calls.length;
    const first = provider.query("SELECT 1", undefined, "q-1", "scope-in-setup");
    const second = provider.query("SELECT 1", undefined, "q-2", "scope-in-setup");
    await eventually(() => configCalls === 2);
    expect(provider.hasRunsInFlight()).toBe(true);
    expect(await provider.cancelQuery("q-1")).toBe(true);
    releaseConfig();
    await expect(first).rejects.toBeInstanceOf(QueryCancelledError);
    expect((await second).rows).toStrictEqual([{ "1": 1 }]);
    const after = script.calls.slice(before);
    expect(after.filter((call) => call.rpc === "Config")).toHaveLength(1);
    expect(after.filter((call) => call.rpc === "ExecutePlan" && statementOf(call.request) === "SELECT 1")).toHaveLength(
      1,
    );
    // A run stopped before its ExecutePlan has nothing on Spark to stop, so it sends no Interrupt.
    expect(after.filter((call) => call.rpc === "Interrupt")).toHaveLength(0);
    expect(await provider.cancelQuery("q-2")).toBe(false);
    expect(await provider.cancelQuery("unknown")).toBe(false);
    await provider.endOpenQueryTransaction("scope-in-setup");
    expect(provider.hasRunsInFlight()).toBe(false);
    await provider.disconnect();
  });

  test("Stop as the deadline fires: one Interrupt, the run ends cancelled, and Stop answers true", async () => {
    const slow = gate();
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      stream: (sql, request, call) => (sql === "SLOW" ? heldStream(request, call, ONE, slow.promise) : undefined),
    });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    const running = failureOf(provider.query("SLOW", undefined, "q-at-once", "scope-at-once"));
    await eventually(() => script.calls.some((call) => statementOf(call.request) === "SLOW"));
    const stopped = provider.cancelQuery("q-at-once");
    slow.fail(deadlineStatus());
    expect(await stopped).toBe(true);
    expect(await running).toBeInstanceOf(QueryCancelledError);
    expect(script.calls.filter((call) => call.rpc === "Interrupt")).toHaveLength(1);
    expect(provider.hasRunsInFlight()).toBe(false);
    await provider.disconnect();
  });

  test("Stop just after the deadline: one Interrupt, the deadline's sentence, and Stop answers false", async () => {
    const slow = gate();
    const interrupt = gate();
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      stream: (sql, request, call) => (sql === "SLOW" ? heldStream(request, call, ONE, slow.promise) : undefined),
      unary: (rpc) => (rpc === "Interrupt" ? interrupt.promise.then(() => ({ interrupted_ids: [] })) : undefined),
    });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    const running = failureOf(provider.query("SLOW", undefined, "q-late", "scope-late"));
    await eventually(() => script.calls.some((call) => statementOf(call.request) === "SLOW"));
    slow.fail(deadlineStatus());
    // The run's own Interrupt is on its way when Stop arrives.
    await eventually(() => script.calls.some((call) => call.rpc === "Interrupt"));
    const stopped = provider.cancelQuery("q-late");
    interrupt.open();
    expect(await stopped).toBe(false);
    const failure = await running;
    expect(failure).toBeInstanceOf(QueryError);
    expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.deadline(DEFAULT_QUERY_TIMEOUT));
    expect(script.calls.filter((call) => call.rpc === "Interrupt")).toHaveLength(1);
    expect(provider.hasRunsInFlight()).toBe(false);
    await provider.disconnect();
  });

  test("Stop after the stream ended answers false and sends nothing", async () => {
    const provider = new SparkProvider(connection(port));
    await provider.connect();
    try {
      await provider.query("SELECT 1", undefined, "q-ended");
      fake.log.clear();
      expect(await provider.cancelQuery("q-ended")).toBe(false);
      expect(fake.log.entries).toStrictEqual([]);
      expect(provider.hasRunsInFlight()).toBe(false);
    } finally {
      await provider.disconnect();
    }
  });

  test("Stop reaches a run on a connection that a second connect is closing", async () => {
    const slow = gate();
    const interrupt = gate();
    let holdInterrupts = false;
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      stream: (sql, request, call) => (sql === "SLOW" ? heldStream(request, call, ONE, slow.promise) : undefined),
      unary: (rpc) =>
        rpc === "Interrupt" && holdInterrupts ? interrupt.promise.then(() => ({ interrupted_ids: [] })) : undefined,
    });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    const running = failureOf(provider.query("SLOW", undefined, "q-old", "scope-old"));
    await eventually(() => script.calls.some((call) => statementOf(call.request) === "SLOW"));
    holdInterrupts = true;
    const reconnected = provider.connect();
    // The close of the first connection is stopping the run: its Interrupt waits on the test.
    await eventually(() => script.calls.some((call) => call.rpc === "Interrupt"));
    expect(provider.hasRunsInFlight()).toBe(true);
    expect(await provider.cancelQuery("q-old")).toBe(true);
    interrupt.open();
    await reconnected;
    expect(await running).toBeInstanceOf(QueryCancelledError);
    expect(script.calls.filter((call) => call.rpc === "Interrupt")).toHaveLength(1);
    expect(provider.isConnected()).toBe(true);
    expect(provider.hasRunsInFlight()).toBe(false);
    await provider.disconnect();
  });

  test("Stop reaches a run on the first connection while a failed second connect still closes its own connection", async () => {
    const slow = gate();
    const interrupt = gate();
    const release = gate();
    let shows = 0;
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      stream: (sql, request, call) => {
        if (sql === "SLOW") return heldStream(request, call, ONE, slow.promise);
        if (sql !== "SHOW CURRENT NAMESPACE") return undefined;
        shows += 1;
        return shows === 2 ? failingStream(unavailable()) : undefined;
      },
      unary: (rpc, request) => {
        if (rpc === "Interrupt" && sessionOf(request) === runSession()) {
          return interrupt.promise.then(() => ({ interrupted_ids: [] }));
        }
        return rpc === "ReleaseSession" && sessionOf(request) === secondMetadata()
          ? release.promise.then(() => ({}))
          : undefined;
      },
    });
    const runSession = () => script.calls.find((call) => statementOf(call.request) === "SLOW")?.sessionId;
    // The first connection's metadata session, the run's scope session, then the second connect's metadata session.
    const secondMetadata = () => script.calls.filter((call) => call.rpc === "Config")[2]?.sessionId;
    const channels = channelsOf(script);
    const provider = new SparkProvider(connection(1), {}, channels);
    await provider.connect();
    const running = failureOf(provider.query("SLOW", undefined, "q-first", "scope-first"));
    await eventually(() => runSession() !== undefined);
    const failed = failureOf(provider.connect());
    // The failed connect is closing what it opened: the release of its metadata session waits on the test.
    await eventually(() =>
      script.calls.some((call) => call.rpc === "ReleaseSession" && call.sessionId === secondMetadata()),
    );
    expect(provider.isConnected()).toBe(false);
    expect(await provider.cancelQuery("q-first")).toBe(true);
    // The first connection's close stopped the run while that release is still held.
    expect(await running).toBeInstanceOf(QueryCancelledError);
    interrupt.open();
    release.open();
    const failure = await failed;
    expect(failure).toBeInstanceOf(ConnectionError);
    expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.notConnected("127.0.0.1", 1));
    expect(script.calls.filter((call) => call.rpc === "Interrupt" && call.sessionId === runSession())).toHaveLength(1);
    expect(channels.closed()).toStrictEqual([true, true]);
    expect(provider.isConnected()).toBe(false);
    expect(provider.hasRunsInFlight()).toBe(false);
  });

  test("a beat goes out under the heartbeat deadline on the run's own session, the setup under the control deadline", async () => {
    const slow = gate();
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      stream: (sql, request, call) => (sql === "SLOW" ? heldStream(request, call, ONE, slow.promise) : undefined),
    });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    const realSetInterval = globalThis.setInterval;
    const realClearInterval = globalThis.clearInterval;
    const ticks: (() => void)[] = [];
    const handles: object[] = [];
    const cleared: unknown[] = [];
    const setSpy = spyOn(globalThis, "setInterval").mockImplementation(((callback: () => void, ms?: number) => {
      if (ms !== SPARK_HEARTBEAT_INTERVAL_MS) return realSetInterval(callback, ms);
      const handle = {};
      ticks.push(callback);
      handles.push(handle);
      return handle;
    }) as unknown as typeof setInterval);
    const clearSpy = spyOn(globalThis, "clearInterval").mockImplementation(((handle?: unknown) => {
      if (handles.includes(handle as object)) cleared.push(handle);
      else realClearInterval(handle as ReturnType<typeof setInterval>);
    }) as unknown as typeof clearInterval);
    const now = spyOn(Date, "now").mockReturnValue(1_000_000);
    try {
      const running = provider.query("SLOW", undefined, "q-beat", "scope-beat");
      await eventually(() => ticks.length === 1);
      expect(provider.hasRunsInFlight()).toBe(true);
      ticks[0]?.();
      const isBeat = (call: ScriptedCall) =>
        call.rpc === "Config" &&
        (call.request as { operation?: { get_option?: unknown } }).operation?.get_option !== undefined;
      await eventually(() => script.calls.some(isBeat));
      const run = script.calls.find((call) => statementOf(call.request) === "SLOW") as ScriptedCall;
      const setup = script.calls.find(
        (call) => call.rpc === "Config" && call.sessionId === run.sessionId && setsConfig(call.request),
      ) as ScriptedCall;
      const beat = script.calls.find(isBeat) as ScriptedCall;
      expect(beat.sessionId).toBe(run.sessionId);
      expect((beat.request as { operation?: unknown }).operation).toStrictEqual({
        get_option: { keys: ["spark.sql.ansi.enabled"] },
      });
      expect(beat.deadline - beat.at).toBe(SPARK_HEARTBEAT_DEADLINE_MS);
      expect(setup.deadline - setup.at).toBe(SPARK_CONTROL_DEADLINE_MS);
      // The run itself is bounded by the connection's query timeout.
      expect(run.deadline - run.at).toBe(DEFAULT_QUERY_TIMEOUT);
      slow.open();
      expect((await running).rows).toStrictEqual([{ "1": 1 }]);
      expect(cleared).toStrictEqual(handles);
      expect(provider.hasRunsInFlight()).toBe(false);
    } finally {
      now.mockRestore();
      setSpy.mockRestore();
      clearSpy.mockRestore();
      await provider.disconnect();
    }
  });

  test("a session release in progress counts as in flight until it settles", async () => {
    const release = gate();
    let holdReleases = false;
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT, "SELECT 1": ONE },
      unary: (rpc) =>
        rpc === "ReleaseSession" && holdReleases ? release.promise.then(() => ({ session_id: "" })) : undefined,
    });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    await provider.query("SELECT 1", undefined, "q-release", "scope-release");
    holdReleases = true;
    const before = script.calls.length;
    const ending = provider.endOpenQueryTransaction("scope-release");
    await eventually(() => script.calls.slice(before).some((call) => call.rpc === "ReleaseSession"));
    expect(provider.hasRunsInFlight()).toBe(true);
    release.open();
    expect(await ending).toBe("none");
    expect(provider.hasRunsInFlight()).toBe(false);
    holdReleases = false;
    await provider.disconnect();
  });
});

describe("SparkProvider: the panels through getMonitoringData", () => {
  test("every panel getter is called: the overview, {}, the three [] and the two refusals", async () => {
    const script = scripted({
      commands: {
        "SHOW CURRENT NAMESPACE": CURRENT,
        [`SHOW VIEWS IN ${DEFAULT}`]: listing("viewName", []),
        [`SHOW TABLES IN ${DEFAULT}`]: listing("tableName", ["t1", "t2"]),
      },
      unary: (rpc, request) => {
        const prefix = (request as { operation?: { get_all?: { prefix?: string } } }).operation?.get_all?.prefix;
        if (rpc !== "Config" || prefix !== "spark.app.") return undefined;
        return Promise.resolve({
          pairs: [
            { key: "startTime", value: "1700000000000" },
            { key: "name", value: "Spark Connect server" },
          ],
          warnings: [],
        });
      },
    });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    const data = await provider.getMonitoringData({ includeTables: true, includeIndexes: true, includeStorage: true });
    // The uptime is read against the clock, so the overview is matched without it.
    expect(data.overview).toMatchObject({
      version: "4.2.0",
      startTime: new Date(1_700_000_000_000),
      maxConnections: 0,
      databaseSize: "N/A",
      tableCount: 2,
      indexCount: 0,
    });
    expect(data.performance).toStrictEqual({});
    expect(data.slowQueries).toStrictEqual([]);
    expect(data.activeSessions).toStrictEqual([]);
    expect(data.indexes).toStrictEqual([]);
    expect(data.tables).toBeUndefined();
    expect(data.storage).toBeUndefined();
    expect(data.errors).toStrictEqual({
      tables: SPARK_MONITORING_LABELS.tablesRefused,
      storage: SPARK_MONITORING_LABELS.storageRefused,
    });
    expect(await provider.getHealth()).toStrictEqual({
      databaseSize: "N/A",
      cacheHitRatio: "N/A",
      slowQueries: [],
      activeSessions: [],
    });
    await provider.disconnect();
  });

  test("the overview's version is the server's text as a version sentence quotes it: a token withheld, a long text cut", async () => {
    let version = "4.2.0";
    const script = scripted({
      commands: {
        "SHOW CURRENT NAMESPACE": CURRENT,
        [`SHOW VIEWS IN ${DEFAULT}`]: listing("viewName", []),
        [`SHOW TABLES IN ${DEFAULT}`]: listing("tableName", ["t1"]),
      },
      unary: (rpc) => (rpc === "AnalyzePlan" ? Promise.resolve({ spark_version: { version } }) : undefined),
    });
    const provider = new SparkProvider(connection(1, { password: "t0ken" }), {}, script);
    await provider.connect();
    const overview = (shown: string) => ({
      version: shown,
      uptime: "N/A",
      maxConnections: 0,
      databaseSize: "N/A",
      tableCount: 1,
      tableCountSampledFrom: "the tables of spark_catalog.default, the connection's current namespace",
      indexCount: 0,
    });
    const health = { databaseSize: "N/A", cacheHitRatio: "N/A", slowQueries: [], activeSessions: [] };
    try {
      version = "4.2.0 (served with Bearer t0ken)";
      expect(await provider.getOverview()).toStrictEqual(
        overview("(the server's text was withheld because it contained the configured credential)"),
      );
      expect(await provider.getHealth()).toStrictEqual(health);
      version = `4.2.0-${"x".repeat(9_994)}`;
      expect(await provider.getOverview()).toStrictEqual(
        overview(`4.2.0-${"x".repeat(SPARK_VERSION_TEXT_MAX_CHARS - 6)}`),
      );
      expect(await provider.getHealth()).toStrictEqual(health);
    } finally {
      await provider.disconnect();
    }
  });
});

describe("SparkProvider: the metadata session after the server closed it", () => {
  test("after a restart, getHealth answers on a new metadata session after exactly one repeat", async () => {
    const provider = new SparkProvider(connection(port));
    await provider.connect();
    try {
      const start = fake.log.entries.length;
      fake.restart();
      expect(await provider.getHealth()).toBeDefined();
      const after = fake.log.entries.slice(start);
      const versions = after.filter((entry) => entry.rpc === "AnalyzePlan").map((entry) => entry.sessionId);
      expect(versions).toHaveLength(2);
      expect(versions[1]).not.toBe(versions[0] as string);
      // The one setup between the two reads is the new metadata session's.
      expect(after.filter((entry) => entry.rpc === "Config").map((entry) => entry.sessionId)).toStrictEqual([
        versions[1] as string,
      ]);
    } finally {
      await provider.disconnect();
    }
  });

  test("a metadata session the server released is replaced, and the statement that met it is sent again once", async () => {
    fake.log.clear();
    const provider = new SparkProvider(connection(port));
    await provider.connect();
    try {
      const metadata = fake.log.entries.find((entry) => entry.rpc === "Config")?.sessionId as string;
      const outside = grpcWireTransport(buildSparkConnectionOptions(connection(port), { queryTimeout: 5_000 }));
      try {
        await outside.unary("ReleaseSession", releaseSessionRequest({ sessionId: metadata }), {
          metadata: {},
          deadline: new Date(Date.now() + 5_000),
          signal: new AbortController().signal,
        });
      } finally {
        outside.close();
      }
      const start = fake.log.entries.length;
      expect(await provider.listContainers()).toStrictEqual([
        { path: ["spark_catalog"], name: "spark_catalog", level: 0, isSessionDefault: true },
      ]);
      const after = fake.log.entries.slice(start);
      const reads = after
        .filter((entry) => statementOf(entry.request) === "SHOW CURRENT NAMESPACE")
        .map((entry) => entry.sessionId);
      expect(reads).toHaveLength(2);
      expect(reads[0]).toBe(metadata);
      expect(reads[1]).not.toBe(metadata);
      expect(
        after.filter((entry) => entry.rpc === "Config" && setsConfig(entry.request)).map((entry) => entry.sessionId),
      ).toStrictEqual([reads[1] as string]);
    } finally {
      await provider.disconnect();
    }
  });

  test("a metadata statement that meets a closed session once is sent again on a new metadata session", async () => {
    const closed = new Set<string>();
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT, "SHOW CATALOGS": CATALOGS },
      stream: (_sql, request) =>
        closed.has(sessionOf(request))
          ? failingStream(new SparkError("session-closed", SPARK_ERROR_SENTENCES.sessionClosed))
          : undefined,
    });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    const metadata = script.calls.find((call) => call.rpc === "Config")?.sessionId as string;
    closed.add(metadata);
    const start = script.calls.length;
    expect(await provider.listContainers()).toStrictEqual([
      { path: ["spark_catalog"], name: "spark_catalog", level: 0, isSessionDefault: true },
    ]);
    const after = script.calls.slice(start);
    const reads = after
      .filter((call) => statementOf(call.request) === "SHOW CURRENT NAMESPACE")
      .map((call) => call.sessionId);
    expect(reads).toHaveLength(2);
    expect(reads[0]).toBe(metadata);
    expect(
      after.filter((call) => call.rpc === "Config" && setsConfig(call.request)).map((call) => call.sessionId),
    ).toStrictEqual([reads[1] as string]);
    await provider.disconnect();
  });

  test("a metadata statement that meets a closed session twice fails with the metadata-session sentence", async () => {
    let closing = false;
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      stream: (sql) =>
        closing && sql === "SHOW CURRENT NAMESPACE"
          ? failingStream(new SparkError("session-closed", SPARK_ERROR_SENTENCES.sessionClosed))
          : undefined,
    });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    closing = true;
    const start = script.calls.length;
    const failure = await failureOf(provider.listContainers());
    expect(failure).toBeInstanceOf(QueryError);
    expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.metadataSessionLost);
    const reads = script.calls.slice(start).filter((call) => statementOf(call.request) === "SHOW CURRENT NAMESPACE");
    expect(new Set(reads.map((call) => call.sessionId)).size).toBe(2);
    await provider.disconnect();
  });

  test("the new metadata session's failed setup reaches getHealth as its mapped sentence, never the channel's own text", async () => {
    let metadataClosed = false;
    let configCalls = 0;
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      unary: (rpc) => {
        // The version read meets a closed session, in the form the provider's mapping gives it, so the read is repeated.
        if (rpc === "AnalyzePlan" && metadataClosed) {
          return Promise.reject(new SparkError("session-closed", SPARK_ERROR_SENTENCES.sessionClosed));
        }
        if (rpc !== "Config") return undefined;
        configCalls += 1;
        return configCalls === 1 ? undefined : Promise.reject(new Error("a local channel failure"));
      },
    });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    metadataClosed = true;
    const failure = await failureOf(provider.getHealth());
    expect(failure).toBeInstanceOf(QueryError);
    expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.localClientError);
    expect(configCalls).toBe(2);
    await provider.disconnect();
  });

  test("a setup statement of the new metadata session that fails reaches getHealth as its mapped sentence", async () => {
    let metadataClosed = false;
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT, "SET CATALOG 'spark_catalog'": SET_ANSWER },
      unary: (rpc) =>
        rpc === "AnalyzePlan" && metadataClosed
          ? Promise.reject(new SparkError("session-closed", SPARK_ERROR_SENTENCES.sessionClosed))
          : undefined,
      stream: (sql) =>
        metadataClosed && sql === "SET CATALOG 'spark_catalog'"
          ? failingStream(new Error("a local channel failure"))
          : undefined,
    });
    const provider = new SparkProvider(connection(1, { database: "spark_catalog" }), {}, script);
    await provider.connect();
    metadataClosed = true;
    const failure = await failureOf(provider.getHealth());
    expect(failure).toBeInstanceOf(QueryError);
    expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.localClientError);
    await provider.disconnect();
  });

  test("a Config read of the metadata session that fails reaches the panel as its mapped sentence", async () => {
    const script = scripted({
      commands: { "SHOW CURRENT NAMESPACE": CURRENT },
      unary: (rpc, request) =>
        rpc === "Config" && !setsConfig(request) ? Promise.reject(new Error("a local channel failure")) : undefined,
    });
    const provider = new SparkProvider(connection(1), {}, script);
    await provider.connect();
    const failure = await failureOf(provider.getOverview());
    expect(failure).toBeInstanceOf(QueryError);
    expect((failure as Error).message).toBe(SPARK_ERROR_SENTENCES.localClientError);
    await provider.disconnect();
  });
});
