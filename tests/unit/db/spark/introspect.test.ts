/**
 * The object surface over the metadata session, read through a stub of `SparkIntrospectDeps` that answers each
 * statement from a table, so every test names each statement it lets the module send.
 */
import { describe, expect, test } from "bun:test";
import { INVENTORY_TRUNCATION_REASON } from "@/lib/db/inventory-bounds";
import { callerBoundTruncationReason } from "@/lib/db/object-kinds";
import { SPARK_DESCRIBE_OBJECTS_MAX, SPARK_INTROSPECT_IN_FLIGHT_MAX } from "@/lib/db/providers/sql/spark/constants";
import { SPARK_ERROR_SENTENCES, SparkError } from "@/lib/db/providers/sql/spark/errors";
import type { SparkStatementResult } from "@/lib/db/providers/sql/spark/execute";
import {
  countObjects,
  describeObject,
  describeObjects,
  listCatalogs,
  listNamespaces,
  listObjects,
  readObjectSource,
  SPARK_INTROSPECT_SENTENCES,
  type SparkIntrospectDeps,
} from "@/lib/db/providers/sql/spark/introspect";
import { parseNamespaceSpelling, spellNamespace } from "@/lib/db/providers/sql/spark/namespace";
import type { ProviderCapabilities } from "@/lib/db/types";
import { generateCountQuery, quoteObjectPath } from "@/lib/query-generators";

type Row = Record<string, unknown>;
type Answer = Row[] | SparkStatementResult | Error;

const result = (rows: Row[], cut: SparkStatementResult["cut"] = null): SparkStatementResult => ({
  fields: Object.keys(rows[0] ?? {}),
  rows,
  columnTypes: {},
  cut,
});

/** A metadata runner that answers each statement from `answers` and records every statement it was sent. */
function stub(answers: Record<string, Answer>, conf: readonly string[] = []) {
  const sent: string[] = [];
  const prefixes: string[] = [];
  const deps: SparkIntrospectDeps = {
    runSql: async (sql) => {
      sent.push(sql);
      const answer = answers[sql];
      if (answer === undefined) throw new Error(`unexpected statement: ${sql}`);
      if (answer instanceof Error) throw answer;
      return Array.isArray(answer) ? result(answer) : answer;
    },
    confKeys: async (prefix) => {
      prefixes.push(prefix);
      return conf;
    },
  };
  return { deps, sent, prefixes };
}

const CURRENT = { "SHOW CURRENT NAMESPACE": [{ catalog: "spark_catalog", namespace: "studio" }] };
const spark = (message: string) => new SparkError("spark", message);
/** `count` namespaces named `<parent>.c0`, `<parent>.c1`, ..., as SHOW NAMESPACES lists the children of `parent`. */
const children = (parent: string, count: number) =>
  Array.from({ length: count }, (_, i) => ({ namespace: `${parent}.c${i}` }));

/** The parts of a name written as bare or backticked parts joined by dots, the inverse of Spark's spelling. */
function nameParts(text: string): string[] {
  const parts: string[] = [];
  let i = 0;
  while (i < text.length) {
    if (text[i] === "`") {
      let part = "";
      let j = i + 1;
      while (!(text[j] === "`" && text[j + 1] !== "`")) {
        if (j >= text.length) throw new Error(`unterminated quoted part in ${text}`);
        part += text[j];
        j += text[j] === "`" ? 2 : 1;
      }
      parts.push(part);
      i = j + 1;
    } else {
      const bare = /^[A-Za-z_][A-Za-z0-9_]*/.exec(text.slice(i));
      if (bare === null) throw new Error(`no name part at ${i} in ${text}`);
      parts.push(bare[0]);
      i += bare[0].length;
    }
    if (i < text.length) {
      if (text[i] !== ".") throw new Error(`no dot at ${i} in ${text}`);
      i++;
    }
  }
  return parts;
}

// The capabilities the Spark provider declares for the generators.
const SPARK_CAPS: ProviderCapabilities = {
  queryLanguage: "sql",
  supportsExplain: true,
  supportsExternalQueryLimiting: true,
  supportsCreateTable: false,
  supportsInlineRowEdit: false,
  supportsMaintenance: false,
  maintenanceOperations: [],
  supportsConnectionString: false,
  defaultPort: 15002,
  schemaRefreshPattern: "^\\s*(CREATE|DROP|ALTER|REPLACE|REPAIR|MSCK)\\b",
  identifierQuoting: "backtick-always",
  containerNameSpelling: "engine",
  containerLevels: [
    { id: "catalog", label: "Catalog", labelPlural: "Catalogs" },
    { id: "schema", label: "Namespace", labelPlural: "Namespaces" },
  ],
};

describe("catalogs", () => {
  test("the current catalog first and flagged, then SHOW CATALOGS and the configured keys by name, values unread", async () => {
    const { deps, prefixes } = stub(
      { ...CURRENT, "SHOW CATALOGS": [{ catalog: "spark_catalog" }, { catalog: "lake" }] },
      ["studio_unloaded", "lake", "lake.uri", "spark_catalog"],
    );
    expect(await listCatalogs(deps)).toStrictEqual([
      { path: ["spark_catalog"], name: "spark_catalog", level: 0, isSessionDefault: true },
      { path: ["lake"], name: "lake", level: 0 },
      { path: ["studio_unloaded"], name: "studio_unloaded", level: 0 },
    ]);
    expect(prefixes).toStrictEqual(["spark.sql.catalog."]);
  });

  test("SHOW CATALOGS and the configured keys are sorted together, and an empty key names no catalog", async () => {
    const { deps } = stub({ ...CURRENT, "SHOW CATALOGS": [{ catalog: "zeta" }] }, ["alpha", ""]);
    expect(await listCatalogs(deps)).toStrictEqual([
      { path: ["spark_catalog"], name: "spark_catalog", level: 0, isSessionDefault: true },
      { path: ["alpha"], name: "alpha", level: 0 },
      { path: ["zeta"], name: "zeta", level: 0 },
    ]);
  });

  test("an empty SHOW CURRENT NAMESPACE answer is unreadable", async () => {
    const { deps } = stub({ "SHOW CURRENT NAMESPACE": [] });
    await expect(listCatalogs(deps)).rejects.toMatchObject({ category: "unreadable" });
  });
});

describe("namespaces", () => {
  test("spark_catalog costs one statement however many databases it has, and flags the current one", async () => {
    const { deps, sent } = stub({
      ...CURRENT,
      "SHOW NAMESPACES IN `spark_catalog`": [{ namespace: "default" }, { namespace: "studio" }, { namespace: "x" }],
    });
    const containers = await listNamespaces(deps, "spark_catalog");
    expect(containers.map((c) => c.name)).toStrictEqual(["default", "studio", "x"]);
    expect(containers.find((c) => c.name === "studio")?.isSessionDefault).toBe(true);
    expect(containers.find((c) => c.name === "default")?.isSessionDefault).toBeUndefined();
    expect(containers[0]).toStrictEqual({ path: ["spark_catalog", "default"], name: "default", level: 1 });
    expect(sent.filter((sql) => sql.startsWith("SHOW NAMESPACES"))).toHaveLength(1);
  });

  test("another catalog is walked breadth first, children named by their full spelling", async () => {
    const { deps, sent } = stub({
      ...CURRENT,
      "SHOW NAMESPACES IN `lake`": [{ namespace: "a" }, { namespace: "b" }],
      "SHOW NAMESPACES IN `lake`.a": [{ namespace: "a.c" }],
      "SHOW NAMESPACES IN `lake`.b": [],
      "SHOW NAMESPACES IN `lake`.a.c": [],
    });
    expect((await listNamespaces(deps, "lake")).map((c) => c.name)).toStrictEqual(["a", "b", "a.c"]);
    expect(sent.slice(1)).toStrictEqual([
      "SHOW NAMESPACES IN `lake`",
      "SHOW NAMESPACES IN `lake`.a",
      "SHOW NAMESPACES IN `lake`.b",
      "SHOW NAMESPACES IN `lake`.a.c",
    ]);
  });

  test("the current namespace is flagged by its parts, inside the current catalog only", async () => {
    const elsewhere = stub({
      ...CURRENT,
      "SHOW NAMESPACES IN `lake`": [{ namespace: "studio" }],
      "SHOW NAMESPACES IN `lake`.studio": [],
    });
    expect(await listNamespaces(elsewhere.deps, "lake")).toStrictEqual([
      { path: ["lake", "studio"], name: "studio", level: 1 },
    ]);
    const nested = stub({
      "SHOW CURRENT NAMESPACE": [{ catalog: "lake", namespace: "a.`b c`" }],
      "SHOW NAMESPACES IN `lake`": [{ namespace: "a" }],
      "SHOW NAMESPACES IN `lake`.a": [{ namespace: "a.`b c`" }],
      "SHOW NAMESPACES IN `lake`.a.`b c`": [],
    });
    expect(await listNamespaces(nested.deps, "lake")).toStrictEqual([
      { path: ["lake", "a"], name: "a", level: 1 },
      { path: ["lake", "a.`b c`"], name: "a.`b c`", level: 1, isSessionDefault: true },
    ]);
  });

  test("a namespace below the current one is not flagged with it", async () => {
    const { deps } = stub({
      "SHOW CURRENT NAMESPACE": [{ catalog: "lake", namespace: "a" }],
      "SHOW NAMESPACES IN `lake`": [{ namespace: "a" }],
      "SHOW NAMESPACES IN `lake`.a": [{ namespace: "a.b" }],
      "SHOW NAMESPACES IN `lake`.a.b": [],
    });
    expect(await listNamespaces(deps, "lake")).toStrictEqual([
      { path: ["lake", "a"], name: "a", level: 1, isSessionDefault: true },
      { path: ["lake", "a.b"], name: "a.b", level: 1 },
    ]);
  });

  test(`at most ${SPARK_INTROSPECT_IN_FLIGHT_MAX} statements are in flight at once`, async () => {
    let inFlight = 0;
    let most = 0;
    const top = Array.from({ length: 20 }, (_, i) => ({ namespace: `n${i}` }));
    const deps: SparkIntrospectDeps = {
      runSql: async (sql) => {
        inFlight++;
        most = Math.max(most, inFlight);
        await new Promise((resolve) => setTimeout(resolve, 1));
        inFlight--;
        if (sql === "SHOW CURRENT NAMESPACE") return result([{ catalog: "spark_catalog", namespace: "studio" }]);
        return result(sql === "SHOW NAMESPACES IN `lake`" ? top : []);
      },
      confKeys: async () => [],
    };
    expect(await listNamespaces(deps, "lake")).toHaveLength(20);
    expect(most).toBe(SPARK_INTROSPECT_IN_FLIGHT_MAX);
  });

  test("a batch with a failure answers only once every statement in it has, with the first failure in its order", async () => {
    const first = spark("[SCHEMA_NOT_FOUND] a");
    const later = spark("[SCHEMA_NOT_FOUND] c");
    let open: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      open = () => resolve();
    });
    let running = 0;
    const deps: SparkIntrospectDeps = {
      runSql: async (sql) => {
        if (sql === "SHOW CURRENT NAMESPACE") return result([{ catalog: "spark_catalog", namespace: "studio" }]);
        if (sql === "SHOW NAMESPACES IN `lake`") {
          return result([{ namespace: "a" }, { namespace: "b" }, { namespace: "c" }]);
        }
        // c fails at once; a and b answer only once the gate opens, a with a failure.
        if (sql === "SHOW NAMESPACES IN `lake`.c") throw later;
        running++;
        await gate;
        running--;
        if (sql === "SHOW NAMESPACES IN `lake`.a") throw first;
        return result([]);
      },
      confKeys: async () => [],
    };
    let settled = false;
    const walk = listNamespaces(deps, "lake");
    // Watched rather than awaited, so the test can look at the walk before the gate opens.
    walk.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    await new Promise((resolve) => setTimeout(resolve, 1));
    expect(running).toBe(2);
    expect(settled).toBe(false);
    open();
    await expect(walk).rejects.toBe(first);
    expect(running).toBe(0);
  });

  test("more than 1,000 namespaces is refused with the walk's sentence, never a shorter list", async () => {
    const many = Array.from({ length: 1_001 }, (_, i) => ({ namespace: `n${i}` }));
    const { deps } = stub({ ...CURRENT, "SHOW NAMESPACES IN `lake`": many });
    await expect(listNamespaces(deps, "lake")).rejects.toThrow(SPARK_INTROSPECT_SENTENCES.walkBound);
  });

  test("the walk's bound counts every level: 1,000 in all are listed, and 1,001 are refused", async () => {
    const fits: Record<string, Answer> = {
      ...CURRENT,
      "SHOW NAMESPACES IN `lake`": [{ namespace: "a" }],
      "SHOW NAMESPACES IN `lake`.a": children("a", 999),
    };
    for (let i = 0; i < 999; i++) fits[`SHOW NAMESPACES IN \`lake\`.a.c${i}`] = [];
    expect(await listNamespaces(stub(fits).deps, "lake")).toHaveLength(1_000);
    const over = stub({
      ...CURRENT,
      "SHOW NAMESPACES IN `lake`": [{ namespace: "a" }],
      "SHOW NAMESPACES IN `lake`.a": children("a", 1_000),
    });
    await expect(listNamespaces(over.deps, "lake")).rejects.toThrow(SPARK_INTROSPECT_SENTENCES.walkBound);
    expect(over.sent.filter((sql) => sql.startsWith("SHOW NAMESPACES"))).toHaveLength(2);
  });

  test("the walk stops at the batch that passes the bound, before the next batch is sent", async () => {
    const answers: Record<string, Answer> = {
      ...CURRENT,
      "SHOW NAMESPACES IN `lake`": Array.from({ length: SPARK_INTROSPECT_IN_FLIGHT_MAX + 1 }, (_, i) => ({
        namespace: `n${i}`,
      })),
      "SHOW NAMESPACES IN `lake`.n0": children("n0", 1_000 - SPARK_INTROSPECT_IN_FLIGHT_MAX),
    };
    for (let i = 1; i < SPARK_INTROSPECT_IN_FLIGHT_MAX; i++) answers[`SHOW NAMESPACES IN \`lake\`.n${i}`] = [];
    const { deps, sent } = stub(answers);
    // Nine namespaces and the 992 children of the first batch make 1,001: the ninth is never asked for its children.
    await expect(listNamespaces(deps, "lake")).rejects.toThrow(SPARK_INTROSPECT_SENTENCES.walkBound);
    expect(sent.filter((sql) => sql.startsWith("SHOW NAMESPACES"))).toHaveLength(1 + SPARK_INTROSPECT_IN_FLIGHT_MAX);
  });

  test("a namespace nested deeper than 8 parts is refused with the walk's sentence", async () => {
    const answers: Record<string, Answer> = { ...CURRENT };
    const letters = "abcdefghi".split("");
    answers["SHOW NAMESPACES IN `lake`"] = [{ namespace: "a" }];
    for (let depth = 1; depth <= 8; depth++) {
      answers[`SHOW NAMESPACES IN \`lake\`.${letters.slice(0, depth).join(".")}`] = [
        { namespace: letters.slice(0, depth + 1).join(".") },
      ];
    }
    await expect(listNamespaces(stub(answers).deps, "lake")).rejects.toThrow(SPARK_INTROSPECT_SENTENCES.walkBound);
  });

  test("eight levels with nothing below the deepest are listed whole", async () => {
    const answers: Record<string, Answer> = { ...CURRENT, "SHOW NAMESPACES IN `lake`": [{ namespace: "a" }] };
    const letters = "abcdefgh".split("");
    for (let depth = 1; depth <= 8; depth++) {
      answers[`SHOW NAMESPACES IN \`lake\`.${letters.slice(0, depth).join(".")}`] =
        depth < 8 ? [{ namespace: letters.slice(0, depth + 1).join(".") }] : [];
    }
    expect(await listNamespaces(stub(answers).deps, "lake")).toHaveLength(8);
  });

  test("a child that is not inside the namespace asked about is refused", async () => {
    const { deps } = stub({
      ...CURRENT,
      "SHOW NAMESPACES IN `lake`": [{ namespace: "a" }],
      "SHOW NAMESPACES IN `lake`.a": [{ namespace: "b.c" }],
    });
    await expect(listNamespaces(deps, "lake")).rejects.toThrow(SPARK_INTROSPECT_SENTENCES.namespaceListingNotReadable);
  });

  test("a child listed as the namespace asked about is refused, not walked again", async () => {
    const { deps } = stub({
      ...CURRENT,
      "SHOW NAMESPACES IN `lake`": [{ namespace: "a" }],
      "SHOW NAMESPACES IN `lake`.a": [{ namespace: "a" }],
    });
    await expect(listNamespaces(deps, "lake")).rejects.toThrow(SPARK_INTROSPECT_SENTENCES.namespaceListingNotReadable);
  });

  test("a catalog that cannot load fails its expansion with Spark's own text, and the catalog list still names it", async () => {
    const text = "requirement failed: Option 'url' is required.";
    const { deps } = stub({ ...CURRENT, "SHOW NAMESPACES IN `studio_unloaded`": spark(text) });
    await expect(listNamespaces(deps, "studio_unloaded")).rejects.toThrow(text);
  });
});

describe("objects", () => {
  const Q = "`spark_catalog`.studio";
  const container = ["spark_catalog", "studio"] as const;

  test("tables are SHOW TABLES minus temporary rows and the views SHOW VIEWS lists", async () => {
    const { deps } = stub({
      [`SHOW TABLES IN ${Q}`]: [
        { namespace: "studio", tableName: "orders", isTemporary: false },
        { namespace: "studio", tableName: "v_orders", isTemporary: false },
        { namespace: "", tableName: "tmp", isTemporary: true },
      ],
      [`SHOW VIEWS IN ${Q}`]: [
        { namespace: "studio", viewName: "v_orders", isTemporary: false },
        { namespace: "", viewName: "tmp_v", isTemporary: true },
      ],
    });
    expect(await listObjects(deps, container, "table")).toStrictEqual([
      { path: ["spark_catalog", "studio", "orders"], name: "orders", kind: "table" },
    ]);
    expect(await listObjects(deps, container, "view")).toStrictEqual([
      { path: ["spark_catalog", "studio", "v_orders"], name: "v_orders", kind: "view" },
    ]);
  });

  test("functions are named after the asked prefix, whole, and a row without the prefix is skipped", async () => {
    const { deps } = stub({
      [`SHOW USER FUNCTIONS IN ${Q}`]: [
        { function: "spark_catalog.studio.f.x" },
        { function: "spark_catalog.studio.f y" },
        { function: "spark_catalog.studio.f`q" },
        { function: "spark_catalog.studio.f2" },
        { function: "t.x" },
        { function: "spark_catalog.t.x" },
      ],
    });
    expect((await listObjects(deps, container, "function")).map((o) => o.name)).toStrictEqual([
      "f.x",
      "f y",
      "f`q",
      "f2",
    ]);
  });

  test("a function row is matched to the asked prefix without case, and a row that is the prefix alone is skipped", async () => {
    const { deps } = stub({
      "SHOW USER FUNCTIONS IN `spark_catalog`.STUDIO": [
        { function: "spark_catalog.studio.f" },
        { function: "SPARK_CATALOG.Studio.g" },
        { function: "spark_catalog.studio." },
      ],
    });
    expect(await listObjects(deps, ["spark_catalog", "STUDIO"], "function")).toStrictEqual([
      { path: ["spark_catalog", "STUDIO", "f"], name: "f", kind: "function" },
      { path: ["spark_catalog", "STUDIO", "g"], name: "g", kind: "function" },
    ]);
  });

  test("a catalog without views or functions answers none of them", async () => {
    const { deps } = stub({
      [`SHOW TABLES IN ${Q}`]: [{ namespace: "studio", tableName: "orders", isTemporary: false }],
      [`SHOW VIEWS IN ${Q}`]: spark("[MISSING_CATALOG_ABILITY.VIEWS] Catalog lake does not support VIEWS."),
      [`SHOW USER FUNCTIONS IN ${Q}`]: spark(
        "[MISSING_CATALOG_ABILITY.FUNCTIONS] Catalog lake does not support FUNCTIONS.",
      ),
    });
    expect(await countObjects(deps, container)).toStrictEqual({
      table: { count: 1 },
      view: { count: 0 },
      function: { count: 0 },
    });
  });

  test("countObjects counts each kind from its own listing", async () => {
    const { deps } = stub({
      [`SHOW TABLES IN ${Q}`]: [
        { namespace: "studio", tableName: "orders", isTemporary: false },
        { namespace: "studio", tableName: "v1", isTemporary: false },
      ],
      [`SHOW VIEWS IN ${Q}`]: [
        { namespace: "studio", viewName: "v1", isTemporary: false },
        { namespace: "studio", viewName: "v2", isTemporary: false },
      ],
      [`SHOW USER FUNCTIONS IN ${Q}`]: [
        { function: "spark_catalog.studio.f1" },
        { function: "spark_catalog.studio.f2" },
        { function: "spark_catalog.studio.f3" },
      ],
    });
    expect(await countObjects(deps, container)).toStrictEqual({
      table: { count: 1 },
      view: { count: 2 },
      function: { count: 3 },
    });
  });

  test("another Spark failure of SHOW VIEWS or SHOW USER FUNCTIONS is not read as none", async () => {
    const { deps } = stub({
      [`SHOW VIEWS IN ${Q}`]: spark("[TABLE_OR_VIEW_NOT_FOUND] x"),
      [`SHOW USER FUNCTIONS IN ${Q}`]: spark("[SCHEMA_NOT_FOUND] x"),
    });
    await expect(listObjects(deps, container, "view")).rejects.toThrow("[TABLE_OR_VIEW_NOT_FOUND] x");
    await expect(listObjects(deps, container, "function")).rejects.toThrow("[SCHEMA_NOT_FOUND] x");
  });

  test.each<[string, Answer, string]>([
    [
      "a cut answer",
      result([{ namespace: "studio", tableName: "t", isTemporary: false }], "rows"),
      SPARK_INTROSPECT_SENTENCES.listTooLarge,
    ],
    ["a receive-cap failure", new SparkError("receive-cap", "x"), SPARK_INTROSPECT_SENTENCES.listTooLarge],
    [
      "a command-batch-bound failure",
      new SparkError("command-batch-bound", "x"),
      SPARK_INTROSPECT_SENTENCES.batchBound,
    ],
  ])("%s of SHOW TABLES or SHOW VIEWS is refused with the tree's sentence", async (_label, answer, sentence) => {
    const views = stub({ [`SHOW VIEWS IN ${Q}`]: answer });
    await expect(listObjects(views.deps, container, "table")).rejects.toThrow(sentence);
    const tables = stub({ [`SHOW VIEWS IN ${Q}`]: [], [`SHOW TABLES IN ${Q}`]: answer });
    await expect(listObjects(tables.deps, container, "table")).rejects.toThrow(sentence);
  });

  test("any other failure passes through unchanged", async () => {
    const dropped = new SparkError("connection-dropped", "dropped");
    const { deps } = stub({ [`SHOW VIEWS IN ${Q}`]: dropped });
    await expect(listObjects(deps, container, "view")).rejects.toBe(dropped);
  });

  test("a failure that is not Spark's own passes through even when its text names a missing ability", async () => {
    const views = new SparkError("connection-dropped", "[MISSING_CATALOG_ABILITY.VIEWS] x");
    const functions = new SparkError("connection-dropped", "[MISSING_CATALOG_ABILITY.FUNCTIONS] x");
    const { deps } = stub({ [`SHOW VIEWS IN ${Q}`]: views, [`SHOW USER FUNCTIONS IN ${Q}`]: functions });
    await expect(listObjects(deps, container, "view")).rejects.toBe(views);
    await expect(listObjects(deps, container, "function")).rejects.toBe(functions);
  });

  test.each<[string, SparkError, string]>([
    ["receive-cap", new SparkError("receive-cap", "x"), SPARK_INTROSPECT_SENTENCES.listTooLarge],
    ["command-batch-bound", new SparkError("command-batch-bound", "x"), SPARK_INTROSPECT_SENTENCES.batchBound],
  ])("the refusal of a %s failure keeps that failure as its cause", async (_label, failure, sentence) => {
    const { deps } = stub({ [`SHOW VIEWS IN ${Q}`]: failure });
    const refusal = await listObjects(deps, container, "view").catch((error: unknown) => error);
    expect(refusal).toBeInstanceOf(SparkError);
    expect(refusal).toMatchObject({ category: "refused", message: sentence });
    expect((refusal as SparkError).cause).toBe(failure);
  });

  test("a namespace segment the spelling reader cannot read is refused before any statement", async () => {
    const { deps, sent } = stub({});
    await expect(listObjects(deps, ["spark_catalog", "a b"], "table")).rejects.toThrow(
      SPARK_INTROSPECT_SENTENCES.namespaceNotReadable,
    );
    await expect(describeObject(deps, ["spark_catalog", "x;y", "t"], "table")).rejects.toThrow(
      SPARK_INTROSPECT_SENTENCES.namespaceNotReadable,
    );
    expect(sent).toStrictEqual([]);
  });

  test("a container or object path of another length is refused before any statement", async () => {
    const { deps, sent } = stub({});
    const refused = SPARK_INTROSPECT_SENTENCES.namespaceNotReadable;
    await Promise.all([
      expect(listObjects(deps, ["spark_catalog"], "table")).rejects.toThrow(refused),
      expect(countObjects(deps, ["spark_catalog", "studio", "orders"])).rejects.toThrow(refused),
      expect(describeObjects(deps, ["spark_catalog", "studio", "orders"], "view")).rejects.toThrow(refused),
      expect(describeObject(deps, ["spark_catalog", "studio"], "table")).rejects.toThrow(refused),
      expect(readObjectSource(deps, ["spark_catalog", "studio", "orders", "x"], "view")).rejects.toThrow(refused),
    ]);
    expect(sent).toStrictEqual([]);
  });

  test("an unknown kind, a missing answer column and a flag that is not a boolean are refused", async () => {
    const { deps } = stub({
      [`SHOW USER FUNCTIONS IN ${Q}`]: [{ name: "f" }],
      [`SHOW VIEWS IN ${Q}`]: [{ namespace: "studio", viewName: "v", isTemporary: "false" }],
    });
    await expect(listObjects(deps, container, "index")).rejects.toThrow(SPARK_INTROSPECT_SENTENCES.noKind("index"));
    await expect(listObjects(deps, container, "function")).rejects.toMatchObject({
      category: "unreadable",
      message: SPARK_ERROR_SENTENCES.unreadable("the function column of Spark's answer"),
    });
    await expect(listObjects(deps, container, "view")).rejects.toMatchObject({
      category: "unreadable",
      message: SPARK_ERROR_SENTENCES.unreadable("the isTemporary column of Spark's answer"),
    });
  });
});

describe("columns and definitions", () => {
  const path = ["spark_catalog", "studio", "orders"] as const;
  const D = "DESCRIBE TABLE `spark_catalog`.studio.`orders`";

  test("DESCRIBE TABLE is read to the first empty row or `#` header, and a column named `#tag` keeps its type", async () => {
    const { deps } = stub({
      [D]: [
        { col_name: "id", data_type: "bigint", comment: null },
        { col_name: "#tag", data_type: "string", comment: null },
        { col_name: "# Partition Information", data_type: "", comment: "" },
        { col_name: "day", data_type: "date", comment: null },
      ],
    });
    expect(await describeObject(deps, path, "table")).toStrictEqual({
      path: [...path],
      columns: [
        { name: "id", type: "bigint", nullable: true, isPrimary: false },
        { name: "#tag", type: "string", nullable: true, isPrimary: false },
      ],
      indexes: [],
      foreignKeys: [],
    });
    const blank = stub({
      [D]: [
        { col_name: "id", data_type: "int" },
        { col_name: "", data_type: "" },
        { col_name: "x", data_type: "int" },
      ],
    });
    expect((await describeObject(blank.deps, path, "view")).columns.map((c) => c.name)).toStrictEqual(["id"]);
  });

  test("a function answers no columns without a statement, and a kind with none to describe is refused", async () => {
    const { deps, sent } = stub({});
    expect(await describeObject(deps, ["spark_catalog", "studio", "f1"], "function")).toStrictEqual({
      path: ["spark_catalog", "studio", "f1"],
      columns: [],
      indexes: [],
      foreignKeys: [],
    });
    await expect(describeObject(deps, path, "index")).rejects.toThrow(SPARK_INTROSPECT_SENTENCES.noColumns("index"));
    expect(sent).toStrictEqual([]);
  });

  test("describeObjects runs one DESCRIBE per object in listing order and reports the caller's bound", async () => {
    const tables = ["a", "b", "c"].map((tableName) => ({ namespace: "studio", tableName, isTemporary: false }));
    const answers: Record<string, Answer> = {
      "SHOW VIEWS IN `spark_catalog`.studio": [],
      "SHOW TABLES IN `spark_catalog`.studio": tables,
    };
    for (const name of ["a", "b", "c"]) {
      answers[`DESCRIBE TABLE \`spark_catalog\`.studio.\`${name}\``] = [{ col_name: "id", data_type: "int" }];
    }
    const { deps, sent } = stub(answers);
    const batch = await describeObjects(deps, ["spark_catalog", "studio"], "table", 2);
    expect(batch.details.map((d) => d.path[2])).toStrictEqual(["a", "b"]);
    expect(batch.truncated).toStrictEqual({ limit: 2, reason: callerBoundTruncationReason(2) });
    expect(sent.filter((sql) => sql.startsWith("DESCRIBE"))).toHaveLength(2);
    expect((await describeObjects(deps, ["spark_catalog", "studio"], "table")).truncated).toBeUndefined();
    expect(await describeObjects(deps, ["spark_catalog", "studio"], "function")).toStrictEqual({ details: [] });
  });

  test(`with no limit, describeObjects stops at ${SPARK_DESCRIBE_OBJECTS_MAX} objects with the inventory reason`, async () => {
    const count = SPARK_DESCRIBE_OBJECTS_MAX + 1;
    const tables = Array.from({ length: count }, (_, i) => ({
      namespace: "studio",
      tableName: `t${i}`,
      isTemporary: false,
    }));
    const deps: SparkIntrospectDeps = {
      runSql: async (sql) =>
        result(
          sql.startsWith("SHOW TABLES")
            ? tables
            : sql.startsWith("SHOW VIEWS")
              ? []
              : [{ col_name: "id", data_type: "int" }],
        ),
      confKeys: async () => [],
    };
    const batch = await describeObjects(deps, ["spark_catalog", "studio"], "table");
    expect(batch.details).toHaveLength(SPARK_DESCRIBE_OBJECTS_MAX);
    expect(batch.truncated).toStrictEqual({ limit: SPARK_DESCRIBE_OBJECTS_MAX, reason: INVENTORY_TRUNCATION_REASON });
  });

  /** A namespace of `count` tables, t0 onwards, each with one column, and how many DESCRIBEs it was sent. */
  function tables(count: number) {
    const rows = Array.from({ length: count }, (_, i) => ({
      namespace: "studio",
      tableName: `t${i}`,
      isTemporary: false,
    }));
    let described = 0;
    const deps: SparkIntrospectDeps = {
      runSql: async (sql) => {
        if (sql === "SHOW VIEWS IN `spark_catalog`.studio") return result([]);
        if (sql === "SHOW TABLES IN `spark_catalog`.studio") return result(rows);
        if (!sql.startsWith("DESCRIBE TABLE `spark_catalog`.studio.")) throw new Error(`unexpected statement: ${sql}`);
        described++;
        return result([{ col_name: "id", data_type: "int" }]);
      },
      confKeys: async () => [],
    };
    return { deps, described: () => described };
  }

  test("a caller's limit above 1,000 is held to 1,000 with Studio's reason, and one at or below it keeps the caller's", async () => {
    const container = ["spark_catalog", "studio"];
    const many = tables(1_001);
    const capped = await describeObjects(many.deps, container, "table", 5_000);
    expect(capped.details).toHaveLength(1_000);
    expect(capped.truncated).toStrictEqual({ limit: 1_000, reason: SPARK_INTROSPECT_SENTENCES.describeBound });
    expect(many.described()).toBe(1_000);
    expect(SPARK_INTROSPECT_SENTENCES.describeBound).toContain("1,000");
    const atCap = await describeObjects(many.deps, container, "table", 1_000);
    expect(atCap.truncated).toStrictEqual({ limit: 1_000, reason: callerBoundTruncationReason(1_000) });

    const few = tables(20);
    const bounded = await describeObjects(few.deps, container, "table", 10);
    expect(bounded.details.map((d) => d.path[2])).toStrictEqual(Array.from({ length: 10 }, (_, i) => `t${i}`));
    expect(bounded.truncated).toStrictEqual({ limit: 10, reason: callerBoundTruncationReason(10) });
    expect(few.described()).toBe(10);
  });

  const NOT_FOUND =
    "[TABLE_OR_VIEW_NOT_FOUND] The table or view `spark_catalog`.`studio`.`b` cannot be found. Verify the spelling and correctness of the schema and catalog.";

  /** Tables a, b and c in listing order, where the DESCRIBE of b answers `answer`. */
  function threeTables(answer: Answer) {
    return stub({
      "SHOW VIEWS IN `spark_catalog`.studio": [],
      "SHOW TABLES IN `spark_catalog`.studio": ["a", "b", "c"].map((tableName) => ({
        namespace: "studio",
        tableName,
        isTemporary: false,
      })),
      "DESCRIBE TABLE `spark_catalog`.studio.`a`": [{ col_name: "id", data_type: "int" }],
      "DESCRIBE TABLE `spark_catalog`.studio.`b`": answer,
      "DESCRIBE TABLE `spark_catalog`.studio.`c`": [{ col_name: "day", data_type: "date" }],
    });
  }

  test("a table dropped after the listing is left out of describeObjects, and the tables around it are described", async () => {
    const { deps, sent } = threeTables(spark(NOT_FOUND));
    expect(await describeObjects(deps, ["spark_catalog", "studio"], "table")).toStrictEqual({
      details: [
        {
          path: ["spark_catalog", "studio", "a"],
          columns: [{ name: "id", type: "int", nullable: true, isPrimary: false }],
          indexes: [],
          foreignKeys: [],
        },
        {
          path: ["spark_catalog", "studio", "c"],
          columns: [{ name: "day", type: "date", nullable: true, isPrimary: false }],
          indexes: [],
          foreignKeys: [],
        },
      ],
    });
    expect(sent.filter((sql) => sql.startsWith("DESCRIBE"))).toStrictEqual([
      "DESCRIBE TABLE `spark_catalog`.studio.`a`",
      "DESCRIBE TABLE `spark_catalog`.studio.`b`",
      "DESCRIBE TABLE `spark_catalog`.studio.`c`",
    ]);
  });

  test.each<[string, SparkError]>([
    ["a permission refusal", spark("Permission denied: user [studio] does not have [select] privilege on [studio/b]")],
    [
      "a table Spark cannot read",
      spark(
        "org.apache.hadoop.hive.serde2.SerDeException: java.lang.ClassNotFoundException: Class org.example.Csv not found",
      ),
    ],
    ["another error class with a missing table in its cause", spark(`[INTERNAL_ERROR] x\nCause: ${NOT_FOUND}`)],
    ["a failure that is not Spark's own, whatever its text", new SparkError("connection-dropped", NOT_FOUND)],
  ])("%s on one DESCRIBE fails the whole describeObjects read", async (_label, failure) => {
    const { deps } = threeTables(failure);
    await expect(describeObjects(deps, ["spark_catalog", "studio"], "table")).rejects.toBe(failure);
  });

  test("SHOW CREATE TABLE gives one part: regenerated for a table, stored for a view, bounded by the limit", async () => {
    const S = "SHOW CREATE TABLE `spark_catalog`.studio.`orders`";
    const ddl = "CREATE TABLE spark_catalog.studio.orders (id BIGINT) USING parquet";
    const { deps } = stub({ [S]: [{ createtab_stmt: ddl }] });
    expect(await readObjectSource(deps, path, "table")).toStrictEqual({
      path: [...path],
      kind: "table",
      parts: [{ id: "ddl", label: "Definition", text: ddl, language: "sql", form: "complete", origin: "regenerated" }],
    });
    expect((await readObjectSource(deps, path, "view")).parts[0]).toMatchObject({ origin: "stored" });
    expect((await readObjectSource(deps, path, "table", 10)).parts[0]).toMatchObject({ truncated: { limit: 10 } });
  });

  test("Spark's refusal becomes the part's unavailable sentence; other failures and an empty answer do not", async () => {
    const S = "SHOW CREATE TABLE `spark_catalog`.studio.`orders`";
    const refused = "[UNSUPPORTED_SHOW_CREATE_TABLE.ON_TEMPORARY_VIEW] x";
    expect((await readObjectSource(stub({ [S]: spark(refused) }).deps, path, "view")).parts).toStrictEqual([
      { id: "ddl", label: "Definition", unavailable: refused },
    ]);
    const dropped = new SparkError("connection-dropped", "dropped");
    await expect(readObjectSource(stub({ [S]: dropped }).deps, path, "table")).rejects.toBe(dropped);
    await expect(readObjectSource(stub({ [S]: [] }).deps, path, "table")).rejects.toMatchObject({
      category: "unreadable",
    });
    await expect(readObjectSource(stub({}).deps, path, "function")).rejects.toThrow(
      SPARK_INTROSPECT_SENTENCES.noSource("function"),
    );
  });
});

// Names that need quoting on every path they travel: the tree, the statements and the generators all name one object.
describe("every listed name round-trips to the statement that reads it", () => {
  const NAMESPACES: [string, string[]][] = [
    ["`a.b`", ["a.b"]],
    ["`x``y`", ["x`y"]],
    ["`a b`", ["a b"]],
    ["select", ["select"]],
    ["`café`", ["café"]],
  ];

  test.each(NAMESPACES)("namespace %s and a table named #tag", async (spelling, parts) => {
    const q = `\`lake\`.${spelling}`;
    const { deps, sent } = stub({
      ...CURRENT,
      "SHOW NAMESPACES IN `lake`": [{ namespace: spelling }],
      [`SHOW NAMESPACES IN ${q}`]: [],
      [`SHOW VIEWS IN ${q}`]: [],
      [`SHOW TABLES IN ${q}`]: [{ namespace: spelling, tableName: "#tag", isTemporary: false }],
      [`DESCRIBE TABLE ${q}.\`#tag\``]: [{ col_name: "id", data_type: "int" }],
      [`SHOW CREATE TABLE ${q}.\`#tag\``]: [{ createtab_stmt: "CREATE TABLE x (id INT)" }],
    });
    const [container] = await listNamespaces(deps, "lake");
    expect(container.name).toBe(spelling);
    const [object] = await listObjects(deps, container.path, "table");
    await describeObject(deps, object.path, "table");
    await readObjectSource(deps, object.path, "table");

    const expected = ["lake", ...parts, "#tag"];
    const tail = (prefix: string) => sent.find((sql) => sql.startsWith(prefix))?.slice(prefix.length) ?? "";
    expect(nameParts(tail("SHOW TABLES IN "))).toStrictEqual(["lake", ...parts]);
    expect(nameParts(tail("DESCRIBE TABLE "))).toStrictEqual(expected);
    expect(nameParts(tail("SHOW CREATE TABLE "))).toStrictEqual(expected);
    const path = quoteObjectPath(object.path, SPARK_CAPS);
    expect(nameParts(path)).toStrictEqual(expected);
    expect(generateCountQuery(object.path, SPARK_CAPS)).toContain(`FROM ${path}`);
    // The session's USE NAMESPACE writes the connection's namespace with the same spelling.
    expect(parseNamespaceSpelling(spellNamespace(parts))).toStrictEqual(parts);
  });
});
