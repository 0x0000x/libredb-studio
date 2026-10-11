/**
 * BigQuery provider, end to end over a fake network.
 *
 * The real provider, the real REST transport (`bigquery/transport.ts`) and the real token source (`bigquery/auth.ts`,
 * signing a genuine RS256 service-account assertion) all run. Only `createNodeTransport` is replaced, by a fake that
 * routes on the URL the provider sent: Google's token endpoint and the BigQuery REST v2 jobs, datasets and tables
 * resources. No live project is reachable from CI, so every answer is built from the documented REST shapes; the
 * fixture is generic (project `demo-project`, datasets `shop` and `archive`). `mock.module()` is process-wide, so it
 * lives in this file only and the runner gives the file its own process.
 */
import { afterEach, beforeEach, describe, expect, mock, test } from "bun:test";
import { createVerify, generateKeyPairSync } from "node:crypto";
import { AuthenticationError, QueryError, TimeoutError } from "@/lib/db/errors";
import type { NodeRequest, NodeResponse, NodeTransportOptions } from "@/lib/db/http/node-transport";
import { BIGQUERY_DRY_RUN_MARKER } from "@/lib/explain/bigquery-dryrun";
import type { DatabaseConnection } from "@/lib/types";
import { assertObjectSurface } from "../../helpers/object-surface-conformance";

const realNode = { ...(await import("@/lib/db/http/node-transport")) };
const { TransportError } = realNode;

// ============================================================================
// The fake network
// ============================================================================

type Wire = Record<string, unknown>;
type Reply = { status: number; json: Wire };
interface Col {
  name: string;
  type: string;
  mode?: "NULLABLE" | "REQUIRED" | "REPEATED";
  fields?: Col[];
}
interface Relation {
  type: "TABLE" | "VIEW" | "MATERIALIZED_VIEW";
  fields: Col[];
}
interface Logged {
  method: string;
  path: string;
  params: URLSearchParams;
  body: Wire | null;
  authorization: string | undefined;
}

const { privateKey, publicKey } = generateKeyPairSync("rsa", {
  modulusLength: 2048,
  privateKeyEncoding: { type: "pkcs8", format: "pem" },
  publicKeyEncoding: { type: "spki", format: "pem" },
});
const CLIENT_EMAIL = "studio@demo-project.iam.gserviceaccount.com";
const KEY_JSON = JSON.stringify({ type: "service_account", client_email: CLIENT_EMAIL, private_key: privateKey });

const API = "https://bigquery.googleapis.com/bigquery/v2/projects/demo-project";

const ORDER_COLUMNS: Col[] = [
  { name: "order_id", type: "INT64", mode: "REQUIRED" },
  { name: "customer_id", type: "INT64" },
  { name: "placed_at", type: "TIMESTAMP" },
  { name: "total", type: "NUMERIC" },
];
const CUSTOMER_COLUMNS: Col[] = [
  { name: "customer_id", type: "INT64", mode: "REQUIRED" },
  { name: "name", type: "STRING" },
  {
    name: "address",
    type: "RECORD",
    fields: [
      { name: "city", type: "STRING" },
      { name: "zip", type: "INT64" },
    ],
  },
  { name: "tags", type: "STRING", mode: "REPEATED" },
];

function freshModel(): Record<string, { location: string; relations: Record<string, Relation> }> {
  return {
    shop: {
      location: "EU",
      relations: {
        orders: { type: "TABLE", fields: ORDER_COLUMNS },
        customers: { type: "TABLE", fields: CUSTOMER_COLUMNS },
        order_summary: {
          type: "VIEW",
          fields: [
            { name: "customer_id", type: "INT64" },
            { name: "spent", type: "NUMERIC" },
          ],
        },
        daily_totals: {
          type: "MATERIALIZED_VIEW",
          fields: [
            { name: "day", type: "DATE" },
            { name: "total", type: "NUMERIC" },
          ],
        },
      },
    },
    archive: {
      location: "EU",
      relations: { orders_2023: { type: "TABLE", fields: ORDER_COLUMNS } },
    },
  };
}

const dataType = (col: Col): string => {
  const base = col.fields ? `STRUCT<${col.fields.map((f) => `${f.name} ${dataType(f)}`).join(", ")}>` : col.type;
  return col.mode === "REPEATED" ? `ARRAY<${base}>` : base;
};
const infoType = (type: Relation["type"]) => (type === "TABLE" ? "BASE TABLE" : type.replace("_", " "));

const stringSchema = (names: string[]) => ({ fields: names.map((name) => ({ name, type: "STRING" })) });
const stringRows = (rows: string[][]) => rows.map((row) => ({ f: row.map((v) => ({ v })) }));
const ok = (json: Wire): Reply => ({ status: 200, json });
const apiError = (status: number, message: string, reason: string): Reply => ({
  status,
  json: { error: { code: status, message, errors: [{ reason, message }] } },
});
const done = (names: string[], rows: string[][], extra: Wire = {}): Reply =>
  ok({
    jobComplete: true,
    jobReference: { jobId: "job-meta", location: "EU" },
    schema: stringSchema(names),
    rows: stringRows(rows),
    totalBytesProcessed: "10485760",
    totalBytesBilled: "10485760",
    ...extra,
  });

class FakeGoogle {
  requests: Logged[] = [];
  model = freshModel();
  informationSchema = true;
  tokenReply: Reply | null = null;
  datasetsReply: Reply | null = null;
  tokensIssued = 0;
  closed = 0;
  /** A test's own answer for a jobs.query / GET getQueryResults; return undefined to fall through. */
  onQuery: (sql: string, body: Wire) => Reply | undefined = () => undefined;
  onPoll: (jobId: string, params: URLSearchParams) => Reply | undefined = () => undefined;

  create(options: NodeTransportOptions) {
    const authorization = options.headers.authorization;
    return {
      request: async (request: NodeRequest): Promise<NodeResponse> => {
        const url = new URL(request.url);
        const reply = this.route(request, url, authorization);
        return {
          status: reply.status,
          contentType: "application/json",
          retryAfter: null,
          text: JSON.stringify(reply.json),
        };
      },
      close: () => {
        this.closed += 1;
      },
    };
  }

  private route(request: NodeRequest, url: URL, authorization: string | undefined): Reply {
    if (url.host === "oauth2.googleapis.com") return this.token(request);
    const path = url.pathname.replace("/bigquery/v2/projects/demo-project", "");
    const body = request.body ? (JSON.parse(request.body) as Wire) : null;
    this.requests.push({ method: request.method, path, params: url.searchParams, body, authorization });
    if (url.host !== "bigquery.googleapis.com" || !url.pathname.startsWith("/bigquery/v2/projects/demo-project")) {
      return apiError(404, "no such route", "notFound");
    }

    if (request.method === "POST" && path === "/queries") return this.jobsQuery(body!);
    if (request.method === "POST" && /^\/jobs\/[^/]+\/cancel$/.test(path)) return ok({ job: {} });
    const poll = /^\/queries\/([^/]+)$/.exec(path);
    if (request.method === "GET" && poll) {
      return this.onPoll(decodeURIComponent(poll[1]), url.searchParams) ?? apiError(404, "Not found: Job", "notFound");
    }
    if (request.method === "GET" && path === "/datasets") {
      if (this.datasetsReply) return this.datasetsReply;
      return ok({
        datasets: Object.entries(this.model).map(([id, d]) => ({
          datasetReference: { projectId: "demo-project", datasetId: id },
          location: d.location,
        })),
      });
    }
    const tables = /^\/datasets\/([^/]+)\/tables$/.exec(path);
    if (request.method === "GET" && tables) {
      const dataset = this.model[decodeURIComponent(tables[1])];
      if (!dataset) return apiError(404, "Not found: Dataset", "notFound");
      return ok({
        tables: Object.entries(dataset.relations).map(([id, r]) => ({ tableReference: { tableId: id }, type: r.type })),
      });
    }
    const table = /^\/datasets\/([^/]+)\/tables\/([^/]+)$/.exec(path);
    if (request.method === "GET" && table) {
      const found = this.model[decodeURIComponent(table[1])]?.relations[decodeURIComponent(table[2])];
      return found ? ok({ schema: { fields: found.fields } }) : apiError(404, "Not found: Table", "notFound");
    }
    return apiError(404, `no route for ${request.method} ${path}`, "notFound");
  }

  private token(request: NodeRequest): Reply {
    if (this.tokenReply) return this.tokenReply;
    const form = request.form!;
    expect(form.grant_type).toBe("urn:ietf:params:oauth:grant-type:jwt-bearer");
    const [header, payload, signature] = form.assertion.split(".");
    const verified = createVerify("RSA-SHA256")
      .update(`${header}.${payload}`)
      .verify(publicKey, signature, "base64url");
    expect(verified).toBe(true);
    const claims = JSON.parse(Buffer.from(payload, "base64url").toString()) as Wire;
    expect(claims).toMatchObject({ iss: CLIENT_EMAIL, scope: "https://www.googleapis.com/auth/bigquery" });
    this.tokensIssued += 1;
    return ok({ access_token: `ya29.fake-token-${this.tokensIssued}`, expires_in: 3600 });
  }

  private jobsQuery(body: Wire): Reply {
    const sql = String(body.query);
    if (body.dryRun === true) return ok({ jobComplete: true, totalBytesProcessed: "2048" });
    const own = this.onQuery(sql, body);
    if (own) return own;
    if (/INFORMATION_SCHEMA/.test(sql)) {
      if (!this.informationSchema) return apiError(403, "Access Denied: INFORMATION_SCHEMA", "accessDenied");
      return this.informationSchemaReply(sql);
    }
    if (/^\s*CREATE TABLE shop\.refunds/i.test(sql)) {
      this.model.shop.relations.refunds = { type: "TABLE", fields: [{ name: "refund_id", type: "INT64" }] };
      return ok({ jobComplete: true, jobReference: { jobId: "job-ddl", location: "EU" }, numDmlAffectedRows: "0" });
    }
    if (sql === "SELECT 1") {
      return ok({
        jobComplete: true,
        schema: { fields: [{ name: "f0_", type: "INTEGER" }] },
        rows: stringRows([["1"]]),
      });
    }
    return apiError(400, `Syntax error: unexpected "${sql}"`, "invalidQuery");
  }

  private informationSchemaReply(sql: string): Reply {
    if (/INFORMATION_SCHEMA\.TABLES/.test(sql)) {
      return done(
        ["table_schema", "table_name", "table_type"],
        Object.entries(this.model).flatMap(([ds, d]) =>
          Object.entries(d.relations).map(([id, r]) => [ds, id, infoType(r.type)]),
        ),
      );
    }
    if (/INFORMATION_SCHEMA\.COLUMNS/.test(sql)) {
      return done(
        ["table_schema", "table_name", "column_name", "data_type", "is_nullable"],
        Object.entries(this.model).flatMap(([ds, d]) =>
          Object.entries(d.relations).flatMap(([id, r]) =>
            r.fields.map((f) => [ds, id, f.name, dataType(f), f.mode === "REQUIRED" ? "NO" : "YES"]),
          ),
        ),
      );
    }
    if (/COUNT\(\*\) AS tables/.test(sql)) return done(["tables", "bytes"], [["5", "3145728"]]);
    if (/COUNTIF\(cache_hit\)/.test(sql)) return done(["total", "cached"], [["7200", "1800"]]);
    if (/GROUP BY query/.test(sql)) {
      return done(
        ["query", "calls", "total_ms", "min_ms", "max_ms"],
        [["SELECT * FROM shop.orders", "4", "2000", "300", "900"]],
      );
    }
    if (/state != 'DONE'/.test(sql)) {
      return done(
        ["job_id", "user_email", "state", "query", "creation_time", "start_time", "ms"],
        [
          [
            "job-live",
            "analyst@example.com",
            "RUNNING",
            "SELECT 2",
            "2026-10-11T08:00:00Z",
            "2026-10-11T08:00:01Z",
            "5400",
          ],
        ],
      );
    }
    if (/total_rows, total_logical_bytes/.test(sql)) {
      return done(
        ["table_schema", "table_name", "total_rows", "total_logical_bytes"],
        [
          ["shop", "orders", "1200", "2097152"],
          ["shop", "customers", "300", "1048576"],
        ],
      );
    }
    if (/GROUP BY table_schema/.test(sql))
      return done(
        ["table_schema", "bytes"],
        [
          ["shop", "3145728"],
          ["archive", "1024"],
        ],
      );
    return apiError(400, `unexpected INFORMATION_SCHEMA read: ${sql}`, "invalidQuery");
  }
}

let google = new FakeGoogle();
mock.module("@/lib/db/http/node-transport", () => ({
  ...realNode,
  createNodeTransport: (options: NodeTransportOptions) => google.create(options),
}));
const { BigQueryProvider } = await import("@/lib/db/providers/sql/bigquery");

// ============================================================================
// Harness
// ============================================================================

const connection = (over: Partial<DatabaseConnection> = {}): DatabaseConnection =>
  ({
    id: "bq-demo",
    name: "Demo BigQuery",
    type: "bigquery",
    database: "demo-project",
    password: KEY_JSON,
    createdAt: new Date(),
    ...over,
  }) as DatabaseConnection;

const opened: InstanceType<typeof BigQueryProvider>[] = [];
async function open(over: Partial<DatabaseConnection> = {}) {
  const provider = new BigQueryProvider(connection(over));
  opened.push(provider);
  await provider.connect();
  return provider;
}
const posted = (path: string) => google.requests.filter((r) => r.method === "POST" && r.path === path);
const queriesWith = (needle: RegExp) => posted("/queries").filter((r) => needle.test(String(r.body?.query)));

beforeEach(() => {
  google = new FakeGoogle();
});
afterEach(async () => {
  await Promise.all(opened.splice(0).map((p) => p.disconnect()));
});

// ============================================================================
// Connect
// ============================================================================

describe("connect", () => {
  test("signs a service-account assertion, detects the dataset location and probes with SELECT 1 there", async () => {
    const provider = await open();
    expect(provider.isConnected()).toBe(true);
    expect(google.tokensIssued).toBe(1);
    const probe = queriesWith(/^SELECT 1$/)[0];
    expect(probe.body).toMatchObject({ location: "EU", useLegacySql: false, dryRun: false });
    expect(probe.authorization).toBe("Bearer ya29.fake-token-1");

    await provider.query("SELECT 1");
    expect(google.tokensIssued).toBe(1);
  });

  test("an explicit location skips the datasets listing", async () => {
    await open({ location: "europe-west3" });
    expect(google.requests.some((r) => r.path === "/datasets")).toBe(false);
    expect(queriesWith(/^SELECT 1$/)[0].body).toMatchObject({ location: "europe-west3" });
  });

  test("the default dataset's location wins over the majority", async () => {
    google.model.archive.location = "US";
    google.model.extra = { location: "EU", relations: {} };
    await open({ schema: "archive" });
    expect(queriesWith(/^SELECT 1$/)[0].body).toMatchObject({ location: "US" });
  });

  test("a principal without datasets.list still connects, leaving the location to BigQuery", async () => {
    google.datasetsReply = apiError(403, "Access Denied: datasets.list", "accessDenied");
    const provider = await open();
    expect(provider.isConnected()).toBe(true);
    expect(queriesWith(/^SELECT 1$/)[0].body?.location).toBeUndefined();
  });

  test("a keyless connection that is not operator-seeded is refused before anything is sent", async () => {
    const provider = new BigQueryProvider(connection({ password: "" }));
    await expect(provider.connect()).rejects.toThrow(/only for operator-seeded connections/);
    expect(google.requests).toHaveLength(0);
    expect(google.tokensIssued).toBe(0);
  });

  test("a rejected token request is an AuthenticationError and leaves the provider disconnected", async () => {
    google.tokenReply = { status: 400, json: { error: "invalid_grant", error_description: "Invalid JWT Signature." } };
    const provider = new BigQueryProvider(connection());
    opened.push(provider);
    const failure = await provider.connect().catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(AuthenticationError);
    expect((failure as Error).message).toContain("Invalid JWT Signature.");
    expect(provider.isConnected()).toBe(false);
    expect(google.closed).toBeGreaterThan(0);
  });
});

// ============================================================================
// query()
// ============================================================================

describe("query", () => {
  const orderFields: Col[] = [
    { name: "order_id", type: "INT64" },
    { name: "placed_at", type: "TIMESTAMP" },
    {
      name: "ship",
      type: "RECORD",
      fields: [
        { name: "city", type: "STRING" },
        { name: "zip", type: "INT64" },
      ],
    },
    { name: "tags", type: "STRING", mode: "REPEATED" },
    { name: "total", type: "FLOAT" },
    { name: "paid", type: "BOOLEAN" },
  ];
  const wireRow = (id: string, micros: string, city: string, tags: string[], total: string, paid: string) => ({
    f: [
      { v: id },
      { v: micros },
      { v: { f: [{ v: city }, { v: "10115" }] } },
      { v: tags.map((v) => ({ v })) },
      { v: total },
      { v: paid },
    ],
  });

  test("polls an unfinished job, follows pageToken and decodes INT64, TIMESTAMP, STRUCT and ARRAY", async () => {
    google.onQuery = (sql) =>
      sql.startsWith("SELECT order_id")
        ? ok({ jobComplete: false, jobReference: { jobId: "job-1", location: "EU" } })
        : undefined;
    const polls: URLSearchParams[] = [];
    google.onPoll = (_job, params) => {
      polls.push(params);
      if (polls.length === 1) {
        return ok({
          jobComplete: true,
          schema: { fields: orderFields },
          rows: [
            wireRow("9007199254740993", "1700000000123456", "Berlin", ["a", "b"], "12.5", "true"),
            wireRow("2", "1700000100000000", "Hamburg", [], "0.5", "false"),
          ],
          pageToken: "p2",
        });
      }
      return ok({
        jobComplete: true,
        schema: { fields: orderFields },
        rows: [wireRow("3", "1700000200000000", "Bonn", ["c"], "7", "true")],
        totalBytesProcessed: "2097152",
        totalBytesBilled: "10485760",
        cacheHit: false,
      });
    };
    const provider = await open({ schema: "shop" });
    const result = await provider.query("SELECT order_id, placed_at, ship, tags, total, paid FROM orders");

    expect(posted("/queries").at(-1)!.body).toMatchObject({
      location: "EU",
      defaultDataset: { projectId: "demo-project", datasetId: "shop" },
      formatOptions: { useInt64Timestamp: true },
    });
    expect(polls[0].get("location")).toBe("EU");
    expect(polls[0].get("pageToken")).toBeNull();
    expect(polls[1].get("pageToken")).toBe("p2");

    expect(result.fields).toEqual(["order_id", "placed_at", "ship", "tags", "total", "paid"]);
    expect(result.rowCount).toBe(3);
    expect(result.rows[0]).toEqual({
      order_id: "9007199254740993",
      placed_at: "2023-11-14T22:13:20.123Z",
      ship: { city: "Berlin", zip: "10115" },
      tags: ["a", "b"],
      total: 12.5,
      paid: true,
    });
    expect(result.rows[1].tags).toEqual([]);
    expect(result.rows[2].ship).toEqual({ city: "Bonn", zip: "10115" });
    expect(result.columnTypes).toEqual({
      order_id: "INT64",
      placed_at: "TIMESTAMP",
      ship: "STRUCT<city STRING, zip INT64>",
      tags: "ARRAY<STRING>",
      total: "FLOAT64",
      paid: "BOOL",
    });
    expect(result.warnings).toEqual([{ message: "Processed 2 MB, billed 10 MB", severity: "info" }]);
  });

  test("says when the answer came from the cache and reports DML rows as rowCount", async () => {
    google.onQuery = (sql) =>
      sql.startsWith("SELECT cached")
        ? ok({
            jobComplete: true,
            schema: { fields: [{ name: "n", type: "INT64" }] },
            rows: stringRows([["1"]]),
            totalBytesProcessed: "0",
            totalBytesBilled: "0",
            cacheHit: true,
          })
        : sql.startsWith("DELETE")
          ? ok({ jobComplete: true, numDmlAffectedRows: "7", totalBytesProcessed: "1024" })
          : undefined;
    const provider = await open();
    expect((await provider.query("SELECT cached")).warnings).toEqual([
      { message: "Processed 0 B, billed 0 B (cache hit)", severity: "info" },
    ]);
    const dml = await provider.query("DELETE FROM shop.orders WHERE true");
    expect(dml.rowCount).toBe(7);
    expect(dml.columnTypes).toBeUndefined();
  });

  test("the dry-run marker sends dryRun:true and answers one bytes row without executing", async () => {
    const provider = await open();
    const result = await provider.query(`${BIGQUERY_DRY_RUN_MARKER}SELECT * FROM shop.orders`);
    const sent = posted("/queries").at(-1)!;
    expect(sent.body).toMatchObject({ dryRun: true });
    expect(result.rows).toEqual([{ bytes_processed: 2048, bytes_processed_human: "2 KB" }]);
    expect(result.fields).toEqual(["bytes_processed", "bytes_processed_human"]);
    expect(result.rowCount).toBe(1);
    expect(result.warnings).toBeUndefined();
  });

  test("refuses parameters before anything is sent", async () => {
    const provider = await open();
    const before = google.requests.length;
    await expect(provider.query("SELECT ?", [1])).rejects.toBeInstanceOf(QueryError);
    expect(google.requests).toHaveLength(before);
  });

  test("a 400 envelope is a QueryError carrying BigQuery's message", async () => {
    const provider = await open();
    const failure = await provider.query("SELEC nope").catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(QueryError);
    expect((failure as Error).message).toContain("Syntax error");
  });

  test("a 401 is an AuthenticationError and a 403 a QueryError", async () => {
    const provider = await open();
    google.onQuery = (sql) =>
      sql === "SELECT unauth"
        ? apiError(401, "Request had invalid authentication credentials.", "authError")
        : sql === "SELECT denied"
          ? apiError(403, "Access Denied: Table demo-project:shop.orders", "accessDenied")
          : undefined;
    await expect(provider.query("SELECT unauth")).rejects.toBeInstanceOf(AuthenticationError);
    const denied = await provider.query("SELECT denied").catch((error: unknown) => error);
    expect(denied).toBeInstanceOf(QueryError);
    expect((denied as Error).message).toContain("Access Denied");
  });

  test("a transport timeout is a TimeoutError", async () => {
    const provider = await open();
    google.onQuery = () => {
      throw new TransportError("timeout", "request timed out");
    };
    await expect(provider.query("SELECT slow")).rejects.toBeInstanceOf(TimeoutError);
  });

  test("a timeout while polling cancels the job so it stops billing", async () => {
    google.onQuery = (sql) =>
      sql === "SELECT slow"
        ? ok({ jobComplete: false, jobReference: { jobId: "job-slow", location: "EU" } })
        : undefined;
    google.onPoll = () => {
      throw new TransportError("timeout", "request timed out");
    };
    const provider = await open();
    await expect(provider.query("SELECT slow")).rejects.toBeInstanceOf(TimeoutError);
    const cancel = posted("/jobs/job-slow/cancel");
    expect(cancel).toHaveLength(1);
    expect(cancel[0].params.get("location")).toBe("EU");
  });
});

// ============================================================================
// Object surface and catalog
// ============================================================================

describe("object surface", () => {
  // BigQuery declares no source-bearing or editable kind, so the source half of the helper has nothing to read and no
  // absentSource / emptyKinds apply; every declared kind carries columns, hence noAbstainingKinds.
  const expectation = {
    containers: [["archive"], ["shop"]],
    container: ["shop"],
    kinds: { table: 2, view: 1, materialized_view: 1 },
    sampleObject: { path: ["shop", "orders"], kind: "table" },
    noAbstainingKinds: true,
  } as const;

  test("satisfies the conformance contract with INFORMATION_SCHEMA readable", async () => {
    const provider = await open({ schema: "shop" });
    await assertObjectSurface(provider, expectation);
    // One catalog read of each view serves every listing, count and bulk column read.
    expect(queriesWith(/INFORMATION_SCHEMA\.TABLES/)).toHaveLength(1);
    expect(queriesWith(/INFORMATION_SCHEMA\.COLUMNS/)).toHaveLength(1);
    expect(queriesWith(/INFORMATION_SCHEMA\.TABLES/)[0].body).toMatchObject({ jobTimeoutMs: expect.any(String) });
    expect(String(queriesWith(/INFORMATION_SCHEMA\.TABLES/)[0].body?.query)).toContain(
      "`demo-project`.`region-eu`.INFORMATION_SCHEMA.TABLES",
    );
  });

  test("satisfies it again over REST alone when INFORMATION_SCHEMA is refused", async () => {
    google.informationSchema = false;
    const provider = await open();
    await assertObjectSurface(provider, expectation);
    expect(google.requests.some((r) => r.method === "GET" && r.path === "/datasets/shop/tables")).toBe(true);
    expect(google.requests.some((r) => r.method === "GET" && r.path === "/datasets/shop/tables/orders")).toBe(true);
  });

  test("lists datasets as containers, marking the connection's default", async () => {
    const provider = await open({ schema: "shop" });
    expect(await provider.listContainers()).toEqual([
      { path: ["archive"], name: "archive", level: 0, isSessionDefault: false },
      { path: ["shop"], name: "shop", level: 0, isSessionDefault: true },
    ]);
    expect(await provider.listContainers(["shop"])).toEqual([]);
  });

  test("describeObjects reads columns from the catalog without a per-table call, and from REST per table otherwise", async () => {
    const withCatalog = await open();
    const batch = await withCatalog.describeObjects(["shop"], "table");
    expect(batch.details.map((d) => d.path)).toEqual([
      ["shop", "customers"],
      ["shop", "orders"],
    ]);
    expect(batch.details[0].columns).toContainEqual({
      name: "address",
      type: "STRUCT<city STRING, zip INT64>",
      nullable: true,
      isPrimary: false,
    });
    expect(batch.details[1].columns[0]).toEqual({ name: "order_id", type: "INT64", nullable: false, isPrimary: false });
    expect(google.requests.filter((r) => r.path.startsWith("/datasets/shop/tables/"))).toHaveLength(0);

    google.informationSchema = false;
    google.requests.length = 0;
    const viaRest = await (await open()).describeObjects(["shop"], "table");
    expect(viaRest.details[0].columns.map((c) => c.type)).toEqual([
      "INT64",
      "STRING",
      "STRUCT<city STRING, zip INT64>",
      "ARRAY<STRING>",
    ]);
    expect(google.requests.filter((r) => /^\/datasets\/shop\/tables\/[^/]+$/.test(r.path))).toHaveLength(2);
  });

  test("a table whose columns cannot be read loses its columns, not the inventory", async () => {
    google.informationSchema = false;
    const provider = await open();
    expect((await provider.listObjects(["shop"], "table")).map((o) => o.name)).toEqual(["customers", "orders"]);
    // Dropped after the (cached) listing: its columns read 404.
    delete google.model.shop.relations.customers;
    const batch = await provider.describeObjects(["shop"], "table");
    expect(batch.details.map((d) => [d.path[1], d.columns.length])).toEqual([
      ["customers", 0],
      ["orders", 4],
    ]);
  });

  test("a DDL statement invalidates the cached catalog, a dry run does not", async () => {
    const provider = await open();
    expect((await provider.listObjects(["shop"], "table")).map((o) => o.name)).toEqual(["customers", "orders"]);
    await provider.query(`${BIGQUERY_DRY_RUN_MARKER}CREATE TABLE shop.refunds (refund_id INT64)`);
    await provider.listObjects(["shop"], "table");
    expect(queriesWith(/INFORMATION_SCHEMA\.TABLES/)).toHaveLength(1);

    await provider.query("CREATE TABLE shop.refunds (refund_id INT64)");
    expect((await provider.listObjects(["shop"], "table")).map((o) => o.name)).toEqual([
      "customers",
      "orders",
      "refunds",
    ]);
    expect(queriesWith(/INFORMATION_SCHEMA\.TABLES/)).toHaveLength(2);
  });

  test("refuses an undeclared kind and a malformed container path", async () => {
    const provider = await open();
    await expect(provider.listObjects(["shop"], "routine")).rejects.toBeInstanceOf(QueryError);
    await expect(provider.listObjects(["demo-project", "shop"], "table")).rejects.toThrow();
  });
});

// ============================================================================
// Monitoring
// ============================================================================

describe("monitoring", () => {
  test("every panel reads its region's INFORMATION_SCHEMA view and maps the rows", async () => {
    const provider = await open();
    const overview = await provider.getOverview();
    expect(overview).toMatchObject({
      version: "BigQuery (EU)",
      tableCount: 5,
      databaseSize: "3 MB",
      databaseSizeBytes: 3145728,
      indexCount: 0,
    });
    expect(await provider.getPerformanceMetrics()).toEqual({ cacheHitRatio: 25, queriesPerSecond: 2 });
    expect(await provider.getSlowQueries({ limit: 5 })).toEqual([
      {
        query: "SELECT * FROM shop.orders",
        calls: 4,
        totalTime: 2000,
        avgTime: 500,
        minTime: 300,
        maxTime: 900,
        rows: 0,
      },
    ]);
    const sessions = await provider.getActiveSessions();
    expect(sessions).toHaveLength(1);
    expect(sessions[0]).toMatchObject({
      pid: "job-live",
      user: "analyst@example.com",
      database: "demo-project",
      state: "running",
      duration: "5s",
      durationMs: 5400,
    });
    expect(sessions[0].queryStart).toEqual(new Date("2026-10-11T08:00:01Z"));
    expect(await provider.getTableStats({ schema: "shop" })).toEqual([
      { schemaName: "shop", tableName: "orders", rowCount: 1200, totalSize: "2 MB", totalSizeBytes: 2097152 },
      { schemaName: "shop", tableName: "customers", rowCount: 300, totalSize: "1 MB", totalSizeBytes: 1048576 },
    ]);
    expect(await provider.getStorageStats()).toEqual([
      { name: "shop", location: "EU", size: "3 MB", sizeBytes: 3145728 },
      { name: "archive", location: "EU", size: "1 KB", sizeBytes: 1024 },
    ]);
    expect(await provider.getIndexStats()).toEqual([]);
    expect((await provider.getHealth()).slowQueries).toEqual([]);
    await expect(provider.runMaintenance("vacuum" as never)).rejects.toBeInstanceOf(QueryError);

    const sent = posted("/queries").map((r) => r.body!);
    const storage = sent.find((b) => /COUNT\(\*\) AS tables/.test(String(b.query)))!;
    expect(storage).toMatchObject({ location: "EU" });
    expect(String(storage.query)).toContain("`demo-project`.`region-eu`.INFORMATION_SCHEMA.TABLE_STORAGE");
    expect(String(storage.query)).toContain("project_id = 'demo-project' AND NOT deleted");
    expect(sent.some((b) => String(b.query).includes("table_schema = 'shop'"))).toBe(true);
    expect(sent.some((b) => String(b.query).includes("LIMIT 5"))).toBe(true);
  });

  test("a repeated poll within a minute is served from the cache", async () => {
    const provider = await open();
    await provider.getOverview();
    await provider.getOverview();
    expect(queriesWith(/COUNT\(\*\) AS tables/)).toHaveLength(1);
  });

  test("a refused read leaves the panels empty rather than failing", async () => {
    google.informationSchema = false;
    const provider = await open();
    expect(await provider.getOverview()).toMatchObject({ databaseSize: "N/A", tableCount: 0 });
    expect(await provider.getPerformanceMetrics()).toEqual({});
    expect(await provider.getSlowQueries()).toEqual([]);
    expect(await provider.getActiveSessions()).toEqual([]);
    expect(await provider.getTableStats()).toEqual([]);
    expect(await provider.getStorageStats()).toEqual([]);
  });
});
