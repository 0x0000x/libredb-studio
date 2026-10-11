/**
 * Hand-run live check of the real BigQuery provider against goccy/bigquery-emulator; not in `bun run test`.
 * Coverage and the paging proxy are explained in docs/providers/bigquery.md section 9.1.
 *
 *   docker run --rm -p 127.0.0.1:9050:9050 ghcr.io/goccy/bigquery-emulator:0.8.1 --project=demo-project --dataset=libredb_live
 *   BIGQUERY_EMULATOR_HOST=127.0.0.1:9050 bun tests/live/bigquery-live-check.ts
 *
 * Exits 1 on any failed check, 2 without BIGQUERY_EMULATOR_HOST.
 */
import { createServer, request as httpRequest, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { createDatabaseProvider } from "@/lib/db/factory";
import type { DatabaseProvider } from "@/lib/db/types";
import type { DatabaseConnection } from "@/lib/types";

const PROJECT = "demo-project";
const DATASET = "libredb_live";
/** Over the transport's 10,000-row page. */
const PAGED_ROWS = 12_000;

const EMULATOR = process.env.BIGQUERY_EMULATOR_HOST?.trim();
if (!EMULATOR) {
  console.error("bigquery-live-check: set BIGQUERY_EMULATOR_HOST (e.g. 127.0.0.1:9050) to the running emulator");
  process.exit(2);
}

function connection(): DatabaseConnection {
  return {
    id: "bigquery-live-check",
    name: "BigQuery emulator",
    type: "bigquery",
    database: PROJECT,
    schema: DATASET,
    // Any token passes the emulator, and an access token skips the token endpoint.
    password: "ya29.emulator-accepts-any-token",
    createdAt: new Date(),
  };
}

// ============================================================================
// Checks
// ============================================================================

const tally = { passed: 0, total: 0 };

async function check(name: string, body: () => Promise<string | undefined | void>): Promise<void> {
  tally.total += 1;
  const started = performance.now();
  try {
    const note = await body();
    tally.passed += 1;
    console.log(`PASS ${name} (${Math.round(performance.now() - started)} ms)${note ? `: ${note}` : ""}`);
  } catch (error) {
    console.log(`FAIL ${name}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

function same(actual: unknown, expected: unknown, what: string): void {
  if (!Bun.deepEquals(actual, expected, true))
    throw new Error(`${what}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

async function connected(): Promise<DatabaseProvider> {
  const provider = await createDatabaseProvider(connection());
  await provider.connect();
  return provider;
}

const sequence = (count: number) => `SELECT n FROM UNNEST(GENERATE_ARRAY(1, ${count})) AS n ORDER BY n`;

function inOrder(rows: readonly Record<string, unknown>[], count: number): void {
  same(rows.length, count, "row count");
  const misplaced = rows.findIndex((row, index) => String(row.n) !== String(index + 1));
  if (misplaced >= 0) throw new Error(`row ${misplaced + 1} is ${JSON.stringify(rows[misplaced])}`);
}

// ============================================================================
// The paging proxy
// ============================================================================

/** Cuts `jobs.query` at `maxResults` with a `pageToken`, as BigQuery does (the emulator does not); the rest passes. */
async function pagingProxy(): Promise<{ host: string; pageRequests: () => number; close: () => Promise<void> }> {
  const upstream = new URL(EMULATOR!.includes("://") ? EMULATOR! : `http://${EMULATOR}`);
  let pages = 0;
  const server = createServer((incoming: IncomingMessage, outgoing: ServerResponse) => {
    const chunks: Buffer[] = [];
    incoming.on("data", (chunk: Buffer) => chunks.push(chunk));
    incoming.on("end", () => {
      const body = Buffer.concat(chunks);
      const isQuery = incoming.method === "POST" && /\/queries$/.test(incoming.url ?? "");
      if (incoming.method === "GET" && /\/queries\/[^/?]+\?/.test(incoming.url ?? "")) pages += 1;
      const forward = httpRequest(
        {
          host: upstream.hostname,
          port: upstream.port || 80,
          method: incoming.method,
          path: incoming.url,
          headers: { ...incoming.headers, host: upstream.host, "content-length": String(body.length) },
        },
        (answer) => {
          const parts: Buffer[] = [];
          answer.on("data", (chunk: Buffer) => parts.push(chunk));
          answer.on("end", () => {
            let text = Buffer.concat(parts).toString("utf8");
            const limit = isQuery
              ? Number((JSON.parse(body.toString("utf8")) as { maxResults?: number }).maxResults)
              : 0;
            if (isQuery && answer.statusCode === 200 && limit > 0) {
              const page = JSON.parse(text) as { rows?: unknown[]; pageToken?: string };
              if (Array.isArray(page.rows) && page.rows.length > limit) {
                page.rows = page.rows.slice(0, limit);
                page.pageToken = String(limit);
                text = JSON.stringify(page);
              }
            }
            outgoing.writeHead(answer.statusCode ?? 502, { "content-type": "application/json" });
            outgoing.end(text);
          });
        },
      );
      forward.on("error", (error) => {
        outgoing.writeHead(502, { "content-type": "application/json" });
        outgoing.end(JSON.stringify({ error: { code: 502, message: error.message } }));
      });
      forward.end(body);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return {
    host: `127.0.0.1:${port}`,
    pageRequests: () => pages,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

// ============================================================================
// Main
// ============================================================================

async function scenarios(provider: DatabaseProvider): Promise<void> {
  await check("seed: a table with rows and a view over it, through the provider", async () => {
    await provider.query(
      `CREATE OR REPLACE TABLE ${DATASET}.orders (order_id INT64, customer STRING, total NUMERIC, placed TIMESTAMP)`,
    );
    await provider.query(
      `INSERT INTO ${DATASET}.orders (order_id, customer, total, placed) VALUES ` +
        `(1, 'ada', 10.5, TIMESTAMP '2026-01-01 10:00:00+00'), (2, 'grace', 20, TIMESTAMP '2026-01-02 11:30:00+00')`,
    );
    await provider.query(
      `CREATE OR REPLACE VIEW ${DATASET}.big_orders AS SELECT order_id, total FROM ${DATASET}.orders WHERE total > 15`,
    );
  });

  await check("datasets: the seeded dataset is listed", async () => {
    const containers = await provider.listContainers!();
    const names = containers.map((container) => container.path.at(-1));
    if (!names.includes(DATASET)) throw new Error(`datasets listed: ${JSON.stringify(names)}`);
    return `${names.length} dataset(s)`;
  });

  await check("tables and views: listed under their kinds", async () => {
    const tables = (await provider.listObjects!([DATASET], "table")).map((object) => object.path.at(-1));
    const views = (await provider.listObjects!([DATASET], "view")).map((object) => object.path.at(-1));
    if (!tables.includes("orders")) throw new Error(`tables: ${JSON.stringify(tables)}`);
    if (!views.includes("big_orders")) throw new Error(`views: ${JSON.stringify(views)}`);
    return `tables ${JSON.stringify(tables)}, views ${JSON.stringify(views)}`;
  });

  await check("columns: names and types of the table and the view", async () => {
    const table = await provider.describeObject!([DATASET, "orders"], "table");
    same(
      table.columns?.map((column) => [column.name, column.type]),
      [
        ["order_id", "INT64"],
        ["customer", "STRING"],
        ["total", "NUMERIC"],
        ["placed", "TIMESTAMP"],
      ],
      "orders columns",
    );
    const view = await provider.describeObject!([DATASET, "big_orders"], "view");
    same(
      view.columns?.map((column) => column.name),
      ["order_id", "total"],
      "big_orders columns",
    );
  });

  await check("query: typed cells come back decoded", async () => {
    const result = await provider.query(`SELECT order_id, customer, total FROM ${DATASET}.orders ORDER BY order_id`);
    same(result.fields, ["order_id", "customer", "total"], "fields");
    same(
      result.rows.map((row) => [String(row.order_id), row.customer, String(row.total)]),
      [
        ["1", "ada", "10.5"],
        ["2", "grace", "20"],
      ],
      "rows",
    );
  });

  await check(`query: ${PAGED_ROWS} rows arrive whole and in order`, async () => {
    inOrder((await provider.query(sequence(PAGED_ROWS))).rows, PAGED_ROWS);
  });
}

async function paging(): Promise<void> {
  await check(`query across two pages: jobs.query, then getQueryResults with the page token`, async () => {
    const proxy = await pagingProxy();
    const emulator = process.env.BIGQUERY_EMULATOR_HOST;
    process.env.BIGQUERY_EMULATOR_HOST = proxy.host;
    let provider: DatabaseProvider | undefined;
    try {
      provider = await connected();
      const before = proxy.pageRequests();
      inOrder((await provider.query(sequence(PAGED_ROWS))).rows, PAGED_ROWS);
      const pages = proxy.pageRequests() - before;
      if (pages < 1) throw new Error("no getQueryResults page was requested");
      return `1 jobs.query page and ${pages} getQueryResults page(s)`;
    } finally {
      await provider?.disconnect();
      process.env.BIGQUERY_EMULATOR_HOST = emulator;
      await proxy.close();
    }
  });
}

async function run(): Promise<void> {
  let provider: DatabaseProvider | undefined;
  await check(`connect: ${PROJECT} on ${EMULATOR}`, async () => {
    provider = await connected();
  });
  if (!provider) return;
  try {
    await scenarios(provider);
    await paging();
  } finally {
    await provider.query(`DROP VIEW IF EXISTS ${DATASET}.big_orders`).catch(() => undefined);
    await provider.query(`DROP TABLE IF EXISTS ${DATASET}.orders`).catch(() => undefined);
    await provider.disconnect();
  }
}

try {
  await run();
} catch (error) {
  console.error(`bigquery-live-check: stopped: ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
}
console.log(`${tally.passed} of ${tally.total} checks passed on ${EMULATOR}`);
process.exit(tally.passed === tally.total ? 0 : 1);
