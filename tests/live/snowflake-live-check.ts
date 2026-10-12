/**
 * Hand-run live check of the Snowflake provider against the trial account, the list the gate 4 session and every
 * later change of the provider run: connect by key pair and by PAT, the tree under each role, the every-type round
 * trip and its export replay, a multi-partition read with a cut, the cancel of a long statement, a wrong key, a wrong
 * PAT, the no-warehouse refusal, a read-only role refusing DML, background calls leaving a suspended warehouse
 * suspended, the monitoring dashboard's panels and the readings inspect_operations takes, and a role edit producing a
 * new provider.
 *
 * WHY THIS EXISTS, AND WHY IT CANNOT BE A UNIT TEST. The integration test replays fixtures through the provider's
 * `deps`; only the account can say that the statements, the partitions, a cancel or a refusal do what the fixtures
 * claim on the release that answers them.
 *
 * It is NOT in `bun run test`: the runner excludes `tests/live/` by name. Run it with the operator's env file loaded
 * (the variables of tests/helpers/snowflake-live.ts), the account set up as tests/fixtures/snowflake/README.md says:
 *
 *   (set -a; . <the operator's env file>; set +a; bun tests/live/snowflake-live-check.ts)
 *
 * Each provider is built by the factory (`createDatabaseProvider`), as the server builds it, except where a check's
 * pass condition is what went over the wire: there the same `SnowflakeProvider` is built with the production node
 * transports wrapped in a recorder, so the check can see the cancel by handle, count the partition GETs and prove a
 * background read sent no statement that needs a warehouse.
 *
 * It writes only `STUDIO_DEMO.PROBE.EVERY_TYPE_REPLAY`, as `studio_svc`, and drops it before it ends. It prints one
 * line per check, `PASS <name> (<ms> ms)`, `FAIL <name>: <error>` or `SKIP <name>: <why>`, every line masked, then
 * `<passed> of <total> checks passed`, and exits 1 on any failure; a missing or malformed variable exits 2 before any
 * socket opens.
 */
import { generateKeyPairSync } from "node:crypto";
import { AuthenticationError, QueryCancelledError, QueryError } from "@/lib/db/errors";
import { createDatabaseProvider, getOrCreateProvider, removeProvider } from "@/lib/db/factory";
import {
  createNodeByteTransport,
  createNodeTransport,
  type NodeByteTransportOptions,
  type NodeTransportOptions,
} from "@/lib/db/http/node-transport";
import { SNOWFLAKE_ERROR_SENTENCES } from "@/lib/db/providers/sql/snowflake/errors";
import { SnowflakeProvider } from "@/lib/db/providers/sql/snowflake/index";
import { SNOWFLAKE_MONITORING_SENTENCES } from "@/lib/db/providers/sql/snowflake/monitoring";
import type { DatabaseProvider } from "@/lib/db/types";
import { buildResultExport } from "@/lib/export/result-export";
import type { DatabaseConnection } from "@/lib/types";
import { type LiveArguments, liveSecrets, mask, readLiveArguments } from "../helpers/snowflake-live";

const read = readLiveArguments(process.env);
if ("exitCode" in read) {
  console.error(`snowflake-live-check: ${read.message}`);
  process.exit(read.exitCode);
}
const args: LiveArguments = read;
const secrets = liveSecrets(args);
const say = (line: string): void => console.log(mask(line, secrets));

const WAREHOUSE = args.warehouse;
const REPLAY_TABLE = "STUDIO_DEMO.PROBE.EVERY_TYPE_REPLAY";
const QUERY_TIMEOUT_MS = 120_000;

// ============================================================================
// Connections and the recorder
// ============================================================================

type Principal = "svc" | "reader" | "pat";

function connection(principal: Principal, overrides: Record<string, unknown> = {}): DatabaseConnection {
  const base = {
    id: `snowflake-live-${principal}`,
    name: `Snowflake live ${principal}`,
    type: "snowflake",
    host: args.account,
    warehouse: WAREHOUSE,
    database: "STUDIO_DEMO",
    schema: "PROBE",
    createdAt: new Date(),
  };
  const credential =
    principal === "pat"
      ? { user: args.pat.user, role: "STUDIO_RO", authMethod: "pat", password: args.pat.token }
      : {
          user: args[principal].user,
          role: principal === "svc" ? "STUDIO_RW" : "STUDIO_RO",
          authMethod: "keypair",
          privateKey: args[principal].keyPem,
          ...(args[principal].passphrase === undefined ? {} : { privateKeyPassphrase: args[principal].passphrase }),
        };
  return { ...base, ...credential, ...overrides } as DatabaseConnection;
}

interface Wire {
  readonly method: string;
  readonly path: string;
  readonly status: number | null;
  readonly statement: string | null;
}

/** A provider on the production transports, every request it sends recorded with its answer's status. */
function recorded(conn: DatabaseConnection): { provider: SnowflakeProvider; wire: Wire[] } {
  const wire: Wire[] = [];
  const provider = new SnowflakeProvider(
    conn,
    { queryTimeout: QUERY_TIMEOUT_MS },
    {
      createNodeTransport: (options: NodeTransportOptions) => {
        const inner = createNodeTransport(options);
        return {
          async request(request) {
            const entry = {
              method: request.method,
              path: new URL(request.url).pathname,
              status: null as number | null,
              statement: request.body === undefined ? null : String(JSON.parse(request.body).statement),
            };
            wire.push(entry);
            const response = await inner.request(request);
            entry.status = response.status;
            return response;
          },
          close: () => inner.close(),
        };
      },
      createNodeByteTransport: (options: NodeByteTransportOptions) => {
        const inner = createNodeByteTransport(options);
        return {
          async request(request) {
            const entry = {
              method: request.method,
              path: `${request.target.path}?${request.target.query}`,
              status: null as number | null,
              statement: null,
            };
            wire.push(entry);
            const response = await inner.request(request);
            entry.status = response.status;
            return response;
          },
          close: () => inner.close(),
        };
      },
    },
  );
  return { provider, wire };
}

async function built(conn: DatabaseConnection): Promise<DatabaseProvider> {
  const provider = await createDatabaseProvider(conn, { queryTimeout: QUERY_TIMEOUT_MS });
  await provider.connect();
  return provider;
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

async function refusal(promise: Promise<unknown>): Promise<Error> {
  try {
    await promise;
  } catch (error) {
    return error as Error;
  }
  throw new Error("expected a refusal, and the call succeeded");
}

/** The warehouse's state, from the service user's `SHOW WAREHOUSES LIKE`. */
async function warehouseState(): Promise<string> {
  const provider = await built(connection("svc"));
  try {
    const result = await provider.query(`SHOW WAREHOUSES LIKE '${WAREHOUSE}'`);
    return String(result.rows.find((row) => row.name === WAREHOUSE)?.state ?? "ABSENT");
  } finally {
    await provider.disconnect();
  }
}

async function awaitSuspended(): Promise<void> {
  for (let waited = 0; waited < 300_000; waited += 15_000) {
    // oxlint-disable-next-line no-await-in-loop -- the state is read again after each wait.
    if ((await warehouseState()) === "SUSPENDED") return;
    // oxlint-disable-next-line no-await-in-loop -- see above.
    await new Promise((resolve) => setTimeout(resolve, 15_000));
  }
  throw new Error(`${WAREHOUSE} did not suspend within 5 minutes`);
}

// ============================================================================
// The checks
// ============================================================================

const CHECKS: readonly (readonly [string, () => Promise<string | void>])[] = [
  [
    "connect by key pair",
    async () => {
      const provider = await built(connection("svc"));
      await provider.disconnect();
    },
  ],
  [
    "connect by PAT",
    async () => {
      const provider = await built(connection("pat"));
      await provider.disconnect();
    },
  ],
  [
    "the tree under each role",
    async () => {
      for (const principal of ["svc", "reader"] as const) {
        // oxlint-disable-next-line no-await-in-loop -- one role at a time.
        const provider = await built(connection(principal));
        try {
          // oxlint-disable-next-line no-await-in-loop -- see above.
          const databases = await provider.listContainers?.([]);
          assert(
            databases?.some((database) => database.name === "STUDIO_DEMO"),
            `${principal} sees no STUDIO_DEMO`,
          );
          // oxlint-disable-next-line no-await-in-loop -- see above.
          const tables = await provider.listObjects?.(["STUDIO_DEMO", "PROBE"], "table");
          assert(
            tables?.some((table) => table.name === "EVERY_TYPE"),
            `${principal} sees no EVERY_TYPE`,
          );
        } finally {
          // oxlint-disable-next-line no-await-in-loop -- see above.
          await provider.disconnect();
        }
      }
    },
  ],
  [
    "every-type round trip and export replay",
    async () => {
      const provider = await built(connection("svc"));
      try {
        const select = "SELECT * FROM STUDIO_DEMO.PROBE.EVERY_TYPE ORDER BY 1 NULLS LAST";
        const source = await provider.query(select);
        assert(source.rows.length > 0, "every_type answered no rows");
        await provider.query(`CREATE OR REPLACE TABLE ${REPLAY_TABLE} LIKE STUDIO_DEMO.PROBE.EVERY_TYPE`);
        const file = buildResultExport("sql-insert", {
          rows: source.rows,
          fields: source.fields,
          tabName: REPLAY_TABLE,
          dialect: "snowflake",
          columnTypes: source.columnTypes,
        });
        for (const line of file.content.split("\n")) {
          if (line.trim() === "" || line.startsWith("-- ")) continue;
          // A regular expression, not a string: only an INSERT into the replay table is sent.
          const target = /^INSERT INTO (\S+) /.exec(line)?.[1];
          assert(target === REPLAY_TABLE, `the export wrote a line whose target is ${target ?? "missing"}`);
          // oxlint-disable-next-line no-await-in-loop -- one statement at a time, as the file runs.
          await provider.query(line);
        }
        const replayed = await provider.query(select.replace("STUDIO_DEMO.PROBE.EVERY_TYPE", REPLAY_TABLE));
        const text = (rows: readonly Record<string, unknown>[]) =>
          JSON.stringify(rows, (_key, value) => (Buffer.isBuffer(value) ? value.toString("hex") : value));
        assert(text(replayed.rows) === text(source.rows), "the replayed rows differ from every_type's");
      } finally {
        await provider.query(`DROP TABLE IF EXISTS ${REPLAY_TABLE}`).catch(() => undefined);
        await provider.disconnect();
      }
    },
  ],
  [
    "a multi-partition read with a cut",
    async () => {
      const { provider, wire } = recorded(connection("svc"));
      await provider.connect();
      try {
        const result = await provider.query("SELECT * FROM STUDIO_DEMO.PROBE.BIG ORDER BY 1");
        assert(result.pagination?.wasLimited === true, "the result was not reported as cut");
        assert((result.warnings ?? []).length > 0, "the cut gave no warning");
        const partitions = wire.filter((entry) => entry.path.includes("partition=")).length;
        assert(partitions > 0, "no partition GET was sent");
        return `${result.rows.length} rows, ${partitions} partition GETs`;
      } finally {
        await provider.disconnect();
      }
    },
  ],
  [
    "the cancel of a long statement",
    async () => {
      const { provider, wire } = recorded(connection("svc"));
      await provider.connect();
      try {
        const running = refusal(provider.query("SELECT SYSTEM$WAIT(120)", undefined, "live-cancel"));
        await new Promise((resolve) => setTimeout(resolve, 5_000));
        assert(await provider.cancelQuery("live-cancel"), "cancelQuery answered false");
        assert((await running) instanceof QueryCancelledError, "the statement did not end cancelled");
        assert(wire.filter((entry) => entry.path.endsWith("/cancel")).length === 1, "not exactly one cancel was sent");
      } finally {
        await provider.disconnect();
      }
    },
  ],
  [
    "a wrong key",
    async () => {
      const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
      const pem = privateKey.export({ type: "pkcs8", format: "pem" }).toString();
      const provider = await createDatabaseProvider(
        connection("reader", { privateKey: pem, privateKeyPassphrase: undefined }),
      );
      const error = await refusal(provider.connect());
      assert(error instanceof AuthenticationError, `refused as ${error.name}, not AuthenticationError`);
      assert(error.message.startsWith("Snowflake refused the key pair"), "not the key-pair sentence");
      assert((error as AuthenticationError & { fixStatement?: string }).fixStatement !== undefined, "no fix statement");
    },
  ],
  [
    "a wrong PAT",
    async () => {
      const provider = await createDatabaseProvider(
        connection("pat", { password: `${args.pat.token.slice(0, -4)}xxxx` }),
      );
      const error = await refusal(provider.connect());
      assert(error instanceof AuthenticationError, `refused as ${error.name}, not AuthenticationError`);
      assert(error.message === SNOWFLAKE_ERROR_SENTENCES.patRefused, "not the token sentence");
    },
  ],
  [
    "the no-warehouse refusal",
    async () => {
      const provider = await built(connection("reader", { role: "STUDIO_NOWH", warehouse: "" }));
      try {
        const error = await refusal(provider.query("SELECT 1"));
        assert(error instanceof QueryError, `refused as ${error.name}, not QueryError`);
        assert(error.message === SNOWFLAKE_ERROR_SENTENCES.noWarehouse, "not the no-warehouse sentence");
      } finally {
        await provider.disconnect();
      }
    },
  ],
  [
    "a read-only role refusing DML",
    async () => {
      const provider = await built(connection("reader"));
      try {
        const error = await refusal(provider.query("INSERT INTO STUDIO_DEMO.PROBE.WIDE (C01) VALUES (1)"));
        assert(error instanceof QueryError, `refused as ${error.name}, not QueryError`);
        assert(error.message.startsWith("Role STUDIO_RO lacks a privilege"), "not the privileges sentence");
      } finally {
        await provider.disconnect();
      }
    },
  ],
  [
    "background calls leave a suspended warehouse suspended",
    async () => {
      await awaitSuspended();
      const { provider, wire } = recorded(connection("reader"));
      await provider.connect();
      try {
        await provider.getHealth();
        await provider.listContainers([]);
        await provider.listObjects(["STUDIO_DEMO", "PROBE"], "table");
        await provider.describeObject(["STUDIO_DEMO", "PROBE", "EVERY_TYPE"], "table");
        const panel = await refusal(provider.getPerformanceMetrics());
        assert(
          panel.message === SNOWFLAKE_MONITORING_SENTENCES.suspended(WAREHOUSE),
          "the panel did not say suspended",
        );
        const sent = wire.flatMap((entry) => (entry.statement === null ? [] : [entry.statement]));
        assert(
          sent.every((sql) => sql.startsWith("SHOW ")),
          "a background call sent a statement that is not a SHOW",
        );
      } finally {
        await provider.disconnect();
      }
      assert((await warehouseState()) === "SUSPENDED", `${WAREHOUSE} is no longer suspended`);
    },
  ],
  [
    "the monitoring dashboard and the readings inspect_operations takes",
    async () => {
      const provider = await built(connection("svc"));
      try {
        // The dashboard's panels; inspect_operations reads sessions, slow queries, table and index stats, storage
        // and health through the same calls. A degraded panel answers its own sentence, which counts as an answer.
        const readings: [string, () => Promise<unknown>][] = [
          ["overview", () => provider.getOverview()],
          ["performance", () => provider.getPerformanceMetrics()],
          ["slow queries", () => provider.getSlowQueries({ limit: 10 })],
          ["sessions", () => provider.getActiveSessions({ limit: 10 })],
          ["table stats", () => provider.getTableStats({ schema: "PROBE" })],
          ["index stats", () => provider.getIndexStats({ schema: "PROBE" })],
          ["storage", () => provider.getStorageStats()],
          ["health", () => provider.getHealth()],
        ];
        const outcomes: string[] = [];
        for (const [name, readOne] of readings) {
          try {
            // oxlint-disable-next-line no-await-in-loop -- one panel at a time, as the dashboard polls them.
            await readOne();
            outcomes.push(`${name} answered`);
          } catch (error) {
            assert(error instanceof QueryError, `${name} failed as ${(error as Error).name}`);
            outcomes.push(`${name}: ${error.message}`);
          }
        }
        return outcomes.join("; ");
      } finally {
        await provider.disconnect();
      }
    },
  ],
  [
    "a role edit produces a new provider",
    async () => {
      const first = await getOrCreateProvider(connection("svc", { id: "snowflake-live-role-edit" }));
      const second = await getOrCreateProvider(
        connection("svc", { id: "snowflake-live-role-edit", role: "STUDIO_RO" }),
      );
      try {
        assert(first !== second, "the edited role was served the provider of the old one");
      } finally {
        await removeProvider("snowflake-live-role-edit");
      }
    },
  ],
];

async function main(): Promise<number> {
  let passed = 0;
  for (const [name, check] of CHECKS) {
    const started = performance.now();
    try {
      // oxlint-disable-next-line no-await-in-loop -- the checks share one account and run one at a time.
      const note = await check();
      passed += 1;
      say(`PASS ${name} (${Math.round(performance.now() - started)} ms)${note ? `: ${note}` : ""}`);
    } catch (error) {
      say(`FAIL ${name}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  say(`${passed} of ${CHECKS.length} checks passed`);
  return passed === CHECKS.length ? 0 : 1;
}

main().then(
  (code) => process.exit(code),
  (error: unknown) => {
    say(`snowflake-live-check failed: ${error instanceof Error ? error.message : String(error)}`);
    process.exit(1);
  },
);
