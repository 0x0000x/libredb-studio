/**
 * Opt-in live check for the MySQL and MariaDB agent read-only execution profile (#1612).
 *
 * WHY THIS EXISTS, AND WHY IT CANNOT BE A UNIT TEST. The provider suite drives
 * `queryReadOnly` against a session model, and a model answers what its author wrote. The
 * first composed column inventory left its schema filter unqualified over a two-view join
 * and both engines refused it with 1052; a killed statement was reported as "Query was
 * cancelled"; a `flags=MULTI_STATEMENTS` connection string ran two statements in one call.
 * All three passed the suite. Only the servers can say what the profile holds, so this
 * script asks them:
 *
 *  - a write-capable account (`root`) opens under the profile, and its first read is refused
 *    with PROFILE_PRIVILEGES_TOO_BROAD;
 *  - every catalog composition runs as the least-privilege account and answers lower-case keys;
 *  - the estimating plan reads through a view (it needs the seeds' SHOW VIEW);
 *  - each escape of #1612 is refused, and the database is read back unchanged by `root`;
 *  - a `flags=MULTI_STATEMENTS` connection string still gets one statement per call;
 *  - the deadline holds against the statement's own override, and says it was the deadline;
 *  - a statement with its own huge LIMIT is refused at the row budget without the process
 *    growing past a fixed ceiling;
 *  - nothing a call leaves behind (a named lock) reaches the next call, and the session's
 *    character set is the same on every call.
 *
 * It is NOT in `bun run test`: the runner excludes `tests/live/` by name (`EXCLUDED` in
 * `tests/runner/discover.ts`). With `database-compose.yml`'s `mysql` and `mariadb` services up
 * (the `02-agent-principal.sql` seeds create `libredb_agent`):
 *
 *   LIBREDB_LIVE_MYSQL_ROOT_URLS=mysql://root:root@127.0.0.1:3306/app,mysql://root:root@127.0.0.1:3307/app \
 *     bun tests/live/mysql-agent-profile.ts
 *
 * Each URL is a `root` URL; the agent URL is the same server as `libredb_agent`. Point it at
 * DISPOSABLE servers: it creates and drops `app.live_profile_probe`. It exits non-zero when any
 * check fails and prints each check on its own line.
 */
import mysql from "mysql2/promise";
import { MySQLProvider } from "../../src/lib/db/providers/sql/mysql";
import { ExecutionProfileError } from "../../src/lib/db/errors";
import { composeCatalogRead } from "../../src/lib/agent/composed-sql";
import type { DatabaseConnection } from "../../src/lib/types";

const BUDGET = { statementTimeoutMs: 1500, maxResultRows: 200, maxResultBytes: 4 * 1024 * 1024 };
const RSS_CEILING_BYTES = 64 * 1024 * 1024;

function rootUrls(): URL[] {
  const raw = process.env.LIBREDB_LIVE_MYSQL_ROOT_URLS;
  if (!raw) {
    throw new Error(
      "Set LIBREDB_LIVE_MYSQL_ROOT_URLS to a comma-separated list of disposable MySQL-wire root URLs, " +
        "for example mysql://root:root@127.0.0.1:3306/app,mysql://root:root@127.0.0.1:3307/app",
    );
  }
  return raw.split(",").map((url) => new URL(url.trim()));
}

function connectionFor(url: URL, user: string, password: string, extra = ""): DatabaseConnection {
  const target = new URL(url);
  target.username = user;
  target.password = password;
  return {
    id: `live-${user}-${target.port}`,
    name: `live ${user}`,
    type: "mysql",
    connectionString: `${target.toString()}${extra}`,
    createdAt: new Date(),
  };
}

async function openProfiled(connection: DatabaseConnection) {
  const provider = new MySQLProvider(connection, {}, { readOnly: true });
  await provider.connect();
  return provider;
}

async function refusal(run: () => Promise<unknown>): Promise<string | null> {
  try {
    await run();
    return null;
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
}

async function checkServer(rootUrl: URL): Promise<string[]> {
  const failures: string[] = [];
  const check = (ok: boolean, label: string, detail = "") => {
    console.log(`${ok ? "PASS" : "FAIL"} ${label}${detail ? ` - ${detail}` : ""}`);
    if (!ok) failures.push(`${rootUrl.port}: ${label} ${detail}`);
  };

  const root = await mysql.createConnection({ uri: rootUrl.toString() });
  const [[{ version }]] = (await root.query("SELECT VERSION() AS version")) as unknown as [[{ version: string }]];
  const mariadb = /mariadb/i.test(version);
  console.log(`\n=== ${version} (${rootUrl.host}) ===`);
  await root.query("DROP TABLE IF EXISTS app.live_profile_probe");
  await root.query("DROP TABLE IF EXISTS app.live_profile_created");
  await root.query("CREATE TABLE app.live_profile_probe (id INT PRIMARY KEY)");
  await root.query("INSERT INTO app.live_profile_probe VALUES (1), (2), (3)");

  try {
    // A write-capable account opens: plan-mode grounding and the operations workflow need it.
    const asRoot = await openProfiled(connectionFor(rootUrl, rootUrl.username, rootUrl.password));
    check(asRoot.isConnected(), "root opens under the profile");
    try {
      await asRoot.queryReadOnly("SELECT 1", BUDGET);
      check(false, "root's first read is refused");
    } catch (error) {
      check(
        error instanceof ExecutionProfileError && error.reasonCode === "PROFILE_PRIVILEGES_TOO_BROAD",
        "root's first read is refused with PROFILE_PRIVILEGES_TOO_BROAD",
        error instanceof Error ? error.message.slice(0, 80) : "",
      );
    }
    await asRoot.disconnect();

    const agent = await openProfiled(connectionFor(rootUrl, "libredb_agent", "libredb_agent"));
    try {
      for (const kind of ["columns", "relations", "indexes", "statistics"] as const) {
        const message = await refusal(async () => {
          const result = await agent.queryReadOnly(composeCatalogRead("mysql", { kind }), BUDGET);
          const keys = result.rows.length > 0 ? Object.keys(result.rows[0] as object) : ["table_schema"];
          if (!keys.includes("table_schema")) throw new Error(`keys were ${keys.join(", ")}`);
        });
        check(message === null, `catalog ${kind} runs as libredb_agent with lower-case keys`, message ?? "");
      }

      {
        // EXPLAIN of a view needs SHOW VIEW (1345 without it), which the seeds grant.
        const message = await refusal(() =>
          agent.queryReadOnly("SELECT * FROM app.order_summary", BUDGET, "estimate-plan"),
        );
        check(message === null, "the estimating plan reads through a view", message?.slice(0, 80) ?? "");
      }

      const escapes = [
        "CREATE TABLE app.live_profile_created (id INT)",
        "DROP TABLE app.live_profile_probe",
        "TRUNCATE TABLE app.live_profile_probe",
        "INSERT INTO app.live_profile_probe VALUES (4)",
        "GRANT SELECT ON app.* TO 'libredb_agent'@'%'",
        "SET GLOBAL max_connections = 999",
        "SELECT id FROM app.live_profile_probe FOR UPDATE",
        "SELECT id FROM app.live_profile_probe INTO OUTFILE '/tmp/live_profile_probe.txt'",
        "SELECT 1; DELETE FROM app.live_profile_probe",
      ];
      for (const statement of escapes) {
        const message = await refusal(() => agent.queryReadOnly(statement, BUDGET));
        check(message !== null, `refused: ${statement}`, message?.slice(0, 70) ?? "it ran");
      }
      const [[state]] = (await root.query(
        "SELECT (SELECT COUNT(*) FROM app.live_profile_probe) AS kept, " +
          "(SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'app' AND table_name = 'live_profile_created') AS created",
      )) as unknown as [[{ kept: number; created: number }]];
      check(
        Number(state.kept) === 3 && Number(state.created) === 0,
        "the database reads back unchanged",
        JSON.stringify(state),
      );

      const override = mariadb
        ? "SET STATEMENT max_statement_time = 60 FOR SELECT SLEEP(5)"
        : "SELECT /*+ MAX_EXECUTION_TIME(60000) */ SLEEP(5)";
      for (const statement of ["SELECT SLEEP(5)", override]) {
        const started = Date.now();
        const message = await refusal(() => agent.queryReadOnly(statement, BUDGET));
        const elapsed = Date.now() - started;
        check(
          message !== null && /statement deadline of 1500ms/.test(message) && elapsed < 4000,
          `the deadline holds: ${statement}`,
          `${elapsed} ms, ${message?.slice(0, 60) ?? "it ran"}`,
        );
      }
      {
        const started = Date.now();
        const message = await refusal(() =>
          agent.queryReadOnly(
            "SELECT COUNT(*) FROM information_schema.columns a, information_schema.columns b, information_schema.columns c",
            BUDGET,
          ),
        );
        check(
          message !== null && /statement deadline of 1500ms/.test(message),
          "a long statement the KILL interrupts says it was the deadline",
          `${Date.now() - started} ms, ${message?.slice(0, 60) ?? "it ran"}`,
        );
      }

      const before = process.memoryUsage().rss;
      const bounded = await refusal(() =>
        agent.queryReadOnly(
          // A cross join, not a recursive CTE: MySQL stops recursion at 1000 levels by default.
          "SELECT a.column_name, REPEAT('x', 200) AS pad FROM information_schema.columns a, " +
            "information_schema.columns b LIMIT 100000000",
          { ...BUDGET, statementTimeoutMs: 30000 },
        ),
      );
      const grown = process.memoryUsage().rss - before;
      check(
        bounded !== null && /row budget: 201 rows > 200 allowed/.test(bounded) && grown < RSS_CEILING_BYTES,
        "a statement's own LIMIT is refused at the row budget",
        `${(grown / 1024 / 1024).toFixed(1)} MB grown, ${bounded?.slice(0, 60) ?? "it ran"}`,
      );

      await agent.queryReadOnly("SELECT GET_LOCK('live_profile_lock', 0) AS got, @@character_set_client AS cs", BUDGET);
      const after = await agent.queryReadOnly(
        "SELECT IS_USED_LOCK('live_profile_lock') AS holder, @@character_set_client AS cs",
        BUDGET,
      );
      const row = after.rows[0] as { holder: unknown; cs: string };
      check(row.holder === null, "a named lock does not outlive its call", JSON.stringify(row));
      check(row.cs === "utf8mb4", "the session keeps the handshake character set", row.cs);
    } finally {
      await agent.disconnect();
    }

    const multi = await openProfiled(
      connectionFor(rootUrl, "libredb_agent", "libredb_agent", `${rootUrl.search ? "&" : "?"}flags=MULTI_STATEMENTS`),
    );
    try {
      const message = await refusal(() => multi.queryReadOnly("SELECT 1 AS a; SELECT 2 AS b", BUDGET));
      check(
        message !== null,
        "flags=MULTI_STATEMENTS in the string still sends one statement",
        message?.slice(0, 60) ?? "it ran",
      );
    } finally {
      await multi.disconnect();
    }
  } finally {
    await root.query("DROP TABLE IF EXISTS app.live_profile_probe");
    await root.query("DROP TABLE IF EXISTS app.live_profile_created");
    await root.end();
  }
  return failures;
}

const failures: string[] = [];
for (const url of rootUrls()) failures.push(...(await checkServer(url)));
if (failures.length > 0) {
  console.error(`\n${failures.length} check(s) failed:\n${failures.join("\n")}`);
  process.exit(1);
}
console.log("\nEvery check passed.");
