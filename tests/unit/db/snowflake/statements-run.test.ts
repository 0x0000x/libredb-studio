import { afterEach, describe, expect, test } from "bun:test";
import { gzipSync } from "node:zlib";
import { TransportError } from "@/lib/db/http/node-transport";
import { SNOWFLAKE_ERROR_SENTENCES, SnowflakeError } from "@/lib/db/providers/sql/snowflake/errors";
import {
  SNOWFLAKE_FIRST_ANSWER_MAX_BYTES,
  SNOWFLAKE_RESULT_BYTE_BUDGET,
  SNOWFLAKE_ROWS_SENTENCES,
} from "@/lib/db/providers/sql/snowflake/rows";
import {
  clampedTimeoutS,
  deadlineMsFor,
  foldIdentifier,
  SNOWFLAKE_CANCEL_TIMEOUT_MS,
  SNOWFLAKE_CHUNK_SIZE_MB,
  SNOWFLAKE_DEADLINE_GRACE_MS,
  SNOWFLAKE_LIMITER_OPTIONS,
  SNOWFLAKE_MAX_TIMEOUT_S,
  SNOWFLAKE_POLL_FIRST_MS,
  SNOWFLAKE_POLL_MAX_MS,
  SNOWFLAKE_QUERY_TAGS,
  SNOWFLAKE_ROLE_CHECK_SQL,
  SNOWFLAKE_ROWS_PER_RESULTSET,
  SNOWFLAKE_STATEMENT_SENTENCES,
  SNOWFLAKE_SURFACE_TIMEOUT_S,
  surfaceTimeoutS,
} from "@/lib/db/providers/sql/snowflake/statements";
import { SNOWFLAKE_CONTROL_ANSWER_MAX_BYTES } from "@/lib/db/providers/sql/snowflake/transport";
import {
  failureAnswer,
  handleAnswer,
  partitionBody,
  resultAnswer,
  TEST_HANDLE,
} from "../../../helpers/snowflake-answers";
import { deferredAnswer, type RecordedRequest } from "../../../helpers/snowflake-node-transport";
import {
  bodyOf,
  CANCEL,
  caught,
  cancels,
  closeOpened,
  HOST,
  ONE_COLUMN,
  one,
  STATUS,
  STATUS_PATH,
  SUBMIT,
  setup,
  submits,
} from "../../../helpers/snowflake-statements";

afterEach(closeOpened);

describe("constants", () => {
  test("the poll, timeout, deadline, cancel, chunk, row and limiter values", () => {
    expect(SNOWFLAKE_POLL_FIRST_MS).toBe(250);
    expect(SNOWFLAKE_POLL_MAX_MS).toBe(2_000);
    expect(SNOWFLAKE_MAX_TIMEOUT_S).toBe(604_800);
    expect(SNOWFLAKE_SURFACE_TIMEOUT_S).toBe(30);
    expect(SNOWFLAKE_DEADLINE_GRACE_MS).toBe(5_000);
    expect(SNOWFLAKE_CANCEL_TIMEOUT_MS).toBe(5_000);
    expect(SNOWFLAKE_CHUNK_SIZE_MB).toBe(16);
    expect(SNOWFLAKE_ROWS_PER_RESULTSET).toBe(100_001);
    expect(SNOWFLAKE_LIMITER_OPTIONS).toEqual({ perProvider: 4, perEngine: 8, queueDepth: 64 });
    expect(SNOWFLAKE_ROLE_CHECK_SQL).toBe("SELECT CURRENT_ROLE() AS r, CURRENT_SECONDARY_ROLES() AS s");
  });

  test("the query_tag set, in order", () => {
    expect(Object.values(SNOWFLAKE_QUERY_TAGS)).toEqual([
      "libredb-studio:editor",
      "libredb-studio:connect",
      "libredb-studio:role-check",
      "libredb-studio:tree",
      "libredb-studio:describe",
      "libredb-studio:inventory",
      "libredb-studio:source",
      "libredb-studio:health",
      "libredb-studio:monitoring",
    ]);
  });

  test("the timeout clamps and the deadline", () => {
    expect(clampedTimeoutS(60_000)).toBe(60);
    expect(clampedTimeoutS(1)).toBe(1);
    expect(clampedTimeoutS(0)).toBe(1);
    expect(clampedTimeoutS(1_500)).toBe(2);
    expect(clampedTimeoutS(2_147_483_647)).toBe(604_800);
    expect(surfaceTimeoutS(60_000)).toBe(30);
    expect(surfaceTimeoutS(10_000)).toBe(10);
    expect(deadlineMsFor(60)).toBe(65_000);
    expect(deadlineMsFor(604_800)).toBe(604_805_000);
  });

  test("the identifier fold", () => {
    expect(foldIdentifier("studio_ro")).toBe("STUDIO_RO");
    expect(foldIdentifier("a$1")).toBe("A$1");
    expect(foldIdentifier('"Mixed Role"')).toBe("Mixed Role");
    expect(foldIdentifier('"say ""hi"""')).toBe('say "hi"');
    expect(foldIdentifier("has space")).toBe("has space");
    expect(foldIdentifier("1abc")).toBe("1abc");
  });
});

describe("the request body", () => {
  test("every body carries the parameters, the tag and the context as Snowflake resolves each name, never nullable", async () => {
    const context = { database: "Sales", schema: "public", warehouse: '"Studio WH"', role: '"Mixed Role"' };
    const harness = setup({ context });
    harness.transports.answer(SUBMIT, one(), one());
    await harness.statements.runUser("SHOW TABLES", undefined);
    await harness.statements.runSurface("SHOW DATABASES", SNOWFLAKE_QUERY_TAGS.tree);
    const sent = submits(harness);
    expect(sent).toHaveLength(2);
    expect(bodyOf(sent[0])).toEqual({
      statement: "SHOW TABLES",
      timeout: 60,
      database: "SALES",
      schema: "PUBLIC",
      warehouse: "Studio WH",
      role: "Mixed Role",
      parameters: {
        multi_statement_count: "1",
        query_tag: "libredb-studio:editor",
        client_result_chunk_size: 16,
        rows_per_resultset: 100_001,
        timezone: "UTC",
      },
    });
    expect(sent[0].body).toContain('"role":"Mixed Role"');
    expect(bodyOf(sent[1])).toMatchObject({
      statement: "SHOW DATABASES",
      timeout: 30,
      parameters: { query_tag: "libredb-studio:tree" },
    });
    for (const request of harness.transports.requests) {
      expect(request.url.toLowerCase()).not.toContain("nullable");
      expect((request.body ?? "").toLowerCase()).not.toContain("nullable");
    }
    expect(new URL(sent[0].url).searchParams.get("async")).toBe("true");
    expect(sent[0].maxResponseBytes).toBe(SNOWFLAKE_CONTROL_ANSWER_MAX_BYTES);
  });

  test("a bare role is sent upper-cased, since the SQL API reads the body's names exactly", async () => {
    const harness = setup({
      context: { database: "sales", schema: "public", warehouse: "studio_wh", role: "studio_ro" },
    });
    harness.transports.answer(SUBMIT, one());
    await harness.statements.runSurface("SHOW SCHEMAS", SNOWFLAKE_QUERY_TAGS.tree);
    expect(bodyOf(submits(harness)[0])).toMatchObject({
      database: "SALES",
      schema: "PUBLIC",
      warehouse: "STUDIO_WH",
      role: "STUDIO_RO",
    });
  });

  test("an empty context field is left out, and so is an absent one", async () => {
    const harness = setup({ context: { database: "", warehouse: "WH", role: "R" } });
    harness.transports.answer(SUBMIT, one());
    await harness.statements.runSurface("SHOW SCHEMAS", SNOWFLAKE_QUERY_TAGS.tree);
    const body = bodyOf(submits(harness)[0]);
    expect("database" in body).toBe(false);
    expect("schema" in body).toBe(false);
    expect(body.warehouse).toBe("WH");
    expect(body.role).toBe("R");
  });

  for (const [queryTimeoutMs, userTimeout, surface, deadline] of [
    [60_000, 60, 30, 65_000],
    [2_147_483_647, 604_800, 30, 604_805_000],
    [1, 1, 1, 6_000],
    [10_000, 10, 10, 15_000],
  ] as const) {
    test(`a query timeout of ${queryTimeoutMs} ms sends ${userTimeout} s, ${surface} s on a surface read`, async () => {
      const harness = setup({ queryTimeoutMs });
      harness.transports.answer(SUBMIT, one(), one());
      await harness.statements.runUser("SHOW TABLES", undefined);
      expect(harness.deps.deadlines.requested[0]).toBe(deadline);
      await harness.statements.runSurface("SHOW TABLES", SNOWFLAKE_QUERY_TAGS.health);
      const [user, read] = submits(harness).map(bodyOf);
      expect(user.timeout).toBe(userTimeout);
      expect(read.timeout).toBe(surface);
      expect(harness.deps.deadlines.requested[1]).toBe(surface * 1000 + 5_000);
    });
  }
});

describe("the poll", () => {
  test("three 202 answers then 200 sleep 250, 500 and 1000 ms, each status read under the first-answer cap", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, handleAnswer());
    harness.transports.answer(STATUS, handleAnswer(), handleAnswer(), one("7"));
    const result = await harness.statements.runSurface("SHOW TABLES", SNOWFLAKE_QUERY_TAGS.tree);
    expect(harness.clock.sleeps).toEqual([250, 500, 1000]);
    expect(result.rows).toEqual([{ N: 7 }]);
    const polls = harness.transports.requests.filter((r) => r.method === "GET");
    expect(polls).toHaveLength(3);
    for (const poll of polls) {
      expect(new URL(poll.url).pathname).toBe(STATUS_PATH);
      expect(new URL(poll.url).search).toBe("");
      expect(poll.maxResponseBytes).toBe(SNOWFLAKE_FIRST_ANSWER_MAX_BYTES);
    }
  });

  test("the interval stops doubling at 2000 ms", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, handleAnswer());
    harness.transports.answer(STATUS, ...Array.from({ length: 6 }, () => handleAnswer()), one());
    await harness.statements.runSurface("SHOW TABLES", SNOWFLAKE_QUERY_TAGS.tree);
    expect(harness.clock.sleeps).toEqual([250, 500, 1000, 2000, 2000, 2000, 2000]);
  });

  test("a 429 status answer sets the next interval to the jittered maximum", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, handleAnswer());
    harness.transports.answer(STATUS, { status: 429, text: "" }, one());
    await harness.statements.runSurface("SHOW TABLES", SNOWFLAKE_QUERY_TAGS.tree);
    expect(harness.clock.sleeps).toEqual([250, 1500]);
  });

  test("a first answer of 200 is read at once, with no sleep and no poll", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, one("3"));
    const result = await harness.statements.runSurface("SHOW TABLES", SNOWFLAKE_QUERY_TAGS.tree);
    expect(harness.clock.sleeps).toEqual([]);
    expect(harness.transports.requests).toHaveLength(1);
    expect(result).toEqual({
      rows: [{ N: 3 }],
      fields: ["N"],
      columnTypes: { N: "NUMBER(38,0)" },
      warnings: [],
      cut: false,
      numRows: 1,
    });
  });

  test("a run crossing a renewal polls, reads partitions and cancels under the later token", async () => {
    const harness = setup();
    const submit = deferredAnswer();
    harness.transports.answer(SUBMIT, submit.answer);
    harness.transports.answer(STATUS, resultAnswer({ columns: ONE_COLUMN, rows: [["1"]], partitions: 2 }));
    harness.transports.answer(
      { method: "GET", path: /./ },
      ...Array.from({ length: 5 }, () => ({ status: 503, bytes: Buffer.from("") })),
    );
    harness.transports.answer(CANCEL, { status: 200, json: { code: "000604" } });
    const running = harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree);
    await harness.transports.received(1);
    harness.tokens.advance();
    submit.resolve(handleAnswer());
    await caught(running);
    const [first, ...later] = harness.transports.requests;
    expect(first.headers.authorization).toBe("Bearer token-one");
    expect(later.length).toBeGreaterThan(2);
    for (const request of later) expect(request.headers.authorization).toBe("Bearer token-two");
  });
});

describe("partitions and cuts", () => {
  test("a three-partition result reads partitions 1 and 2 through the byte transport", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, handleAnswer());
    harness.transports.answer(
      STATUS,
      resultAnswer({ columns: [{ name: "S", type: "text" }], rows: [["a"]], partitions: 3, numRows: 3 }),
    );
    harness.transports.answer(
      { method: "GET", path: /./ },
      partitionBody([["b"]], "gzip"),
      partitionBody([["c"]], "identity"),
    );
    const result = await harness.statements.runSurface("SELECT s FROM t", SNOWFLAKE_QUERY_TAGS.tree);
    expect(result.rows).toEqual([{ S: "a" }, { S: "b" }, { S: "c" }]);
    expect(result.cut).toBe(false);
    expect(result.numRows).toBe(3);
    const reads = harness.transports.requests.filter((r) => r.lane === -1);
    expect(reads.map((r) => new URL(r.url).search)).toEqual(["?partition=1", "?partition=2"]);
    for (const read of reads) {
      expect(new URL(read.url).pathname).toBe(STATUS_PATH);
      expect(read.headers.authorization).toBe("Bearer token-one");
    }
  });

  test("a 200 with code 391908 keeps its rows as a cut with Snowflake's warning", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, resultAnswer({ columns: ONE_COLUMN, rows: [["1"], ["2"]], code: "391908" }));
    const result = await harness.statements.runSurface("SELECT n FROM t", SNOWFLAKE_QUERY_TAGS.tree);
    expect(result.rows).toEqual([{ N: 1 }, { N: 2 }]);
    expect(result.cut).toBe(true);
    expect(result.warnings).toEqual([{ message: SNOWFLAKE_ROWS_SENTENCES.snowflakeCut }]);
  });

  test("a body the transport cut is the byte cut, naming Snowflake's row count", async () => {
    const harness = setup();
    harness.transports.answer(
      SUBMIT,
      resultAnswer({ columns: ONE_COLUMN, rows: [["1"]], partitions: 2, numRows: 5000 }),
    );
    harness.transports.answer(
      { method: "GET", path: /./ },
      { status: 200, bytes: Buffer.from('{"data":[["2"],["3'), truncated: true },
    );
    const result = await harness.statements.runSurface("SELECT n FROM t", SNOWFLAKE_QUERY_TAGS.tree);
    expect(result.rows).toEqual([{ N: 1 }, { N: 2 }]);
    expect(result.cut).toBe(true);
    expect(result.warnings).toEqual([{ message: SNOWFLAKE_ROWS_SENTENCES.byteCut(5000) }]);
  });

  test("100,000 rows with a partition left is the row cut, and a result with no numRows reads null", async () => {
    const harness = setup();
    const answer = resultAnswer({
      columns: ONE_COLUMN,
      rows: Array.from({ length: 100_000 }, () => ["1"]),
      partitions: 2,
    }) as { status: 200; json: { resultSetMetaData: Record<string, unknown> } };
    delete answer.json.resultSetMetaData.numRows;
    harness.transports.answer(SUBMIT, answer);
    const result = await harness.statements.runSurface("SELECT n FROM t", SNOWFLAKE_QUERY_TAGS.tree);
    expect(result.rows).toHaveLength(100_000);
    expect(result.numRows).toBeNull();
    expect(result.warnings).toEqual([{ message: SNOWFLAKE_ROWS_SENTENCES.rowCut }]);
  });

  test("an answer without partitionInfo reads partition 0 alone", async () => {
    const harness = setup();
    const answer = one("4") as { status: 200; json: { resultSetMetaData: Record<string, unknown> } };
    delete answer.json.resultSetMetaData.partitionInfo;
    harness.transports.answer(SUBMIT, answer);
    const result = await harness.statements.runSurface("SELECT 4", SNOWFLAKE_QUERY_TAGS.tree);
    expect(result.rows).toEqual([{ N: 4 }]);
    expect(harness.transports.requests).toHaveLength(1);
  });

  test("a repeated column name is numbered in the fields and the column types", async () => {
    const harness = setup();
    harness.transports.answer(
      SUBMIT,
      resultAnswer({
        columns: [
          { name: "A", type: "text" },
          { name: "A", type: "boolean" },
        ],
        rows: [["x", "true"]],
      }),
    );
    const result = await harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree);
    expect(result.fields).toHaveLength(2);
    expect(Object.keys(result.columnTypes)).toEqual(result.fields);
    expect(Object.keys(result.rows[0])).toEqual(result.fields);
  });
});

describe("link discipline", () => {
  for (const handle of ["../x", "a?b", "a/b", "", "019c06a4-0000-df4f-0000-00100006589e/x"]) {
    test(`a handle ${JSON.stringify(handle)} is refused and nothing more is sent`, async () => {
      const harness = setup();
      harness.transports.answer(SUBMIT, { status: 202, json: { code: "333334", statementHandle: handle } });
      const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
      expect(error).toBeInstanceOf(SnowflakeError);
      expect((error as SnowflakeError).category).toBe("defect");
      expect((error as SnowflakeError).message).toBe(SNOWFLAKE_STATEMENT_SENTENCES.badHandle);
      expect(harness.transports.requests).toHaveLength(1);
    });
  }

  test("a handle that is not a string is refused", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, { status: 202, json: { code: "333334" } });
    const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).message).toBe(SNOWFLAKE_STATEMENT_SENTENCES.badHandle);
  });

  test("an upper-case handle is accepted", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, handleAnswer(TEST_HANDLE.toUpperCase()));
    harness.transports.answer(STATUS, one());
    await harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree);
    expect(new URL(harness.transports.requests[1].url).pathname).toBe(
      `/api/v2/statements/${TEST_HANDLE.toUpperCase()}`,
    );
  });

  for (const link of [
    `https://evil.example/api/v2/statements/${TEST_HANDLE}`,
    "/session/v1/x",
    `/api/v2/statements/../../session/${TEST_HANDLE}`,
    "http://[",
    42,
  ]) {
    test(`a status link ${JSON.stringify(link)} is refused and nothing more is sent`, async () => {
      const harness = setup();
      harness.transports.answer(SUBMIT, {
        status: 202,
        json: { code: "333334", statementHandle: TEST_HANDLE, statementStatusUrl: link },
      });
      const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
      expect((error as SnowflakeError).category).toBe("defect");
      expect((error as SnowflakeError).message).toBe(SNOWFLAKE_STATEMENT_SENTENCES.foreignLink);
      expect(harness.transports.requests).toHaveLength(1);
    });
  }

  test("an absolute link on the origin is accepted but never followed", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, {
      status: 202,
      json: { statementHandle: TEST_HANDLE, statementStatusUrl: `https://${HOST}/api/v2/statements/other` },
    });
    harness.transports.answer(STATUS, one());
    await harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree);
    expect(new URL(harness.transports.requests[1].url).pathname).toBe(STATUS_PATH);
  });

  test("a foreign link on the final answer is refused", async () => {
    const harness = setup();
    const answer = one() as { status: 200; json: Record<string, unknown> };
    answer.json.statementStatusUrl = "https://evil.example/api/v2/statements/x";
    harness.transports.answer(SUBMIT, handleAnswer());
    harness.transports.answer(STATUS, answer);
    const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).message).toBe(SNOWFLAKE_STATEMENT_SENTENCES.foreignLink);
  });
});

describe("failures", () => {
  test("a status GET past the cap sends exactly one cancel and throws the unavailable sentence", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, handleAnswer());
    harness.transports.answer(STATUS, ...Array.from({ length: 5 }, () => ({ status: 503, text: "" })));
    harness.transports.answer(CANCEL, { status: 200, json: { code: "000604" } });
    const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).category).toBe("unavailable");
    expect((error as SnowflakeError).message).toBe(SNOWFLAKE_ERROR_SENTENCES.unavailable(503, 5));
    expect(cancels(harness)).toHaveLength(1);
    expect(cancels(harness)[0].maxResponseBytes).toBe(SNOWFLAKE_CONTROL_ANSWER_MAX_BYTES);
    expect(new URL(cancels(harness)[0].url).pathname).toBe(`${STATUS_PATH}/cancel`);
    expect(harness.deps.deadlines.requested.at(-1)).toBe(5_000);
  });

  test("a partition GET past the cap sends exactly one cancel and throws the 429 sentence", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, resultAnswer({ columns: ONE_COLUMN, rows: [["1"]], partitions: 2 }));
    harness.transports.answer(
      { method: "GET", path: /./ },
      ...Array.from({ length: 5 }, () => ({ status: 429, bytes: Buffer.from('{"code":"390505"}') })),
    );
    harness.transports.answer(CANCEL, { status: 200, json: { code: "000604" } });
    const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).message).toBe(SNOWFLAKE_ERROR_SENTENCES.rateLimited);
    expect(cancels(harness)).toHaveLength(1);
  });

  test("a partition answered 404 throws without a cancel", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, resultAnswer({ columns: ONE_COLUMN, rows: [["1"]], partitions: 2 }));
    harness.transports.answer({ method: "GET", path: /./ }, { status: 404, bytes: Buffer.from("") });
    const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).message).toBe(SNOWFLAKE_ERROR_SENTENCES.resultGone);
    expect(cancels(harness)).toHaveLength(0);
  });

  test("a failed partition answer is gunzipped, by its encoding or the gzip magic bytes, before it is read", async () => {
    const failed = Buffer.from(
      JSON.stringify({ code: "000904", message: "invalid identifier 'X'", sqlState: "42000" }),
    );
    for (const contentEncoding of ["gzip", null]) {
      const harness = setup();
      harness.transports.answer(SUBMIT, resultAnswer({ columns: ONE_COLUMN, rows: [["1"]], partitions: 2 }));
      harness.transports.answer(
        { method: "GET", path: /./ },
        { status: 422, bytes: gzipSync(failed), contentEncoding },
      );
      // oxlint-disable-next-line no-await-in-loop -- each harness runs its own statement.
      const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
      expect((error as SnowflakeError).message).toBe(
        SNOWFLAKE_ERROR_SENTENCES.statementFailed("invalid identifier 'X'", "000904", "42000"),
      );
    }
  });

  test("a gzipped failed partition answer that inflates past its cap is classified by its status alone", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, resultAnswer({ columns: ONE_COLUMN, rows: [["1"]], partitions: 2 }));
    const huge = Buffer.from(JSON.stringify({ code: "000904", message: "x".repeat(SNOWFLAKE_RESULT_BYTE_BUDGET * 2) }));
    harness.transports.answer(
      { method: "GET", path: /./ },
      { status: 422, bytes: gzipSync(huge), contentEncoding: "gzip" },
    );
    const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).message).toBe(SNOWFLAKE_ERROR_SENTENCES.statementFailed("", null, null));
  });

  test("a statement that fails during the poll throws its sentence without a cancel", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, handleAnswer());
    harness.transports.answer(STATUS, failureAnswer(422, "000904", "invalid identifier 'AFAF'", "42000"));
    const error = await caught(harness.statements.runSurface("SELECT afaf", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).message).toBe(
      SNOWFLAKE_ERROR_SENTENCES.statementFailed("invalid identifier 'AFAF'", "000904", "42000"),
    );
    expect(cancels(harness)).toHaveLength(0);
  });

  test("a refused submit is not accepted, and its server text passes the secret forms", async () => {
    const harness = setup({ secrets: ["s3cret-pat"] });
    harness.transports.answer(SUBMIT, failureAnswer(422, "001003", "syntax error near s3cret-pat"));
    const error = await caught(harness.statements.runSurface("SELEC 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).category).toBe("query");
    expect((error as SnowflakeError).message).not.toContain("s3cret-pat");
    expect(harness.transports.requests).toHaveLength(1);
  });

  test("a server text holding the current token is withheld", async () => {
    const harness = setup({ secrets: [] });
    harness.transports.answer(SUBMIT, failureAnswer(422, "001003", "echo token-one"));
    const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).message).not.toContain("token-one");
  });

  test("a failure body that is not JSON still maps by its status", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, { status: 403, text: "<html>Forbidden</html>" });
    const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).message).toBe(SNOWFLAKE_ERROR_SENTENCES.forbidden);
  });

  test("a submit answered with an unexpected success status is a failure", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, { status: 204, text: "" });
    const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect(error).toBeInstanceOf(SnowflakeError);
    expect(harness.transports.requests).toHaveLength(1);
  });

  test("a status answer past 24 MiB throws the first-too-large sentence", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, handleAnswer());
    harness.transports.answer(STATUS, { fail: new TransportError("too-large", "The response exceeded the limit") });
    const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).category).toBe("query");
    expect((error as SnowflakeError).message).toBe(SNOWFLAKE_ROWS_SENTENCES.firstTooLarge);
  });

  test("an answer that is not a JSON object is the unreadable sentence", async () => {
    for (const text of ["not json", "[1]", "null"]) {
      const harness = setup();
      harness.transports.answer(SUBMIT, { status: 200, text });
      // oxlint-disable-next-line no-await-in-loop -- one harness per body.
      const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
      expect((error as SnowflakeError).message).toBe(SNOWFLAKE_ROWS_SENTENCES.unreadable);
    }
  });

  test("a host that does not answer is the unreachable sentence", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, { fail: new TransportError("unreachable", "no answer") });
    const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).message).toBe(SNOWFLAKE_ERROR_SENTENCES.unreachable(HOST));
  });

  test("any other transport failure is unavailable with the transport's own sentence", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, { fail: new TransportError("redirect", "The server redirected the request") });
    const error = await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree));
    expect((error as SnowflakeError).category).toBe("unavailable");
    expect((error as SnowflakeError).message).toBe("The server redirected the request");
  });

  test("an error that is not the transport's leaves unchanged", async () => {
    const harness = setup();
    const own = new Error("signing failed");
    harness.transports.answer(SUBMIT, { fail: own });
    expect(await caught(harness.statements.runSurface("SELECT 1", SNOWFLAKE_QUERY_TAGS.tree))).toBe(own);
  });
});

describe("session notices, USE and the lone transaction", () => {
  const SETTING_NOTICE = {
    code: "SNOWFLAKE_SETTING_NOT_CARRIED",
    message: SNOWFLAKE_STATEMENT_SENTENCES.settingNotCarried,
  };

  for (const [sql, notice] of [
    ["ALTER SESSION SET TIMEZONE = 'UTC'", SETTING_NOTICE],
    ["alter /* c */ session unset timezone", SETTING_NOTICE],
    ["SET v = 1", SETTING_NOTICE],
    ["UNSET v", SETTING_NOTICE],
  ] as const) {
    test(`${JSON.stringify(sql)} runs and carries its notice`, async () => {
      const harness = setup();
      harness.transports.answer(SUBMIT, one());
      const result = await harness.statements.runUser(sql, undefined);
      expect(result.warnings).toContainEqual(notice);
      expect(bodyOf(submits(harness).at(-1) as RecordedRequest).statement).toBe(sql);
    });
  }

  for (const sql of ["ALTER TABLE t ADD COLUMN c INT", "SHOW TABLES", "ALTER"]) {
    test(`${JSON.stringify(sql)} carries no notice`, async () => {
      const harness = setup();
      harness.transports.answer(SUBMIT, one());
      const result = await harness.statements.runUser(sql, undefined);
      expect(result.warnings).toEqual([]);
    });
  }

  // The SQL API refuses USE (422, 391911, "Command not supported by SQL API: USE" on the trial), so it is never sent.
  for (const sql of [
    "USE DATABASE x",
    "use schema s;",
    "USE SCHEMA STUDIO_DEMO.PROBE",
    "USE WAREHOUSE w",
    "USE ROLE r",
    "USE SECONDARY ROLES ALL",
    "USE x",
    "-- c\nUSE DATABASE x",
    "/* lead */ use role r",
  ]) {
    test(`${JSON.stringify(sql)} is refused before anything is sent`, async () => {
      const harness = setup();
      const error = await caught(harness.statements.runUser(sql, "q-use"));
      expect(error).toBeInstanceOf(SnowflakeError);
      expect((error as SnowflakeError).category).toBe("query");
      expect((error as SnowflakeError).message).toBe(SNOWFLAKE_STATEMENT_SENTENCES.useRefused);
      expect(harness.transports.requests).toHaveLength(0);
      expect(harness.statements.runsInFlight()).toBe(0);
    });
  }

  test("the USE refusal points to the connection's fields and to qualified names", () => {
    expect(SNOWFLAKE_STATEMENT_SENTENCES.useRefused).toBe(
      "Snowflake's SQL API does not run USE. Set Database, Schema, Warehouse or Role on the connection instead, or use fully qualified names such as database.schema.table. Nothing was sent.",
    );
    expect(Object.keys(SNOWFLAKE_STATEMENT_SENTENCES)).not.toContain("useNotCarried");
  });

  for (const sql of ['SELECT "USE" FROM t', "SHOW USERS", "SELECT * FROM t JOIN u USING (x)"]) {
    test(`${JSON.stringify(sql)} is not a USE and is sent`, async () => {
      const harness = setup();
      harness.transports.answer(SUBMIT, one());
      await harness.statements.runUser(sql, undefined);
      expect(submits(harness).map((r) => bodyOf(r).statement)).toContain(sql);
    });
  }

  for (const sql of [
    "BEGIN",
    "begin;",
    "BEGIN TRANSACTION",
    "BEGIN WORK",
    "BEGIN NAME t1",
    'BEGIN NAME "My Tx"',
    "BEGIN TRANSACTION NAME t1",
    "BEGIN WORK NAME t1",
    "START TRANSACTION",
    "start transaction name t1 ;",
    "COMMIT",
    "COMMIT WORK",
    "ROLLBACK",
    "ROLLBACK WORK",
    "-- c\nCOMMIT",
    "/* lead */ ROLLBACK /* tail */",
  ]) {
    test(`a lone ${JSON.stringify(sql)} is refused and nothing is sent`, async () => {
      const harness = setup();
      const error = await caught(harness.statements.runUser(sql, "q-lone"));
      expect(error).toBeInstanceOf(SnowflakeError);
      expect((error as SnowflakeError).category).toBe("query");
      expect((error as SnowflakeError).message).toBe(SNOWFLAKE_STATEMENT_SENTENCES.noTransactions);
      expect(harness.transports.requests).toHaveLength(0);
      expect(harness.statements.runsInFlight()).toBe(0);
    });
  }

  for (const sql of [
    "BEGIN SELECT 1; END",
    "BEGIN LET x := 1; END",
    "COMMIT; COMMIT",
    "BEGIN NAME",
    "BEGIN NAME 'x'",
    "COMMIT;;",
    "BEGIN TRANSACTION NAME t1 x",
    "START",
    "ROLLBACK TO x",
    "COMMIT 1",
  ]) {
    test(`${JSON.stringify(sql)} is not a lone transaction statement and is sent`, async () => {
      const harness = setup();
      harness.transports.answer(SUBMIT, one());
      await harness.statements.runUser(sql, undefined);
      expect(submits(harness).map((r) => bodyOf(r).statement)).toContain(sql);
    });
  }
});

describe("a run by its queryId", () => {
  test("a polling run is cancelled through its id, and its id is free again once it ended", async () => {
    const harness = setup();
    harness.transports.answer(SUBMIT, handleAnswer());
    harness.transports.answer(STATUS, { hang: true });
    harness.transports.answer(CANCEL, { status: 200, json: { code: "000604" } });
    const running = caught(harness.statements.runUser("SHOW TABLES", "q-run"));
    await harness.transports.received(2);
    expect(harness.statements.runsInFlight()).toBe(1);
    expect(await harness.statements.cancel("q-run")).toBe(true);
    expect(((await running) as SnowflakeError).category).toBe("cancelled");
    expect(await harness.statements.cancel("q-run")).toBe(false);
    expect(cancels(harness)).toHaveLength(1);
  });
});
