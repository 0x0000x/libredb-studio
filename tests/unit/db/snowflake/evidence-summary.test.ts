/**
 * The row summary of the Snowflake evidence harness's measurement items, checked without an account: an answer that
 * carries rows keeps everything but them, and records their count, the first and the last row, and the answer's byte
 * length on the wire and decoded; an answer without rows is kept whole.
 */
import { describe, expect, test } from "bun:test";
import type { SnowflakeScrubbedExchange } from "../../../helpers/snowflake-evidence-scrub";
import { summariseRows } from "../../../helpers/snowflake-evidence-summary";

const request: SnowflakeScrubbedExchange["request"] = {
  method: "GET",
  path: "/api/v2/statements/00000000-0000-0000-0000-000000000001?partition=1",
  headers: { authorization: "Bearer <token-1>" },
  dropped: [],
};

const answer = (body: unknown, headers: Record<string, string> = {}): SnowflakeScrubbedExchange => ({
  request,
  response: { status: 200, headers, body, dropped: ["stats.extra"] },
});

describe("summariseRows", () => {
  test("a partition answer keeps its status and headers and records its rows as a summary", () => {
    const exchange = answer(
      {
        data: [
          ["1", "pad-0000001"],
          ["2", "pad-0000002"],
          ["3", "pad-0000003"],
        ],
      },
      {
        "content-type": "application/json",
        "content-encoding": "gzip",
      },
    );
    expect(summariseRows(exchange, { wireBytes: 40, decodedBytes: 61 })).toEqual({
      request,
      response: {
        status: 200,
        headers: { "content-type": "application/json", "content-encoding": "gzip" },
        body: {},
        dropped: ["stats.extra"],
        rows: {
          wireBytes: 40,
          decodedBytes: 61,
          rowCount: 3,
          firstRow: ["1", "pad-0000001"],
          lastRow: ["3", "pad-0000003"],
        },
      },
    });
  });

  test("a statement's own inline rows are summarised and every other field of its answer is kept", () => {
    const body = {
      code: "090001",
      statementHandle: "00000000-0000-0000-0000-000000000001",
      resultSetMetaData: { numRows: 2, partitionInfo: [{ rowCount: 2 }] },
      data: [["a"], ["b"]],
    };
    const summarised = summariseRows(answer(body), { wireBytes: 300, decodedBytes: 300 });
    expect(summarised.response.body).toEqual({
      code: "090001",
      statementHandle: "00000000-0000-0000-0000-000000000001",
      resultSetMetaData: { numRows: 2, partitionInfo: [{ rowCount: 2 }] },
    });
    expect("rows" in summarised.response && summarised.response.rows).toEqual({
      wireBytes: 300,
      decodedBytes: 300,
      rowCount: 2,
      firstRow: ["a"],
      lastRow: ["b"],
    });
  });

  test("an empty result records no first or last row", () => {
    const summarised = summariseRows(answer({ data: [] }), { wireBytes: 11, decodedBytes: 11 });
    expect("rows" in summarised.response && summarised.response.rows).toEqual({
      wireBytes: 11,
      decodedBytes: 11,
      rowCount: 0,
      firstRow: null,
      lastRow: null,
    });
  });

  test("an answer without rows is kept whole", () => {
    for (const body of [{ code: "333334", statementHandle: "x" }, { text: "<html></html>" }, { data: "not rows" }]) {
      const exchange = answer(body);
      expect(summariseRows(exchange, { wireBytes: 1, decodedBytes: 1 })).toBe(exchange);
    }
  });
});
