import { describe, expect, test } from "bun:test";
import {
  columnTypeOf,
  decodeCell,
  decodeRows,
  parseCellJson,
  readColumns,
  SNOWFLAKE_DECODE_SENTENCES,
  type SnowflakeColumn,
} from "@/lib/db/providers/sql/snowflake/decode";
import { SnowflakeError } from "@/lib/db/providers/sql/snowflake/errors";

const col = (type: string, extra: Partial<SnowflakeColumn> = {}): SnowflakeColumn => ({
  name: "c",
  type,
  precision: null,
  scale: null,
  length: null,
  ...extra,
});

function refusal(run: () => unknown): SnowflakeError {
  try {
    run();
  } catch (error) {
    expect(error).toBeInstanceOf(SnowflakeError);
    return error as SnowflakeError;
  }
  throw new Error("expected a refusal");
}

/** The deepest nesting `parseCellJson` reads, `decode.ts`'s own bound. */
const SNOWFLAKE_CELL_MAX_DEPTH = 64;
describe("decodeCell", () => {
  test("fixed scale 0: a safe integer is a number, anything past it exact text", () => {
    expect(decodeCell(col("fixed", { precision: 38, scale: 0 }), "42")).toBe(42);
    expect(decodeCell(col("fixed", { precision: 38, scale: 0 }), "-17")).toBe(-17);
    expect(decodeCell(col("FIXED", { precision: 38, scale: 0 }), "9007199254740993")).toBe("9007199254740993");
    expect(decodeCell(col("fixed", { precision: 38, scale: 0 }), "-0")).toBe("-0");
    expect(decodeCell(col("fixed", { precision: 38, scale: 0 }), "007")).toBe("007");
    expect(decodeCell(col("fixed", { precision: 38 }), "5")).toBe(5);
  });

  test("fixed with a scale stays text", () => {
    expect(decodeCell(col("fixed", { precision: 10, scale: 2 }), "1.10")).toBe("1.10");
    expect(decodeCell(col("fixed", { precision: 10, scale: 2 }), "-3")).toBe("-3");
  });

  test("decfloat stays text in every documented form", () => {
    for (const text of ["1.5", "-12", "1.", ".5", "1e400", "-2.5E-10"]) {
      expect(decodeCell(col("decfloat"), text)).toBe(text);
    }
  });

  test("real: finite numbers, and the non-finite words by table", () => {
    expect(decodeCell(col("real"), "1.5e3")).toBe(1500);
    expect(decodeCell(col("real"), "-0.25")).toBe(-0.25);
    expect(decodeCell(col("real"), "NaN")).toBe("NaN");
    expect(decodeCell(col("real"), "inf")).toBe("Infinity");
    expect(decodeCell(col("real"), "-inf")).toBe("-Infinity");
    expect(decodeCell(col("real"), "Infinity")).toBe("Infinity");
    expect(decodeCell(col("real"), "-Infinity")).toBe("-Infinity");
  });

  test("boolean, binary, text, null", () => {
    expect(decodeCell(col("boolean"), "TRUE")).toBe(true);
    expect(decodeCell(col("boolean"), "1")).toBe(true);
    expect(decodeCell(col("boolean"), "False")).toBe(false);
    expect(decodeCell(col("boolean"), "0")).toBe(false);
    expect(decodeCell(col("boolean"), "maybe")).toBe("maybe");
    expect(decodeCell(col("binary"), "DEADBEEF")).toEqual(Buffer.from([0xde, 0xad, 0xbe, 0xef]));
    expect(decodeCell(col("binary"), "")).toEqual(Buffer.alloc(0));
    expect(decodeCell(col("text"), "x")).toBe("x");
    expect(decodeCell(col("text"), null)).toBeNull();
    expect(decodeCell(col("timestamp_tz"), null)).toBeNull();
  });

  test("the two documented time strings", () => {
    // 82919 s is 23 h, 1 min, 59 s.
    expect(decodeCell(col("time", { scale: 9 }), "82919.000000000")).toBe("23:01:59.000000000");
    // 1616173619 is 2021-03-19 17:06:59 UTC; 1500 - 1440 is +60 minutes.
    expect(decodeCell(col("timestamp_tz", { scale: 9 }), "1616173619.000000000 1500")).toBe(
      "2021-03-19 18:06:59.000000000+01:00",
    );
  });

  test("time keeps the received fraction digits, and none without a fraction", () => {
    expect(decodeCell(col("time", { scale: 3 }), "0.5")).toBe("00:00:00.5");
    expect(decodeCell(col("time", { scale: 0 }), "86399")).toBe("23:59:59");
  });

  test("ntz is the wall time, exact to the nanosecond", () => {
    expect(decodeCell(col("timestamp_ntz", { scale: 9 }), "1616173619.123456789")).toBe(
      "2021-03-19 17:06:59.123456789",
    );
    expect(decodeCell(col("timestamp_ntz", { scale: 0 }), "0")).toBe("1970-01-01 00:00:00");
    expect(decodeCell(col("timestamp_ntz", { scale: 9 }), "253402300799.999999999")).toBe(
      "9999-12-31 23:59:59.999999999",
    );
    expect(decodeCell(col("timestamp_ntz", { scale: 0 }), "-62135596800")).toBe("0001-01-01 00:00:00");
  });

  test("ltz is the UTC instant with +00:00", () => {
    expect(decodeCell(col("timestamp_ltz", { scale: 3 }), "0.123")).toBe("1970-01-01 00:00:00.123+00:00");
  });

  test("tz offsets beyond 720 to 2160 shift by 1440", () => {
    expect(decodeCell(col("timestamp_tz", { scale: 0 }), "0 2400")).toBe("1970-01-01 16:00:00+16:00");
    expect(decodeCell(col("timestamp_tz", { scale: 0 }), "0 0")).toBe("1969-12-31 00:00:00-24:00");
    expect(decodeCell(col("timestamp_tz", { scale: 0 }), "0 1440")).toBe("1970-01-01 00:00:00+00:00");
    expect(decodeCell(col("timestamp_tz", { scale: 0 }), "0 1110")).toBe("1969-12-31 18:30:00-05:30");
    expect(decodeCell(col("timestamp_tz", { scale: 0 }), "0 -60")).toBe("1969-12-30 23:00:00-25:00");
  });

  test("a pre-1970 fraction is one signed decimal", () => {
    expect(decodeCell(col("timestamp_ntz", { scale: 1 }), "-1.5")).toBe("1969-12-31 23:59:58.5");
    expect(decodeCell(col("timestamp_ntz", { scale: 2 }), "-0.25")).toBe("1969-12-31 23:59:59.75");
  });

  // The cells the trial (Snowflake 10.37.100, 2026-10-12) answered for TO_TIME('23:01:59.123'),
  // TO_TIMESTAMP_NTZ('2021-03-19 17:06:59.5'), TO_TIMESTAMP_LTZ('2021-03-19 17:06:59'),
  // TO_TIMESTAMP_TZ('2021-03-19 18:06:59 +01:00') and TO_TIMESTAMP_NTZ('1969-12-31 23:59:58.5'): decimal seconds with
  // nine fraction digits at scale 9, and a pre-1970 instant as one signed decimal.
  test.each([
    ["time", "82919.123000000", "23:01:59.123000000"],
    ["timestamp_ntz", "1616173619.500000000", "2021-03-19 17:06:59.500000000"],
    ["timestamp_ltz", "1616173619.000000000", "2021-03-19 17:06:59.000000000+00:00"],
    ["timestamp_tz", "1616173619.000000000 1500", "2021-03-19 18:06:59.000000000+01:00"],
    ["timestamp_ntz", "-1.500000000", "1969-12-31 23:59:58.500000000"],
  ])("the captured %s cell %s", (type, cell, expected) => {
    expect(decodeCell(col(type, { scale: 9, precision: 0 }), cell)).toBe(expected);
  });

  test("date from days since epoch", () => {
    expect(decodeCell(col("date"), "0")).toBe("1970-01-01");
    expect(decodeCell(col("date"), "-719162")).toBe("0001-01-01");
    expect(decodeCell(col("date"), "2932896")).toBe("9999-12-31");
    expect(decodeCell(col("date"), "18705")).toBe("2021-03-19");
  });

  test("the refusals name the column", () => {
    for (const [column, cell] of [
      [col("binary", { name: "blob" }), "ZZ"],
      [col("binary", { name: "odd" }), "ABC"],
      [col("fixed", { name: "n", precision: 38, scale: 0 }), "abc"],
      [col("fixed", { name: "n1", precision: 38, scale: 0 }), "1.5"],
      [col("fixed", { name: "n2", precision: 10, scale: 2 }), "1e5"],
      [col("decfloat", { name: "df" }), "x"],
      [col("real", { name: "r" }), "1e999"],
      [col("real", { name: "r2" }), "nan"],
      [col("timestamp_ntz", { name: "ts", scale: 9 }), "1611871777123456789"],
      [col("timestamp_ntz", { name: "ts2", scale: 9 }), "1.1234567890"],
      [col("timestamp_ntz", { name: "ts3", scale: 0 }), "-62135596801"],
      [col("timestamp_ltz", { name: "lt", scale: 0 }), "2021-03-19"],
      [col("timestamp_tz", { name: "tz", scale: 0 }), "0"],
      [col("timestamp_tz", { name: "tz2", scale: 0 }), "253402300799 1500"],
      [col("time", { name: "t", scale: 0 }), "86400"],
      [col("time", { name: "t2", scale: 0 }), "-1"],
      [col("date", { name: "d" }), "99999999"],
      [col("date", { name: "d2" }), "-719163"],
      [col("date", { name: "d3" }), "1.5"],
    ] as const) {
      expect(() => decodeCell(column, cell)).toThrow(SnowflakeError);
      expect(() => decodeCell(column, cell)).toThrow(`column "${column.name}"`);
    }
  });

  test("the refusal sentence names the catalog type and the column, and is a query error", () => {
    const error = refusal(() => decodeCell(col("binary", { name: "blob" }), "ZZ"));
    expect(error.category).toBe("query");
    expect(error.message).toBe(
      'Snowflake sent a BINARY value in column "blob" that Studio could not read, so the result is not shown.',
    );
    expect(error.message).toBe(SNOWFLAKE_DECODE_SENTENCES.unreadable("BINARY", "blob"));
  });

  test("a cell that is neither text nor null is refused", () => {
    for (const cell of [42, true, {}, ["x"]]) {
      expect(() => decodeCell(col("text", { name: "v" }), cell as unknown as string)).toThrow('column "v"');
    }
  });

  test("semi-structured, structured, vector and unknown cells stay their text", () => {
    for (const type of [
      "variant",
      "object",
      "array",
      "map",
      "vector",
      "geography",
      "geometry",
      "uuid",
      "file",
      "interval",
      "made_up",
    ]) {
      expect(decodeCell(col(type), '{"a":[1,2]}')).toBe('{"a":[1,2]}');
    }
  });

  test("a 10,000-deep VARIANT passes as a string", () => {
    const deep = `${"[".repeat(10_000)}${"]".repeat(10_000)}`;
    expect(decodeCell(col("variant"), deep)).toBe(deep);
  });
});

describe("decodeRows", () => {
  test("each row keyed by the column names, a repeated name numbered, every cell decoded", () => {
    const columns = [
      col("fixed", { name: "ID", precision: 38, scale: 0 }),
      col("text", { name: "ID" }),
      col("boolean", { name: "OK" }),
    ];
    expect(
      decodeRows(columns, [
        ["1", "a", "true"],
        [null, null, null],
      ]),
    ).toEqual([
      { ID: 1, "ID (2)": "a", OK: true },
      { ID: null, "ID (2)": null, OK: null },
    ]);
  });

  test("a column named __proto__ is an ordinary key", () => {
    const [row] = decodeRows([col("text", { name: "__proto__" })], [["x"]]);
    expect(Object.keys(row)).toEqual(["__proto__"]);
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
  });

  test("a row whose width is not the column count is refused", () => {
    const error = refusal(() => decodeRows([col("text", { name: "A" })], [["x", "y"]]));
    expect(error.category).toBe("defect");
    expect(error.message).toBe(SNOWFLAKE_DECODE_SENTENCES.columns);
  });
});

describe("columnTypeOf", () => {
  test("catalog spelling", () => {
    expect(columnTypeOf(col("fixed", { precision: 38, scale: 0 }))).toBe("NUMBER(38,0)");
    expect(columnTypeOf(col("fixed", { precision: 38 }))).toBe("NUMBER(38,0)");
    expect(columnTypeOf(col("fixed", { precision: 10, scale: 2 }))).toBe("NUMBER(10,2)");
    expect(columnTypeOf(col("fixed"))).toBe("NUMBER");
    expect(columnTypeOf(col("decfloat"))).toBe("DECFLOAT");
    expect(columnTypeOf(col("real"))).toBe("FLOAT");
    expect(columnTypeOf(col("text", { length: 16777216 }))).toBe("VARCHAR(16777216)");
    expect(columnTypeOf(col("text"))).toBe("VARCHAR");
    expect(columnTypeOf(col("boolean"))).toBe("BOOLEAN");
    expect(columnTypeOf(col("binary", { length: 8 }))).toBe("BINARY");
    expect(columnTypeOf(col("date"))).toBe("DATE");
    expect(columnTypeOf(col("time", { scale: 9 }))).toBe("TIME(9)");
    expect(columnTypeOf(col("time"))).toBe("TIME");
    expect(columnTypeOf(col("timestamp_ntz", { scale: 3 }))).toBe("TIMESTAMP_NTZ(3)");
    expect(columnTypeOf(col("timestamp_ltz", { scale: 0 }))).toBe("TIMESTAMP_LTZ(0)");
    expect(columnTypeOf(col("timestamp_tz", { scale: 9 }))).toBe("TIMESTAMP_TZ(9)");
    expect(columnTypeOf(col("variant"))).toBe("VARIANT");
    expect(columnTypeOf(col("vector"))).toBe("VECTOR");
    expect(columnTypeOf(col("geography"))).toBe("GEOGRAPHY");
  });
});

describe("parseCellJson and readColumns", () => {
  test("a text nested past the depth bound is refused before any parse", () => {
    const deep = `${"[".repeat(SNOWFLAKE_CELL_MAX_DEPTH + 1)}${"]".repeat(SNOWFLAKE_CELL_MAX_DEPTH + 1)}`;
    expect(() => parseCellJson(deep, "data_type")).toThrow(SnowflakeError);
    expect(() => parseCellJson(deep, "data_type")).toThrow('a VARIANT value in column "data_type"');
    const atBound = `${"[".repeat(SNOWFLAKE_CELL_MAX_DEPTH)}${"]".repeat(SNOWFLAKE_CELL_MAX_DEPTH)}`;
    expect(parseCellJson(atBound, "x")).toBeArray();
    expect(parseCellJson('{"type":"FIXED","precision":38,"scale":0,"nullable":true}', "data_type")).toEqual({
      type: "FIXED",
      precision: 38,
      scale: 0,
      nullable: true,
    });
    expect(parseCellJson('{"a":"]]]]"}', "x")).toEqual({ a: "]]]]" });
  });

  test("brackets inside strings, escaped quotes among them, do not count toward the depth", () => {
    const inside = `"${'\\"'.repeat(3)}${"[{".repeat(200)}"`;
    expect(parseCellJson(`[${inside}]`, "x")).toEqual([`${'"'.repeat(3)}${"[{".repeat(200)}`]);
    expect(parseCellJson('"a\\\\"', "x")).toBe("a\\");
  });

  test("text that is not JSON is refused with the column name", () => {
    expect(() => parseCellJson("{not json", "data_type")).toThrow('column "data_type"');
  });

  test("a column list that is not an array of named, typed columns is refused", () => {
    for (const rowType of [
      [{ name: 1, type: "text" }],
      [{ name: "A" }],
      [{ name: "A", type: "text", precision: "38" }],
      [{ name: "A", type: "text", length: Number.NaN }],
      [null],
      ["A"],
      { name: "A", type: "text" },
      null,
    ]) {
      const error = refusal(() => readColumns(rowType));
      expect(error.category).toBe("defect");
      expect(error.message).toBe("Snowflake sent a result whose column list Studio could not read.");
    }
    expect(readColumns([{ name: "A", type: "TEXT", length: 5, precision: null, scale: null, nullable: true }])).toEqual(
      [{ name: "A", type: "text", precision: null, scale: null, length: 5 }],
    );
    expect(readColumns([{ name: "N", type: "fixed", precision: 38, scale: 0 }])).toEqual([
      { name: "N", type: "fixed", precision: 38, scale: 0, length: null },
    ]);
    expect(readColumns([])).toEqual([]);
  });
});
