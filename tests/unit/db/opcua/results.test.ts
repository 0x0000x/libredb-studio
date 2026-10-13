/**
 * Rows, and the two facts a grid must keep apart (#1677 section 2).
 */
import { describe, expect, test } from "bun:test";
import {
  emptyResult,
  historyRows,
  OPCUA_HISTORY_COLUMNS,
  OPCUA_VALUE_COLUMNS,
  valueRows,
} from "@/lib/db/providers/industrial/opcua/results";

describe("a value read", () => {
  test("one row per node, in the order the request named them", () => {
    const ids = ["ns=3;s=C", "ns=3;s=A", "ns=3;s=B"];
    const result = valueRows(ids, [
      { nodeId: "ns=3;s=A", value: 1, statusCode: "Good" },
      { nodeId: "ns=3;s=B", value: 2, statusCode: "Good" },
      { nodeId: "ns=3;s=C", value: 3, statusCode: "Good" },
    ]);
    expect(result.rows.map((row) => row.nodeId)).toEqual(ids);
    expect(result.rowCount).toBe(3);
    expect(result.fields).toEqual([...OPCUA_VALUE_COLUMNS]);
  });

  /** A Bad status is a row: dropping it would say the node does not exist. */
  test("a node whose read failed is still a row, with its status", () => {
    const result = valueRows(["i=2277"], [{ nodeId: "i=2277", value: null, statusCode: "BadNotReadable" }]);
    expect(result.rowCount).toBe(1);
    expect(result.rows[0]?.statusCode).toBe("BadNotReadable");
    expect(result.rows[0]?.value).toBe("");
  });

  test("a node the server did not answer for at all is reported as such", () => {
    const result = valueRows(["ns=3;s=Missing"], []);
    expect(result.rows[0]?.statusCode).toBe("BadNoAnswer");
    expect(result.rows[0]?.value).toBeNull();
  });

  /** The awkward values, as rows rather than as a throw. */
  test("bytes, bigints and markup each become a cell", () => {
    const result = valueRows(
      ["a", "b", "c"],
      [
        {
          nodeId: "a",
          value: new Uint8Array([0xff, 0x41]),
          statusCode: "Good",
        },
        { nodeId: "b", value: BigInt("9007199254740993"), statusCode: "Good" },
        { nodeId: "c", value: "<b>x</b>", statusCode: "Good" },
      ],
    );
    expect(result.rows[0]?.value).toBe("ff41");
    expect(result.rows[0]?.shape).toBe("bytes");
    expect(result.rows[1]?.value).toBe("9007199254740993");
    expect(result.rows[2]?.value).toBe("<b>x</b>");
  });
});

describe("a history read", () => {
  test("one row per value, in the server's order, all naming the node", () => {
    const result = historyRows("ns=3;s=H", [
      {
        value: 1,
        statusCode: "Good",
        sourceTimestamp: "2026-10-12T01:00:00.000Z",
      },
      {
        value: 2,
        statusCode: "Good",
        sourceTimestamp: "2026-10-12T01:00:10.000Z",
      },
    ]);
    expect(result.rowCount).toBe(2);
    expect(result.rows.every((row) => row.nodeId === "ns=3;s=H")).toBe(true);
    /*
     * THE COLUMNS ARE WRITTEN OUT, not compared against the constant that produced them. `toEqual`
     * on `OPCUA_HISTORY_COLUMNS` is a tautology - renaming a column renames both sides and the test
     * still passes, which a mutation run proved - and a grid's column names are a contract with the
     * reader and with the exporters, so they are spelled here where a rename has to be deliberate.
     */
    expect(result.fields).toEqual(["nodeId", "value", "shape", "statusCode", "sourceTimestamp", "serverTimestamp"]);
    expect(result.rows[0]?.sourceTimestamp).toBe("2026-10-12T01:00:00.000Z");
    // And the shape travels with a history row too, which nothing asserted before: a plain number
    // is `number`, and a row whose shape were always null would have passed every test here.
    expect(result.rows[0]?.shape).toBe("number");
  });

  test("a value with a non-Good status keeps it", () => {
    const result = historyRows("ns=3;s=H", [{ value: null, statusCode: "UncertainInitialValue" }]);
    expect(result.rows[0]?.statusCode).toBe("UncertainInitialValue");
  });
});

describe("the two facts a grid keeps apart", () => {
  /**
   * Columns with no rows reads as "this ran and found nothing"; no columns reads as "this did not
   * run". Measured why it matters: asyncua answers a history read of a non-historized node with
   * Good and zero values, so the first is a real answer.
   */
  test("an empty result still carries its columns", () => {
    const result = emptyResult(OPCUA_HISTORY_COLUMNS);
    expect(result.fields).toEqual(["nodeId", "value", "shape", "statusCode", "sourceTimestamp", "serverTimestamp"]);
    expect(result.rows).toEqual([]);
    expect(result.rowCount).toBe(0);
  });

  test("the two column sets are stated, so a grid is the same shape every time", () => {
    expect(OPCUA_VALUE_COLUMNS).toContain("statusCode");
    expect(OPCUA_VALUE_COLUMNS).toContain("sourceTimestamp");
    expect(OPCUA_HISTORY_COLUMNS).toContain("sourceTimestamp");
  });
});
