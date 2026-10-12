/**
 * The JSON read request, and every refusal it makes before a request is sent (#1677 section 2).
 *
 * A REFUSAL HERE IS A SENTENCE; A STATUS CODE FROM THE SERVER IS NOT. The reason this parser is
 * strict is measured: asyncua answers a history read of a node that keeps no history with Good and
 * ZERO VALUES, so a reader who mistyped a node id would see an empty table and conclude the node is
 * empty rather than that they were wrong.
 */
import { describe, expect, test } from "bun:test";
import { DatabaseConfigError } from "@/lib/db/errors";
import { readRequest } from "@/lib/db/providers/industrial/opcua/read-request";
import { OPCUA_HISTORY_VALUE_CAP } from "@/lib/db/providers/industrial/opcua/constants";

describe("a value read", () => {
  test("one node id, in every form a measured server answered with", () => {
    for (const id of [
      "i=85",
      "ns=3;s=SlowUInt1",
      "ns=8;i=271426160",
      "nsu=http://microsoft.com/Opc/OpcPlc/;s=SlowUInt1",
      "g=09087e75-8e5e-499b-954f-f2a8624db28a",
    ]) {
      const request = readRequest(JSON.stringify({ read: [id] }));
      expect(request).toEqual({ kind: "read", nodeIds: [id] });
    }
  });

  test("many node ids keep their order, because the rows come back in it", () => {
    const ids = ["i=85", "ns=3;s=A", "ns=3;s=B"];
    expect(readRequest(JSON.stringify({ read: ids }))).toEqual({
      kind: "read",
      nodeIds: ids,
    });
  });

  test("an empty list is refused, because it would send a request that asks nothing", () => {
    expect(() => readRequest('{"read":[]}')).toThrow(DatabaseConfigError);
  });

  test("a node id the grammar refuses is named, with the form it should have taken", () => {
    expect(() => readRequest('{"read":["Objects"]}')).toThrow(/not a node id/);
    expect(() => readRequest('{"read":["Objects"]}')).toThrow(/ns=3;s=SlowUInt1/);
  });

  test("the item that was wrong is named by its position", () => {
    expect(() => readRequest('{"read":["i=85","bozuk"]}')).toThrow(/item 1/);
  });
});

describe("a history read", () => {
  test("one node, with a window and a limit", () => {
    const request = readRequest(
      '{"history":"ns=3;s=HistorianInt32","start":"2026-10-12T00:00:00Z","end":"2026-10-12T01:00:00Z","limit":500}',
    );
    expect(request.kind).toBe("history");
    if (request.kind !== "history") throw new Error("unreachable");
    expect(request.nodeId).toBe("ns=3;s=HistorianInt32");
    expect(request.start?.toISOString()).toBe("2026-10-12T00:00:00.000Z");
    expect(request.limit).toBe(500);
  });

  test("no limit means the cap, which is what makes a read finish", () => {
    const request = readRequest('{"history":"ns=3;s=H"}');
    if (request.kind !== "history") throw new Error("unreachable");
    expect(request.limit).toBe(OPCUA_HISTORY_VALUE_CAP);
  });

  /**
   * The cap's own reason, in the refusal: the server appends history while it is read. The same
   * 24-hour window measured 11, then 12, then 13 pages over nineteen minutes.
   */
  test("a limit past the cap is refused, and the sentence says why a cap exists", () => {
    expect(() => readRequest('{"history":"i=1","limit":99999}')).toThrow(/at most/);
    expect(() => readRequest('{"history":"i=1","limit":99999}')).toThrow(/appends history while it is read/);
  });

  test("an end before its start is refused rather than sent as an empty window", () => {
    expect(() => readRequest('{"history":"i=1","start":"2026-10-12T02:00:00Z","end":"2026-10-12T01:00:00Z"}')).toThrow(
      /before "start"/,
    );
  });

  test("an instant that is not one is named as such", () => {
    expect(() => readRequest('{"history":"i=1","end":"yesterday"}')).toThrow(/not an instant/);
    expect(() => readRequest('{"history":"i=1","start":5}')).toThrow(/ISO 8601/);
  });

  test("a limit that is not a whole number is refused", () => {
    for (const bad of ["0", "-1", "1.5", '"10"']) {
      expect(() => readRequest(`{"history":"i=1","limit":${bad}}`)).toThrow(DatabaseConfigError);
    }
  });
});

describe("what is not a request at all", () => {
  test("a key this parser does not know is a mistake, not something to ignore", () => {
    expect(() => readRequest('{"browse":"i=85"}')).toThrow(/"read" or "history"/);
    expect(() => readRequest('{"read":["i=1"],"limit":5}')).toThrow(/takes "read" only/);
    expect(() => readRequest('{"history":"i=1","depth":3}')).toThrow(/"depth"/);
  });

  test("both shapes at once is refused, because it would run one and drop the other", () => {
    expect(() => readRequest('{"read":["i=1"],"history":"i=2"}')).toThrow(/not both/);
  });

  test("text that is not JSON, or not an object, is named as such", () => {
    expect(() => readRequest("bozuk")).toThrow(/is JSON/);
    expect(() => readRequest("[]")).toThrow(/is a JSON object/);
    expect(() => readRequest("null")).toThrow(/is a JSON object/);
    expect(() => readRequest("{}")).toThrow(/names what to read/);
  });

  /** Every refusal is a config error, so the dialog shows it rather than the results pane. */
  test("every refusal is a config error", () => {
    for (const text of ["bozuk", "[]", "{}", '{"read":[]}', '{"history":"x"}', '{"browse":1}']) {
      expect(() => readRequest(text)).toThrow(DatabaseConfigError);
    }
  });
});
