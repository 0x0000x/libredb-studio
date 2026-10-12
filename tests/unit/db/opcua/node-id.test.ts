/**
 * Node ids, and the escaped path keys the Keys panel walks (#1677).
 *
 * THE ESCAPING EXISTS BECAUSE OF ONE NODE ON THE ISSUE'S OWN FIRST FIXTURE. Microsoft's OPC PLC
 * ships a BrowseName holding `/ : . \ | ; =` at once, deliberately, as a torture test, and this is
 * it as the server really publishes it - the backslash and the acute accent included, which an
 * earlier transcription dropped:
 * `Special_"!§$%&/()=?`´\+~*'#_-:.;,<>|@^°€µ{[]}`. Three more names contain `/` because they are
 * namespace URIs. So no separator could be chosen, and the one used is escaped instead. These tests
 * are the real names, re-measured on 12 Oct 2026 from a walk of the fixture's 6,769 nodes, rather
 * than names invented here.
 */
import { describe, expect, test } from "bun:test";
import {
  escapeSegment,
  isNodeId,
  OPCUA_PATH_SEPARATOR,
  pathKey,
  pathSegments,
  unescapeSegment,
} from "@/lib/db/providers/industrial/opcua/node-id";

/** Names measured on the pinned fixture, including the one that contains every candidate separator. */
const MEASURED_NAMES = [
  "Special_\"!§$%&/()=?`´\\+~*'#_-:.;,<>|@^°€µ{[]}",
  "http://microsoft.com/Opc/OpcPlc/Boiler",
  "http://opcfoundation.org/UA/DI/",
  "http://opcfoundation.org/UA/",
  "SlowUInt1",
  "HistorianInt32",
  "Objects",
] as const;

describe("escaping a path segment", () => {
  test("every name measured on the fixture round-trips unchanged", () => {
    for (const name of MEASURED_NAMES) {
      expect(unescapeSegment(escapeSegment(name))).toBe(name);
    }
  });

  /**
   * The trap: a name that already contains the escape sequence. `%` is escaped first, so `a%2Fb`
   * becomes `a%252Fb` and comes back as itself, rather than coming back as `a/b` and inventing a
   * level that the server does not have.
   */
  test("a name already holding the escape sequence round-trips, and does not grow a level", () => {
    expect(escapeSegment("a%2Fb")).toBe("a%252Fb");
    expect(unescapeSegment("a%252Fb")).toBe("a%2Fb");
    expect(pathSegments(pathKey(["a%2Fb"]))).toEqual(["a%2Fb"]);
  });

  test("a name holding the separator does not split into two segments", () => {
    const name = "http://microsoft.com/Opc/OpcPlc/Boiler";
    const key = pathKey(["Objects", name]);
    expect(key.split(OPCUA_PATH_SEPARATOR)).toHaveLength(2);
    expect(pathSegments(key)).toEqual(["Objects", name]);
  });

  test("a bare percent sign round-trips", () => {
    expect(unescapeSegment(escapeSegment("100%"))).toBe("100%");
  });

  test("an empty name and an empty key are each themselves", () => {
    expect(unescapeSegment(escapeSegment(""))).toBe("");
    expect(pathSegments("")).toEqual([]);
  });

  /** A deep path, deeper than the fixture's measured depth of 7, with the awkward name in the middle. */
  test("a nine-deep path with an awkward segment round-trips whole", () => {
    const segments = ["Objects", "OpcPlc", MEASURED_NAMES[0], "Telemetry", "Slow", "a/b", "c%d", "x", "y"];
    expect(pathSegments(pathKey(segments))).toEqual(segments);
  });
});

describe("the node id grammar (Part 6 5.1.12)", () => {
  /** Ids measured on the four stacks, in every identifier and namespace form. */
  test("accepts every form a measured server answered with", () => {
    for (const id of [
      "i=85",
      "i=2277",
      "ns=3;s=SlowUInt1",
      "ns=2;i=6078",
      "ns=4;i=5",
      "ns=8;i=271426160",
      "nsu=http://microsoft.com/Opc/OpcPlc/;s=SlowUInt1",
      "g=09087e75-8e5e-499b-954f-f2a8624db28a",
      "b=M/RbKBsRVkePCePcx24oRA==",
      "ns=1;s=myUintValue",
    ]) {
      if (!isNodeId(id)) throw new Error(`refused a node id a real server answered with: ${id}`);
    }
  });

  test("refuses what the grammar refuses", () => {
    for (const bad of ["", "Objects", "ns=3", "x=1", "ns=;s=Foo", "i=", "i=abc", "g=not-a-guid"]) {
      if (isNodeId(bad)) throw new Error(`accepted a node id the grammar refuses: ${bad}`);
    }
  });

  /**
   * THE RANGES AND THE GUID SHAPE, each one measured against the real server with the pattern that
   * only counted characters, and each one a sentence the reader could not act on:
   *  - 36 dashes passed as a GUID and the library threw with an EMPTY message, so the answer on
   *    screen was "A read failed: " and nothing else;
   *  - `ns=70000` passed and the library answered its own `NodeId: invalid namespace value`.
   * A namespace index is a UInt16 and a numeric identifier a UInt32 (Part 6 5.2.2.9), so the ends
   * belong in the grammar, where the refusal is a sentence written here.
   */
  test("the numeric ranges end where the encoding ends", () => {
    for (const id of ["i=0", "i=4294967295", "ns=0;i=1", "ns=65535;i=1", "nsu=http://x/;ns=65535;i=1"]) {
      if (!isNodeId(id)) throw new Error(`refused a node id inside the encoding's range: ${id}`);
    }
    for (const bad of ["i=4294967296", "i=99999999999", "ns=65536;i=1", "ns=70000;s=x", "nsu=http://x/;ns=65536;i=1"]) {
      if (isNodeId(bad)) throw new Error(`accepted a node id outside the encoding's range: ${bad}`);
    }
  });

  test("a GUID is the five-group form, not 36 characters of anything", () => {
    expect(isNodeId("g=72962B91-FA75-4AE6-8D28-B404DC7DAF63")).toBe(true);
    expect(isNodeId("g=09087e75-8e5e-499b-954f-f2a8624db28a")).toBe(true);
    for (const bad of [
      "g=------------------------------------",
      "g=72962B91FA754AE68D28B404DC7DAF6333333",
      "g=72962B91-FA75-4AE6-8D28-B404DC7DAF6",
      "g=72962B91-FA75-4AE6-8D28-B404DC7DAF63Z",
    ]) {
      if (isNodeId(bad)) throw new Error(`accepted a GUID the encoding refuses: ${bad}`);
    }
  });

  /** A String identifier is any non-empty string, and a newline is part of "any". */
  test("a string identifier may hold a newline, which the grammar once refused", () => {
    expect(isNodeId("s=line one\nline two")).toBe(true);
    expect(isNodeId("ns=3;s=\n")).toBe(true);
  });
});
