/**
 * Chunk sizes, against what four real servers publish and enforce (#1677 O-S5).
 *
 * THE SERVER'S PUBLISHED LIMIT IS NOT A BOUND, and every row below is a server measured on
 * 2026-10-11/12 rather than a case invented here. Part 5 6.3.11 reads "Any operational limits
 * Property that is provided shall have a non zero value", so a reader of the specification alone
 * would trust a published limit. Three of the four stacks break that sentence.
 */
import { describe, expect, test } from "bun:test";
import { browseChunkSize, chunked, readChunkSize, statedLimit } from "@/lib/db/providers/industrial/opcua/limits";
import {
  OPCUA_MAX_NODES_PER_BROWSE,
  OPCUA_MAX_NODES_PER_READ,
  OPCUA_READ_ATTRIBUTES,
} from "@/lib/db/providers/industrial/opcua/constants";

describe("what counts as a published limit", () => {
  /**
   * The three ways a server says nothing, which mean the same thing. open62541 publishes eight
   * limits that all read Good with the value 0; asyncua publishes twelve and reads six as Good with
   * an empty Variant; OPC PLC publishes MaxSessions as 0.
   */
  test("absent, empty and zero all mean no limit stated", () => {
    expect(statedLimit(undefined)).toBeUndefined();
    expect(statedLimit(0)).toBeUndefined();
    expect(statedLimit(-1)).toBeUndefined();
    expect(statedLimit(1.5)).toBeUndefined();
  });

  test("a real limit is a whole number above zero", () => {
    expect(statedLimit(1)).toBe(1);
    expect(statedLimit(10)).toBe(10);
    expect(statedLimit(2_500)).toBe(2_500);
  });
});

/**
 * A STATED LIMIT COUNTS OPERATIONS AND ONE NODE COSTS TEN. A Read takes (node, attribute) pairs,
 * and this provider asks for the value and the nine attributes beside it in one request, so a
 * server publishing 2,500 will take 250 nodes and not 2,500. The division is what these rows pin.
 */
describe("the Read chunk, per measured server", () => {
  const PER_NODE = OPCUA_READ_ATTRIBUTES.length;
  const SERVERS: readonly [string, number | undefined, number][] = [
    // [server, what it publishes, what we send]
    ["OPC PLC 2.15.10, publishes 2500 and enforces it", 2_500, OPCUA_MAX_NODES_PER_READ],
    ["Milo demo, publishes 10000 and enforces it", 10_000, OPCUA_MAX_NODES_PER_READ],
    ["asyncua 2.1.0, publishes 10000 and served 10500", 10_000, OPCUA_MAX_NODES_PER_READ],
    ["open62541 1.5.9, publishes 0 and served 20000", 0, OPCUA_MAX_NODES_PER_READ],
    ["a server publishing nothing at all", undefined, OPCUA_MAX_NODES_PER_READ],
    // Smaller than ours once the attributes are counted: 900 operations is 90 nodes.
    ["a server publishing 900, which is 90 nodes", 900, 90],
    ["OPC PLC's 2,500 operations, which is 250 nodes and above our own 200", 2_500, OPCUA_MAX_NODES_PER_READ],
    ["a server publishing 10, which is one node", 10, 1],
    ["a server publishing 1, which cannot hold one node's attributes and is read as one node", 1, 1],
  ];

  for (const [server, published, expected] of SERVERS) {
    test(server, () => {
      expect(readChunkSize(published === undefined ? {} : { maxNodesPerRead: published })).toBe(expected);
    });
  }

  /** Ours is never exceeded, whatever the server says, because two of four do not enforce theirs. */
  test("a server's larger limit never raises ours", () => {
    for (const published of [2_001, 10_000, 1_000_000]) {
      expect(readChunkSize({ maxNodesPerRead: published })).toBe(OPCUA_MAX_NODES_PER_READ);
    }
  });
});

describe("the Browse chunk, which is not the Read chunk", () => {
  /**
   * ONE SIZE PER SERVER WOULD BE WRONG FOR ONE OF THE TWO SERVICES. Milo publishes 10,000 for Read
   * and 250 for Browse on the same server, a forty-fold difference, because a Browse answer carries
   * every child of every node asked about. Measured: a Browse of 100 Milo nodes returned 2,162,655
   * bytes, over the client's own 2 MiB ceiling, and the channel then closed for good.
   */
  test("it reads the Browse limit and not the Read limit", () => {
    const milo = { maxNodesPerRead: 10_000, maxNodesPerBrowse: 250 };
    expect(readChunkSize(milo)).toBe(OPCUA_MAX_NODES_PER_READ);
    // The control for the division: 10,000 operations is 1,111 nodes, so ours is the smaller one.
    expect(Math.floor(10_000 / OPCUA_READ_ATTRIBUTES.length)).toBeGreaterThan(OPCUA_MAX_NODES_PER_READ);
    expect(browseChunkSize(milo)).toBe(OPCUA_MAX_NODES_PER_BROWSE);
  });

  test("a Browse limit smaller than ours wins", () => {
    expect(browseChunkSize({ maxNodesPerBrowse: 5 })).toBe(5);
  });

  test("the Browse chunk is smaller than the Read chunk, because its answer is unbounded", () => {
    expect(OPCUA_MAX_NODES_PER_BROWSE).toBeLessThan(OPCUA_MAX_NODES_PER_READ);
  });
});

describe("chunking", () => {
  test("a list splits in order with no empty chunk", () => {
    expect(chunked([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]]);
    expect(chunked([1, 2], 5)).toEqual([[1, 2]]);
    expect(chunked([], 3)).toEqual([]);
  });

  test("a chunk of zero is a mistake, not an infinite loop", () => {
    expect(() => chunked([1], 0)).toThrow();
  });

  /** The real case: 6,769 nodes at the Browse chunk, which is how a whole-tree walk is bounded. */
  test("the measured address space splits into the expected number of Browse requests", () => {
    const nodes = Array.from({ length: 6_769 }, (_, at) => at);
    const batches = chunked(nodes, OPCUA_MAX_NODES_PER_BROWSE);
    expect(batches).toHaveLength(Math.ceil(6_769 / OPCUA_MAX_NODES_PER_BROWSE));
    expect(batches.flat()).toHaveLength(6_769);
  });
});
