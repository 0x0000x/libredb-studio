/**
 * The bounded walk, and the shapes a real address space has (#1677 O-S5, section 6 item 2).
 *
 * THE GRAPH IS ACYCLIC AND STILL NEEDS A VISITED SET. Re-measured on the documented fixture: no
 * cycles, but 41 of its 6,769 nodes hang off more than one PARENT - 39 off two and two of them off
 * thirty - which gives 215 nodes more than one PATH. Without a visited set the same subtree is browsed again for every path into
 * it. The cycle case is staged below anyway, because a server that has one is allowed to.
 */
import { describe, expect, test } from "bun:test";
import type {
  OpcuaBrowseResult,
  OpcuaNodeId,
  OpcuaReadOnlyClient,
  OpcuaReference,
} from "@/lib/db/providers/industrial/opcua/client";
import { walkAddressSpace, walkEndSentence } from "@/lib/db/providers/industrial/opcua/walk";

function child(nodeId: string, mayHaveChildren = false): OpcuaReference {
  return {
    nodeId,
    browseName: nodeId,
    displayName: nodeId,
    nodeClass: mayHaveChildren ? "Object" : "Variable",
    referenceTypeId: "i=47",
    mayHaveChildren,
  };
}

/** A server whose address space is a map from node id to its children, and that counts its requests. */
function serverFrom(
  tree: Readonly<Record<string, readonly OpcuaReference[]>>,
  record: { requests: number },
): OpcuaReadOnlyClient {
  return {
    endpoints: async () => [],
    limits: async () => ({}),
    browse: async (nodeIds: readonly OpcuaNodeId[]) => {
      record.requests += 1;
      return nodeIds.map<OpcuaBrowseResult>((nodeId) => ({
        nodeId,
        references: tree[nodeId] ?? [],
        statusCode: "Good",
      }));
    },
    browseNext: async () => [],
    releaseBrowseContinuationPoints: async () => {},
    read: async () => [],
    historyRead: async () => ({ nodeId: "", values: [], statusCode: "Good" }),
    historyReadNext: async () => ({
      nodeId: "",
      values: [],
      statusCode: "Good",
    }),
    releaseHistoryContinuationPoint: async () => {},
    close: async () => {},
  };
}

describe("a walk that finishes", () => {
  test("every node is reached once, and the depth is reported", async () => {
    const record = { requests: 0 };
    const client = serverFrom(
      {
        "i=85": [child("A", true), child("B", true)],
        A: [child("A1"), child("A2")],
        B: [child("B1")],
      },
      record,
    );
    const result = await walkAddressSpace(client, {
      nodeBudget: 1_000,
      depthBudget: 20,
    });
    expect(result.end).toBe("exhausted");
    expect(new Set(result.nodeIds).size).toBe(result.nodeIds.length);
    expect(result.nodeIds).toHaveLength(6);
    expect(result.depth).toBe(2);
  });
});

describe("a node reachable by more than one path", () => {
  /** The measured shape: one node under two parents. It is browsed once and counted once. */
  test("it is visited once and counted as multi-path", async () => {
    const record = { requests: 0 };
    const shared = child("Shared", true);
    const client = serverFrom(
      {
        "i=85": [child("P1", true), child("P2", true)],
        P1: [shared],
        P2: [shared],
        Shared: [child("Leaf")],
      },
      record,
    );
    const result = await walkAddressSpace(client, {
      nodeBudget: 1_000,
      depthBudget: 20,
    });
    expect(result.nodeIds.filter((id) => id === "Shared")).toHaveLength(1);
    expect(result.multiPathNodes).toBe(1);
    // And the leaf under it was browsed once, not twice.
    expect(result.nodeIds).toContain("Leaf");
  });
});

describe("a reference cycle", () => {
  /**
   * The fixture has none, but a server may. Without the visited set this walk would not return at
   * all, which is why the case is staged rather than reasoned about.
   */
  test("a two-node cycle terminates", async () => {
    const record = { requests: 0 };
    const client = serverFrom(
      {
        "i=85": [child("X", true)],
        X: [child("Y", true)],
        Y: [child("X", true)],
      },
      record,
    );
    const result = await walkAddressSpace(client, {
      nodeBudget: 1_000,
      depthBudget: 50,
    });
    expect(result.end).toBe("exhausted");
    expect(new Set(result.nodeIds).size).toBe(result.nodeIds.length);
  });

  test("a self loop terminates", async () => {
    const record = { requests: 0 };
    const client = serverFrom({ "i=85": [child("S", true)], S: [child("S", true)] }, record);
    const result = await walkAddressSpace(client, {
      nodeBudget: 1_000,
      depthBudget: 50,
    });
    expect(result.end).toBe("exhausted");
    expect(result.nodeIds).toHaveLength(2);
  });
});

describe("the budgets, which is what makes a walk safe on a large server", () => {
  /** Milo's Demo.Mass.Flat answers 10,000 children in one level, so breadth needs a bound too. */
  test("a node budget stops a wide level and says so", async () => {
    const record = { requests: 0 };
    const wide = Array.from({ length: 10_000 }, (_, at) => child(`N${at}`));
    const client = serverFrom({ "i=85": wide }, record);
    const result = await walkAddressSpace(client, {
      nodeBudget: 500,
      depthBudget: 20,
    });
    expect(result.end).toBe("node-budget");
    expect(result.nodeIds.length).toBeLessThanOrEqual(500);
  });

  test("a depth budget stops a deep tree and says so", async () => {
    const record = { requests: 0 };
    const deep: Record<string, readonly OpcuaReference[]> = {
      "i=85": [child("D0", true)],
    };
    for (let at = 0; at < 50; at += 1) deep[`D${at}`] = [child(`D${at + 1}`, true)];
    const client = serverFrom(deep, record);
    const result = await walkAddressSpace(client, {
      nodeBudget: 10_000,
      depthBudget: 5,
    });
    expect(result.end).toBe("depth-budget");
    expect(result.depth).toBe(5);
  });

  /** The server's own Browse limit is honoured where it is smaller than ours. */
  test("a server publishing a small Browse limit gets small batches", async () => {
    const record = { requests: 0 };
    const nodes = Array.from({ length: 12 }, (_, at) => child(`F${at}`, true));
    const tree: Record<string, readonly OpcuaReference[]> = { "i=85": nodes };
    for (const node of nodes) tree[node.nodeId] = [];
    const client = serverFrom(tree, record);
    const withLimit = { requests: 0 };
    await walkAddressSpace(serverFrom(tree, withLimit), {
      nodeBudget: 1_000,
      depthBudget: 20,
      limits: { maxNodesPerBrowse: 3 },
    });
    // 1 request for the root, then 12 nodes in batches of 3.
    expect(withLimit.requests).toBe(1 + 4);
    expect(record.requests).toBe(0);
  });
});

describe("what the reader is told", () => {
  test("a finished walk reports its size, depth and cost", () => {
    expect(
      walkEndSentence({
        nodeIds: new Array(6_581).fill("x"),
        end: "exhausted",
        depth: 8,
        requests: 325,
        multiPathNodes: 40,
      }),
    ).toMatch(/6,581 nodes, to depth 8, over 325 requests/);
  });

  test("a walk stopped by its budget says the server has more", () => {
    expect(
      walkEndSentence({
        nodeIds: new Array(500).fill("x"),
        end: "node-budget",
        depth: 2,
        requests: 30,
        multiPathNodes: 0,
      }),
    ).toMatch(/this server\s+has more|sixty thousand/);
  });
});

/**
 * A LEVEL THAT PAGES IS STILL ONE LEVEL. A server may answer a node's children across several
 * answers, handing back a continuation point each time; the walk consumes them inside the request
 * it received them in, because they are session state a plant's own clients share.
 */
describe("a level that the server pages", () => {
  /** A server that answers one child per Browse and pages the rest. */
  function pagingServer(children: readonly string[], record: { requests: number }): OpcuaReadOnlyClient {
    let served = 0;
    const page = (nodeId: string): OpcuaBrowseResult => {
      const next = children[served];
      served += 1;
      return {
        nodeId,
        references: next === undefined ? [] : [child(next)],
        ...(served < children.length ? { continuationPoint: new Uint8Array([served]) } : {}),
        statusCode: "Good",
      };
    };
    return {
      endpoints: async () => [],
      limits: async () => ({}),
      browse: async (nodeIds) => {
        record.requests += 1;
        return nodeIds.map((nodeId) => page(nodeId));
      },
      browseNext: async () => {
        record.requests += 1;
        return [page("i=85")];
      },
      releaseBrowseContinuationPoints: async () => {},
      read: async () => [],
      historyRead: async () => ({ nodeId: "", values: [], statusCode: "Good" }),
      historyReadNext: async () => ({
        nodeId: "",
        values: [],
        statusCode: "Good",
      }),
      releaseHistoryContinuationPoint: async () => {},
      close: async () => {},
    };
  }

  test("every page of a level is followed, and each continuation costs a request", async () => {
    const record = { requests: 0 };
    const client = pagingServer(["A", "B", "C"], record);
    const result = await walkAddressSpace(client, {
      nodeBudget: 100,
      depthBudget: 3,
    });
    expect(result.nodeIds).toContain("A");
    expect(result.nodeIds).toContain("B");
    expect(result.nodeIds).toContain("C");
    // One Browse plus one BrowseNext per further page, counted as what the walk cost the server.
    expect(result.requests).toBeGreaterThanOrEqual(3);
  });

  /** A server that pages and then answers nothing at all leaves the walk where it was, not hung. */
  test("a continuation that answers nothing ends the level", async () => {
    const record = { requests: 0 };
    const client: OpcuaReadOnlyClient = {
      ...pagingServer(["A"], record),
      browse: async (nodeIds) =>
        nodeIds.map((nodeId) => ({
          nodeId,
          references: [child("A")],
          continuationPoint: new Uint8Array([1]),
          statusCode: "Good",
        })),
      browseNext: async () => [],
    };
    const result = await walkAddressSpace(client, {
      nodeBudget: 100,
      depthBudget: 2,
    });
    expect(result.nodeIds).toContain("A");
  });
});

describe("the two budgets each have their own sentence", () => {
  test("a depth budget says so, with the depth it reached", () => {
    expect(
      walkEndSentence({
        nodeIds: new Array(42).fill("x"),
        end: "depth-budget",
        depth: 3,
        requests: 7,
        multiPathNodes: 0,
      }),
    ).toMatch(/42 nodes, to depth 3, where the walk's depth budget stopped it/);
  });

  test("a walk stopped by its depth reports that end, not exhaustion", async () => {
    const record = { requests: 0 };
    const client = serverFrom(
      {
        "i=85": [child("A", true)],
        A: [child("B", true)],
        B: [child("C", true)],
        C: [child("D")],
      },
      record,
    );
    const result = await walkAddressSpace(client, {
      nodeBudget: 1_000,
      depthBudget: 2,
    });
    expect(result.end).toBe("depth-budget");
    expect(result.depth).toBe(2);
  });
});
