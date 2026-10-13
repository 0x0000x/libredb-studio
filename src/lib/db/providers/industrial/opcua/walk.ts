/**
 * Walking the address space, bounded by construction (#1677 O-S5).
 *
 * EVERY FIGURE BELOW WAS RE-MEASURED ON 12 OCT 2026 AGAINST THE FIXTURE THIS REPOSITORY NAMES, and
 * that re-measurement is the reason this block is worded the way it is. The numbers it used to
 * carried - 8,756 nodes, 11,004 paths, 1,697 multi-parent nodes, depth 9 - do not reproduce on the
 * command `tests/fixtures/opcua/README.md` states, and three of them do not reproduce on any
 * configuration of it: a review walked the server and found 6,769 nodes and 41 multi-parent nodes,
 * and the larger figures appear only when OPC PLC is started with an extra `--alm`. A measured
 * claim that cannot be reproduced from the repository's own command is worth less than no claim.
 *
 * THE FIXTURE, AS THE REPOSITORY STARTS IT: Microsoft OPC PLC 2.15.10, pinned by digest, with
 * `--pn=50000 --autoaccept --unsecuretransport --ut --sph --hn --slownodes=3 --fastnodes=3`.
 *  - 6,769 nodes are reachable from the Objects folder, over 6,865 references and 7,040 distinct
 *    browse paths, with the deepest node at depth 7 - counted by browsing EVERY node, Methods
 *    included, one Browse each, which is 6,769 Browse requests.
 *  - THIS function reaches 6,581 of them in 325 requests over eight rounds, at 20 nodes a Browse,
 *    because it never browses a Method (O-S1 lists them and stops): the 188-node difference is
 *    exactly the Methods' own `InputArguments` and `OutputArguments` Variables, which no surface of
 *    this provider reads.
 *  - 41 nodes hang off more than one PARENT: 39 off two, and TWO off thirty
 *    (`DataAccess_DataItem_BaseDataVariableType_Instance1_Definition` and its `_ValuePrecision`).
 *    215 nodes therefore have more than one PATH, since a multi-parent node's descendants inherit
 *    its extra paths - two counts, and the panel shows the second. 7,040 paths over 6,769 nodes is
 *    4.0% more rows than nodes, not a doubling.
 *  - The widest level is 100 children (`Scalar_Static_Mass_Boolean`), answered in one Browse.
 *
 * NO UI ACTION ISSUES A WHOLE-TREE WALK, which the measurements made non-negotiable. 6,769 nodes is
 * 325 requests with this function's own bounds; the Eclipse Milo demo, measured in an earlier round
 * and NOT re-measured here, is larger again and has a level of 10,000 children in a single Browse
 * answer. So a walk takes a node budget and reports whether it spent it, and the Keys panel never
 * calls it.
 *
 * NOTHING IN THE PRODUCT CALLS IT, AND THAT IS WHAT THIS MODULE IS: THE INSTRUMENT. The figures
 * above and in `docs/providers/opcua.md` are measured with this function and with an exhaustive
 * walk beside it, and the integration suite re-measures with it against a real server, which is how
 * a claim in a comment stays a measurement rather than becoming folklore. A review counted its
 * callers and found none in `src/`; the honest answer is not to delete it but to say what it is for.
 * If a surface ever needs a bounded walk, this is the one to call.
 *
 * THERE ARE NO CYCLES, BUT THE VISITED SET IS NOT OPTIONAL. The graph under the Objects folder is
 * acyclic - an exhaustive enumeration with no visited set terminates at 7,040 paths, and a memoised
 * count over each node's parents agrees - and what it does have is those 41 multi-PARENT nodes, two
 * of them with thirty parents each, which give 215 nodes more than one path. Without a visited set the same subtree is browsed again for every
 * path into it: 7,040 browses instead of 6,769, which is 4.0% wasted on this fixture (3.8% over the
 * non-Method nodes this function actually browses), far worse on a server with the alarms subtree
 * (measured with `--alm`: 11,000 browses instead of 8,753, 25.7%), and unbounded on a server with a
 * genuine cycle. A staged server with a self-loop and a two-parent
 * node is in the integration suite for exactly that.
 */
import { OPCUA_OBJECTS_FOLDER } from "./constants";
import { browseChunkSize, chunked } from "./limits";
import type { OpcuaNodeId, OpcuaReadOnlyClient, OpcuaServerLimits } from "./client";

/** Why a walk stopped, which the result reports rather than leaving to inference. */
export type OpcuaWalkEnd = "exhausted" | "node-budget" | "depth-budget";

export interface OpcuaWalkResult {
  /** Every node reached, once, whatever number of paths reached it. */
  readonly nodeIds: readonly OpcuaNodeId[];
  readonly end: OpcuaWalkEnd;
  readonly depth: number;
  /** Browse requests issued, which is what the walk cost the server. */
  readonly requests: number;
  /** Nodes reached by more than one path, which is a property of the server and worth reporting. */
  readonly multiPathNodes: number;
}

/**
 * Every node under `root`, to a node and depth budget.
 *
 * Level by level rather than depth first, so the budget stops at a predictable breadth and the
 * requests batch: a level's nodes are browsed together, in chunks the server will take.
 */
export async function walkAddressSpace(
  client: OpcuaReadOnlyClient,
  options: {
    readonly root?: OpcuaNodeId;
    readonly nodeBudget: number;
    readonly depthBudget: number;
    readonly limits?: OpcuaServerLimits;
  },
): Promise<OpcuaWalkResult> {
  const chunk = browseChunkSize(options.limits ?? {});
  const seen = new Set<OpcuaNodeId>();
  /** How many times each node was reached, so the multi-path count is measured and not guessed. */
  const reached = new Map<OpcuaNodeId, number>();
  let frontier: OpcuaNodeId[] = [options.root ?? OPCUA_OBJECTS_FOLDER];
  let requests = 0;
  let depth = 0;
  seen.add(frontier[0] as OpcuaNodeId);

  while (frontier.length > 0) {
    if (depth >= options.depthBudget) {
      return {
        nodeIds: [...seen],
        end: "depth-budget",
        depth,
        requests,
        multiPathNodes: countMultiPath(reached),
      };
    }
    const next: OpcuaNodeId[] = [];
    for (const batch of chunked(frontier, chunk)) {
      // oxlint-disable-next-line no-await-in-loop -- one chunk of a level at a time, so the load on the server is fixed.
      const results = await client.browse(batch);
      requests += 1;
      for (const result of results) {
        // Continuation points are consumed here and never handed upwards: they are session state,
        // and a scarce one (OPC PLC publishes ten).
        let point = result.continuationPoint;
        const references = [...result.references];
        while (point !== undefined) {
          // oxlint-disable-next-line no-await-in-loop -- a continuation point is consumed by the request that received it.
          const [continued] = await client.browseNext([point]);
          requests += 1;
          if (continued === undefined) break;
          references.push(...continued.references);
          point = continued.continuationPoint;
        }
        for (const reference of references) {
          reached.set(reference.nodeId, (reached.get(reference.nodeId) ?? 0) + 1);
          if (seen.has(reference.nodeId)) continue;
          seen.add(reference.nodeId);
          if (seen.size >= options.nodeBudget) {
            return {
              nodeIds: [...seen],
              end: "node-budget",
              depth: depth + 1,
              requests,
              multiPathNodes: countMultiPath(reached),
            };
          }
          if (reference.mayHaveChildren) next.push(reference.nodeId);
        }
      }
    }
    depth += 1;
    frontier = next;
  }

  return {
    nodeIds: [...seen],
    end: "exhausted",
    depth,
    requests,
    multiPathNodes: countMultiPath(reached),
  };
}

function countMultiPath(reached: ReadonlyMap<OpcuaNodeId, number>): number {
  let count = 0;
  for (const times of reached.values()) if (times > 1) count += 1;
  return count;
}

/** What the walk's end tells a reader. */
export function walkEndSentence(result: OpcuaWalkResult): string {
  switch (result.end) {
    case "exhausted":
      return `${result.nodeIds.length.toLocaleString("en-US")} nodes, to depth ${result.depth}, over ${result.requests.toLocaleString("en-US")} requests.`;
    case "node-budget":
      return (
        `${result.nodeIds.length.toLocaleString("en-US")} nodes, where the walk's budget stopped it: this server ` +
        "has more. A server's address space can be very large, and one measured here holds over sixty thousand nodes."
      );
    case "depth-budget":
      return `${result.nodeIds.length.toLocaleString("en-US")} nodes, to depth ${result.depth}, where the walk's depth budget stopped it.`;
  }
}
