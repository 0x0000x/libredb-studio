/**
 * How many nodes one request asks for (#1677 O-S5).
 *
 * THE SERVER'S PUBLISHED LIMIT IS NOT A BOUND, and four stacks proved it four different ways.
 * Part 5 6.3.11 reads "Any operational limits Property that is provided shall have a non zero
 * value", so a reader of the specification alone would take a published limit as either real or
 * absent. Measured:
 *
 * | Server | MaxNodesPerRead | Enforced? |
 * |---|---|---|
 * | OPC PLC 2.15.10 | 2,500 | yes, 2,501 fails whole with BadTooManyOperations |
 * | Milo demo 1.1.0 | 10,000, and 250 for Browse on the same server | yes, both |
 * | asyncua 2.1.0 | 10,000 | NO: a 10,500-node Read returned 10,500 results |
 * | open62541 1.5.9 | published as 0 | NO: a 20,000-node Read returned 20,000 |
 *
 * So the chunk size is OURS first, and the server's only when the server's is smaller AND real. A
 * published limit counts as real only when the read was Good, the Variant was not empty, and the
 * value is above zero: open62541 publishes eight limits that all read Good with the value 0, and
 * asyncua publishes twelve of which six read Good with an empty Variant. `BadTooManyOperations` is
 * a status to map in the error table, never the safety net.
 *
 * AND THE TWO SERVICES NEED TWO SIZES. Milo publishes 10,000 for Read and 250 for Browse, a
 * forty-fold difference on one server, because a Browse answer carries every child of every node
 * asked about and a Read answer carries one value each. One size per server would be wrong for one
 * of the two.
 */
import { OPCUA_MAX_NODES_PER_BROWSE, OPCUA_MAX_NODES_PER_READ, OPCUA_READ_ATTRIBUTES } from "./constants";
import type { OpcuaServerLimits } from "./client";

/**
 * A limit the server stated, or undefined.
 *
 * The three ways a server says nothing are not distinguished, because they mean the same thing:
 * absent, an empty Variant, and zero. Anything at or below zero, or not an integer, is nothing.
 */
export function statedLimit(value: number | undefined): number | undefined {
  if (value === undefined) return undefined;
  if (!Number.isInteger(value) || value <= 0) return undefined;
  return value;
}

/**
 * How many NODES one Read asks for: ours, or the server's where the server's is smaller and real.
 *
 * THE SERVER'S LIMIT COUNTS OPERATIONS, NOT NODES, and one node costs
 * `OPCUA_READ_ATTRIBUTES.length` of them: a Read takes (node, attribute) pairs, and this provider
 * asks for the value and the nine attributes beside it in one request rather than in one each.
 * So a stated limit is divided before it is compared, which is the difference between 200 nodes and
 * 2,000 operations against OPC PLC's enforced 2,500 (measured: 2,501 fails whole with
 * BadTooManyOperations, so the margin is not decorative).
 */
export function readChunkSize(limits: OpcuaServerLimits): number {
  const stated = statedLimit(limits.maxNodesPerRead);
  if (stated === undefined) return OPCUA_MAX_NODES_PER_READ;
  // At least one node, whatever the server says: a limit below the attribute count would otherwise
  // make every read ask for nothing, and a server publishing such a limit cannot be read at all.
  const nodes = Math.max(1, Math.floor(stated / OPCUA_READ_ATTRIBUTES.length));
  return Math.min(OPCUA_MAX_NODES_PER_READ, nodes);
}

/** The same for Browse, read from the Browse limit, because the two differ on real servers. */
export function browseChunkSize(limits: OpcuaServerLimits): number {
  const stated = statedLimit(limits.maxNodesPerBrowse);
  return stated === undefined ? OPCUA_MAX_NODES_PER_BROWSE : Math.min(OPCUA_MAX_NODES_PER_BROWSE, stated);
}

/** The items of one list in chunks of `size`, in order, with no empty chunk. */
export function chunked<T>(items: readonly T[], size: number): T[][] {
  if (size < 1) throw new Error("a chunk holds at least one item");
  const out: T[][] = [];
  for (let at = 0; at < items.length; at += size) out.push(items.slice(at, at + size));
  return out;
}
