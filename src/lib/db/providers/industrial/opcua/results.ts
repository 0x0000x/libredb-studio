/**
 * Rows, so the result of a read is a table like any other (#1677 section 2).
 *
 * TABULAR BECAUSE THE DUCKDB LAYER QUERIES IT. A read answers one row per node and a history read
 * one row per value, with the same columns in the same order every time, so a reader can group and
 * filter the answer in Studio as they would any other result.
 *
 * A BAD STATUS IS A ROW, NOT A DROPPED ROW. A Read answers Good with a value, Uncertain with a
 * value, or Bad with none, per node. Dropping the Bad ones would tell a reader the node does not
 * exist, which is a different and wrong statement; measured, Milo answers BadNotReadable for
 * `CurrentSessionCount` even to an authenticated session, and that is information.
 */
import type { QueryResult } from "@/lib/types";
import type { OpcuaDataValue, OpcuaHistoryValue, OpcuaNodeId } from "./client";
import { valueCell } from "./objects";

/** The columns a value read answers with, in this order. */
export const OPCUA_VALUE_COLUMNS = Object.freeze([
  "nodeId",
  "value",
  "shape",
  "statusCode",
  "sourceTimestamp",
  "serverTimestamp",
] as const);

/** The columns a history read answers with. */
export const OPCUA_HISTORY_COLUMNS = Object.freeze([
  "nodeId",
  "value",
  "shape",
  "statusCode",
  "sourceTimestamp",
  "serverTimestamp",
] as const);

/**
 * One row per node named in the request, in the order they were named.
 *
 * The order is the request's, not the server's, because a reader who wrote three node ids expects
 * three rows in that order; a server is free to answer in any order and `node-opcua` preserves the
 * request's, which is asserted rather than assumed.
 */
export function valueRows(nodeIds: readonly OpcuaNodeId[], values: readonly OpcuaDataValue[]): QueryResult {
  const byId = new Map<OpcuaNodeId, OpcuaDataValue>();
  for (const value of values) byId.set(value.nodeId, value);

  const rows = nodeIds.map((nodeId) => {
    const value = byId.get(nodeId);
    const cell = value === undefined ? undefined : valueCell(value.value);
    return {
      nodeId,
      value: cell?.text ?? null,
      shape: cell?.shape ?? null,
      // A node the server did not answer for at all is reported as such rather than as empty.
      statusCode: value?.statusCode ?? "BadNoAnswer",
      sourceTimestamp: value?.sourceTimestamp ?? null,
      serverTimestamp: value?.serverTimestamp ?? null,
    };
  });

  return {
    fields: [...OPCUA_VALUE_COLUMNS],
    rows,
    rowCount: rows.length,
    executionTime: 0,
  };
}

/** One row per historical value, in the order the server returned them. */
export function historyRows(nodeId: OpcuaNodeId, values: readonly OpcuaHistoryValue[]): QueryResult {
  const rows = values.map((entry) => {
    const cell = valueCell(entry.value);
    return {
      nodeId,
      value: cell.text,
      shape: cell.shape,
      statusCode: entry.statusCode,
      sourceTimestamp: entry.sourceTimestamp ?? null,
      serverTimestamp: entry.serverTimestamp ?? null,
    };
  });

  return {
    fields: [...OPCUA_HISTORY_COLUMNS],
    rows,
    rowCount: rows.length,
    executionTime: 0,
  };
}

/**
 * An empty result with its columns, for a read that answered nothing.
 *
 * The columns are present even with no rows, because a grid with no columns reads as "this did not
 * run" where a grid with columns and no rows reads as "this ran and found nothing", and those are
 * different facts. Measured why it matters: asyncua answers a history read of a non-historized node
 * with Good and zero values, so "ran and found nothing" is a real answer a reader will see.
 */
export function emptyResult(columns: readonly string[]): QueryResult {
  return { fields: [...columns], rows: [], rowCount: 0, executionTime: 0 };
}
