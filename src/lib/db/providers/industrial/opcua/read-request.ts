/**
 * The editor text is one JSON read request (#1677 section 2), in the Kafka style.
 *
 * TWO SHAPES AND NOTHING ELSE. `{ "read": [...] }` answers one row per node with its current
 * DataValue; `{ "history": "...", "start": "...", "end": "...", "limit": n }` answers raw history,
 * one row per value. A key this parser does not know is a mistake the caller should hear about,
 * because ignoring it would run a different read from the one they wrote.
 *
 * EVERY REFUSAL HAPPENS HERE, BEFORE A REQUEST. A node id that the grammar refuses, a window whose
 * end is before its start, a limit past the cap: each is a sentence the reader can act on, where a
 * status code from the server is not. Measured why it matters: asyncua answers a history read of a
 * node that keeps no history with Good and zero values, so a reader who mistyped would see an empty
 * table and conclude the node is empty rather than wrong.
 */
import { DatabaseConfigError } from "@/lib/db/errors";
import { OPCUA_HISTORY_VALUE_CAP, OPCUA_TYPE } from "./constants";
import { isNodeId, type OpcuaNodeId } from "./node-id";

/** A read of current values: one row per node. */
export interface OpcuaValueRead {
  readonly kind: "read";
  readonly nodeIds: readonly OpcuaNodeId[];
}

/** A read of raw history for one node: one row per value. */
export interface OpcuaHistoryRead {
  readonly kind: "history";
  readonly nodeId: OpcuaNodeId;
  readonly start?: Date;
  readonly end?: Date;
  readonly limit: number;
}

export type OpcuaReadRequest = OpcuaValueRead | OpcuaHistoryRead;

const READ_KEYS: ReadonlySet<string> = new Set(["read"]);
const HISTORY_KEYS: ReadonlySet<string> = new Set(["history", "start", "end", "limit"]);

function refuse(message: string): never {
  throw new DatabaseConfigError(message, OPCUA_TYPE);
}

/** One instant from the text a reader typed, or a refusal naming what was wrong with it. */
function readInstant(raw: unknown, field: string): Date {
  if (typeof raw !== "string") refuse(`${field} is an ISO 8601 instant in a string, such as 2026-10-12T00:00:00Z.`);
  const at = new Date(raw);
  if (Number.isNaN(at.getTime())) refuse(`${field} is not an instant this reads: ${raw}`);
  return at;
}

/**
 * The request one editor text names.
 *
 * Empty text never reaches here: the provider refuses it with its own sentence, because "nothing to
 * run" is a different thing from "this is not a request".
 */
export function readRequest(text: string): OpcuaReadRequest {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    refuse(`A read request is JSON, and this did not parse: ${(error as Error).message}`);
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed))
    refuse('A read request is a JSON object: { "read": ["ns=3;s=Foo"] } or { "history": "ns=3;s=Foo" }.');

  const body = parsed as Record<string, unknown>;
  const keys = Object.keys(body);
  if (keys.length === 0) refuse('A read request names what to read: { "read": [...] } or { "history": "..." }.');

  const isHistory = "history" in body;
  const isRead = "read" in body;
  if (isHistory && isRead) refuse("A request reads current values or history, not both. Run them as two requests.");

  if (isRead) {
    for (const key of keys) if (!READ_KEYS.has(key)) refuse(`A value read takes "read" only, and this names "${key}".`);
    const list = body.read;
    if (!Array.isArray(list) || list.length === 0) refuse('"read" is a non-empty array of node ids.');
    const nodeIds: string[] = [];
    for (const [at, candidate] of list.entries()) {
      if (typeof candidate !== "string") refuse(`"read" holds node ids as strings, and item ${at} is not one.`);
      if (!isNodeId(candidate))
        refuse(
          `"read" item ${at} is not a node id: ${candidate}. A node id reads i=, s=, g= or b=, with ns= or nsu= before it, such as ns=3;s=SlowUInt1.`,
        );
      nodeIds.push(candidate);
    }
    return { kind: "read", nodeIds };
  }

  if (!isHistory) refuse(`A request names "read" or "history", and this names "${keys[0]}".`);
  for (const key of keys)
    if (!HISTORY_KEYS.has(key))
      refuse(`A history read takes "history", "start", "end" and "limit", and this names "${key}".`);

  const nodeId = body.history;
  if (typeof nodeId !== "string" || !isNodeId(nodeId))
    refuse('"history" is one node id, such as "ns=3;s=HistorianInt32".');

  const start = body.start === undefined ? undefined : readInstant(body.start, '"start"');
  const end = body.end === undefined ? undefined : readInstant(body.end, '"end"');
  if (start !== undefined && end !== undefined && end.getTime() < start.getTime())
    refuse('"end" is before "start", so the window holds no time at all.');

  let limit = OPCUA_HISTORY_VALUE_CAP;
  if (body.limit !== undefined) {
    if (typeof body.limit !== "number" || !Number.isInteger(body.limit) || body.limit < 1)
      refuse('"limit" is a whole number of values, one or more.');
    if (body.limit > OPCUA_HISTORY_VALUE_CAP)
      refuse(
        `"limit" is at most ${OPCUA_HISTORY_VALUE_CAP.toLocaleString("en-US")} values. A server appends history while it is read: the same window measured 11, then 12, then 13 pages over nineteen minutes, so a read without a cap has no end.`,
      );
    limit = body.limit;
  }
  return {
    kind: "history",
    nodeId,
    ...(start === undefined ? {} : { start }),
    ...(end === undefined ? {} : { end }),
    limit,
  };
}
