/**
 * Node ids and the path keys the Keys panel walks (#1677, Part 6 5.1.12).
 *
 * THE KEY IS AN ESCAPED BROWSE PATH, AND THAT IS A DECISION WITH A MEASUREMENT BEHIND IT. The Keys
 * panel addresses a level by a literal text prefix and derives folders from a separator inside the
 * key's own text (`KeyScanOptions.level`). A browse path is the natural fit, `Objects/OpcPlc/Slow`,
 * except that a BrowseName may contain the separator: measured on the issue's own first fixture,
 * four names contain `/`, four contain `:`, four contain `.`, and one contains every candidate at
 * once, `Special_"!§$%&/()=?`´\+~*'#_-:.;,<>|@^°€µ{[]}`. Microsoft ships it deliberately.
 *
 * So the separator is escaped rather than chosen. A segment's `/` becomes `%2F` and a literal `%`
 * becomes `%25`, which is percent-encoding's own rule and reversible. The panel still sees what it
 * expects, a literal prefix of a `/`-separated key, and the shared contract is not widened for one
 * engine: every other engine's walk keeps the meaning it had.
 *
 * WHAT THE PANEL WILL SHOW IS MORE ROWS THAN NODES, and that is correct rather than a defect. The
 * address space is a directed acyclic graph: re-measured on the documented fixture, 6,769 nodes
 * reachable from `i=85` have 7,040 distinct browse paths, because 41 of them hang off more than one
 * parent - 39 off two and two of them off thirty - which gives 215 nodes more than one path. Each path is a real path the server
 * publishes. A tree that showed a node once would have to pick a parent the server does not
 * privilege, and the node ids themselves cannot stand in for a path: 1,299 of those 6,769 cannot be
 * built from their own browse name at all.
 *
 * Measured: no cycles. An exhaustive enumeration with no visited set terminates at 7,040 paths, a
 * memoised count over each node's parents agrees, and the deepest node is at depth 7.
 *
 * A NODE ID WRITTEN WITH `ns=` IS NOT A NAME, AND NOTHING HERE REWRITES ONE. A namespace index is a
 * position in the server's own NamespaceArray, which Part 6 5.1.12 says "is supplied per Session",
 * and the `nsu=` form exists for places with no such context, "such as column in a database table".
 * Measured on OPC PLC: the documented fixture publishes EIGHT namespaces, and a second container
 * from the same digest with the same flags publishes the same eight in the same order - except
 * entry 1, `urn:OpcPlc:<hostname>`, which names that container alone. One more start-up flag moves
 * the rest: with `--alm` the array is nine entries and `AlarmsInstance` is inserted at index 6, so
 * `ReferenceTest` and `Diagnostics` shift from 6 and 7 to 7 and 8. So a request
 * a reader saves with `ns=` may address a different node after a restart; this provider sends what
 * the reader typed and converts nothing, which `docs/providers/opcua.md` section 12 states as the
 * limitation it is.
 */

/** A node id in its string form, as the server spells it and as a reader may type it. */
export type OpcuaNodeId = string;

/** The separator a path key uses, and the one the Keys panel is told about. */
export const OPCUA_PATH_SEPARATOR = "/";

/**
 * One segment of a path key, escaped so the separator inside a name cannot be mistaken for one.
 *
 * `%` first, or escaping the separator would then be escaped again and `%2F` would round-trip to
 * `%252F`. The pair is a bijection on every string, which `unescapeSegment` relies on.
 */
export function escapeSegment(name: string): string {
  return name.replaceAll("%", "%25").replaceAll(OPCUA_PATH_SEPARATOR, "%2F");
}

/** The inverse. A segment that was never escaped round-trips unchanged. */
export function unescapeSegment(segment: string): string {
  return segment.replaceAll("%2F", OPCUA_PATH_SEPARATOR).replaceAll("%25", "%");
}

/** A path key from its segments, each escaped: what one row in the Keys panel is named. */
export function pathKey(segments: readonly string[]): string {
  return segments.map(escapeSegment).join(OPCUA_PATH_SEPARATOR);
}

/**
 * The segments of a path key, unescaped: what a level request has to resolve against the server.
 *
 * A TRAILING SEPARATOR NAMES THE LEVEL, NOT A NAMELESS CHILD, and that is what the Keys panel
 * sends: asked about the folder `OpcPlc`, the panel's pattern is `OpcPlc/`. Splitting that on the
 * separator leaves an empty last segment, and resolving one looked for a child whose browse name is
 * the empty string - measured, the panel's first click answered "No node named  under OpcPlc".
 * Interior empties cannot occur, because `escapeSegment` never writes a bare separator.
 */
export function pathSegments(key: string): string[] {
  if (key === "") return [];
  const segments = key.split(OPCUA_PATH_SEPARATOR).map(unescapeSegment);
  while (segments.length > 0 && segments[segments.length - 1] === "") segments.pop();
  return segments;
}

/**
 * Whether a node id is one this provider will dial a server with.
 *
 * Part 6 5.1.12 gives four identifier forms and two namespace forms. This refuses what the grammar
 * refuses rather than what a server happens to dislike, because a refusal before the request is a
 * sentence a reader can act on and a refusal after it is a status code they cannot.
 *
 * THE RANGES AND THE GUID SHAPE ARE PART OF THE GRAMMAR, which a looser pattern let through and the
 * reader paid for in library strings. Measured against the real server with the pattern that only
 * counted characters:
 *  - `g=------------------------------------` (36 dashes) was accepted as a GUID, and the library
 *    threw an Error whose message was EMPTY: the reader was answered "A read failed: ".
 *  - `ns=70000;s=x` was accepted, and the library answered `NodeId: invalid namespace value` - its
 *    own internal string, reaching a reader who cannot act on it. A namespace index is a UInt16
 *    (Part 6 5.2.2.9) and a numeric identifier a UInt32, so both have an end.
 * The GUID is the five-group form Part 6 5.1.12 writes, 8-4-4-4-12, not 36 characters of anything.
 */
const GUID = String.raw`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`;
/** 0 to 65535, and 0 to 4294967295, written as patterns because this is one regular expression. */
const UINT16 = String.raw`(?:0|[1-9]\d{0,3}|[1-5]\d{4}|6[0-4]\d{3}|65[0-4]\d{2}|655[0-2]\d|6553[0-5])`;
const UINT32 = String.raw`(?:0|[1-9]\d{0,8}|[1-3]\d{9}|4[01]\d{8}|42[0-8]\d{7}|429[0-3]\d{6}|4294[0-8]\d{5}|42949[0-5]\d{4}|429496[0-6]\d{3}|4294967[01]\d{2}|42949672[0-8]\d|429496729[0-5])`;
// `[\s\S]` and not `.`, because a String identifier is any non-empty string and `.` stops at a
// newline: `s=` with a newline in it is a node id Part 6 allows and this refused.
const NODE_ID = new RegExp(
  `^(?:ns=${UINT16};|nsu=[^;]+;(?:ns=${UINT16};)?)?(?:i=${UINT32}|s=[\\s\\S]+|g=${GUID}|b=[A-Za-z0-9+/=]+)$`,
);

/** True for a node id this provider can send. */
export function isNodeId(candidate: string): boolean {
  return NODE_ID.test(candidate);
}
