/**
 * The `node` object kind and the Source tab (#1677 section 3).
 *
 * ONE KIND, `node`, enumerated by the Keys panel. Declaring it is what opens a node's value in a
 * Source tab rather than a generated read, and it is also what keeps the kind out of the Objects
 * tree, out of inventory and search, and out of the agent's grounding walk.
 *
 * Counts are the exception: a server publishes no count of its nodes, so this provider counts
 * nothing. Measured, the address space under the Objects folder is 6,769 nodes on the documented
 * fixture and larger again on the Eclipse Milo demo, and neither publishes the figure; a number
 * produced from what one Browse happened to return would be a total with nothing behind it.
 */
import type { ObjectKindSpec } from "@/lib/db/types";
import type { OpcuaDataValue, OpcuaNodeId } from "./client";
import { OPCUA_METHOD_NOTE, opcuaNoValueNote } from "./labels";

/** The one kind's id, for the provider's own guard. */
export const OPCUA_OBJECT_KINDS = Object.freeze(["node"] as const);

/**
 * The one kind, declared, AND ENUMERATED BY THE KEYS PANEL.
 *
 * `hasSource` is what opens a node's value in a Source tab rather than a generated read, and
 * `hasColumns` is false because a node has attributes rather than columns: the Source tab shows
 * them, and a column list would promise a grid the node does not have.
 *
 * `enumeratedBy: "key-browser"` is the half that was missing and it is load bearing, measured by
 * clicking a node in the panel: without it `keyBrowserKind` answers undefined, the panel falls
 * through to the generated-read branch, and the generated read for this engine is a `//` comment,
 * so a reader who clicked a node to see its value was answered
 * `JSON Parse error: Unrecognized token '/'`. Declaring it also keeps nodes out of every walk over
 * all kinds - no Objects-tree folder, no inventory or search listing, no line of the agent's
 * grounding walk - all of which are directions we want: `listObjects` refuses here, and a folder
 * that throws on expand is worse than no folder. It is why `readObjectSource` and
 * `nodeSourceDocument` below are reached at all.
 */
export const OPCUA_OBJECT_KIND_SPECS: readonly ObjectKindSpec[] = Object.freeze([
  {
    id: "node",
    role: "relation",
    enumeratedBy: "key-browser",
    label: "Node",
    labelPlural: "Nodes",
    hasColumns: false,
    hasSource: true,
    sourceLanguage: "json",
  },
]);

/**
 * How a value becomes a table cell.
 *
 * O-S9: a value is DATA. A string holding HTML or ANSI escapes, a ByteString of invalid UTF-8, an
 * ExtensionObject: each is shown as text, JSON or hex and never rendered as markup. This function
 * decides the shape; nothing here interprets the content.
 */
export function valueCell(value: unknown): {
  readonly text: string;
  readonly shape: string;
} {
  if (value === null || value === undefined) return { text: "", shape: "null" };
  if (typeof value === "boolean" || typeof value === "number") return { text: String(value), shape: typeof value };
  if (typeof value === "bigint") return { text: value.toString(), shape: "bigint" };
  if (typeof value === "string") return { text: value, shape: "string" };
  if (value instanceof Date) return { text: value.toISOString(), shape: "datetime" };
  if (value instanceof Uint8Array) {
    // Hex, because a ByteString is bytes and need not be text at all: measured, one node on the
    // first fixture holds bytes that are not valid UTF-8, and decoding them would substitute
    // replacement characters for the reader's own data.
    return { text: Buffer.from(value).toString("hex"), shape: "bytes" };
  }
  // A TYPED array is an array, and the library answers one for every numeric array: measured, a
  // 100x100 Int32 matrix arrives as an `Int32Array` and a Double array as a `Float64Array`.
  // `Uint8Array` is handled above, because a ByteString is bytes rather than a list of numbers.
  if (ArrayBuffer.isView(value) && !(value instanceof Uint8Array)) {
    return {
      text: JSON.stringify(Array.from(value as unknown as ArrayLike<number>)),
      shape: "array",
    };
  }
  if (Array.isArray(value)) {
    // An array arrives as an array, a matrix as an array with its dimensions beside it. Measured on
    // a staged server: a 100x100 Int32 matrix arrives with arrayType Matrix and dimensions
    // [100, 100], and a 50,000-element Double array is 400 KB on the wire.
    return { text: JSON.stringify(value, jsonSafe), shape: "array" };
  }
  return { text: JSON.stringify(value, jsonSafe), shape: "object" };
}

/** `JSON.stringify` cannot carry a bigint or a byte array, and a throw here would lose the row. */
function jsonSafe(_key: string, value: unknown): unknown {
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Uint8Array) return Buffer.from(value).toString("hex");
  return value;
}

/** What the AccessLevel bits mean (Part 3 8.57), decoded for a reader rather than shown as a number. */
export function accessLevelWords(bits: number | undefined): string {
  if (bits === undefined) return "not stated";
  const words: string[] = [];
  if ((bits & 0x01) !== 0) words.push("read");
  if ((bits & 0x02) !== 0) words.push("write");
  if ((bits & 0x04) !== 0) words.push("history");
  return words.length === 0 ? "none" : words.join(", ");
}

/**
 * Whether a node keeps history, which decides whether a history read can answer at all.
 *
 * Read from the node's own Historizing attribute rather than attempted and recovered from, because
 * the three stacks answer a history read of a non-historized node three ways: OPC PLC
 * `BadHistoryOperationUnsupported` per node, open62541's ci_server `BadNotSupported` at service
 * level so the whole request throws, and asyncua Good with zero values. Only the first is a usable
 * signal, so the attribute is read instead.
 */
export function keepsHistory(attributes: { readonly historizing?: boolean; readonly accessLevel?: number }): boolean {
  if (attributes.historizing === true) return true;
  // The HistoryRead bit of AccessLevel, for a server that sets it without setting Historizing.
  return attributes.accessLevel !== undefined && (attributes.accessLevel & 0x04) !== 0;
}

/** The Source document for one node: its value, its attributes, and what it cannot do. */
export function nodeSourceDocument(
  nodeId: OpcuaNodeId,
  nodeClass: string,
  value: OpcuaDataValue | undefined,
  attributes: {
    readonly dataType?: string;
    readonly valueRank?: number;
    readonly arrayDimensions?: readonly number[];
    readonly accessLevel?: number;
    readonly userAccessLevel?: number;
    readonly historizing?: boolean;
    readonly description?: string;
  },
): string {
  const cell = value === undefined ? undefined : valueCell(value.value);
  return JSON.stringify(
    {
      nodeId,
      nodeClass,
      /*
       * A NODE WHOSE CLASS HAS NO VALUE SAYS SO, rather than showing a status a reader reads as a
       * failure. A Method is listed and never called; an Object carries no Value attribute at all,
       * so a Read of one answers `BadAttributeIdInvalid` - measured on the first page the Keys panel
       * draws, where `Locations` and `NetworkSet` are both childless Objects.
       */
      ...(nodeClass === "Method" ? { note: OPCUA_METHOD_NOTE } : {}),
      // Part 3 gives a Value attribute to Variables alone, so every other class - Object, View,
      // ObjectType, DataType - answers a Read with BadAttributeIdInvalid and says so here.
      ...(nodeClass === "Method" || nodeClass === "Variable" || nodeClass === "Unspecified"
        ? {}
        : { note: opcuaNoValueNote(nodeClass) }),
      /*
       * A NODE WITH NO VALUE SAYS `null`, NOT `""`. A Read of a class that has no Value attribute
       * answers a DataValue that carries the Bad status and nothing else, so the cell exists and its
       * text is empty - and the document printed that empty text, which in JSON reads as a node
       * holding the empty string. Measured live on `Locations` and `NetworkSet`, two childless
       * Objects on the first page the Keys panel draws. `valueShape` already said `null` beside it,
       * which is how the two disagreed. The shape is the test rather than the text, so a Variable
       * that really does hold the empty string still says `""`.
       */
      value: cell === undefined || cell.shape === "null" ? null : cell.text,
      valueShape: cell?.shape ?? null,
      // The status is part of the answer: Good with a value, Uncertain with a value, or Bad with
      // none. A row whose status is Bad is still a row, because dropping it would tell the reader
      // the node does not exist.
      statusCode: value?.statusCode ?? null,
      sourceTimestamp: value?.sourceTimestamp ?? null,
      serverTimestamp: value?.serverTimestamp ?? null,
      dataType: attributes.dataType ?? null,
      valueRank: attributes.valueRank ?? null,
      arrayDimensions: attributes.arrayDimensions ?? null,
      accessLevel: accessLevelWords(attributes.accessLevel),
      // Measured on the first fixture: an admin identity saw UserAccessLevel 3 against AccessLevel
      // 1 on Server/Auditing, so the two differ in practice and both are shown.
      userAccessLevel: accessLevelWords(attributes.userAccessLevel),
      /*
       * TWO ATTRIBUTES, TWO KEYS. `historizing` is what the SERVER answered, null where it said
       * nothing; `keepsHistory` is that OR'd with the `HistoryRead` bit of `AccessLevel`, which is
       * the question a reader actually has - whether a history read can answer. Printing the derived
       * value under the attribute's own name reported an attribute the server did not set: measured
       * on a Variable whose `Historizing` is false and whose AccessLevel carries the history bit,
       * the document said `historizing: true`. `docs/providers/opcua.md` 6.2 says so in prose, and
       * said it while this line did the opposite.
       */
      historizing: attributes.historizing ?? null,
      keepsHistory: keepsHistory(attributes),
      description: attributes.description ?? null,
      /*
       * `engineeringUnits` and `euRange` ARE NOT HERE, and that is the honest shape. They were
       * rendered as `null` in every document and populated by nobody: no caller ever passed them,
       * the read never asks for them, and `null` in a document reads as "this node has none" rather
       * than "this was not read" - measured on `OpcPlc/Telemetry/Basic/AlternatingBoolean`, a node
       * that really does carry an EURange. Reading them costs two more attributes on every node, so
       * the decision stands as it was; what changes is that the document no longer answers a
       * question it never asked. `docs/providers/opcua.md` section 6.2 says so in prose.
       */
    },
    null,
    2,
  );
}
