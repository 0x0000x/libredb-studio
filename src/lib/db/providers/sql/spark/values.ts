/**
 * The Studio value mapper of spec 5.2: one cell of a raw Arrow column to an exact, JSON-safe value. No bigint leaves
 * (5.3): an int64 is a number when safe and its digits otherwise, a decimal its text. Every object keyed by server
 * text is built from entries, so a key named __proto__ is an own property. A cell is read only under the Arrow type
 * its kind reads: the relation path takes a column's kind from the DataType Spark sends first, which spec 5.1 holds to
 * the batch by column count and names only, so a column whose batch is laid out otherwise is refused before a byte of
 * it is read under the wrong type. Pure: no I/O, no logger.
 */
import { uniqueFieldNames } from "@/lib/db/utils/result-fields";
import type { ArrowColumnData, ArrowTypeName } from "./arrow";
import { SPARK_RESULT_BYTE_BUDGET } from "./constants";
import { SPARK_ERROR_SENTENCES, SparkError } from "./errors";
import type { SparkColumn, SparkValueKind } from "./schema";
import { calendarIntervalText, dateText, dayTimeText, timeText, timestampText, yearMonthText } from "./temporal";
import { float32Number, variantJson } from "./variant";
import { geoText } from "./wkb";

// BigInt(...) rather than literals: the repository's TypeScript target, ES2017, has no BigInt literal (TS2737).
const ZERO = BigInt(0);
const SIXTY_FOUR = BigInt(64);
const EMPTY = new Uint8Array(0);
// One decoder for every cell: fatal refuses bytes that are not UTF-8, and ignoreBOM keeps a leading U+FEFF in the
// string, where a default decoder would drop it from the value (variant.ts reads its strings alike).
const UTF8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
/** The key kinds whose values are not scalar text: a map of such keys is a list of [key, value] pairs (spec 5.2). */
const NESTED_KEYS: ReadonlySet<SparkValueKind["type"]> = new Set([
  "array",
  "map",
  "struct",
  "variant",
  "geometry",
  "geography",
]);

/** The Arrow type a cell of each kind is read from, as Spark writes it (spec 5.2, the Arrow column). */
const ARROW_TYPES: Readonly<Record<SparkValueKind["type"], ArrowTypeName>> = {
  void: "null",
  boolean: "bool",
  tinyint: "int8",
  smallint: "int16",
  int: "int32",
  bigint: "int64",
  float: "float32",
  double: "float64",
  decimal: "decimal128",
  string: "utf8",
  binary: "binary",
  date: "date32",
  timestamp: "timestamp",
  timestamp_ntz: "timestamp",
  time: "time64",
  "year-month-interval": "interval-year-month",
  "day-time-interval": "duration",
  "calendar-interval": "interval-month-day-nano",
  array: "list",
  map: "map",
  struct: "struct",
  variant: "struct",
  geometry: "struct",
  geography: "struct",
};
/** Spark's own two-child struct layouts, which schema.ts names by the children's names: each child's name and type. */
const VARIANT_PARTS: readonly (readonly [string, ArrowTypeName])[] = [
  ["value", "binary"],
  ["metadata", "binary"],
];
const GEOSPATIAL_PARTS: readonly (readonly [string, ArrowTypeName])[] = [
  ["srid", "int32"],
  ["wkb", "binary"],
];

/**
 * Raised when a VARIANT's text would pass the characters left in the statement's budget; the caller cuts before the
 * row.
 */
export class SparkCellOverBudget extends Error {
  constructor() {
    super("A VARIANT text passes the statement's decoded budget.");
    this.name = "SparkCellOverBudget";
  }
}

/**
 * The characters a statement's decoded budget still has for VARIANT text; `cellValue` subtracts every text it writes.
 */
export interface SparkTextBudget {
  charsLeft: number;
}

/** A cell Studio cannot read, named by its type; cellValue turns it into the sentence naming the column. */
class CellRefusal {
  constructor(readonly type: string) {}
}

/** Thrown for a cell whose Arrow data is not laid out as its kind; cellValue words it, naming the column. */
const LAYOUT_REFUSAL = Symbol("layout refusal");

const isNull = (data: ArrowColumnData, row: number): boolean =>
  data.validity !== undefined && (data.validity[row >> 3] & (1 << (row & 7))) === 0;

function fixed(data: ArrowColumnData): DataView {
  const values = data.values ?? EMPTY;
  return new DataView(values.buffer, values.byteOffset, values.byteLength);
}

/** A string or binary value's bytes, a view of the batch's buffer within the offsets the stream check bounded. */
function bytesAt(data: ArrowColumnData, row: number): Uint8Array {
  return (data.values ?? EMPTY).subarray(data.offsets![row], data.offsets![row + 1]);
}

/** Whether a struct's children are exactly `parts`, by name and Arrow type, in order. */
function hasParts(data: ArrowColumnData, parts: readonly (readonly [string, ArrowTypeName])[]): boolean {
  return (
    data.children.length === parts.length &&
    parts.every(([name, type], index) => data.children[index].name === name && data.children[index].type === type)
  );
}

/**
 * Whether `data` is laid out as `kind` reads it: its Arrow type; a decimal's scale, which places the point (the
 * precision does not); a struct's fields, by count and name; Spark's own two children of a VARIANT or a geospatial
 * value. A timestamp's zone is not held: it labels the same microseconds, and the kind decides whether the text
 * carries +00:00. Every child is held the same way when it is read, at every depth.
 */
function laidOutAs(kind: SparkValueKind, data: ArrowColumnData): boolean {
  if (data.type !== ARROW_TYPES[kind.type]) return false;
  switch (kind.type) {
    case "decimal":
      return data.scale === kind.scale;
    case "struct":
      return (
        data.children.length === kind.fields.length &&
        kind.fields.every((field, index) => data.children[index].name === field.name)
      );
    case "variant":
      return hasParts(data, VARIANT_PARTS);
    case "geometry":
    case "geography":
      return hasParts(data, GEOSPATIAL_PARTS);
    default:
      return true;
  }
}

// The input is bounded by checkIpcStream (T6.1) and by schema.ts: precision at most 38, scale between -38 and the
// precision, so the text of one value stays within about 80 characters.
function decimalText(view: DataView, at: number, scale: number): string {
  const unscaled = (view.getBigInt64(at + 8, true) << SIXTY_FOUR) | view.getBigUint64(at, true);
  if (unscaled === ZERO && scale <= 0) return "0";
  const sign = unscaled < ZERO ? "-" : "";
  const digits = (unscaled < ZERO ? -unscaled : unscaled).toString();
  if (scale <= 0) return `${sign}${digits}${"0".repeat(-scale)}`;
  const padded = digits.padStart(scale + 1, "0");
  return `${sign}${padded.slice(0, -scale)}.${padded.slice(-scale)}`;
}

/** A refusal temporal.ts or variant.ts raised, a SparkError, as the cell refusal of `type`; any other error stays. */
function asCellRefusal(error: unknown, type: string): unknown {
  return error instanceof SparkError ? new CellRefusal(type) : error;
}

function valueOf(
  kind: SparkValueKind,
  data: ArrowColumnData,
  row: number,
  nested: boolean,
  budget: SparkTextBudget,
): unknown {
  if (!laidOutAs(kind, data)) throw LAYOUT_REFUSAL;
  if (kind.type === "void" || isNull(data, row)) return null;
  switch (kind.type) {
    case "boolean":
      return ((data.values ?? EMPTY)[row >> 3] & (1 << (row & 7))) !== 0;
    case "tinyint":
      return fixed(data).getInt8(row);
    case "smallint":
      return fixed(data).getInt16(2 * row, true);
    case "int":
      return fixed(data).getInt32(4 * row, true);
    case "bigint": {
      // Number() rounds to the nearest double and keeps order, so it is a safe integer exactly when the value is one;
      // the bound is never restated (tests/unit/db/sqlite-int64.test.ts keeps it in one provider file).
      const value = fixed(data).getBigInt64(8 * row, true);
      const number = Number(value);
      return Number.isSafeInteger(number) ? number : value.toString();
    }
    case "float":
      return float32Number(fixed(data).getFloat32(4 * row, true));
    case "double":
      return fixed(data).getFloat64(8 * row, true);
    case "decimal":
      return decimalText(fixed(data), 16 * row, kind.scale);
    case "string":
      try {
        return UTF8.decode(bytesAt(data, row));
      } catch {
        // The one error decode raises: bytes that are not UTF-8.
        throw new CellRefusal("string");
      }
    case "binary": {
      const bytes = bytesAt(data, row);
      // A top-level binary is a Buffer of its own, which the grid renders as binary (spec 5.3). A nested one is the
      // \x<hex> text binaryText (src/lib/export/binary.ts) writes, from Buffer's own hex encoder: binaryText builds the
      // text a byte at a time, which for a 16 MiB value measured 1.6 s and 1 GB under Node and 4.4 s and 1.5 GB under
      // Bun, against 15 ms here.
      return nested
        ? `\\x${Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString("hex")}`
        : Buffer.from(bytes);
    }
    case "date":
      return dateText(fixed(data).getInt32(4 * row, true));
    case "timestamp":
    case "timestamp_ntz":
      return timestampText(fixed(data).getBigInt64(8 * row, true), kind.type === "timestamp");
    // timeText refuses a count outside one day, and the interval texts a field range outside their type or in reverse
    // order (PLAN-00 row `D/temporal.ts`); each refusal becomes the cell refusal that names the column.
    case "time":
      try {
        return timeText(fixed(data).getBigInt64(8 * row, true));
      } catch (error) {
        throw asCellRefusal(error, "time");
      }
    case "year-month-interval":
      try {
        return yearMonthText(fixed(data).getInt32(4 * row, true), kind.fields);
      } catch (error) {
        throw asCellRefusal(error, "year-month interval");
      }
    case "day-time-interval":
      try {
        return dayTimeText(fixed(data).getBigInt64(8 * row, true), kind.fields);
      } catch (error) {
        throw asCellRefusal(error, "day-time interval");
      }
    case "calendar-interval": {
      const view = fixed(data);
      return calendarIntervalText(
        view.getInt32(16 * row, true),
        view.getInt32(16 * row + 4, true),
        view.getBigInt64(16 * row + 8, true),
      );
    }
    case "array": {
      const [start, end] = [data.offsets![row], data.offsets![row + 1]];
      return Array.from({ length: end - start }, (_, index) =>
        valueOf(kind.element, data.children[0], start + index, true, budget),
      );
    }
    case "map": {
      const [start, end] = [data.offsets![row], data.offsets![row + 1]];
      // checkIpcStream holds a map's one child to a struct of exactly two children, its keys and its values.
      const [keys, items] = data.children[0].children;
      const pairs = Array.from({ length: end - start }, (_, index) => [
        valueOf(kind.key, keys, start + index, true, budget),
        valueOf(kind.value, items, start + index, true, budget),
      ]);
      return NESTED_KEYS.has(kind.key.type)
        ? pairs
        : Object.fromEntries(pairs.map(([key, value]) => [String(key), value]));
    }
    case "struct": {
      const names = uniqueFieldNames(kind.fields.map((field) => field.name));
      return Object.fromEntries(
        kind.fields.map((field, index) => [names[index], valueOf(field.kind, data.children[index], row, true, budget)]),
      );
    }
    case "variant": {
      let text: string | undefined;
      try {
        text = variantJson(bytesAt(data.children[0], row), bytesAt(data.children[1], row), budget.charsLeft);
      } catch (error) {
        // variantJson refuses a value off the encoding as a SparkError; a budget that is not a count of characters is
        // a TypeError, the caller's defect, which stays one.
        throw asCellRefusal(error, "variant");
      }
      if (text === undefined) throw new SparkCellOverBudget();
      budget.charsLeft -= text.length;
      return text;
    }
    case "geometry":
    case "geography":
      return geoText(fixed(data.children[0]).getInt32(4 * row, true), bytesAt(data.children[1], row));
  }
}

/**
 * One cell as Studio shows it (spec 5.2). `text` is the statement's decoded budget still left, which a VARIANT's text
 * counts against while it is written and is charged with; past it, `SparkCellOverBudget` is thrown before more is
 * built (spec 5.2, 5.6). Absent, a whole statement budget (SPARK_RESULT_BYTE_BUDGET), never an unbounded one: a VARIANT
 * of a few hundred bytes can stand for 2^64 values, and variantJson refuses a budget that is not a non-negative safe
 * integer. A cell Studio cannot read is refused as `SparkError("unreadable")` naming the column; a row outside the
 * column is a RangeError, as it is for rowCost.
 */
export function cellValue(
  column: SparkColumn,
  data: ArrowColumnData,
  row: number,
  text: SparkTextBudget = { charsLeft: SPARK_RESULT_BYTE_BUDGET },
): unknown {
  if (!Number.isInteger(row) || row < 0 || row >= data.length) {
    throw new RangeError(`row ${row} is not one of the column's ${data.length} rows`);
  }
  try {
    return valueOf(column.kind, data, row, false, text);
  } catch (error) {
    if (error instanceof CellRefusal) {
      throw new SparkError("unreadable", SPARK_ERROR_SENTENCES.cellUnreadable(error.type, column.name));
    }
    if (error === LAYOUT_REFUSAL) {
      throw new SparkError(
        "unreadable",
        SPARK_ERROR_SENTENCES.unreadable(`a column "${column.name}" whose Arrow type does not match its Spark type`),
      );
    }
    throw error;
  }
}
