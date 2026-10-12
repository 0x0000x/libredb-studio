/**
 * Builders for the SQL API answers the statements tests script: the asynchronous submit answer, a result answer with
 * `resultSetMetaData` and partition 0, a failure, and a later partition's body.
 *
 * Each builder writes the shape the trial capture recorded (tests/fixtures/snowflake/trial-2026-10-12-v10.37.100/,
 * Snowflake 10.37.100): the same fields, with the reference's example values where a test needs none of its own.
 */
import { gzipSync } from "node:zlib";

/** The handle of the SQL API reference's asynchronous example. */
export const TEST_HANDLE = "019c06a4-0000-df4f-0000-00100006589e";

/**
 * The 202 answer of an `async=true` submit: code 333334 with the handle and its status link.
 * The captured shape.
 */
export function handleAnswer(handle: string = TEST_HANDLE): { status: 202; json: unknown } {
  return {
    status: 202,
    json: {
      code: "333334",
      message: "Asynchronous execution in progress. Use provided query id to perform query monitoring and management.",
      statementHandle: handle,
      statementStatusUrl: `/api/v2/statements/${handle}`,
    },
  };
}

/**
 * A 200 result: `resultSetMetaData` with `numRows`, `format`, `rowType` and `partitionInfo`, and partition 0 as
 * `data`. Each column defaults its sizes to null, and `partitions` sets the `partitionInfo` length.
 * The captured shape.
 */
export function resultAnswer(input: {
  readonly columns: readonly {
    name: string;
    type: string;
    precision?: number | null;
    scale?: number | null;
    length?: number | null;
  }[];
  readonly rows: readonly (readonly (string | null)[])[];
  readonly partitions?: number;
  readonly numRows?: number;
  readonly code?: string;
  readonly handle?: string;
}): { status: 200; json: unknown } {
  const handle = input.handle ?? TEST_HANDLE;
  const partitions = input.partitions ?? 1;
  return {
    status: 200,
    json: {
      code: input.code ?? "090001",
      statementHandle: handle,
      sqlState: "00000",
      message: "successfully executed",
      createdOn: 1597090533987,
      statementStatusUrl: `/api/v2/statements/${handle}`,
      resultSetMetaData: {
        numRows: input.numRows ?? input.rows.length,
        format: "jsonv2",
        rowType: input.columns.map((column) => ({
          name: column.name,
          type: column.type,
          length: column.length ?? null,
          precision: column.precision ?? null,
          scale: column.scale ?? null,
          nullable: true,
        })),
        partitionInfo: Array.from({ length: partitions }, (_, index) => ({
          rowCount: index === 0 ? input.rows.length : 0,
          uncompressedSize: 0,
        })),
      },
      data: input.rows,
    },
  };
}

/**
 * A failure answer: the QueryFailureStatus fields `code`, `message`, `sqlState` and the handle.
 * The captured shape.
 */
export function failureAnswer(
  status: number,
  code: string,
  message: string,
  sqlState?: string,
): { status: number; json: unknown } {
  return {
    status,
    json: { code, message, ...(sqlState === undefined ? {} : { sqlState }), statementHandle: TEST_HANDLE },
  };
}

/**
 * A later partition's body: `{"data": rows}`, gzipped or as plain text, with the encoding header that goes with it.
 * The captured shape.
 */
export function partitionBody(
  rows: readonly (readonly (string | null)[])[],
  encoding: "gzip" | "identity",
): { status: 200; bytes: Buffer; contentEncoding: string | null } {
  const text = Buffer.from(JSON.stringify({ data: rows }));
  return encoding === "gzip"
    ? { status: 200, bytes: gzipSync(text), contentEncoding: "gzip" }
    : { status: 200, bytes: text, contentEncoding: null };
}
