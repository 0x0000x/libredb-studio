/**
 * The row summary of the Snowflake evidence harness: a measurement item's answer that carries rows, a partition or a
 * statement's own inline rows, is written with its rows replaced by their count, the first and the last row, and the
 * answer's byte length on the wire and decoded, so a 2,000,000-row table's items stay a few kilobytes. Everything
 * else of the answer, its status, its allow-listed headers (`content-encoding` among them) and every other body field,
 * is kept. The scenarios are never summarised: the integration test replays them whole.
 */
import type { SnowflakeScrubbedExchange } from "./snowflake-evidence-scrub";

/** The sentence each measurement item's file carries, so a reader knows its rows were summarised. */
export const SNOWFLAKE_ROWS_SUMMARISED =
  "Every answer's rows are summarised: each answer that carried rows keeps its other fields and records `rows` with the byte length on the wire and decoded, the row count, and the first and the last row.";

/** The answer's length in bytes as it came off the socket, and once decoded from its `content-encoding`. */
export interface SnowflakeAnswerSizes {
  readonly wireBytes: number;
  readonly decodedBytes: number;
}

export interface SnowflakeRowSummary extends SnowflakeAnswerSizes {
  readonly rowCount: number;
  readonly firstRow: unknown;
  readonly lastRow: unknown;
}

export interface SnowflakeSummarisedExchange {
  readonly request: SnowflakeScrubbedExchange["request"];
  readonly response: SnowflakeScrubbedExchange["response"] & { readonly rows: SnowflakeRowSummary };
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** The exchange with its answer's `data` rows summarised; an answer without a `data` array is returned as it is. */
export function summariseRows(
  exchange: SnowflakeScrubbedExchange,
  sizes: SnowflakeAnswerSizes,
): SnowflakeScrubbedExchange | SnowflakeSummarisedExchange {
  const body = exchange.response.body;
  if (!isRecord(body) || !Array.isArray(body.data)) return exchange;
  const { data, ...rest } = body;
  return {
    request: exchange.request,
    response: {
      ...exchange.response,
      body: rest,
      rows: {
        wireBytes: sizes.wireBytes,
        decodedBytes: sizes.decodedBytes,
        rowCount: data.length,
        firstRow: data[0] ?? null,
        lastRow: data.at(-1) ?? null,
      },
    },
  };
}
