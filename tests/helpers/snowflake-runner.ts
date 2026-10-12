/**
 * A scripted Snowflake statement runner for the introspection and monitoring tests: each statement is answered by
 * the first pattern it matches, and every `(sql, tag)` it was handed is recorded in order.
 */
import type { SnowflakeResult, SnowflakeStatementRunner } from "@/lib/db/providers/sql/snowflake/statements";

export type RunnerAnswer = (sql: string) => SnowflakeResult | Error;

export interface RunnerCall {
  readonly sql: string;
  readonly tag: string;
}

/** A runner answering by statement pattern; a statement no pattern matches fails the test that sent it. */
export function fakeRunner(script: readonly (readonly [RegExp, RunnerAnswer])[]): {
  run: SnowflakeStatementRunner;
  calls: RunnerCall[];
} {
  const calls: RunnerCall[] = [];
  const run: SnowflakeStatementRunner = async (sql, tag) => {
    calls.push({ sql, tag });
    const entry = script.find(([pattern]) => pattern.test(sql));
    if (entry === undefined) throw new Error(`No answer is scripted for: ${sql}`);
    const answer = entry[1](sql);
    if (answer instanceof Error) throw answer;
    return answer;
  };
  return { run, calls };
}

/**
 * A result with the given field names and text cells. SHOW answers its columns in lower case, a SELECT in upper case
 * unless aliased in quotes, so each test names them as Snowflake would.
 */
export function showResult(columns: readonly string[], rows: readonly (readonly (string | null)[])[]): SnowflakeResult {
  return {
    rows: rows.map((row) => Object.fromEntries(columns.map((column, index) => [column, row[index] ?? null]))),
    fields: [...columns],
    columnTypes: Object.fromEntries(columns.map((column) => [column, "text"])),
    warnings: [],
    cut: false,
    numRows: rows.length,
  };
}

/** `count` rows of one `name` column, named `<prefix>00001` upwards so they sort in the order they are listed. */
export function namedRows(prefix: string, from: number, count: number): string[][] {
  return Array.from({ length: count }, (_, index) => [`${prefix}${String(from + index).padStart(5, "0")}`]);
}
