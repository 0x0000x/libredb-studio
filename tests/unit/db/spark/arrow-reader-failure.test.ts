/**
 * The one refusal of readIpcColumns that no test stream reaches: a stream the pre-check passes and apache-arrow still
 * cannot read. Spec 5.5 holds the pre-check to the streams apache-arrow 21.2.0 reads, so arrow.test.ts has no bytes for
 * this case; here the reader is replaced by one that throws, for this file alone (the runner gives each file its own
 * process). Its failure is refused in Studio's own words, and its message never reaches the sentence.
 */
import { expect, mock, test } from "bun:test";
import { SPARK_ERROR_SENTENCES, SparkError } from "@/lib/db/providers/sql/spark/errors";
import { checkIpcStream } from "@/lib/db/providers/sql/spark/ipc-guard";

const realArrow = { ...(await import("apache-arrow")) };
const reads: Uint8Array[] = [];
mock.module("apache-arrow", () => ({
  ...realArrow,
  tableFromIPC(bytes: Uint8Array): never {
    reads.push(bytes);
    throw new Error("reader failure");
  },
}));

const { readIpcColumns } = await import("@/lib/db/providers/sql/spark/arrow");
const { int64Batch } = await import("../../../helpers/spark-ipc");

test("a stream the pre-check passes and the Arrow reader cannot read is refused as unreadable, never shown", () => {
  const stream = int64Batch(0, 1);
  expect(checkIpcStream(stream).batches).toBe(1);
  let error: unknown;
  try {
    readIpcColumns(stream);
  } catch (caught) {
    error = caught;
  }
  expect(reads).toHaveLength(1);
  expect(reads[0]).toBe(stream);
  expect(error).toBeInstanceOf(SparkError);
  const refused = error as SparkError;
  expect([refused.category, refused.message]).toStrictEqual([
    "unreadable",
    SPARK_ERROR_SENTENCES.unreadable("a stream the Arrow reader could not read"),
  ]);
});
