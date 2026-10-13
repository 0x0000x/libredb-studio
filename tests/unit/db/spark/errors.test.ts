/**
 * Every category of spec 4.10 from its trigger to its class and sentence, over hand-built gRPC statuses and the
 * captured answers of tests/fixtures/spark/; the Cause line; the TIME, maxResultSize and empty-UNKNOWN sentences;
 * FetchErrorDetails read for one root and at most five causes; every sentence in the spec's words; and Review Focus
 * R5: a token in any server text never reaches a sentence.
 */
import { describe, expect, test } from "bun:test";
import {
  AuthenticationError,
  ConnectionError,
  DatabaseConfigError,
  type DatabaseError,
  QueryCancelledError,
  QueryError,
} from "@/lib/db/errors";
import {
  errorDetailsFromResponse,
  SPARK_ERROR_SENTENCES,
  SparkError,
  type SparkErrorCategory,
  type SparkErrorContext,
  toDatabaseError,
  toSparkError,
} from "@/lib/db/providers/sql/spark/errors";
import { secretForms } from "@/lib/db/utils/server-text";
import { captureBytes, readSparkCapture, type SparkCaptureVersion } from "../../../helpers/spark-captures";
import type { SparkCaptureName } from "../../../live/spark-evidence-plan";

const TOKEN = "spark-test-token-2fb1c09e";
const FORMS = secretForms([TOKEN]);
const CONNECTION = { host: "spark.test", port: 15002, secretForms: FORMS };
const context = (phase: SparkErrorContext["phase"], extra: Partial<SparkErrorContext> = {}): SparkErrorContext => ({
  connection: CONNECTION,
  phase,
  timeoutMs: 60_000,
  ...extra,
});
const metadataOf = (bytes?: Uint8Array) => ({
  get: (key: string): unknown[] =>
    key === "grpc-status-details-bin" && bytes !== undefined ? [Buffer.from(bytes)] : [],
});
const statusOf = (code: number, details: string, bytes?: Uint8Array) =>
  Object.assign(new Error(`${code} ${details}`), { code, details, metadata: metadataOf(bytes) });

/** google.rpc.Status with one ErrorInfo, written by hand: reason and metadata entries. */
function details(reason: string, metadata: Readonly<Record<string, string>>): Uint8Array {
  const encoder = new TextEncoder();
  const field = (number: number, bytes: Uint8Array) => {
    const length: number[] = [];
    let rest = bytes.length;
    do {
      length.push((rest & 0x7f) | (rest > 0x7f ? 0x80 : 0));
      rest >>>= 7;
    } while (rest > 0);
    return new Uint8Array([(number << 3) | 2, ...length, ...bytes]);
  };
  const entries = Object.entries(metadata).map(([key, value]) =>
    field(3, new Uint8Array([...field(1, encoder.encode(key)), ...field(2, encoder.encode(value))])),
  );
  const info = new Uint8Array([...field(1, encoder.encode(reason)), ...entries.flatMap((entry) => Array.from(entry))]);
  const any = new Uint8Array([
    ...field(1, encoder.encode("type.googleapis.com/google.rpc.ErrorInfo")),
    ...field(2, info),
  ]);
  return field(3, any);
}

/** One error of a FetchErrorDetails answer as proto-loader reads it (keepCase, defaults, oneofs): an absent optional is null. */
const fetchedError = (message: string, cause?: number, errorClass?: string) => ({
  error_type_hierarchy: ["java.lang.Exception"],
  message,
  stack_trace: [{ declaring_class: "a.B", method_name: "c", file_name: "B.scala", line_number: 1 }],
  cause_idx: cause ?? null,
  spark_throwable:
    errorClass === undefined ? null : { error_class: errorClass, message_parameters: {}, query_contexts: [] },
});

function capturedStatus(name: SparkCaptureName, version: SparkCaptureVersion = "4.2.0") {
  const capture = readSparkCapture(version, name);
  if (capture.outcome !== "error") throw new Error(`${name} is not an error capture`);
  const raw = capture.metadata["grpc-status-details-bin"];
  return statusOf(capture.code, capture.details, raw === undefined ? undefined : captureBytes(raw));
}

function expectCategory(error: SparkError, category: SparkErrorCategory, message?: string) {
  expect(error).toBeInstanceOf(SparkError);
  expect(error.category).toBe(category);
  if (message !== undefined) expect(error.message).toBe(message);
}

describe("the connect and transport rows (spec 3.3)", () => {
  test("nothing listening: UNAVAILABLE without a TLS cause, and a refused socket", () => {
    expectCategory(
      toSparkError(
        statusOf(14, "No connection established. Last error: connect ECONNREFUSED 127.0.0.1:15002"),
        context("connect"),
      ),
      "not-connected",
      SPARK_ERROR_SENTENCES.notConnected("spark.test", 15002),
    );
    expectCategory(
      toSparkError(
        Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:15002"), { code: "ECONNREFUSED" }),
        context("connect"),
      ),
      "not-connected",
    );
    expectCategory(
      toSparkError(statusOf(14, "Name resolution failed for target dns:nope:15002"), context("connect")),
      "not-connected",
    );
    expectCategory(
      toSparkError(statusOf(4, "Deadline exceeded after 9.9s,LB pick: 0.0s (Waiting for LB pick)"), context("command")),
      "not-connected",
    );
  });

  test.each(["ECONNREFUSED", "ENOTFOUND", "EHOSTUNREACH", "ENETUNREACH", "EAI_AGAIN"])(
    "the Node system code %s on a value that is not a status is not-connected",
    (code) => {
      const error = Object.assign(new Error(`connect ${code} spark.test:15002`), { code });
      expectCategory(
        toSparkError(error, context("connect")),
        "not-connected",
        SPARK_ERROR_SENTENCES.notConnected("spark.test", 15002),
      );
    },
  );

  test.each([
    [
      "No connection established. Last error: Error: write EPROTO 0A00010B:SSL routines::wrong version number",
      "tlsPlaintextServer",
    ],
    ["No connection established. Last error: WRONG_VERSION_NUMBER", "tlsPlaintextServer"],
    ["No connection established. Last error: packet length too long", "tlsPlaintextServer"],
    ["No connection established. Last error: Protocol error", "tlsPortExpected"],
    ["No connection established. Last error: unable to verify the first certificate", "tlsUntrusted"],
    ["No connection established. Last error: self-signed certificate", "tlsUntrusted"],
    ["No connection established. Last error: self signed certificate in certificate chain", "tlsUntrusted"],
    ["No connection established. Last error: unable to get local issuer certificate", "tlsUntrusted"],
  ] as const)("TLS: %s", (text, key) => {
    const error = toSparkError(statusOf(14, text), context("connect"));
    expectCategory(error, "tls");
    const sentence = SPARK_ERROR_SENTENCES[key];
    expect(error.message).toBe(typeof sentence === "function" ? sentence("spark.test", 15002) : sentence);
  });

  test("a token server without a token and with a wrong one, as captured", () => {
    expectCategory(
      toSparkError(capturedStatus("auth-no-token"), context("connect")),
      "unauthenticated",
      SPARK_ERROR_SENTENCES.tokenMissing,
    );
    expectCategory(
      toSparkError(capturedStatus("auth-wrong-token"), context("connect")),
      "unauthenticated",
      SPARK_ERROR_SENTENCES.tokenWrong,
    );
  });

  test("not a Spark Connect endpoint", () => {
    expectCategory(
      toSparkError(statusOf(12, "Method not found"), context("connect")),
      "unimplemented",
      SPARK_ERROR_SENTENCES.notSparkConnect("spark.test", 15002),
    );
  });

  test("a configuration refusal stays a configuration refusal", () => {
    expectCategory(
      toSparkError(new DatabaseConfigError("Host is required.", "spark"), context("connect")),
      "config",
      "Host is required.",
    );
    const same = new SparkError("version", SPARK_ERROR_SENTENCES.versionTooOld("3.5.9"));
    expect(toSparkError(same, context("connect"))).toBe(same);
  });

  test("a value that is not a status is the fixed local-error sentence, never its own message", () => {
    expectCategory(toSparkError(new Error("odd"), context("command")), "spark", SPARK_ERROR_SENTENCES.localClientError);
    expectCategory(toSparkError("text", context("command")), "spark", SPARK_ERROR_SENTENCES.localClientError);
    expectCategory(toSparkError(undefined, context("command")), "spark", SPARK_ERROR_SENTENCES.localClientError);
    const reset = Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" });
    expectCategory(toSparkError(reset, context("relation")), "spark", SPARK_ERROR_SENTENCES.localClientError);
    // A grpc-js local error can repeat a header value, and the authorization header holds the token.
    const local = toSparkError(
      new Error('Metadata string value "Bearer abc" contains illegal characters'),
      context("connect"),
    );
    expectCategory(local, "spark", SPARK_ERROR_SENTENCES.localClientError);
    expect(local.message).not.toContain("Bearer");
    expect(local.message).not.toContain("abc");
  });

  test("the SparkError keeps the original failure as its cause, and the boundary drops it", () => {
    const status = statusOf(14, "Connection dropped");
    const error = toSparkError(status, context("relation"));
    expect(error.name).toBe("SparkError");
    expect(error.cause).toBe(status);
    expect(new SparkError("cancelled", SPARK_ERROR_SENTENCES.cancelled).cause).toBeUndefined();
    expect(toDatabaseError(error).cause).toBeUndefined();
  });
});

describe("the run rows (spec 4.10)", () => {
  test("the keepalive drop, the ping GOAWAY and the receive cap by phase", () => {
    expectCategory(
      toSparkError(statusOf(14, "Connection dropped"), context("relation")),
      "connection-dropped",
      SPARK_ERROR_SENTENCES.connectionDropped,
    );
    expectCategory(
      toSparkError(statusOf(8, "Bandwidth exhausted or memory limit exceeded"), context("relation")),
      "ping-goaway",
      SPARK_ERROR_SENTENCES.pingGoaway,
    );
    expectCategory(
      toSparkError(statusOf(8, "Received message larger than max (17000000 vs 16777216)"), context("command")),
      "receive-cap",
      SPARK_ERROR_SENTENCES.receiveCapCommand,
    );
    expectCategory(
      toSparkError(statusOf(8, "Received message larger than max (17000000 vs 16777216)"), context("relation")),
      "receive-cap",
      SPARK_ERROR_SENTENCES.receiveCapRelation,
    );
    expectCategory(toSparkError(statusOf(8, "something else"), context("relation")), "spark", "something else");
    // The GOAWAY text is matched exactly.
    expectCategory(
      toSparkError(statusOf(8, "Bandwidth exhausted or memory limit exceeded, twice"), context("relation")),
      "spark",
    );
  });

  test("a proxy's cut, measured by D03, is stream-failed and is tested before the spark row", () => {
    const text = "Received RST_STREAM with code 2 (Internal server error)";
    expectCategory(
      toSparkError(statusOf(13, text), context("relation")),
      "stream-failed",
      SPARK_ERROR_SENTENCES.streamFailed(text),
    );
    expectCategory(toSparkError(statusOf(13, "Received HTTP status code 502"), context("command")), "stream-failed");
    expectCategory(toSparkError(statusOf(13, text), context("connect")), "not-connected");
    expectCategory(
      toSparkError(statusOf(14, "upstream reset"), context("relation")),
      "stream-failed",
      SPARK_ERROR_SENTENCES.streamFailed("upstream reset"),
    );
    expectCategory(toSparkError(statusOf(14, "upstream reset"), context("control")), "not-connected");
    // Spark's ErrorInfo makes the same text Spark's own error.
    expectCategory(toSparkError(statusOf(13, text, details("x", {})), context("relation")), "spark", text);
  });

  test("the deadline is a QueryError sentence naming the timeout, in a statement's phases only", () => {
    expectCategory(
      toSparkError(statusOf(4, "Deadline exceeded after 60.001s,remote_addr=127.0.0.1:15002"), context("relation")),
      "deadline",
      SPARK_ERROR_SENTENCES.deadline(60_000),
    );
    expectCategory(
      toSparkError(statusOf(4, "Deadline exceeded after 10.001s,remote_addr=127.0.0.1:15002"), context("control")),
      "not-connected",
    );
    expectCategory(
      toSparkError(statusOf(4, "Deadline exceeded after 10.001s,remote_addr=127.0.0.1:15002"), context("connect")),
      "not-connected",
    );
    const sent = "Deadline exceeded after 60.001s,name resolution: 0.002s,LB pick: 0.001s,remote_addr=127.0.0.1:15002";
    expectCategory(
      toSparkError(statusOf(4, sent), context("command")),
      "deadline",
      SPARK_ERROR_SENTENCES.deadline(60_000),
    );
    // grpc-js's texts for a call that never left: no remote_addr, and one of its three waiting markers.
    expectCategory(
      toSparkError(statusOf(4, "Deadline exceeded after 9.902s,waiting for name resolution"), context("relation")),
      "not-connected",
    );
    expectCategory(
      toSparkError(
        statusOf(4, "Deadline exceeded after 9.902s,name resolution: 0.002s,waiting for metadata filters"),
        context("command"),
      ),
      "not-connected",
    );
    expectCategory(
      toSparkError(
        statusOf(4, "Deadline exceeded after 9.902s,name resolution: 0.002s,Waiting for LB pick"),
        context("relation"),
      ),
      "not-connected",
    );
  });

  test("a statement's deadline without the connection's timeout is the caller's defect, raised", () => {
    const withoutTimeout: SparkErrorContext = { connection: CONNECTION, phase: "relation" };
    const status = statusOf(4, "Deadline exceeded after 60.001s,remote_addr=127.0.0.1:15002");
    expect(() => toSparkError(status, withoutTimeout)).toThrow(TypeError);
    expect(() => toSparkError(status, withoutTimeout)).toThrow("context.timeoutMs");
  });

  test("Spark's one-batch bound on a command answer, as captured on a server with maxBatchSize=1m", () => {
    expectCategory(
      toSparkError(capturedStatus("command-over-batch-bound"), context("command")),
      "command-batch-bound",
      SPARK_ERROR_SENTENCES.commandBatchBound,
    );
    const assertion = statusOf(13, "\nassertion failed: remaining batches: 1", details("java.lang.AssertionError", {}));
    expectCategory(toSparkError(assertion, context("command")), "command-batch-bound");
    expectCategory(toSparkError(assertion, context("relation")), "spark");
  });

  test.each([
    ["an errorClass", details("java.lang.AssertionError", { errorClass: "INTERNAL_ERROR" })],
    ["a sqlState", details("java.lang.AssertionError", { sqlState: "XX000" })],
    ["another reason", details("org.apache.spark.SparkException", {})],
    ["no ErrorInfo", undefined],
  ] as const)("the batch-bound text with %s is Spark's own error", (_what, bytes) => {
    const status = statusOf(13, "assertion failed: remaining batches: 1", bytes);
    expectCategory(toSparkError(status, context("command")), "spark", "assertion failed: remaining batches: 1");
  });

  test("cancellation, the session classes and the cursor a release closes", () => {
    expectCategory(
      toSparkError(capturedStatus("cancel-interrupt-relation"), context("relation")),
      "cancelled",
      SPARK_ERROR_SENTENCES.cancelled,
    );
    expectCategory(toSparkError(statusOf(1, "Cancelled on client"), context("relation")), "cancelled");
    expectCategory(
      toSparkError(capturedStatus("session-closed"), context("control")),
      "session-closed",
      SPARK_ERROR_SENTENCES.sessionClosed,
    );
    const notFound = statusOf(
      13,
      "[INVALID_HANDLE.SESSION_NOT_FOUND]",
      details("org.apache.spark.SparkSQLException", { errorClass: "INVALID_HANDLE.SESSION_NOT_FOUND" }),
    );
    expectCategory(toSparkError(notFound, context("command")), "session-closed");
    const changed = statusOf(
      13,
      "[INVALID_HANDLE.SESSION_CHANGED]",
      details("org.apache.spark.SparkSQLException", { errorClass: "INVALID_HANDLE.SESSION_CHANGED" }),
    );
    expectCategory(toSparkError(changed, context("command")), "session-changed", SPARK_ERROR_SENTENCES.sessionChanged);
    const disconnected = statusOf(
      13,
      "[INVALID_CURSOR.DISCONNECTED] The cursor is disconnected.",
      details("org.apache.spark.SparkSQLException", { errorClass: "INVALID_CURSOR.DISCONNECTED", sqlState: "HY109" }),
    );
    expectCategory(
      toSparkError(disconnected, context("relation")),
      "spark",
      "[INVALID_CURSOR.DISCONNECTED] The cursor is disconnected.",
    );
    const bySqlState = statusOf(13, "stopped", details("x", { sqlState: "HY008" }));
    expectCategory(toSparkError(bySqlState, context("relation")), "cancelled");
  });

  test("a class from FetchErrorDetails decides when ErrorInfo has none, and ErrorInfo's wins when it has one", () => {
    const bare = statusOf(13, "failed", details("x", {}));
    expectCategory(
      toSparkError(bare, context("command", { details: { errorClass: "OPERATION_CANCELED" } })),
      "cancelled",
    );
    expectCategory(
      toSparkError(bare, context("control", { details: { errorClass: "INVALID_HANDLE.SESSION_CLOSED" } })),
      "session-closed",
    );
    expectCategory(
      toSparkError(bare, context("command", { details: { errorClass: "INVALID_HANDLE.SESSION_CHANGED" } })),
      "session-changed",
    );
    const named = statusOf(13, "failed", details("x", { errorClass: "TABLE_OR_VIEW_NOT_FOUND" }));
    expectCategory(
      toSparkError(named, context("command", { details: { errorClass: "OPERATION_CANCELED" } })),
      "spark",
      "failed",
    );
  });
});

describe("Spark's own errors (spec 4.10 steps 1 to 4)", () => {
  test("a syntax error keeps Spark's text, trimmed", () => {
    const status = capturedStatus("error-syntax");
    // Spark's text starts with a newline (D02-C36).
    expect(status.details.startsWith("\n")).toBe(true);
    expectCategory(toSparkError(status, context("command")), "spark", status.details.trim());
  });

  test("a fetched answer with one root and no cause gives its class and adds no line", () => {
    const fetched = readSparkCapture("4.2.0", "error-syntax-details");
    if (fetched.outcome !== "pass") throw new Error("the details capture is an error capture");
    const detailsRead = errorDetailsFromResponse(fetched.payload as object);
    expect(detailsRead).toEqual({ errorClass: "PARSE_SYNTAX_ERROR" });
    const plain = toSparkError(capturedStatus("error-syntax"), context("command"));
    expect(toSparkError(capturedStatus("error-syntax"), context("command", { details: detailsRead })).message).toBe(
      plain.message,
    );
  });

  test("a missing table and a TIME literal, the second with Studio's own sentence after Spark's text", () => {
    const missing = capturedStatus("error-missing-table");
    expectCategory(toSparkError(missing, context("command")), "spark", missing.details.trim());
    // Spark refuses TIME on ExecutePlan 2, after the command step answered the lazy sql relation.
    for (const version of ["4.2.0", "4.1.3"] as const) {
      const time = capturedStatus("time-type-refused", version);
      expectCategory(
        toSparkError(time, context("relation")),
        "spark",
        `${time.details.trim()}\n${SPARK_ERROR_SENTENCES.timeTypeOff}`,
      );
    }
  });

  test("spark.driver.maxResultSize, which carries no errorClass, gets its sentence from the text", () => {
    const status = capturedStatus("error-max-result-size");
    expectCategory(
      toSparkError(status, context("relation")),
      "spark",
      `${status.details.trim()}\n${SPARK_ERROR_SENTENCES.maxResultSize}`,
    );
  });

  test("the permission refusal shows its cause, read from FetchErrorDetails", () => {
    const fetched = readSparkCapture("4.2.0", "error-permission-denied-details");
    if (fetched.outcome !== "pass") throw new Error("the details capture is an error capture");
    const detailsRead = errorDetailsFromResponse(fetched.payload as object);
    expect(detailsRead).toEqual({
      errorClass: "FAILED_TO_CREATE_PLAN_FOR_DIRECT_QUERY",
      cause: "/tmp/studio-unreadable/data.csv (Permission denied)",
    });
    const status = capturedStatus("error-permission-denied");
    const error = toSparkError(status, context("command", { details: detailsRead }));
    expectCategory(
      error,
      "spark",
      `${status.details.trim()}\nCause: /tmp/studio-unreadable/data.csv (Permission denied)`,
    );
    // A fetch that failed, or an error without an errorId, leaves the message ErrorInfo alone gives.
    expectCategory(toSparkError(status, context("command")), "spark", status.details.trim());
  });

  test("a cause the text already holds adds no line, and an errorClass from the fetch decides a class", () => {
    const status = statusOf(13, "boom: disk full", details("x", {}));
    expect(toSparkError(status, context("command", { details: { cause: "disk full" } })).message).toBe(
      "boom: disk full",
    );
    const time = toSparkError(
      statusOf(13, "no time", details("x", {})),
      context("command", { details: { errorClass: "UNSUPPORTED_TIME_TYPE" } }),
    );
    expect(time.message).toBe(`no time\n${SPARK_ERROR_SENTENCES.timeTypeOff}`);
  });

  test("a long cause is cut to 1,024 characters in the Cause line", () => {
    const fetched = errorDetailsFromResponse({ errors: [fetchedError("a", 1), fetchedError("x".repeat(5_000))] });
    const error = toSparkError(statusOf(13, "failed", details("x", {})), context("command", { details: fetched }));
    expect(error.message).toBe(`failed\nCause: ${"x".repeat(1_024)}`);
  });

  test("UNKNOWN with an empty text and no details is the empty-UNKNOWN sentence alone", () => {
    expectCategory(toSparkError(statusOf(2, ""), context("relation")), "spark", SPARK_ERROR_SENTENCES.unknownEmpty);
    expectCategory(toSparkError(statusOf(2, "\n"), context("relation")), "spark", SPARK_ERROR_SENTENCES.unknownEmpty);
    expectCategory(toSparkError(statusOf(2, "something"), context("relation")), "spark", "something");
    // With ErrorInfo, Spark did report something, so the sentence does not apply.
    expect(toSparkError(statusOf(2, "", details("x", {})), context("relation")).message).not.toBe(
      SPARK_ERROR_SENTENCES.unknownEmpty,
    );
  });
});

describe("errorDetailsFromResponse (spec 4.10 step 2)", () => {
  test("the root's class and the innermost cause, never a stack frame", () => {
    expect(
      errorDetailsFromResponse({
        root_error_idx: 0,
        errors: [
          fetchedError("root", 1, "FAILED_READ_FILE.FILE_NOT_EXIST"),
          fetchedError("mid", 2),
          fetchedError(" Permission denied "),
        ],
      }),
    ).toEqual({ errorClass: "FAILED_READ_FILE.FILE_NOT_EXIST", cause: "Permission denied" });
  });

  test("the root is the answer's root_error_idx, and the first error when that index is absent", () => {
    const errors = [fetchedError("first", 2), fetchedError("second", 2, "SECOND_CLASS"), fetchedError("last")];
    expect(errorDetailsFromResponse({ root_error_idx: 1, errors })).toEqual({
      errorClass: "SECOND_CLASS",
      cause: "last",
    });
    expect(errorDetailsFromResponse({ root_error_idx: null, errors })).toEqual({ cause: "last" });
    expect(errorDetailsFromResponse({ errors })).toEqual({ cause: "last" });
  });

  test("at most five causes are walked, a cycle ends, and a long cause is kept whole for toSparkError to cut", () => {
    const chain = Array.from({ length: 8 }, (_, index) => fetchedError(`cause ${index}`, index + 1));
    expect(errorDetailsFromResponse({ errors: chain }).cause).toBe("cause 5");
    expect(errorDetailsFromResponse({ errors: [fetchedError("a", 1), fetchedError("b", 0)] }).cause).toBe("b");
    expect(
      errorDetailsFromResponse({ errors: [fetchedError("a", 1), fetchedError("x".repeat(5_000))] }).cause,
    ).toHaveLength(5_000);
  });

  test("no errors, an index out of range and an empty class answer nothing", () => {
    expect(errorDetailsFromResponse({})).toEqual({});
    expect(errorDetailsFromResponse({ errors: [] })).toEqual({});
    expect(errorDetailsFromResponse({ root_error_idx: 4, errors: [fetchedError("a")] })).toEqual({});
    expect(errorDetailsFromResponse({ root_error_idx: -1, errors: [fetchedError("a")] })).toEqual({});
    expect(errorDetailsFromResponse({ errors: [fetchedError("a", 9, "")] })).toEqual({});
    expect(errorDetailsFromResponse({ errors: [fetchedError("a", 1), fetchedError("   ")] })).toEqual({});
    expect(errorDetailsFromResponse({ errors: [{ message: "a", cause_idx: 1 }, { cause_idx: null }] })).toEqual({});
  });
});

describe("toDatabaseError (the provider boundary)", () => {
  test.each([
    ["config", DatabaseConfigError],
    ["unsent", QueryCancelledError],
    ["cancelled", QueryCancelledError],
    ["not-connected", ConnectionError],
    ["tls", ConnectionError],
    ["connection-dropped", ConnectionError],
    ["stream-failed", ConnectionError],
    ["unimplemented", ConnectionError],
    ["version", ConnectionError],
    ["unauthenticated", AuthenticationError],
    ["ping-goaway", QueryError],
    ["receive-cap", QueryError],
    ["command-batch-bound", QueryError],
    ["batch-too-large", QueryError],
    ["deadline", QueryError],
    ["session-closed", QueryError],
    ["session-changed", QueryError],
    ["spark", QueryError],
    ["unreadable", QueryError],
    ["refused", QueryError],
  ] as const)("%s maps to %p with its message", (category, Class) => {
    const mapped = toDatabaseError(new SparkError(category, "the sentence"), "SELECT 1");
    expect(mapped).toBeInstanceOf(Class);
    expect(mapped.message).toBe("the sentence");
    expect((mapped as DatabaseError).provider).toBe("spark");
    if (mapped instanceof QueryError || mapped instanceof QueryCancelledError) expect(mapped.query).toBe("SELECT 1");
  });

  test("an error that is not a SparkError passes unchanged, and a thrown value becomes a QueryError", () => {
    const original = new ConnectionError("x", "spark");
    expect(toDatabaseError(original)).toBe(original);
    expect(toDatabaseError("plain")).toBeInstanceOf(QueryError);
  });
});

describe("R5: a token in any server text never reaches a sentence", () => {
  const holders = [TOKEN, Buffer.from(TOKEN).toString("base64"), encodeURIComponent(TOKEN)];
  const carries = (message: string) => FORMS.some((form) => message.includes(form));

  test.each(holders)("in Spark's status text (%s)", (form) => {
    expect(carries(toSparkError(statusOf(13, `bad token ${form}`, details("x", {})), context("command")).message)).toBe(
      false,
    );
  });

  test.each(holders)("in a FetchErrorDetails cause (%s)", (form) => {
    expect(
      carries(
        toSparkError(
          statusOf(13, "failed", details("x", {})),
          context("command", { details: { cause: `Permission denied ${form}` } }),
        ).message,
      ),
    ).toBe(false);
  });

  test.each(holders)("in a stream failure text (%s)", (form) => {
    expect(carries(toSparkError(statusOf(14, `upstream ${form}`), context("relation")).message)).toBe(false);
  });

  test.each(holders)("in a proxy's cut text (%s)", (form) => {
    const text = `Received RST_STREAM with code 2 triggered by internal client error: ${form}`;
    expect(carries(toSparkError(statusOf(13, text), context("relation")).message)).toBe(false);
  });

  test.each(holders)("in a TLS failure text and an unauthenticated text (%s)", (form) => {
    expect(
      carries(
        toSparkError(
          statusOf(14, `No connection established. Last error: unable to verify the first certificate ${form}`),
          context("connect"),
        ).message,
      ),
    ).toBe(false);
    expect(
      carries(toSparkError(statusOf(16, `Invalid authentication token ${form}`), context("connect")).message),
    ).toBe(false);
  });

  test.each(holders)("in a value that is not a status (%s)", (form) => {
    expect(carries(toSparkError(new Error(`odd ${form}`), context("command")).message)).toBe(false);
  });

  test("a token that a long cause holds across the 1,024-character cut is still withheld", () => {
    const fetched = errorDetailsFromResponse({
      errors: [fetchedError("root", 1), fetchedError(`${"y".repeat(1_010)} ${TOKEN}`)],
    });
    const message = toSparkError(
      statusOf(13, "failed", details("x", {})),
      context("command", { details: fetched }),
    ).message;
    expect(message.startsWith("failed\nCause: ")).toBe(true);
    expect(message).not.toContain(TOKEN.slice(0, 8));
  });
});

describe("every sentence in the spec's words (3.3, 4.5, 4.7, 4.10, 5.2, 5.4, 5.5)", () => {
  const S = SPARK_ERROR_SENTENCES;
  const words: readonly (readonly [keyof typeof SPARK_ERROR_SENTENCES, string, string])[] = [
    [
      "notConnected",
      S.notConnected("spark.test", 15002),
      "No Spark Connect server answered at spark.test:15002. Start it with sbin/start-connect-server.sh, or check Host and Port (15002 by default).",
    ],
    [
      "tlsPlaintextServer",
      S.tlsPlaintextServer("spark.test", 15002),
      "TLS was chosen, but spark.test:15002 answered without TLS. Spark Connect has no TLS of its own: point Host at a TLS proxy in front of it, or set SSL mode to disable and use an SSH tunnel.",
    ],
    [
      "tlsPortExpected",
      S.tlsPortExpected("spark.test", 15002),
      "spark.test:15002 expects TLS. Choose an SSL mode under SSL / TLS.",
    ],
    [
      "tlsUntrusted",
      S.tlsUntrusted,
      "The server's certificate is not signed by a CA Studio trusts. Paste the CA of the TLS proxy under SSL / TLS, or choose verify-ca or require.",
    ],
    [
      "tokenMissing",
      S.tokenMissing,
      "This Spark Connect server requires a token. Set Token to the server's spark.connect.authenticate.token.",
    ],
    [
      "tokenWrong",
      S.tokenWrong,
      "Spark refused the token. Set Token to the server's spark.connect.authenticate.token.",
    ],
    [
      "versionTooOld",
      S.versionTooOld("3.5.9"),
      "This server runs Spark 3.5.9. Studio connects to Spark 4.1 and later, the first release that splits large results into chunks Studio can read.",
    ],
    ["versionUnreadable", S.versionUnreadable("four"), "Studio could not read this server's Spark version (four)."],
    [
      "notSparkConnect",
      S.notSparkConnect("spark.test", 15002),
      "The gRPC server at spark.test:15002 does not answer Spark Connect's AnalyzePlan, so it is not a Spark Connect server.",
    ],
    [
      "tokenlessCaution",
      S.tokenlessCaution("spark.test", 15002),
      "This connection sends no token, and the server ran its calls: any client that reaches spark.test:15002 can run statements on this Spark server. Set spark.connect.authenticate.token on the server and in Token.",
    ],
    [
      "tokenUnchecked",
      S.tokenUnchecked("spark.test", 15002),
      "This connection sends a token, but the server also ran a call sent without one: it does not check tokens, so any client that reaches spark.test:15002 can run statements on this Spark server. Set spark.connect.authenticate.token on the server.",
    ],
    ["unsent", S.unsent, "The statement was cancelled before it was sent."],
    [
      "localClientError",
      S.localClientError,
      "The Spark client stopped the call before it reached the server. Nothing was sent.",
    ],
    [
      "connectionDropped",
      S.connectionDropped,
      "The connection to Spark dropped while the statement ran. Studio asked Spark to stop it; run it again.",
    ],
    [
      "streamFailed",
      S.streamFailed("Received RST_STREAM with code 2 (Internal server error)"),
      "The connection to Spark failed while the statement ran (Received RST_STREAM with code 2 (Internal server error)). Studio asked Spark to stop it; run it again.",
    ],
    [
      "pingGoaway",
      S.pingGoaway,
      "The connection to Spark was closed by the server's keepalive rule while the statement ran. Studio asked Spark to stop the statement; run it again.",
    ],
    [
      "receiveCapCommand",
      S.receiveCapCommand,
      "Spark answered this command in one message larger than Studio reads (16 MiB), and a command's answer cannot be split. Narrow it, for example with LIKE or IN.",
    ],
    [
      "receiveCapRelation",
      S.receiveCapRelation,
      "Spark sent a result message larger than 16 MiB because result chunking is off in this session (spark.connect.session.resultChunking.maxChunkSize is 0 or less). Run SET spark.connect.session.resultChunking.maxChunkSize=4194304, or leave the setting out.",
    ],
    [
      "commandBatchBound",
      S.commandBatchBound,
      "Spark answers a command in one Arrow batch, and this answer is larger than the batch size this server allows (spark.connect.grpc.arrow.maxBatchSize). Narrow it, for example with LIKE or IN.",
    ],
    [
      "batchTooLarge",
      S.batchTooLarge(8),
      "Spark sends this result in Arrow batches of about 8 MiB, more than Studio reads for one statement (32 MiB), so no row is shown. Add a smaller LIMIT, or select fewer or narrower columns.",
    ],
    [
      "deadline",
      S.deadline(60_000),
      "The statement ran past the connection's query timeout (60000 ms); Studio asked Spark to stop it.",
    ],
    ["cancelled", S.cancelled, "The query was cancelled."],
    [
      "sessionChanged",
      S.sessionChanged,
      "The Spark server restarted during this request, and the session it held is gone. Run it again.",
    ],
    [
      "sessionClosed",
      S.sessionClosed,
      "Spark closed the session this request was using (it idled out, or the server restarted). Run it again.",
    ],
    [
      "metadataSessionLost",
      S.metadataSessionLost,
      "Studio's Spark session for the tree and the panels was closed by the server (it idled out or the server restarted), and a new one could not complete the read.",
    ],
    [
      "timeTypeOff",
      S.timeTypeOff,
      "TIME values are switched off on this Spark server; CAST the column to STRING to read it.",
    ],
    [
      "maxResultSize",
      S.maxResultSize,
      "The result is larger than the server's spark.driver.maxResultSize; add a LIMIT or select fewer columns.",
    ],
    [
      "unknownEmpty",
      S.unknownEmpty,
      "Spark stopped this statement with an internal error and reported no reason (gRPC UNKNOWN). A statement nested thousands of levels deep is one known cause; the Spark driver log has the rest.",
    ],
    [
      "compressed",
      S.compressed("zstd"),
      "Spark compressed this result (spark.sql.execution.arrow.compression.codec is zstd in this session), and Studio reads only uncompressed results. Run SET spark.sql.execution.arrow.compression.codec=none, or leave the setting out.",
    ],
    [
      "largeVarTypes",
      S.largeVarTypes,
      "Spark sent this result with large string and binary types (spark.sql.execution.arrow.useLargeVarTypes is true in this session), and Studio reads only the standard ones. Run SET spark.sql.execution.arrow.useLargeVarTypes=false, or leave the setting out.",
    ],
    [
      "unreadable",
      S.unreadable("a dictionary batch"),
      "Spark sent a result Studio could not read (a dictionary batch), so it is not shown.",
    ],
    [
      "cellUnreadable",
      S.cellUnreadable("DECIMAL(38,0)", "amount"),
      'Spark sent a DECIMAL(38,0) value in column "amount" that Studio could not read, so the result is not shown.',
    ],
  ];

  test("the table names every key once", () => {
    const named: string[] = words.map(([key]) => key);
    expect(named.sort()).toEqual(Object.keys(SPARK_ERROR_SENTENCES).sort());
  });

  test.each(words)("%s", (_key, actual, expected) => {
    expect(actual).toBe(expected);
  });

  test("the record is frozen", () => {
    expect(Object.isFrozen(SPARK_ERROR_SENTENCES)).toBe(true);
  });
});
