/**
 * One run of one statement: the run registers before any network call, takes its session, sends the statement as a
 * command and, for a query, its lazy relation as a second ExecutePlan with the same tag, and decodes the answer under
 * the statement budgets. A run marked cancelled, or whose deadline has passed, sends no further ExecutePlan. Every exit
 * before Spark ended the stream, a cut or a refused result among them, sends one Interrupt by the run's tag under its
 * own control deadline, because cancelling a gRPC call or reaching its deadline leaves the job running on Spark. A
 * Spark error is decoded with one FetchErrorDetails on the run's own session before a statement session is released.
 * The heartbeat holds the session for the whole run.
 */
import { keyRowsByPosition } from "@/lib/db/utils/positional-rows";
import { MAX_UNLIMITED_ROWS } from "@/lib/db/utils/query-limiter";
import { uniqueFieldNames } from "@/lib/db/utils/result-fields";
import { readIpcColumns, rowCost } from "./arrow";
import {
  SPARK_BATCH_OVERHEAD_BYTES,
  SPARK_CONTROL_DEADLINE_MS,
  SPARK_RESULT_BYTE_BUDGET,
  SPARK_RUN_TAG_PREFIX,
  SPARK_STREAM_MAX_RECEIVED_BYTES,
} from "./constants";
import {
  errorDetailsFromResponse,
  SPARK_ERROR_SENTENCES,
  SparkError,
  type SparkErrorCategory,
  type SparkErrorConnection,
  type SparkErrorDetails,
  toSparkError,
} from "./errors";
import type { SparkPlanStream, SparkWireCall, SparkWireChannel } from "./grpc-client";
import type { SparkHeartbeat } from "./heartbeat";
import { checkIpcStream } from "./ipc-guard";
import { fetchErrorDetailsRequest, interruptByTagRequest, relationRequest, sqlCommandRequest } from "./requests";
import {
  keepRows,
  readCommandAnswer,
  readRelationAnswer,
  type SparkBudget,
  type SparkKeptBatch,
} from "./result-stream";
import { assertSchemasAgree, columnsFromArrow, columnsFromDataType, type SparkColumn } from "./schema";
import type { SparkSessionHandle, SparkSessions } from "./session";
import { readStatusDetails } from "./status-details";
import { cellValue, SparkCellOverBudget, type SparkTextBudget } from "./values";

export interface SparkStatementResult {
  fields: string[];
  rows: Record<string, unknown>[];
  columnTypes: Record<string, string>;
  cut: "rows" | "bytes" | "stream" | null;
}

export interface SparkRunDeps {
  readonly channel: () => SparkWireChannel;
  readonly metadata: Readonly<Record<string, string>>;
  readonly heartbeat: SparkHeartbeat;
  readonly sessions: Pick<SparkSessions, "observe" | "endStatementSession">;
  readonly errorConnection: SparkErrorConnection;
  readonly timeoutMs: number;
}

/** The statement budgets: rows from the limiter's unlimited bound, decoded cost from 16 MiB. */
const BUDGET: SparkBudget = { rows: MAX_UNLIMITED_ROWS, bytes: SPARK_RESULT_BYTE_BUDGET };
const LIMITS = { maxReceivedBytes: SPARK_STREAM_MAX_RECEIVED_BYTES };
/**
 * Categories whose operation Spark ended itself, or that never reached Spark, so no Interrupt follows; a changed
 * server-side id that the run itself found on a stream Spark had not ended is the one exception (`openStreamRefused`).
 */
const SPARK_ENDED: ReadonlySet<SparkErrorCategory> = new Set([
  "unsent",
  "spark",
  "command-batch-bound",
  "cancelled",
  "session-closed",
  "session-changed",
]);

interface Run {
  readonly operationId: string;
  readonly tag: string;
  readonly controller: AbortController;
  readonly settled: Promise<"cancelled" | "done">;
  readonly settle: (outcome: "cancelled" | "done") => void;
  cancelled: boolean;
  executing: boolean;
  interrupted: boolean;
  /** The run refused a response of a stream Spark had not ended, so its operation may still be running. */
  openStreamRefused: boolean;
  session: SparkSessionHandle | undefined;
}

const unreadable = (what: string) => new SparkError("unreadable", SPARK_ERROR_SENTENCES.unreadable(what));
/**
 * The end of a run marked cancelled, whatever its stream answered: that answer is kept as the cause, and the run's own
 * cancelled refusal, raised before an ExecutePlan, stands as it is.
 */
const cancelledError = (cause?: unknown) =>
  cause instanceof SparkError && cause.category === "cancelled"
    ? cause
    : new SparkError("cancelled", SPARK_ERROR_SENTENCES.cancelled, cause === undefined ? undefined : { cause });
/**
 * One turn of the event loop, by which a call handed to the channel has written its request. grpc-js opens a call's
 * HTTP/2 stream only after promises settle, so under Node a reset of another stream submitted in the same turn is
 * written first (measured with grpc-js 1.14.5 under Node 24.14.0; Bun 1.4.2 writes the call first either way).
 */
const nextTurn = () =>
  new Promise<void>((resolve) => {
    setImmediate(resolve);
  });

export class SparkRuns {
  private readonly byQuery = new Map<string, Run>();
  private readonly active = new Set<Run>();

  constructor(private readonly deps: SparkRunDeps) {}

  async run(input: {
    sql: string;
    queryId?: string;
    session: () => Promise<SparkSessionHandle>;
    deadline: Date;
  }): Promise<SparkStatementResult> {
    const run = this.register(input.queryId);
    let phase: "command" | "relation" = "command";
    let interrupt = false;
    let outcome: "cancelled" | "done" = "done";
    let release: (() => void) | undefined;
    try {
      // The setup is the scope's one memoized promise, so it carries no run signal.
      const session = await input.session();
      run.session = session;
      this.assertMaySend(run, input.deadline);
      release = this.deps.heartbeat.hold(session);
      const call: SparkWireCall = {
        metadata: this.deps.metadata,
        deadline: input.deadline,
        signal: run.controller.signal,
      };
      run.executing = true;
      const first = this.observed(
        this.deps.channel().executePlan(sqlCommandRequest(session, input.sql, run), call, LIMITS),
        session,
        run,
      );
      const { relation } = await readCommandAnswer(first);
      const inline = inlineData(relation);
      if (inline !== undefined) {
        const result = decodeInline(inline, input.sql);
        interrupt = result.cut !== null;
        return result;
      }
      this.assertMaySend(run, input.deadline);
      phase = "relation";
      const second = this.observed(
        this.deps
          .channel()
          .executePlan(
            relationRequest(session, relation, { operationId: crypto.randomUUID(), tag: run.tag }),
            call,
            LIMITS,
          ),
        session,
        run,
      );
      const answer = await readRelationAnswer(second, BUDGET);
      const result = decodeRelation(answer, input.sql);
      interrupt = result.cut !== null;
      return result;
    } catch (error) {
      // Read once, as the failure arrives: a Stop that comes while a failure is decoded does not replace it.
      const failure = run.cancelled ? cancelledError(error) : await this.decode(error, run, phase);
      outcome = failure.category === "cancelled" ? "cancelled" : "done";
      interrupt = run.executing && (run.openStreamRefused || !SPARK_ENDED.has(failure.category));
      throw failure;
    } finally {
      release?.();
      if (interrupt) await this.interrupt(run);
      run.controller.abort();
      if (run.session?.kind === "statement") await this.deps.sessions.endStatementSession(run.session);
      this.unregister(run);
      run.settle(outcome);
    }
  }

  /**
   * Stop: false for an unknown or finished run. A run that has sent no ExecutePlan is marked, which is all it needs,
   * and answers true at once. Any other is sent its Interrupt, and its signal is aborted one turn later without
   * waiting for the Interrupt's answer, so the Interrupt still leaves ahead of the stream's reset on the shared
   * connection. Stop then waits for the Interrupt's answer and for the run to settle, within SPARK_CONTROL_DEADLINE_MS
   * of the Stop, and answers true when the run settled as cancelled by then.
   */
  async cancel(queryId: string): Promise<boolean> {
    const run = this.byQuery.get(queryId);
    if (run === undefined) return false;
    run.cancelled = true;
    if (!run.executing) return true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, SPARK_CONTROL_DEADLINE_MS);
    });
    let outcome: "cancelled" | "done" | undefined;
    const settled = run.settled.then((value) => {
      outcome = value;
    });
    // interrupt() never rejects, so an Interrupt that fails after the abort leaves nothing unhandled.
    const interrupted = this.interrupt(run);
    await nextTurn();
    run.controller.abort();
    await Promise.race([Promise.all([interrupted, settled]), timeout]);
    clearTimeout(timer);
    return outcome === "cancelled";
  }

  /** Stop for every run, without waiting for any to settle. */
  async cancelAll(): Promise<void> {
    await Promise.all(
      [...this.active].map(async (run) => {
        run.cancelled = true;
        if (!run.executing) return;
        await this.interrupt(run);
        run.controller.abort();
      }),
    );
  }

  /** Runs registered and not yet settled. */
  runsInFlight(): number {
    return this.active.size;
  }

  private register(queryId: string | undefined): Run {
    let settle: (outcome: "cancelled" | "done") => void = () => undefined;
    const settled = new Promise<"cancelled" | "done">((resolve) => {
      settle = resolve;
    });
    const run: Run = {
      operationId: crypto.randomUUID(),
      tag: `${SPARK_RUN_TAG_PREFIX}${crypto.randomUUID()}`,
      controller: new AbortController(),
      settled,
      settle,
      cancelled: false,
      executing: false,
      interrupted: false,
      openStreamRefused: false,
      session: undefined,
    };
    if (queryId !== undefined) this.byQuery.set(queryId, run);
    this.active.add(run);
    return run;
  }

  private unregister(run: Run): void {
    this.active.delete(run);
    for (const [queryId, registered] of this.byQuery) if (registered === run) this.byQuery.delete(queryId);
  }

  /** A control call's own deadline and a signal no run aborts. */
  private control(): SparkWireCall {
    return {
      metadata: this.deps.metadata,
      deadline: new Date(Date.now() + SPARK_CONTROL_DEADLINE_MS),
      signal: new AbortController().signal,
    };
  }

  /**
   * At most one Interrupt per run; one that fails changes nothing, since the run's own outcome stands, and the returned
   * promise never rejects.
   */
  private async interrupt(run: Run): Promise<void> {
    if (run.interrupted || run.session === undefined) return;
    run.interrupted = true;
    try {
      await this.deps.channel().unary("Interrupt", interruptByTagRequest(run.session, run.tag), this.control());
    } catch {
      return;
    }
  }

  /**
   * Before each ExecutePlan: a run marked cancelled ends cancelled, and one whose deadline has passed ends at it,
   * either way without sending it.
   */
  private assertMaySend(run: Run, deadline: Date): void {
    if (run.cancelled) throw cancelledError();
    if (Date.now() >= deadline.getTime()) {
      throw new SparkError("deadline", SPARK_ERROR_SENTENCES.deadline(this.deps.timeoutMs));
    }
  }

  /**
   * Every response's server-side id is observed on the run's session. A changed id refuses a response of a stream
   * Spark has not ended, so the run is marked to send its Interrupt before the refusal leaves.
   */
  private observed(stream: SparkPlanStream, session: SparkSessionHandle, run: Run): SparkPlanStream {
    const sessions = this.deps.sessions;
    return {
      async read() {
        const response = await stream.read();
        const id = (response as { server_side_session_id?: unknown } | undefined)?.server_side_session_id;
        if (typeof id === "string") {
          try {
            sessions.observe(session, id);
          } catch (error) {
            run.openStreamRefused = true;
            throw error;
          }
        }
        return response;
      },
      get receivedBytes() {
        return stream.receivedBytes;
      },
      get truncated() {
        return stream.truncated;
      },
      cancel: () => stream.cancel(),
    };
  }

  private async decode(error: unknown, run: Run, phase: "command" | "relation"): Promise<SparkError> {
    if (error instanceof SparkError) return error;
    const details = await this.fetchDetails(error, run);
    return toSparkError(error, {
      connection: this.deps.errorConnection,
      phase,
      timeoutMs: this.deps.timeoutMs,
      ...(details === undefined ? {} : { details }),
    });
  }

  /** One FetchErrorDetails for the error's errorId on the run's session; a failed fetch adds nothing. */
  private async fetchDetails(error: unknown, run: Run): Promise<SparkErrorDetails | undefined> {
    const raw = (error as { metadata?: { get?: (key: string) => unknown[] } } | null)?.metadata?.get?.(
      "grpc-status-details-bin",
    )?.[0];
    const errorId = raw instanceof Uint8Array ? readStatusDetails(raw)?.errorId : undefined;
    if (errorId === undefined || run.session === undefined) return undefined;
    try {
      const answer = await this.deps
        .channel()
        .unary("FetchErrorDetails", fetchErrorDetailsRequest(run.session, errorId), this.control());
      return errorDetailsFromResponse(answer);
    } catch {
      return undefined;
    }
  }
}

/** The inline answer's IPC bytes, or undefined for a lazy relation; an inline answer without data is an empty result. */
function inlineData(relation: object): Uint8Array | undefined {
  const local = (relation as { local_relation?: { data?: unknown } | null }).local_relation;
  if (local === null || local === undefined) return undefined;
  return local.data instanceof Uint8Array ? local.data : new Uint8Array(0);
}

function decodeInline(data: Uint8Array, sql: string): SparkStatementResult {
  if (data.length === 0) return { fields: [], rows: [], columnTypes: {}, cut: null };
  checkIpcStream(data);
  const columns = readIpcColumns(data);
  const kept = keepRows(columns, BUDGET, { rows: 0, bytes: 0 });
  return rowsOf(columnsFromArrow(columns), [{ columns, rows: kept.rows }], kept.cut, sql);
}

function decodeRelation(
  answer: {
    readonly schema: object | undefined;
    readonly batches: readonly SparkKeptBatch[];
    readonly cut: SparkStatementResult["cut"];
  },
  sql: string,
): SparkStatementResult {
  if (answer.schema === undefined) throw unreadable("the result had no schema");
  const declared = columnsFromDataType(answer.schema);
  for (const batch of answer.batches) assertSchemasAgree(declared, columnsFromArrow(batch.columns));
  return rowsOf(declared, answer.batches, answer.cut, sql);
}

/**
 * The result: fields made unique, each row keyed by them as own properties (`__proto__` included), Spark's spellings
 * as column types, and the cut, `bytes` when the decoded budget ended the kept rows early.
 */
function rowsOf(
  columns: readonly SparkColumn[],
  batches: readonly SparkKeptBatch[],
  cut: SparkStatementResult["cut"],
  sql: string,
): SparkStatementResult {
  const fields = uniqueFieldNames(columns.map((column) => column.name));
  const kept = keptValues(columns, batches);
  return {
    fields,
    rows: keyRowsByPosition(fields, kept.values, "spark", sql),
    columnTypes: Object.fromEntries(fields.map((field, index) => [field, columns[index].spelling])),
    cut: kept.over ? "bytes" : cut,
  };
}

/**
 * The kept rows' values, in order, under the decoded budget: each row is charged its rowCost, with its
 * batch's overhead on its first row as keepRows charges it, and then the text its VARIANT cells write, which keepRows
 * does not count. A row whose cost passes what the texts before it left, or whose VARIANT text would pass what is left
 * after its cost, ends the values before it (`over`), as keepRows ends the rows before one that passes the budget.
 */
function keptValues(
  columns: readonly SparkColumn[],
  batches: readonly SparkKeptBatch[],
): { readonly values: unknown[][]; readonly over: boolean } {
  const values: unknown[][] = [];
  let spent = 0;
  for (const batch of batches) {
    for (let row = 0; row < batch.rows; row += 1) {
      spent += rowCost(batch.columns, row) + (row === 0 ? SPARK_BATCH_OVERHEAD_BYTES : 0);
      if (spent > SPARK_RESULT_BYTE_BUDGET) return { values, over: true };
      const text: SparkTextBudget = { charsLeft: SPARK_RESULT_BYTE_BUDGET - spent };
      try {
        values.push(columns.map((column, index) => cellValue(column, batch.columns.columns[index], row, text)));
      } catch (error) {
        if (error instanceof SparkCellOverBudget) return { values, over: true };
        throw error;
      }
      spent = SPARK_RESULT_BYTE_BUDGET - text.charsLeft;
    }
  }
  return { values, over: false };
}
