/**
 * The session registry of D4 (spec 4.5) over the fake Spark Connect server: one session per scope and a new one per
 * statement without a scope; the setup in its order, the catalog as a string literal and the namespace through USE
 * NAMESPACE; the metadata session's extra settings; observed server-side ids; a failed setup failing only the
 * statement that needed it; releases that never throw; the metadata session repeated once after the server closed it,
 * for each of the three INVALID_HANDLE classes (spec 10.8), and discarded once when two reads meet it together.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { SPARK_METADATA_SESSION_CONF, SPARK_SESSION_CONF } from "@/lib/db/providers/sql/spark/constants";
import { buildSparkConnectionOptions } from "@/lib/db/providers/sql/spark/connection-options";
import { SPARK_ERROR_SENTENCES, SparkError, toSparkError } from "@/lib/db/providers/sql/spark/errors";
import { grpcWireTransport, type SparkWireChannel } from "@/lib/db/providers/sql/spark/grpc-client";
import {
  configGetOptionRequest,
  configSetRequest,
  releaseSessionRequest,
  type SparkSessionRef,
  sqlCommandRequest,
} from "@/lib/db/providers/sql/spark/requests";
import { type SparkSessionDeps, type SparkSessionPlace, SparkSessions } from "@/lib/db/providers/sql/spark/session";
import type { DatabaseConnection } from "@/lib/types";
import { type SparkFakeAnswer, type SparkFakeServer, startSparkFake } from "../../../helpers/spark-connect-server";

const OK: SparkFakeAnswer = { kind: "command", ipc: new Uint8Array([0]) };
const statements: Record<string, SparkFakeAnswer> = {
  "SET CATALOG 'spark_catalog'": OK,
  "SET CATALOG 'my\\'cat'": OK,
  "USE NAMESPACE `studio`": OK,
  "USE NAMESPACE `a.b`.`c``d`": OK,
  "SHOW NAMESPACES": OK,
};
let fake: SparkFakeServer;
let channel: SparkWireChannel;
const call = () => ({ metadata: {}, deadline: new Date(Date.now() + 5_000), signal: new AbortController().signal });
const LIMITS = { maxReceivedBytes: 33_554_432 };

/** One statement on `session` over the fake, read to its end; answers the server-side id its answer carried. */
async function runStatement(session: SparkSessionRef, sql: string): Promise<string> {
  const run = { operationId: randomUUID(), tag: `libredb-studio-${randomUUID()}` };
  const stream = channel.executePlan(sqlCommandRequest(session, sql, run), call(), LIMITS);
  let id = "";
  // oxlint-disable-next-line no-await-in-loop -- one reader awaits each message before it asks for the next.
  for (let response = await stream.read(); response !== undefined; response = await stream.read()) {
    id ||= (response as { server_side_session_id?: string }).server_side_session_id ?? "";
  }
  return id;
}

const deps = (overrides: Partial<SparkSessionDeps> = {}): SparkSessionDeps => ({
  configSet: async (session, pairs) =>
    ((await channel.unary("Config", configSetRequest(session, pairs), call())) as { server_side_session_id: string })
      .server_side_session_id,
  runSetup: runStatement,
  release: async (session) => {
    await channel.unary("ReleaseSession", releaseSessionRequest(session), call());
  },
  ...overrides,
});
const PLACE: SparkSessionPlace = { catalog: "spark_catalog", namespace: ["studio"] };
const entriesOf = (sessionId: string) => fake.log.entries.filter((entry) => entry.sessionId === sessionId);
const releasesOf = (sessionId?: string) =>
  fake.log.entries.filter(
    (entry) => entry.rpc === "ReleaseSession" && (sessionId === undefined || entry.sessionId === sessionId),
  );
const setupOf = (sessionId: string) =>
  entriesOf(sessionId).map((entry) =>
    entry.rpc === "Config"
      ? (entry.request as { operation: { set: { pairs: { key: string; value: string }[] } } }).operation.set.pairs.map(
          (pair) => [pair.key, pair.value],
        )
      : (entry.request as { plan: { command: { sql_command: { input: { sql: { query: string } } } } } }).plan.command
          .sql_command.input.sql.query,
  );
/** A failure mapped as the provider maps a control call's (spec 4.10). */
const sparkErrorOf = (error: unknown) =>
  toSparkError(error, { connection: { host: "h", port: 1, secretForms: [] }, phase: "control" });
/** A metadata read over the fake: one Config get_option on the session it is given. */
const configRead = (seen: string[]) => async (session: SparkSessionRef) => {
  seen.push(session.sessionId);
  try {
    await channel.unary("Config", configGetOptionRequest(session, ["k"]), call());
  } catch (error) {
    throw sparkErrorOf(error);
  }
  return 42;
};

beforeAll(async () => {
  fake = await startSparkFake({ statements });
  const port = Number(fake.target.slice(fake.target.lastIndexOf(":") + 1));
  channel = grpcWireTransport(
    buildSparkConnectionOptions(
      { id: "c", name: "c", type: "spark", host: "127.0.0.1", port } as unknown as DatabaseConnection,
      { queryTimeout: 5_000 },
    ),
  );
});
afterAll(() => {
  channel.close();
  fake.close();
});
beforeEach(() => {
  fake.log.clear();
  statements["SET CATALOG 'spark_catalog'"] = OK;
  statements["SHOW NAMESPACES"] = OK;
});

describe("sessions by scope (D4)", () => {
  test("one handle per scope, another per other scope, and a new statement session for each unscoped call", async () => {
    const sessions = new SparkSessions(deps(), {});
    const a = await sessions.forScope("a");
    expect(await sessions.forScope("a")).toBe(a);
    const b = await sessions.forScope("b");
    expect(b.sessionId).not.toBe(a.sessionId);
    expect(a.kind).toBe("request");
    const first = await sessions.forScope(undefined);
    const second = await sessions.forScope(undefined);
    expect(first.kind).toBe("statement");
    expect(second.sessionId).not.toBe(first.sessionId);
    expect(sessions.openCount()).toBe(4);
    await sessions.releaseAll();
    expect(sessions.openCount()).toBe(0);
  });

  test("two statements of one scope that ask at once share one setup", async () => {
    const sessions = new SparkSessions(deps(), {});
    const [x, y] = await Promise.all([sessions.forScope("c"), sessions.forScope("c")]);
    expect(y).toBe(x);
    expect(fake.log.entries.filter((entry) => entry.rpc === "Config")).toHaveLength(1);
    expect(sessions.openCount()).toBe(1);
    await sessions.releaseAll();
  });

  test("the setup in its order: the seven settings, SET CATALOG as a string literal, then USE NAMESPACE", async () => {
    const sessions = new SparkSessions(deps(), PLACE);
    const handle = await sessions.forScope("s");
    const statement = await sessions.forScope(undefined);
    const expected = [
      SPARK_SESSION_CONF.map(([key, value]) => [key, value]),
      "SET CATALOG 'spark_catalog'",
      "USE NAMESPACE `studio`",
    ];
    expect(setupOf(handle.sessionId)).toStrictEqual(expected);
    // A statement session is set up as a request session is: neither gets the metadata session's two settings.
    expect(setupOf(statement.sessionId)).toStrictEqual(expected);
    expect(typeof handle.serverSideSessionId).toBe("string");
    await sessions.releaseAll();
  });

  test("a catalog with a quote is a backslash-escaped literal and every namespace part is quoted", async () => {
    const sessions = new SparkSessions(deps(), { catalog: "my'cat", namespace: ["a.b", "c`d"] });
    const handle = await sessions.forScope("q");
    expect(setupOf(handle.sessionId).slice(1)).toStrictEqual(["SET CATALOG 'my\\'cat'", "USE NAMESPACE `a.b`.`c``d`"]);
    await sessions.releaseAll();
  });

  test("the metadata session gets both settings lists in one call, and is one handle until discarded", async () => {
    const sessions = new SparkSessions(deps(), {});
    const metadata = await sessions.metadata();
    expect(metadata.kind).toBe("metadata");
    expect(await sessions.metadata()).toBe(metadata);
    expect(setupOf(metadata.sessionId)).toStrictEqual([
      [...SPARK_SESSION_CONF, ...SPARK_METADATA_SESSION_CONF].map(([key, value]) => [key, value]),
    ]);
    sessions.discardMetadata();
    expect(sessions.openCount()).toBe(0);
    expect((await sessions.metadata()).sessionId).not.toBe(metadata.sessionId);
    expect(sessions.openCount()).toBe(1);
    await sessions.releaseAll();
  });

  test("the metadata session takes the connection's catalog and namespace too", async () => {
    const sessions = new SparkSessions(deps(), PLACE);
    const metadata = await sessions.metadata();
    expect(setupOf(metadata.sessionId)).toStrictEqual([
      [...SPARK_SESSION_CONF, ...SPARK_METADATA_SESSION_CONF].map(([key, value]) => [key, value]),
      "SET CATALOG 'spark_catalog'",
      "USE NAMESPACE `studio`",
    ]);
    await sessions.releaseAll();
  });
});

describe("a failed setup fails only the statement that needed it", () => {
  test("the scope's next statement starts a new session, the failed one is released, and other scopes are untouched", async () => {
    statements["SET CATALOG 'spark_catalog'"] = {
      kind: "error",
      code: 13,
      message: "[CATALOG_NOT_FOUND] The catalog `spark_catalog` is not found.",
      errorInfo: { errorClass: "CATALOG_NOT_FOUND", sqlState: "42P08" },
    };
    const sessions = new SparkSessions(deps(), PLACE);
    const failed = await sessions.forScope("x").then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(failed).toBeInstanceOf(Error);
    expect((failed as Error).message).toContain("[CATALOG_NOT_FOUND]");
    const released = releasesOf();
    expect(released).toHaveLength(1);
    expect(sessions.openCount()).toBe(0);
    statements["SET CATALOG 'spark_catalog'"] = OK;
    const retried = await sessions.forScope("x");
    expect(retried.sessionId).not.toBe(released[0]?.sessionId);
    expect((await sessions.forScope("y")).sessionId).not.toBe(retried.sessionId);
    await sessions.releaseAll();
  });

  test("the failure is the deps' own error, for a statement session and for a metadata read, and nothing stays open", async () => {
    const refused = new Error("refused");
    let refuse = true;
    const sessions = new SparkSessions(
      deps({ configSet: (session, pairs) => (refuse ? Promise.reject(refused) : deps().configSet(session, pairs)) }),
      {},
    );
    const statement = await sessions.forScope(undefined).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(statement).toBe(refused);
    let reads = 0;
    const read = async () => {
      reads += 1;
      return reads;
    };
    const metadata = await sessions.readMetadata(read).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(metadata).toBe(refused);
    expect(reads).toBe(0);
    expect(sessions.openCount()).toBe(0);
    expect(releasesOf()).toHaveLength(2);
    // The failed metadata setup left nothing behind: the next read sets up a new metadata session.
    refuse = false;
    expect(await sessions.readMetadata(read)).toBe(1);
    expect(sessions.openCount()).toBe(1);
    await sessions.releaseAll();
  });
});

describe("server-side ids (spec 4.5)", () => {
  test("observe records the first id, accepts it again, ignores an empty one and refuses another", async () => {
    const sessions = new SparkSessions(deps(), {});
    const handle = await sessions.forScope("o");
    const id = handle.serverSideSessionId as string;
    sessions.observe(handle, id);
    sessions.observe(handle, "");
    expect(handle.serverSideSessionId).toBe(id);
    expect(() => sessions.observe(handle, randomUUID())).toThrow(SPARK_ERROR_SENTENCES.sessionChanged);
    await sessions.releaseAll();
  });

  test("an id that first arrives after the setup is recorded then", async () => {
    const sessions = new SparkSessions(deps({ configSet: async () => "" }), {});
    const handle = await sessions.forScope("late");
    expect(handle.serverSideSessionId).toBeUndefined();
    const id = randomUUID();
    sessions.observe(handle, id);
    expect(handle.serverSideSessionId).toBe(id);
    expect(() => sessions.observe(handle, randomUUID())).toThrow(SPARK_ERROR_SENTENCES.sessionChanged);
    await sessions.releaseAll();
  });

  test("a setup step that answers another id fails the setup with the restart sentence and releases the session", async () => {
    const released: string[] = [];
    const sessions = new SparkSessions(
      {
        configSet: async () => "first",
        runSetup: async () => "second",
        release: async (session) => {
          released.push(session.sessionId);
        },
      },
      PLACE,
    );
    const changed = await sessions.forScope("m").then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(changed).toBeInstanceOf(SparkError);
    expect((changed as SparkError).category).toBe("session-changed");
    expect((changed as SparkError).message).toBe(SPARK_ERROR_SENTENCES.sessionChanged);
    expect(released).toHaveLength(1);
    expect(sessions.openCount()).toBe(0);
  });

  test("a later request carries the observed id, and after a restart the server refuses it as SESSION_CHANGED", async () => {
    const sessions = new SparkSessions(deps(), {});
    const handle = await sessions.forScope("r");
    await channel.unary("Config", configGetOptionRequest(handle, ["k"]), call());
    const last = fake.log.entries.at(-1)?.request as { client_observed_server_side_session_id?: string };
    expect(last.client_observed_server_side_session_id).toBe(handle.serverSideSessionId);
    fake.restart();
    const refused = await channel.unary("Config", configGetOptionRequest(handle, ["k"]), call()).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(sparkErrorOf(refused).category).toBe("session-changed");
    await sessions.releaseAll();
  });
});

describe("releases (spec 4.5)", () => {
  test("endScope releases the scope's session once; an unknown scope is nothing; a failing release never throws", async () => {
    const sessions = new SparkSessions(deps(), {});
    const handle = await sessions.forScope("e");
    await sessions.endScope("e");
    await sessions.endScope("e");
    await sessions.endScope("never");
    expect(releasesOf(handle.sessionId)).toHaveLength(1);
    const failing = new SparkSessions(deps({ release: () => Promise.reject(new Error("unreachable")) }), {});
    await failing.forScope("f");
    const statement = await failing.forScope(undefined);
    await failing.metadata();
    await failing.endScope("f");
    await failing.endStatementSession(statement);
    await failing.releaseAll();
    expect(failing.openCount()).toBe(0);
    // A release that throws before it answers a promise never throws either.
    const throwing = new SparkSessions(
      deps({
        release: () => {
          throw new Error("thrown");
        },
      }),
      {},
    );
    await throwing.forScope("t");
    const held = await throwing.forScope(undefined);
    await throwing.endStatementSession(held);
    await throwing.endScope("t");
    expect(throwing.openCount()).toBe(0);
  });

  test("endStatementSession releases the statement's session once", async () => {
    const sessions = new SparkSessions(deps(), {});
    const statement = await sessions.forScope(undefined);
    await sessions.endStatementSession(statement);
    await sessions.endStatementSession(statement);
    expect(releasesOf()).toHaveLength(1);
    expect(releasesOf(statement.sessionId)).toHaveLength(1);
    expect(sessions.openCount()).toBe(0);
  });

  test("a scope whose setup failed has nothing to release at its end", async () => {
    const sessions = new SparkSessions(deps({ configSet: () => Promise.reject(new Error("refused")) }), {});
    await sessions.forScope("z").then(
      () => undefined,
      () => undefined,
    );
    await sessions.endScope("z");
    expect(sessions.openCount()).toBe(0);
    // The one release is the failed setup's own.
    expect(releasesOf()).toHaveLength(1);
  });

  test("releaseAll waits for a scope still in setup and releases it too", async () => {
    let unblock: (() => void) | undefined;
    const slow = deps({
      configSet: async (session, pairs) => {
        await new Promise<void>((resolve) => {
          unblock = resolve;
        });
        return deps().configSet(session, pairs);
      },
    });
    const sessions = new SparkSessions(slow, {});
    const pending = sessions.forScope("slow");
    await Promise.resolve();
    const released = sessions.releaseAll();
    unblock?.();
    const handle = await pending;
    await released;
    expect(sessions.openCount()).toBe(0);
    expect(releasesOf(handle.sessionId)).toHaveLength(1);
  });
});

describe("the metadata session after the server closed it (spec 4.5)", () => {
  test("the read that met the closed session is repeated once on a new one", async () => {
    const sessions = new SparkSessions(deps(), {});
    const first = await sessions.metadata();
    await channel.unary("ReleaseSession", releaseSessionRequest(first), call());
    const seen: string[] = [];
    const value = await sessions.readMetadata(configRead(seen));
    expect(value).toBe(42);
    expect(seen).toHaveLength(2);
    expect(seen[0]).toBe(first.sessionId);
    expect(seen[1]).not.toBe(first.sessionId);
    await sessions.releaseAll();
  });

  test("a restarted server's SESSION_CHANGED on the metadata session is read again on a new one", async () => {
    const sessions = new SparkSessions(deps(), {});
    const first = await sessions.metadata();
    fake.restart();
    const seen: string[] = [];
    expect(await sessions.readMetadata(configRead(seen))).toBe(42);
    expect(seen).toHaveLength(2);
    expect(seen[0]).toBe(first.sessionId);
    expect(seen[1]).not.toBe(first.sessionId);
    await sessions.releaseAll();
  });

  test("SESSION_NOT_FOUND on the metadata session is read again on a new one", async () => {
    statements["SHOW NAMESPACES"] = {
      kind: "error",
      code: 13,
      message: "[INVALID_HANDLE.SESSION_NOT_FOUND] The handle is invalid. Session not found.",
      errorInfo: { errorClass: "INVALID_HANDLE.SESSION_NOT_FOUND", sqlState: "HY000" },
    };
    const sessions = new SparkSessions(deps(), {});
    const seen: string[] = [];
    const value = await sessions.readMetadata(async (session) => {
      seen.push(session.sessionId);
      try {
        await runStatement(session, "SHOW NAMESPACES");
      } catch (error) {
        // The fake answers by statement text, so the new session's read is answered as a live session's.
        statements["SHOW NAMESPACES"] = OK;
        throw sparkErrorOf(error);
      }
      return "read";
    });
    expect(value).toBe("read");
    expect(seen).toHaveLength(2);
    expect(seen[1]).not.toBe(seen[0]);
    await sessions.releaseAll();
  });

  test("two reads that meet the closed session together discard it once and repeat on one new session", async () => {
    const sessions = new SparkSessions(deps(), {});
    const first = await sessions.metadata();
    const read = async (session: SparkSessionRef) => {
      if (session.sessionId === first.sessionId) throw new SparkError("session-closed", "closed");
      return session.sessionId;
    };
    const [a, b] = await Promise.all([sessions.readMetadata(read), sessions.readMetadata(read)]);
    expect(a).not.toBe(first.sessionId);
    expect(b).toBe(a);
    expect(fake.log.entries.filter((entry) => entry.rpc === "Config")).toHaveLength(2);
    expect(sessions.openCount()).toBe(1);
    await sessions.releaseAll();
  });

  test("a repeat that meets a closed session too fails with the metadata sentence; another error is never repeated", async () => {
    const sessions = new SparkSessions(deps(), {});
    let calls = 0;
    const closed = await sessions
      .readMetadata(async () => {
        calls += 1;
        throw new SparkError("session-closed", `closed ${calls}`);
      })
      .then(
        () => undefined,
        (error: unknown) => error as SparkError,
      );
    expect(calls).toBe(2);
    expect(closed?.message).toBe(SPARK_ERROR_SENTENCES.metadataSessionLost);
    expect((closed?.cause as SparkError | undefined)?.message).toBe("closed 2");
    let other = 0;
    const plain = await sessions
      .readMetadata(async () => {
        other += 1;
        throw new SparkError("spark", "a Spark error");
      })
      .then(
        () => undefined,
        (error: unknown) => error as SparkError,
      );
    expect(other).toBe(1);
    expect(plain?.message).toBe("a Spark error");
    let local = 0;
    const thrown = new Error("not a SparkError");
    const notSpark = await sessions
      .readMetadata(async () => {
        local += 1;
        throw thrown;
      })
      .then(
        () => undefined,
        (error: unknown) => error,
      );
    expect(local).toBe(1);
    expect(notSpark).toBe(thrown);
    let changed = 0;
    const repeated = await sessions.readMetadata(async () => {
      changed += 1;
      if (changed === 1) throw new SparkError("session-changed", "restarted");
      return "fresh";
    });
    expect(repeated).toBe("fresh");
    const second = await sessions
      .readMetadata(async () => {
        throw new SparkError("session-closed", "first");
      })
      .catch(() => "failed");
    expect(second).toBe("failed");
    const failing = await sessions
      .readMetadata(async () => {
        throw new SparkError("session-closed", "x");
      })
      .then(
        () => "no",
        (error: unknown) => (error as SparkError).category,
      );
    expect(failing).toBe("session-closed");
    // A repeat that fails another way fails with that error, unchanged.
    let attempts = 0;
    const unreachable = new SparkError("not-connected", "unreachable");
    const network = await sessions
      .readMetadata(async () => {
        attempts += 1;
        if (attempts === 1) throw new SparkError("session-closed", "closed");
        throw unreachable;
      })
      .then(
        () => undefined,
        (error: unknown) => error,
      );
    expect(attempts).toBe(2);
    expect(network).toBe(unreachable);
    await sessions.releaseAll();
  });
});
