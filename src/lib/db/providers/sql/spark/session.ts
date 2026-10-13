/**
 * The session registry of D4 (spec 4.5): one Spark session per request scope, a statement session for a call with no
 * scope, and the provider's metadata session, which never receives user text. No two requests share a session, so no
 * Studio user sees another's SET, USE or temp view (spec 6.9).
 *
 * Every new session is set up once, as a memoized promise: Config set of SPARK_SESSION_CONF (and, on the metadata
 * session, SPARK_METADATA_SESSION_CONF in the same call), SET CATALOG as a string literal, USE NAMESPACE with every part
 * quoted. Every answer's server-side id is observed, so every later request of the session carries it and a restarted
 * server answers SESSION_CHANGED instead of opening a session without the setup. A release that fails changes nothing:
 * the deps log it with no server text and Spark drops the session after its idle timeout. This module imports neither
 * the channel nor execute.ts; index.ts builds its deps.
 */
import { quoteLiteral } from "@/lib/sql/values";
import { SPARK_METADATA_SESSION_CONF, SPARK_SESSION_CONF } from "./constants";
import { SPARK_ERROR_SENTENCES, SparkError } from "./errors";
import { quoteSparkName } from "./namespace";
import type { SparkSessionRef } from "./requests";

export interface SparkSessionHandle extends SparkSessionRef {
  readonly kind: "request" | "statement" | "metadata";
}

export interface SparkSessionDeps {
  /** Config set on `session`; answers the server_side_session_id of the answer. */
  configSet(session: SparkSessionRef, pairs: readonly (readonly [string, string])[]): Promise<string>;
  /** One setup statement on `session`; answers the server_side_session_id of its answer. */
  runSetup(session: SparkSessionRef, sql: string): Promise<string>;
  /** ReleaseSession; a failure is logged by the deps with no server text and rejected. */
  release(session: SparkSessionRef): Promise<void>;
}

/** The connection's Catalog and Namespace, which every new session is set to (spec 4.5 steps 2 and 3). */
export interface SparkSessionPlace {
  readonly catalog?: string;
  readonly namespace?: readonly string[];
}

interface MetadataEntry {
  readonly promise: Promise<SparkSessionHandle>;
  handle?: SparkSessionHandle;
}

const closed = (error: unknown) =>
  error instanceof SparkError && (error.category === "session-closed" || error.category === "session-changed");

export class SparkSessions {
  private readonly scopes = new Map<string, Promise<SparkSessionHandle>>();
  private readonly open = new Set<SparkSessionHandle>();
  private metadataEntry: MetadataEntry | undefined;

  constructor(
    private readonly deps: SparkSessionDeps,
    private readonly place: SparkSessionPlace,
  ) {}

  /** The scope's session, set up on its first statement and shared by the rest; no scope, a new statement session. */
  forScope(scope: string | undefined): Promise<SparkSessionHandle> {
    if (scope === undefined) return this.create("statement");
    const existing = this.scopes.get(scope);
    if (existing !== undefined) return existing;
    const created = this.create("request");
    this.scopes.set(scope, created);
    created.catch(() => {
      if (this.scopes.get(scope) === created) this.scopes.delete(scope);
    });
    return created;
  }

  metadata(): Promise<SparkSessionHandle> {
    if (this.metadataEntry === undefined) {
      const entry: MetadataEntry = { promise: this.create("metadata") };
      this.metadataEntry = entry;
      entry.promise.then(
        (handle) => {
          entry.handle = handle;
        },
        () => {
          if (this.metadataEntry === entry) this.metadataEntry = undefined;
        },
      );
    }
    return this.metadataEntry.promise;
  }

  /** A provider-written read on the metadata session, repeated once on a new one when the server closed the session. */
  async readMetadata<T>(read: (session: SparkSessionHandle) => Promise<T>): Promise<T> {
    const session = await this.metadata();
    try {
      return await read(session);
    } catch (error) {
      if (!closed(error)) throw error;
      if (this.metadataEntry?.handle === session) this.discardMetadata();
      const fresh = await this.metadata();
      try {
        return await read(fresh);
      } catch (again) {
        if (closed(again)) {
          throw new SparkError("session-closed", SPARK_ERROR_SENTENCES.metadataSessionLost, { cause: again });
        }
        throw again;
      }
    }
  }

  /** Releases the scope's session once its setup settled; a scope with no session, or a failed one, is nothing. */
  async endScope(scope: string): Promise<void> {
    const entry = this.scopes.get(scope);
    if (entry === undefined) return;
    this.scopes.delete(scope);
    const handle = await entry.catch(() => undefined);
    if (handle !== undefined) await this.release(handle);
  }

  endStatementSession(handle: SparkSessionHandle): Promise<void> {
    return this.release(handle);
  }

  /** Records the session's first server-side id; an empty id is nothing, and another id is a restarted server. */
  observe(handle: SparkSessionHandle, serverSideSessionId: string): void {
    if (serverSideSessionId === "") return;
    if (handle.serverSideSessionId === undefined) {
      handle.serverSideSessionId = serverSideSessionId;
      return;
    }
    if (handle.serverSideSessionId !== serverSideSessionId) {
      throw new SparkError("session-changed", SPARK_ERROR_SENTENCES.sessionChanged);
    }
  }

  /** Drops the metadata session the server closed; the next metadata() sets up a new one. */
  discardMetadata(): void {
    const handle = this.metadataEntry?.handle;
    if (handle !== undefined) this.open.delete(handle);
    this.metadataEntry = undefined;
  }

  /** Releases every session, a setup still in flight included once it settles. */
  async releaseAll(): Promise<void> {
    const pending = [
      ...this.scopes.values(),
      ...(this.metadataEntry === undefined ? [] : [this.metadataEntry.promise]),
    ];
    this.scopes.clear();
    this.metadataEntry = undefined;
    const settled = await Promise.all(pending.map((promise) => promise.catch(() => undefined)));
    const handles = new Set<SparkSessionHandle>([...this.open]);
    for (const handle of settled) if (handle !== undefined) handles.add(handle);
    await Promise.all([...handles].map((handle) => this.release(handle)));
  }

  /** Sessions created and not yet released or discarded. */
  openCount(): number {
    return this.open.size;
  }

  private async create(kind: SparkSessionHandle["kind"]): Promise<SparkSessionHandle> {
    const handle: SparkSessionHandle = { sessionId: crypto.randomUUID(), kind };
    this.open.add(handle);
    try {
      const pairs = kind === "metadata" ? [...SPARK_SESSION_CONF, ...SPARK_METADATA_SESSION_CONF] : SPARK_SESSION_CONF;
      this.observe(handle, await this.deps.configSet(handle, pairs));
      if (this.place.catalog !== undefined) {
        this.observe(
          handle,
          await this.deps.runSetup(handle, `SET CATALOG ${quoteLiteral(this.place.catalog, "spark")}`),
        );
      }
      if (this.place.namespace !== undefined) {
        const spelled = this.place.namespace.map(quoteSparkName).join(".");
        this.observe(handle, await this.deps.runSetup(handle, `USE NAMESPACE ${spelled}`));
      }
      return handle;
    } catch (error) {
      await this.release(handle);
      throw error;
    }
  }

  /** Releases a handle once; a failed release changes nothing (the deps logged it). */
  private async release(handle: SparkSessionHandle): Promise<void> {
    if (!this.open.delete(handle)) return;
    try {
      await this.deps.release(handle);
    } catch {
      // The statement's result is already decided, and Spark drops the session after its idle timeout (spec 4.5).
    }
  }
}
