/**
 * Gate 4's evidence harness for the Apache Spark provider (spec 10.3, D16c).
 *
 * It drives @grpc/grpc-js against the compose services of database-compose.yml with the definition the descriptor
 * generator's loadSparkDescriptor() builds, never an import of proto/descriptor.ts or of the provider, so a capture
 * records what Spark sent independently of the code under test. Each entry of SPARK_CAPTURE_CATALOG runs in a fresh
 * session and writes tests/fixtures/spark/<version>/<name>.json. Client-chosen ids are written <session>, <operation>
 * and <tag>, the authorization metadata <token>, and every value of a Config answer under spark.sql.catalog. as
 * <value>; a file that would still hold the token, or pass 2 MB, is not written. An outcome other than the catalog
 * expects stops the run, so a surprise is reported rather than recorded. Captures run under Bun only.
 *
 * Run by hand, never by `bun run test` (tests/runner/discover.ts excludes tests/live/), with the variables of
 * tests/helpers/spark-live.ts set and the service started:
 *   bun tests/live/spark-evidence.ts --setup <spark|spark-4-1|spark-auth>   create the sample objects
 *   bun tests/live/spark-evidence.ts --capture <spark|spark-4-1|spark-auth|spark-tls>
 *   bun tests/live/spark-evidence.ts --readme                                render tests/fixtures/spark/README.md
 */
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import * as grpc from "@grpc/grpc-js";
import { fromJSON } from "@grpc/proto-loader";
import protobuf from "protobufjs";
import { loadSparkDescriptor } from "../../scripts/generate-spark-descriptor.mjs";
import {
  maskSparkLive,
  readSparkLiveArguments,
  type SparkLiveArguments,
  type SparkLiveService,
  sparkLiveSecrets,
} from "../helpers/spark-live";
import {
  capturesOf,
  SPARK_CAPTURE_CATALOG,
  SPARK_CAPTURE_METADATA_SESSION_CONF,
  SPARK_CAPTURE_SESSION_CONF,
  SPARK_SAMPLE_STATEMENTS,
  SPARK_SERVICE_IMAGES,
  type SparkCaptureEntry,
  type SparkCaptureName,
  type SparkCaptureVersionDir,
} from "./spark-evidence-plan";

const ROOT = path.resolve(import.meta.dir, "../..");
const FIXTURES = path.join(ROOT, "tests", "fixtures", "spark");
const MAX_CAPTURE_BYTES = 2_000_000;
const USAGE =
  "Usage: bun tests/live/spark-evidence.ts --setup <spark|spark-4-1|spark-auth> | --capture <spark|spark-4-1|spark-auth|spark-tls> | --readme";
const USER_CONTEXT = { user_id: "libredb-studio", user_name: "libredb-studio" };
const CLIENT_TYPE = "libredb-studio";
const CHUNKING = [
  { result_chunking_options: { allow_arrow_batch_chunking: true, preferred_arrow_chunk_size: "4194304" } },
];
const VERSION_OF: Readonly<Record<SparkLiveService, SparkCaptureVersionDir>> = {
  spark: "4.2.0",
  "spark-4-1": "4.1.3",
  "spark-auth": "4.2.0",
  "spark-tls": "4.2.0",
};
/** The container whose image a service's captures come from: the proxy is answered by spark-auth. */
const CONTAINER_OF: Readonly<Record<SparkLiveService, string>> = {
  spark: "libredb-spark",
  "spark-4-1": "libredb-spark-4-1",
  "spark-auth": "libredb-spark-auth",
  "spark-tls": "libredb-spark-auth",
};

type Rpc = "ExecutePlan" | "AnalyzePlan" | "Config" | "Interrupt" | "ReleaseSession" | "FetchErrorDetails";
type Token = SparkCaptureEntry["token"];
type Answer =
  | { readonly outcome: "pass"; readonly payload: unknown }
  | {
      readonly outcome: "error";
      readonly code: number;
      readonly details: string;
      readonly metadata: Record<string, unknown>;
    };
type UnaryMethod = (
  request: object,
  metadata: grpc.Metadata,
  options: grpc.CallOptions,
  callback: (error: grpc.ServiceError | null, response?: object) => void,
) => void;
type StreamMethod = (
  request: object,
  metadata: grpc.Metadata,
  options: grpc.CallOptions,
) => grpc.ClientReadableStream<object>;

interface Ids {
  readonly session: string;
  readonly operations: string[];
  readonly tags: string[];
}

interface Context {
  readonly args: SparkLiveArguments;
  readonly service: SparkLiveService;
  readonly version: SparkCaptureVersionDir;
  readonly client: grpc.Client;
  readonly secrets: readonly string[];
}

let constructor: grpc.ServiceClientConstructor | undefined;
function serviceClient(): grpc.ServiceClientConstructor {
  if (constructor === undefined) {
    const definition = fromJSON(loadSparkDescriptor(), {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });
    constructor = grpc.makeGenericClientConstructor(
      definition["spark.connect.SparkConnectService"] as grpc.ServiceDefinition,
      "SparkConnectService",
    );
  }
  return constructor;
}

function openClient(args: SparkLiveArguments, service: SparkLiveService): grpc.Client {
  const { host, port } = args.targets[service];
  const address = `${host.includes(":") ? `[${host}]` : host}:${port}`;
  const Client = serviceClient();
  if (service !== "spark-tls") return new Client(address, grpc.credentials.createInsecure());
  return new Client(address, grpc.credentials.createSsl(readFileSync(path.join(args.tlsDir, "ca.pem"))), {
    "grpc.ssl_target_name_override": "localhost",
    "grpc.default_authority": "localhost",
  });
}

function metadataFor(ctx: Context, token: Token): grpc.Metadata {
  const metadata = new grpc.Metadata();
  if (token === "configured") metadata.set("authorization", `Bearer ${ctx.args.token}`);
  if (token === "wrong") metadata.set("authorization", `Bearer ${ctx.args.token}-wrong`);
  return metadata;
}

function failure(error: grpc.ServiceError): Answer {
  return { outcome: "error", code: error.code, details: error.details, metadata: { ...error.metadata.getMap() } };
}

function unary(ctx: Context, rpc: Exclude<Rpc, "ExecutePlan">, request: object, token: Token): Promise<Answer> {
  const method = (ctx.client as unknown as Record<string, UnaryMethod>)[rpc] as UnaryMethod;
  return new Promise((resolve) => {
    method.call(ctx.client, request, metadataFor(ctx, token), { deadline: Date.now() + 30_000 }, (error, response) =>
      resolve(error === null ? { outcome: "pass", payload: response } : failure(error)),
    );
  });
}

function executePlan(ctx: Context, request: object, token: Token, onFirst?: () => void): Promise<Answer> {
  const method = (ctx.client as unknown as Record<string, StreamMethod>).ExecutePlan as StreamMethod;
  const call = method.call(ctx.client, request, metadataFor(ctx, token), { deadline: Date.now() + 330_000 });
  const responses: object[] = [];
  return new Promise((resolve) => {
    let settled = false;
    const settle = (answer: Answer) => {
      if (!settled) {
        settled = true;
        resolve(answer);
      }
    };
    call.on("data", (response: object) => {
      responses.push(response);
      if (responses.length === 1) onFirst?.();
    });
    call.on("error", (error: grpc.ServiceError) => settle(failure(error)));
    call.on("end", () => settle({ outcome: "pass", payload: responses }));
  });
}

const base = (ids: Ids) => ({ session_id: ids.session, user_context: USER_CONTEXT, client_type: CLIENT_TYPE });
const configSet = (ids: Ids, pairs: readonly (readonly [string, string])[]) => ({
  ...base(ids),
  operation: { set: { pairs: pairs.map(([key, value]) => ({ key, value })) } },
});
const commandRequest = (ids: Ids, sql: string, operation: string, tag: string) => ({
  ...base(ids),
  operation_id: operation,
  tags: [tag],
  request_options: CHUNKING,
  plan: { command: { sql_command: { input: { sql: { query: sql } } } } },
});
const relationRequest = (ids: Ids, relation: object, operation: string, tag: string) => ({
  ...base(ids),
  operation_id: operation,
  tags: [tag],
  request_options: CHUNKING,
  plan: { root: relation },
});

/** The lazy relation a command step answered, or undefined when it answered inline. */
function lazyRelation(payload: unknown): object | undefined {
  const result = (
    payload as readonly {
      readonly sql_command_result?: { readonly relation?: { readonly rel_type?: string } } | null;
    }[]
  )
    .map((response) => response.sql_command_result)
    .find((value) => value !== null && value !== undefined);
  const relation = result?.relation;
  if (relation === undefined || relation === null) throw new Error("the command step answered no sql_command_result");
  return relation.rel_type === "local_relation" ? undefined : relation;
}

function newRun(ids: Ids): { readonly operation: string; readonly tag: string } {
  const operation = randomUUID();
  const tag = `libredb-studio-${randomUUID()}`;
  ids.operations.push(operation);
  ids.tags.push(tag);
  return { operation, tag };
}

/** A statement run for its effect, before a capture: both steps, and any failure stops the run. */
async function runUncaptured(ctx: Context, ids: Ids, sql: string, token: Token): Promise<void> {
  const run = newRun(ids);
  const command = await executePlan(ctx, commandRequest(ids, sql, run.operation, run.tag), token);
  if (command.outcome === "error")
    throw new Error(`setup statement failed (${command.code}): ${maskSparkLive(command.details, ctx.secrets)}`);
  const relation = lazyRelation(command.payload);
  if (relation === undefined) return;
  const second = randomUUID();
  ids.operations.push(second);
  const answer = await executePlan(ctx, relationRequest(ids, relation, second, run.tag), token);
  if (answer.outcome === "error")
    throw new Error(`setup statement failed (${answer.code}): ${maskSparkLive(answer.details, ctx.secrets)}`);
}

function encode(value: unknown): unknown {
  if (value instanceof Uint8Array) return { $bytes: Buffer.from(value).toString("base64") };
  if (Array.isArray(value)) return value.map(encode);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, inner]) => [key, encode(inner)]));
  }
  return value;
}

/** Config answers: a value under spark.sql.catalog. may hold a catalog's credentials, so it is never written. */
function withoutCatalogValues(rpc: Rpc, request: object, answer: Answer): Answer {
  if (rpc !== "Config" || answer.outcome !== "pass") return answer;
  const prefix = (request as { readonly operation?: { readonly get_all?: { readonly prefix?: string } } }).operation
    ?.get_all?.prefix;
  const all = prefix !== undefined && prefix.startsWith("spark.sql.catalog.");
  const payload = answer.payload as { readonly pairs?: readonly { readonly key: string; readonly value?: unknown }[] };
  // oxlint-disable-next-line no-map-spread -- the answer object is decoded once and must stay as Spark sent it.
  const pairs = (payload.pairs ?? []).map((pair) =>
    all || pair.key.startsWith("spark.sql.catalog.") ? { ...pair, value: "<value>" } : pair,
  );
  return { outcome: "pass", payload: { ...payload, pairs } };
}

function maskIds(text: string, ids: Ids): string {
  let masked = text;
  for (const tag of ids.tags) masked = masked.split(tag).join("<tag>");
  for (const operation of ids.operations) masked = masked.split(operation).join("<operation>");
  return masked.split(ids.session).join("<session>");
}

let sparkVersion = "";

function writeCapture(
  ctx: Context,
  name: SparkCaptureName,
  rpc: Rpc,
  surface: string,
  request: object,
  answer: Answer,
  ids: Ids,
  expected: "pass" | "error",
): void {
  if (answer.outcome !== expected) {
    const detail = answer.outcome === "error" ? `${answer.code} ${maskSparkLive(answer.details, ctx.secrets)}` : "pass";
    throw new Error(`${name}: expected ${expected}, Spark answered ${detail}; nothing was written`);
  }
  const { image, digest } = SPARK_SERVICE_IMAGES[ctx.service];
  const shown = withoutCatalogValues(rpc, request, answer);
  const header = {
    service: ctx.service,
    image,
    digest,
    sparkVersion,
    date: new Date().toISOString(),
    runtime: `bun ${Bun.version}`,
    rpc: `SparkConnectService/${rpc}`,
    surface,
    request: encode(request),
  };
  const body =
    shown.outcome === "pass"
      ? { outcome: "pass", payload: encode(shown.payload) }
      : {
          outcome: "error",
          code: shown.code,
          details: shown.details,
          metadata: encode({
            ...shown.metadata,
            ...("authorization" in shown.metadata ? { authorization: "<token>" } : {}),
          }),
        };
  const text = maskIds(`${JSON.stringify({ $captured: header, ...body }, null, 2)}\n`, ids);
  if (ctx.secrets.some((secret) => text.includes(secret))) {
    throw new Error(`${name}: the capture would hold the token; nothing was written`);
  }
  const bytes = Buffer.byteLength(text);
  if (bytes > MAX_CAPTURE_BYTES)
    throw new Error(`${name}: ${bytes} bytes, above the 2 MB capture bound; nothing was written`);
  const directory = path.join(FIXTURES, ctx.version);
  mkdirSync(directory, { recursive: true });
  writeFileSync(path.join(directory, `${name}.json`), text);
  console.log(`wrote ${ctx.version}/${name}.json (${answer.outcome})`);
}

async function runEntry(ctx: Context, entry: SparkCaptureEntry): Promise<void> {
  const ids: Ids = { session: randomUUID(), operations: [], tags: [] };
  const { action, token, surface } = entry;
  if (entry.session !== "none") {
    const pairs =
      entry.session === "metadata"
        ? [...SPARK_CAPTURE_SESSION_CONF, ...SPARK_CAPTURE_METADATA_SESSION_CONF]
        : SPARK_CAPTURE_SESSION_CONF;
    const setup = await unary(ctx, "Config", configSet(ids, pairs), token);
    if (setup.outcome === "error") throw new Error(`${entry.name}: the session setup failed (${setup.code})`);
  }
  if (entry.conf !== undefined) {
    const conf = await unary(ctx, "Config", configSet(ids, entry.conf), token);
    if (conf.outcome === "error")
      throw new Error(`${entry.name}: the Config set of ${entry.conf.map(([key]) => key).join(", ")} failed`);
  }
  for (const sql of entry.before ?? []) {
    // oxlint-disable-next-line no-await-in-loop -- each statement reads what the one before it made in the session.
    await runUncaptured(ctx, ids, sql, token);
  }
  switch (action.kind) {
    case "version": {
      const request = { ...base(ids), spark_version: {} };
      writeCapture(
        ctx,
        entry.name,
        "AnalyzePlan",
        surface,
        request,
        await unary(ctx, "AnalyzePlan", request, token),
        ids,
        entry.expect,
      );
      return;
    }
    case "config-set": {
      const pairs = action.metadata
        ? [...SPARK_CAPTURE_SESSION_CONF, ...SPARK_CAPTURE_METADATA_SESSION_CONF]
        : SPARK_CAPTURE_SESSION_CONF;
      const request = configSet(ids, pairs);
      writeCapture(
        ctx,
        entry.name,
        "Config",
        surface,
        request,
        await unary(ctx, "Config", request, token),
        ids,
        entry.expect,
      );
      return;
    }
    case "config-get": {
      const request = { ...base(ids), operation: { get_option: { keys: [...action.keys] } } };
      writeCapture(
        ctx,
        entry.name,
        "Config",
        surface,
        request,
        await unary(ctx, "Config", request, token),
        ids,
        entry.expect,
      );
      return;
    }
    case "config-get-all": {
      const request = { ...base(ids), operation: { get_all: { prefix: action.prefix } } };
      writeCapture(
        ctx,
        entry.name,
        "Config",
        surface,
        request,
        await unary(ctx, "Config", request, token),
        ids,
        entry.expect,
      );
      return;
    }
    case "statement": {
      const run = newRun(ids);
      const first = commandRequest(ids, action.sql, run.operation, run.tag);
      const command = await executePlan(ctx, first, token);
      if (action.step === "command") {
        writeCapture(ctx, entry.name, "ExecutePlan", `${surface}: command step`, first, command, ids, entry.expect);
        return;
      }
      if (command.outcome === "error")
        throw new Error(`${entry.name}: the command step failed (${command.code}) before the relation step`);
      if (action.commandName !== undefined) {
        writeCapture(ctx, action.commandName, "ExecutePlan", `${surface}: command step`, first, command, ids, "pass");
      }
      const relation = lazyRelation(command.payload);
      if (relation === undefined) throw new Error(`${entry.name}: the statement answered inline, not as a relation`);
      const second = randomUUID();
      ids.operations.push(second);
      const request = relationRequest(ids, relation, second, run.tag);
      writeCapture(
        ctx,
        entry.name,
        "ExecutePlan",
        `${surface}: relation step`,
        request,
        await executePlan(ctx, request, token),
        ids,
        entry.expect,
      );
      return;
    }
    case "interrupt": {
      const run = newRun(ids);
      const first = commandRequest(ids, action.sql, run.operation, run.tag);
      const command = await executePlan(ctx, first, token);
      writeCapture(
        ctx,
        `${entry.name}-command` as SparkCaptureName,
        "ExecutePlan",
        `${surface}: command step`,
        first,
        command,
        ids,
        "pass",
      );
      if (command.outcome === "error") throw new Error(`${entry.name}: the command step failed (${command.code})`);
      const relation = lazyRelation(command.payload);
      if (relation === undefined) throw new Error(`${entry.name}: the statement answered inline, not as a relation`);
      const second = randomUUID();
      ids.operations.push(second);
      const request = relationRequest(ids, relation, second, run.tag);
      const interrupt = { ...base(ids), interrupt_type: "INTERRUPT_TYPE_TAG", operation_tag: run.tag };
      let interrupted: Promise<Answer> | undefined;
      const stream = executePlan(ctx, request, token, () => {
        interrupted = unary(ctx, "Interrupt", interrupt, token);
      });
      const streamAnswer = await stream;
      if (interrupted === undefined) throw new Error(`${entry.name}: the stream ended before its first response`);
      writeCapture(ctx, entry.name, "Interrupt", surface, interrupt, await interrupted, ids, entry.expect);
      writeCapture(
        ctx,
        `${entry.name}-relation` as SparkCaptureName,
        "ExecutePlan",
        `${surface}: relation step`,
        request,
        streamAnswer,
        ids,
        "error",
      );
      return;
    }
    case "release": {
      const request = { ...base(ids), allow_reconnect: false };
      writeCapture(
        ctx,
        entry.name,
        "ReleaseSession",
        surface,
        request,
        await unary(ctx, "ReleaseSession", request, token),
        ids,
        entry.expect,
      );
      return;
    }
    case "after-release": {
      const release = await unary(ctx, "ReleaseSession", { ...base(ids), allow_reconnect: false }, token);
      if (release.outcome === "error") throw new Error(`${entry.name}: the release failed (${release.code})`);
      const request = { ...base(ids), operation: { get_option: { keys: ["spark.sql.ansi.enabled"] } } };
      writeCapture(
        ctx,
        entry.name,
        "Config",
        surface,
        request,
        await unary(ctx, "Config", request, token),
        ids,
        entry.expect,
      );
      return;
    }
    case "fetch-error-details": {
      const run = newRun(ids);
      const failed = await executePlan(ctx, commandRequest(ids, action.sql, run.operation, run.tag), token);
      if (failed.outcome !== "error") throw new Error(`${entry.name}: the statement did not fail`);
      const errorId = errorInfoOf(failed.metadata["grpc-status-details-bin"])?.metadata.errorId;
      if (errorId === undefined) throw new Error(`${entry.name}: the failure carried no errorId`);
      const request = { ...base(ids), error_id: errorId };
      writeCapture(
        ctx,
        entry.name,
        "FetchErrorDetails",
        surface,
        request,
        await unary(ctx, "FetchErrorDetails", request, token),
        ids,
        entry.expect,
      );
      return;
    }
  }
}

// -- status details, read with protobufjs only to render the answers; the provider reads them by hand (status-details.ts)

const RPC_TYPES = protobuf.Root.fromJSON({
  nested: {
    google: {
      nested: {
        rpc: {
          nested: {
            Status: {
              fields: {
                code: { type: "int32", id: 1 },
                message: { type: "string", id: 2 },
                details: { rule: "repeated", type: "Any", id: 3 },
              },
            },
            Any: { fields: { type_url: { type: "string", id: 1 }, value: { type: "bytes", id: 2 } } },
            ErrorInfo: {
              fields: {
                reason: { type: "string", id: 1 },
                domain: { type: "string", id: 2 },
                metadata: { keyType: "string", type: "string", id: 3 } as protobuf.IMapField,
              },
            },
          },
        },
      },
    },
  },
});

function errorInfoOf(
  bytes: unknown,
): { readonly reason: string; readonly metadata: Readonly<Record<string, string>> } | undefined {
  const raw = bytes instanceof Uint8Array ? bytes : (bytes as { readonly $bytes?: string } | undefined)?.$bytes;
  if (raw === undefined) return undefined;
  const buffer = typeof raw === "string" ? Buffer.from(raw, "base64") : raw;
  const status = RPC_TYPES.lookupType("google.rpc.Status").decode(buffer) as unknown as {
    readonly details: readonly { readonly type_url: string; readonly value: Uint8Array }[];
  };
  const info = status.details.find((detail) => detail.type_url === "type.googleapis.com/google.rpc.ErrorInfo");
  if (info === undefined) return undefined;
  return RPC_TYPES.lookupType("google.rpc.ErrorInfo").decode(info.value) as unknown as {
    readonly reason: string;
    readonly metadata: Readonly<Record<string, string>>;
  };
}

// -- the README ---------------------------------------------------------------------------------------------------

interface StoredCapture {
  readonly $captured: {
    readonly service: string;
    readonly sparkVersion: string;
    readonly date: string;
    readonly runtime: string;
    readonly rpc: string;
    readonly surface: string;
  };
  readonly outcome: "pass" | "error";
  readonly payload?: unknown;
  readonly code?: number;
  readonly details?: string;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

function stored(version: SparkCaptureVersionDir, name: string): StoredCapture | undefined {
  const file = path.join(FIXTURES, version, `${name}.json`);
  return existsSync(file) ? (JSON.parse(readFileSync(file, "utf8")) as StoredCapture) : undefined;
}

function cell(text: string): string {
  return text.replace(/\|/g, "\\|").replace(/\n/g, " ");
}

function answerLines(): string[] {
  const lines: string[] = [];
  const fact = (item: string, captures: string, text: string) =>
    lines.push(`| ${item} | ${captures} | ${cell(text)} |`);
  const keys = (version: SparkCaptureVersionDir, name: string) =>
    (
      stored(version, name)?.payload as
        | { readonly pairs?: readonly { readonly key: string; readonly value: string }[] }
        | undefined
    )?.pairs ?? [];
  fact(
    "O4",
    "`4.2.0/config-catalog-keys`, `4.2.0/show-catalogs`",
    `Config get_all keys: ${
      keys("4.2.0", "config-catalog-keys")
        .map((pair) => pair.key)
        .join(", ") || "none"
    }; SHOW CATALOGS rows are in the Arrow answer`,
  );
  for (const version of ["4.2.0", "4.1.3"] as const) {
    fact(
      "O9",
      `\`${version}/session-conf-readback\``,
      keys(version, "session-conf-readback")
        .map((pair) => `${pair.key}=${pair.value}`)
        .join(", "),
    );
    fact(
      "O9",
      `\`${version}/large-var-types-refused\``,
      `outcome ${stored(version, "large-var-types-refused")?.outcome ?? "missing"}; the field types are in the Arrow schema`,
    );
    fact(
      "O10",
      `\`${version}/use-namespace-current\``,
      `outcome ${stored(version, "use-namespace-current")?.outcome ?? "missing"}; the rows are in the Arrow answer`,
    );
    const batches = (
      (stored(version, "batch-rows")?.payload as
        | readonly { readonly arrow_batch?: { readonly row_count: string } | null }[]
        | undefined) ?? []
    ).flatMap((response) => (response.arrow_batch ? [response.arrow_batch.row_count] : []));
    fact("O11", `\`${version}/batch-rows\``, `row_count per batch: ${batches.join(", ")}`);
    const schema = (
      (stored(version, "negative-scale-decimal")?.payload as
        | readonly {
            readonly schema?: {
              readonly struct?: {
                readonly fields: readonly {
                  readonly data_type: { readonly decimal?: { readonly precision?: number; readonly scale?: number } };
                }[];
              };
            } | null;
          }[]
        | undefined) ?? []
    ).find((response) => response.schema)?.schema?.struct?.fields[0]?.data_type.decimal;
    fact(
      "O14",
      `\`${version}/negative-scale-decimal\``,
      schema ? `decimal(${schema.precision},${schema.scale})` : "no schema",
    );
  }
  const proxy = stored("4.2.0", "proxy-silent-timeout");
  fact(
    "O12",
    "`4.2.0/proxy-silent-timeout`",
    proxy?.outcome === "error"
      ? `error ${proxy.code}: ${proxy.details}`
      : `outcome ${proxy?.outcome ?? "missing"} under the proxy's 300 s read timeout`,
  );
  fact(
    "O13",
    "`4.2.0/show-user-functions-quoted`",
    `outcome ${stored("4.2.0", "show-user-functions-quoted")?.outcome ?? "missing"}; the names are in the Arrow answer`,
  );
  const bound = stored("4.2.0", "command-over-batch-bound");
  const info = bound?.metadata === undefined ? undefined : errorInfoOf(bound.metadata["grpc-status-details-bin"]);
  fact(
    "O16",
    "`4.2.0/command-over-batch-bound`",
    bound?.outcome === "error"
      ? `error ${bound.code}, reason ${info?.reason ?? "none"}, errorClass ${info?.metadata.errorClass ?? "none"}: ${(bound.details ?? "").split("\n")[0]}`
      : `outcome ${bound?.outcome ?? "missing"}`,
  );
  fact(
    "O17",
    "`4.2.0/show-tables`, `show-views`, `show-user-functions`, `describe-table`, `describe-view`, `describe-table-struct30`",
    "every statement the object surface issues on the studio namespace",
  );
  return lines;
}

export function renderSparkFixturesReadme(): string {
  const lines = [
    "# Apache Spark captures",
    "",
    "Answers Apache Spark gave to the provider's calls, written by `tests/live/spark-evidence.ts` from the services of `database-compose.yml` (spec 10.3).",
    'Each file is one RPC exchange: a `$captured` header, the outcome, and the decoded answer with every bytes field as `{"$bytes": "<base64>"}`, Arrow bodies byte for byte.',
    "Client-chosen ids are written `<session>`, `<operation>` and `<tag>`, the token `<token>`, and every value under `spark.sql.catalog.` `<value>`.",
    "Never edit a capture by hand: start the service, run `bun tests/live/spark-evidence.ts --setup <service>` and `--capture <service>`, then `--readme`.",
    "",
    "## BUILT cases",
    "",
    "The integration replay builds two cases from these captures and labels them: the version refusal (the 4.2.0 version answer rewritten to `3.5.9` and `4.0.4`) and a nested namespace walk (the `SHOW NAMESPACES` answer rewritten to multi-part spellings), because no catalog of the open-source fixture nests.",
    "",
    "## Answers to the spec's open items",
    "",
    "| Item | Captures | Recorded |",
    "|---|---|---|",
    ...answerLines(),
  ];
  for (const version of ["4.2.0", "4.1.3"] as const) {
    lines.push(
      "",
      `## ${version}`,
      "",
      "| Capture | Service | RPC | Surface | Outcome | Date | Runtime |",
      "|---|---|---|---|---|---|---|",
    );
    const directory = path.join(FIXTURES, version);
    const files = existsSync(directory)
      ? readdirSync(directory)
          .filter((file) => file.endsWith(".json"))
          .sort()
      : [];
    for (const file of files) {
      const capture = stored(version, file.slice(0, -".json".length)) as StoredCapture;
      const { service, rpc, surface, date, runtime } = capture.$captured;
      lines.push(
        `| \`${file.slice(0, -5)}\` | ${service} | ${rpc} | ${cell(surface)} | ${capture.outcome} | ${date} | ${runtime} |`,
      );
    }
  }
  return `${lines.join("\n")}\n`;
}

// -- the command ----------------------------------------------------------------------------------------------------

function checkContainer(service: SparkLiveService): void {
  const { image, digest } = SPARK_SERVICE_IMAGES[service];
  const running = execFileSync("docker", ["inspect", "--format", "{{.Config.Image}}", CONTAINER_OF[service]], {
    encoding: "utf8",
  }).trim();
  if (running !== `${image}@${digest}`)
    throw new Error(`${CONTAINER_OF[service]} runs ${running}, not the pinned ${image}@${digest}`);
}

async function readVersion(ctx: Context, token: Token): Promise<string> {
  const ids: Ids = { session: randomUUID(), operations: [], tags: [] };
  const answer = await unary(ctx, "AnalyzePlan", { ...base(ids), spark_version: {} }, token);
  if (answer.outcome === "error")
    throw new Error(`the version read failed (${answer.code}): ${maskSparkLive(answer.details, ctx.secrets)}`);
  return (answer.payload as { readonly spark_version: { readonly version: string } }).spark_version.version;
}

async function main(argv: readonly string[]): Promise<number> {
  const [flag, value] = argv;
  if (flag === "--readme" && argv.length === 1) {
    writeFileSync(path.join(FIXTURES, "README.md"), renderSparkFixturesReadme());
    console.log("Wrote tests/fixtures/spark/README.md");
    return 0;
  }
  const services: readonly string[] =
    flag === "--setup" ? ["spark", "spark-4-1", "spark-auth"] : ["spark", "spark-4-1", "spark-auth", "spark-tls"];
  if (
    (flag !== "--capture" && flag !== "--setup") ||
    argv.length !== 2 ||
    value === undefined ||
    !services.includes(value)
  ) {
    console.error(USAGE);
    return 2;
  }
  const args = readSparkLiveArguments(process.env);
  if ("exitCode" in args) {
    console.error(args.message);
    return args.exitCode;
  }
  const service = value as SparkLiveService;
  checkContainer(service);
  const ctx: Context = {
    args,
    service,
    version: VERSION_OF[service],
    client: openClient(args, service),
    secrets: sparkLiveSecrets(args),
  };
  const token: Token = service === "spark-auth" || service === "spark-tls" ? "configured" : "none";
  try {
    sparkVersion = await readVersion(ctx, token);
    if (sparkVersion !== ctx.version) throw new Error(`${service} answers Spark ${sparkVersion}, not ${ctx.version}`);
    if (flag === "--setup") {
      if (service === "spark") {
        execFileSync("docker", [
          "exec",
          "-u",
          "0",
          "libredb-spark",
          "sh",
          "-c",
          "mkdir -p /tmp/studio-unreadable && printf 'a\\n1\\n' > /tmp/studio-unreadable/data.csv && chmod 000 /tmp/studio-unreadable/data.csv",
        ]);
      }
      const ids: Ids = { session: randomUUID(), operations: [], tags: [] };
      const setup = await unary(ctx, "Config", configSet(ids, SPARK_CAPTURE_SESSION_CONF), token);
      if (setup.outcome === "error") throw new Error(`the session setup failed (${setup.code})`);
      for (const sql of SPARK_SAMPLE_STATEMENTS) {
        // oxlint-disable-next-line no-await-in-loop -- a sample statement reads the objects the ones before it created.
        await runUncaptured(ctx, ids, sql, token);
        console.log(`ok  ${sql.slice(0, 100)}`);
      }
      await unary(ctx, "ReleaseSession", { ...base(ids), allow_reconnect: false }, token);
      return 0;
    }
    const entries = SPARK_CAPTURE_CATALOG.filter((entry) =>
      service === "spark-4-1"
        ? entry.service === "spark" && entry.versions.includes("4.1.3")
        : entry.service === service && entry.versions.includes("4.2.0"),
    );
    for (const entry of entries) {
      // oxlint-disable-next-line no-await-in-loop -- one capture at a time, in the catalog's order, so the server is idle.
      await runEntry(ctx, entry);
      void capturesOf(entry);
    }
    return 0;
  } catch (error) {
    console.error(maskSparkLive(error instanceof Error ? error.message : String(error), ctx.secrets));
    return 1;
  } finally {
    ctx.client.close();
  }
}

if (import.meta.main) process.exit(await main(process.argv.slice(2)));
