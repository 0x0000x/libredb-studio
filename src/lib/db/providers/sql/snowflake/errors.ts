/**
 * The Snowflake error table: every way a request fails, as one `SnowflakeError` category and the sentence a person
 * reads, and the house class each category becomes at the provider's boundary.
 *
 * Classification reads Snowflake's code, the HTTP status, the transport kind and Studio's own state (the sign-in
 * method, whether the submit was accepted, the unit's operative keyword), never message text. The one value read out
 * of a message is the login-failure id of a refused key pair, and it is copied into a statement only when it has the
 * UUID shape.
 *
 * This module is the `serverText` entry: every server text passes `serverText` with the connection's secret forms
 * (the PAT, the key's passphrase and every JWT still in use, computed by the caller at each signing and passed as
 * `secretForms`) before any sentence is built from it, and is cut afterwards, so a text that holds a form is withheld
 * whole and never shown in part. A copyable statement names identifiers only through `snowflakeIdentifier`, prints
 * the public key only from Studio's own SPKI base64, and never holds the private key.
 *
 * Written for Snowflake rather than shared with Databend's table: a provider imports no other provider. Pure: no I/O,
 * no clock.
 */
import {
  AuthenticationError,
  ConnectionError,
  DatabaseConfigError,
  DatabaseError,
  QueryCancelledError,
  QueryError,
} from "@/lib/db/errors";
import { TransportError } from "@/lib/db/http/node-transport";
import { serverText } from "@/lib/db/utils/server-text";
import { quoteLiteral } from "@/lib/sql/values";
import type { DatabaseType } from "@/lib/types";

/** The one place the type id is written; every module of the provider names it through this constant. */
export const SNOWFLAKE: DatabaseType = "snowflake";

/** The longest server text a sentence carries, counted after `serverText` ran on the whole text. */
export const SNOWFLAKE_SERVER_TEXT_MAX_CHARS = 2_000;

export type SnowflakeErrorCategory = "config" | "auth" | "query" | "cancelled" | "unavailable" | "defect";

/** The one error every module of the provider throws; `toDatabaseError` turns it into the house class. */
export class SnowflakeError extends Error {
  /** A statement to copy and run to fix the refusal; only a refused key pair sets it, the administrator's form. */
  declare readonly fixStatement?: string;

  constructor(
    readonly category: SnowflakeErrorCategory,
    message: string,
    fixStatement?: string,
  ) {
    super(message);
    this.name = "SnowflakeError";
    if (fixStatement !== undefined) this.fixStatement = fixStatement;
    Object.setPrototypeOf(this, SnowflakeError.prototype);
  }
}

/** One Snowflake answer that failed: HTTP status, and the body's code, sqlState and message (raw). */
export interface SnowflakeFailure {
  readonly status: number;
  readonly code: string | null;
  readonly sqlState: string | null;
  readonly message: string | null;
  readonly request: "submit" | "status" | "partition" | "cancel";
}

/** Which bound a statement's `timeout` came from, which decides the sentence its deadline reads. */
export type SnowflakeDeadlineBound = "query-timeout" | "surface";

/** What a sentence names, and the secrets no sentence may echo. */
export interface SnowflakeFailureContext {
  readonly host: string;
  readonly user: string;
  readonly role: string;
  readonly authMethod: "keypair" | "pat";
  readonly accountClaim: string;
  readonly userClaim: string;
  readonly fingerprint: string | null;
  readonly fingerprintHex8: string | null;
  readonly publicKeyBody: string | null;
  readonly secretForms: readonly string[];
  readonly operativeKeyword: string | null;
  readonly attempts: number;
  /** The clamped `timeout` of the request, in seconds, which the deadline sentence names. */
  readonly timeoutS: number;
  /** What set `timeoutS`: the connection's query timeout, or the shorter bound of a statement Studio sends itself. */
  readonly deadlineBound: SnowflakeDeadlineBound;
  readonly submitAccepted: boolean;
}

/**
 * The registration statement an administrator runs, `<name>` standing for the user's NAME, which can differ from the
 * login name Studio knows. It leads the refusal and is its copyable statement, because the trial refused the
 * self-service form with 003001, by a message naming MODIFY PROGRAMMATIC AUTHENTICATION METHODS on the user (M97).
 */
function administratorStatement(fp8: string, body: string): string {
  return `ALTER USER <name> ADD KEY PAIR ${keyPairName(fp8)} PUBLIC_KEY = '${body}'`;
}

/** The self-service registration statement, a user signed in as itself, which the refusal names with that 003001. */
function selfServiceStatement(fp8: string, body: string): string {
  return `ALTER USER ADD KEY PAIR ${keyPairName(fp8)} PUBLIC_KEY = '${body}'`;
}

/**
 * Every fixed sentence of the error table, templates as functions, so the provider doc can quote them and a test
 * read them back. `keyMissing` is this module's own: a key-pair connection always has its key by the time Snowflake
 * answers, so reaching it is a defect.
 */
export const SNOWFLAKE_ERROR_SENTENCES = Object.freeze({
  keyPairRefused: (values: {
    readonly uuid: string | null;
    readonly account: string;
    readonly user: string;
    readonly fingerprint: string;
    readonly fp8: string;
    readonly body: string;
  }) => {
    const id = values.uuid === null ? "" : `, [${values.uuid}]`;
    const cause =
      values.uuid === null
        ? ""
        : `; an administrator can see the exact cause with SELECT SYSTEM$GET_LOGIN_FAILURE_DETAILS('${values.uuid}')`;
    return (
      `Snowflake refused the key pair (JWT token is invalid${id}). ` +
      `Studio signed as ${values.account}.${values.user} with the key whose fingerprint is ${values.fingerprint}. ` +
      `An administrator registers it with ${administratorStatement(values.fp8, values.body)} and checks it with SHOW USER KEY PAIRS FOR USER <name>, where <name> is the user's NAME, which DESC USER and SHOW USERS show and which can differ from the login name; ALTER USER <name> SET RSA_PUBLIC_KEY = '${values.body}' is the documented alternative. ` +
      `Signed in as this user, SHOW USER KEY PAIRS lists your keys; ${selfServiceStatement(values.fp8, values.body)} is refused with 003001 by a message that names MODIFY PROGRAMMATIC AUTHENTICATION METHODS on your own user as the privilege it needs. ` +
      `If the key is listed, check that the account and user are the ones the key was registered on and that this computer's clock is correct${cause}.`
    );
  },
  keyPairMidRun: (code: string) =>
    `Snowflake stopped accepting Studio's key-pair token while this statement ran (${code}). The statement may still be running; Studio could not read or cancel it.`,
  keyMissing:
    "Studio has no public key to name for this key-pair connection. This is a Studio defect; please report it.",
  patRefused:
    "Snowflake did not accept the access token: check that it is complete and has not expired or been revoked. A token also signs in only for a user under a network policy that allows the public address Studio connects from, which on a laptop changes with the network, or under an authentication policy that sets PAT_POLICY NETWORK_POLICY_EVALUATION = ENFORCED_NOT_REQUIRED. Key-pair sign-in avoids the network-policy requirement.",
  roleRefused: (role: string, user: string, roleId: string, userId: string) =>
    `Snowflake refused role ${role} for user ${user}: the role does not exist, it is not granted to this user, or the key pair or access token is restricted to another role. Check the Role field; an administrator grants it with GRANT ROLE ${roleId} TO USER ${userId}.`,
  roleMismatch: (actual: string, role: string) =>
    `Snowflake ran this connection as role ${actual}, not ${role}. Check the Role field; a role-restricted key or token signs in only as its own role.`,
  noWarehouse:
    "No warehouse is selected for this connection. Enter one in the connection settings, or ask an administrator to set DEFAULT_WAREHOUSE on the user. Browsing works without one; queries do not.",
  objectMissing: (role: string, roleLiteral: string, text: string) =>
    withText(
      `Snowflake says this object does not exist or role ${role} may not see it. Check the name, the database and schema, and the role; CALL EXPLAIN_PRIVILEGES(statement => '<statement>', missing_only => true) lists what is missing, and an administrator adds for_role => ${roleLiteral}.`,
      text,
    ),
  privileges: (role: string, text: string) => withText(`Role ${role} lacks a privilege this statement needs.`, text),
  forbidden:
    "Snowflake refused the request (403): the SQL API may not be enabled for this account, or a network policy blocks the address Studio connects from.",
  statementFailed: (message: string, code: string | null, sqlState: string | null) =>
    code === null
      ? `Snowflake: ${message}`
      : sqlState === null
        ? `Snowflake: ${message} (code ${code})`
        : `Snowflake: ${message} (code ${code}, SQLSTATE ${sqlState})`,
  deadline: (seconds: number) =>
    `Snowflake did not finish this statement within the connection's query timeout of ${seconds} s, so Studio cancelled it. Raise Query Timeout (ms) on the connection to allow longer statements; saving it ends statements still running on this connection.`,
  surfaceDeadline: (seconds: number) =>
    `Snowflake did not finish this Studio read within ${seconds} s, the bound Studio keeps for the statements it sends on its own whatever the connection's query timeout, so Studio cancelled it.`,
  statementTimeout: (seconds: number) =>
    `Snowflake ended this statement at a timeout and cancelled it: the connection's query timeout of ${seconds} s, or a lower STATEMENT_TIMEOUT_IN_SECONDS on the warehouse, the user or the account. Snowflake applies the lowest of these, so raising Query Timeout (ms) on the connection cannot lengthen a statement past the others.`,
  surfaceStatementTimeout: (seconds: number) =>
    `Snowflake ended this Studio read at a timeout and cancelled it: the ${seconds} s bound Studio keeps for the statements it sends on its own, or a lower STATEMENT_TIMEOUT_IN_SECONDS on the warehouse, the user or the account.`,
  unsupportedCommand: (text: string) => withText("Snowflake's SQL API does not run this command.", text),
  rateLimited: "Snowflake is limiting requests from this account (429). Try again in a moment.",
  unavailable: (status: number, attempts: number) =>
    `Snowflake did not answer this request (HTTP ${status}) after ${attempts} attempts.`,
  tlsRefused:
    "Snowflake refused the TLS certificate of this request (522). Studio sends no client certificate, so a proxy or gateway between Studio and Snowflake may be presenting one.",
  resultGone: "Snowflake no longer holds this statement's result; run it again.",
  invalidPayload:
    "Snowflake could not read the request Studio sent (390142). This is a Studio defect; please report it.",
  unreachable: (host: string) =>
    `Studio could not reach ${host}. Check the account identifier; a locator outside AWS Oregon needs its region.`,
  certMismatch: (host: string) =>
    `The certificate ${host} presented does not match its name. Check the account identifier, including the region for a locator.`,
  slotsBusy: (seconds: number) =>
    `Studio's Snowflake statement slots stayed busy for ${seconds} seconds, so nothing was sent. Try again when a running statement finishes.`,
  scriptingHint: "A Snowflake Scripting block runs through the SQL API only inside EXECUTE IMMEDIATE $$ ... $$.",
  cancelled: "The query was cancelled.",
});

const sentences = SNOWFLAKE_ERROR_SENTENCES;

/** A sentence, then one space and the server text when there is any. */
function withText(sentence: string, text: string): string {
  return text === "" ? sentence : `${sentence} ${text}`;
}

const BARE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_$]*$/;
const QUOTED_IDENTIFIER = /^"(?:[^"]|"")+"$/;
const UUID_SHAPE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
/** A code or SQLSTATE as Snowflake writes them (`002003`, `42S02`, `PAT_INVALID`); anything else is not printed. */
const CODE_SHAPE = /^[A-Za-z0-9_]{1,32}$/;
/** Snowflake's syntax error code, which a bare Scripting block fails with. */
const SYNTAX_ERROR_CODE = "001003";
const SCRIPTING_KEYWORDS: ReadonlySet<string> = new Set(["BEGIN", "DECLARE"]);
/** 390186 is what Snowflake answers, at HTTP 400 on the submit; 390189 and 3013 are the documented codes. */
const ROLE_REFUSED_CODES: ReadonlySet<string> = new Set(["390186", "390189", "3013"]);
/** The code of a command the SQL API refuses to run, such as `USE`. */
const UNSUPPORTED_COMMAND_CODE = "391911";
/**
 * The no-warehouse refusals: 000606 as documented, and 391920, which the trial answered at HTTP 422 to a statement
 * that needs compute under a role that can use no warehouse, with or without a warehouse field.
 */
const NO_WAREHOUSE_CODES: ReadonlySet<string> = new Set(["000606", "391920"]);
const UNAVAILABLE_STATUSES: ReadonlySet<number> = new Set([500, 502, 503, 504]);
/** The runtime codes, as the transport writes them at the end of a network failure, that mean no server was reached. */
const NOT_REACHED = /\((?:ENOTFOUND|EAI_AGAIN|EAI_NONAME|ECONNREFUSED|ConnectionRefused)\)$/;

/**
 * A server text as a sentence may carry it: `serverText` over the whole text first, so a secret anywhere withholds
 * it all, then cut to `SNOWFLAKE_SERVER_TEXT_MAX_CHARS`, never ending on half of a surrogate pair.
 */
export function serverMessage(raw: string, forms: readonly string[]): string {
  const text = serverText(raw, forms);
  if (text.length <= SNOWFLAKE_SERVER_TEXT_MAX_CHARS) return text;
  const cut = text.slice(0, SNOWFLAKE_SERVER_TEXT_MAX_CHARS);
  const last = cut.charCodeAt(cut.length - 1);
  return last >= 0xd800 && last <= 0xdbff ? cut.slice(0, -1) : cut;
}

/**
 * An identifier for a copyable statement: a bare name as is (Snowflake folds it as it folds the login name), a
 * well-formed quoted identifier as typed, and anything else quoted with each `"` doubled, so no value can end the
 * identifier early.
 */
function snowflakeIdentifier(value: string): string {
  if (BARE_IDENTIFIER.test(value) || QUOTED_IDENTIFIER.test(value)) return value;
  return `"${value.replaceAll('"', '""')}"`;
}

/** The name Studio registers a key under: one per key, so the statement is safe to run again. */
export function keyPairName(fingerprintHex8: string): string {
  return `libredb_studio_${fingerprintHex8}`;
}

/** A code or SQLSTATE for a sentence, or null when it is absent, not code-shaped, or holds a secret form. */
function printable(value: string | null, forms: readonly string[]): string | null {
  if (value === null || !CODE_SHAPE.test(value)) return null;
  return serverText(value, forms) === value ? value : null;
}

/** The login-failure id of a refused key pair: the last bracketed group of the text, when it has the UUID shape. */
function loginFailureId(text: string): string | null {
  const groups = [...text.matchAll(/\[([^\]]*)\]/g)];
  const last = groups.at(-1)?.[1];
  return last !== undefined && UUID_SHAPE.test(last) ? last : null;
}

/** A 401, or a refused token: the mid-run case first, then the sign-in method decides. */
function signInError(failure: SnowflakeFailure, context: SnowflakeFailureContext, text: string): SnowflakeError {
  if (context.authMethod === "keypair" && context.submitAccepted && failure.request !== "submit") {
    return new SnowflakeError(
      "auth",
      sentences.keyPairMidRun(printable(failure.code, context.secretForms) ?? "HTTP 401"),
    );
  }
  if (context.authMethod === "pat" || failure.code === "PAT_INVALID")
    return new SnowflakeError("auth", sentences.patRefused);
  const { fingerprint, fingerprintHex8: fp8, publicKeyBody: body } = context;
  if (fingerprint === null || fp8 === null || body === null) return new SnowflakeError("defect", sentences.keyMissing);
  const sentence = sentences.keyPairRefused({
    uuid: loginFailureId(text),
    account: context.accountClaim,
    user: context.userClaim,
    fingerprint,
    fp8,
    body,
  });
  return new SnowflakeError("auth", sentence, administratorStatement(fp8, body));
}

/** Studio's own deadline passed, worded by the bound that set it, since only the query timeout is the user's. */
export function deadlineError(timeoutS: number, bound: SnowflakeDeadlineBound): SnowflakeError {
  const sentences = SNOWFLAKE_ERROR_SENTENCES;
  return new SnowflakeError(
    "query",
    bound === "surface" ? sentences.surfaceDeadline(timeoutS) : sentences.deadline(timeoutS),
  );
}

/** A failed Snowflake answer as its category and sentence; the first matching row of the table wins. */
export function failureError(failure: SnowflakeFailure, context: SnowflakeFailureContext): SnowflakeError {
  const text = serverMessage(failure.message ?? "", context.secretForms);
  const { status, code } = failure;
  if (code !== null && ROLE_REFUSED_CODES.has(code)) {
    return new SnowflakeError(
      "auth",
      sentences.roleRefused(
        context.role,
        context.user,
        snowflakeIdentifier(context.role),
        snowflakeIdentifier(context.user),
      ),
    );
  }
  if (status === 401 || code === "PAT_INVALID") return signInError(failure, context, text);
  if (status === 403) return new SnowflakeError("auth", sentences.forbidden);
  if (status === 408) {
    // Snowflake ended the statement itself, at the request's `timeout` or a lower STATEMENT_TIMEOUT_IN_SECONDS; its
    // message names the timeout that fired, which is not read, so the sentence names every bound that can.
    return new SnowflakeError(
      "query",
      context.deadlineBound === "surface"
        ? sentences.surfaceStatementTimeout(context.timeoutS)
        : sentences.statementTimeout(context.timeoutS),
    );
  }
  if (status === 429) return new SnowflakeError("unavailable", sentences.rateLimited);
  if (UNAVAILABLE_STATUSES.has(status)) {
    return new SnowflakeError("unavailable", sentences.unavailable(status, context.attempts));
  }
  if (status === 522) return new SnowflakeError("unavailable", sentences.tlsRefused);
  if (status === 404) return new SnowflakeError("query", sentences.resultGone);
  if (code === "390142") return new SnowflakeError("defect", sentences.invalidPayload);
  if (code !== null && NO_WAREHOUSE_CODES.has(code)) return new SnowflakeError("query", sentences.noWarehouse);
  if (code === "002003") {
    return new SnowflakeError("query", sentences.objectMissing(context.role, quoteLiteral(context.role, SNOWFLAKE), text));
  }
  if (code === "003001") return new SnowflakeError("query", sentences.privileges(context.role, text));
  if (code === UNSUPPORTED_COMMAND_CODE) return new SnowflakeError("query", sentences.unsupportedCommand(text));
  const failed = sentences.statementFailed(
    text,
    printable(code, context.secretForms),
    printable(failure.sqlState, context.secretForms),
  );
  const scripting =
    status === 422 &&
    code === SYNTAX_ERROR_CODE &&
    context.operativeKeyword !== null &&
    SCRIPTING_KEYWORDS.has(context.operativeKeyword.toUpperCase());
  return new SnowflakeError("query", scripting ? `${failed} ${sentences.scriptingHint}` : failed);
}

/**
 * The shared transport's failure, when it means Snowflake was not reached or did not prove its name: a host that
 * never answered, a DNS or connection refusal, a TLS failure. Anything else (an abort, a deadline, a cut body, the
 * limiter's refusal, a house error) is returned unchanged, for the caller to read.
 */
export function transportFailureError(error: unknown, host: string): unknown {
  if (!(error instanceof TransportError)) return error;
  if (
    error.kind === "unreachable" ||
    (error.kind === "network" && !error.truncated && NOT_REACHED.test(error.message))
  ) {
    return new SnowflakeError("unavailable", sentences.unreachable(host));
  }
  if (error.kind === "tls") return new SnowflakeError("unavailable", sentences.certMismatch(host));
  return error;
}

/** The house class of each category: the provider's boundary. Anything that is not a `SnowflakeError` is unchanged. */
export function toDatabaseError(error: unknown, sql?: string): unknown {
  if (!(error instanceof SnowflakeError)) return error;
  switch (error.category) {
    case "config":
      return new DatabaseConfigError(error.message, SNOWFLAKE);
    case "auth":
      return new AuthenticationError(
        error.message,
        SNOWFLAKE,
        error.fixStatement === undefined ? {} : { fixStatement: error.fixStatement },
      );
    case "query":
      return new QueryError(error.message, SNOWFLAKE, sql);
    case "cancelled":
      return new QueryCancelledError(error.message, SNOWFLAKE, sql);
    case "unavailable":
      return new ConnectionError(error.message, SNOWFLAKE);
    case "defect":
      return new DatabaseError(error.message, SNOWFLAKE);
  }
}
