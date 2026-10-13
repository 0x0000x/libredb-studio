import type { ParsedConnection } from "@/lib/connection-string-parser";

/**
 * The Paste URL reader of an Apache Spark Connect address (spec 3.2): Spark's documented grammar
 * `sc://host[:port][/][;key=value]*`, the scheme fixed, the path empty, parameter names case-sensitive, port 15002 by
 * default. Pure, with type-only imports: `src/lib/connection-string-parser.ts` imports it, and the browser carries
 * that module. A refusal fills nothing and never repeats the text it refuses.
 */
export const SPARK_PASTE_SENTENCES = Object.freeze({
  pathNotEmpty: "A Spark Connect address has no path: remove everything between the host and the first ;.",
  userInfo: "Host takes no user name or password inside the address: put the token in Token.",
  databricks:
    "This is a Databricks Connect address. Studio's Apache Spark connection reaches open-source Spark Connect servers; Databricks is not supported in this version.",
  jdbcPrefix: "Paste the sc:// address itself, without jdbc:.",
  scheme: "A Spark Connect address starts with sc://.",
  host: "The address names no valid host: expected a host name, an IPv4 address or an IPv6 address in brackets.",
  port: "The address names no valid port: expected an integer from 1 to 65535.",
});

const SCHEME = "sc://";
const JDBC_SCHEME = "jdbc:sc://";
/** Spark Connect's default port, `SPARK_DEFAULT_PORT` in `constants.ts`, written here because this module imports types only. */
const DEFAULT_PORT = "15002";
/** The parameter only a Databricks Connect address carries; the same text can hold a workspace token. */
const DATABRICKS_PARAMETER = "x-databricks-cluster-id";
/** Spark's own parameters Studio does not apply, named in the notice: it sends a fixed identity, opens a session per request and sets its own receive cap. */
const NAMED_NOT_APPLIED: ReadonlySet<string> = new Set([
  "user_id",
  "user_agent",
  "session_id",
  "grpc_max_message_size",
]);

// The host and port rules of src/lib/connection-host-uri.ts:37-45, copied because this module imports types only.
const LABEL = /^[a-z0-9_](?:[a-z0-9_-]{0,61}[a-z0-9_])?$/i;
const NUMERIC_LABEL = /^(?:\d+|0x[0-9a-f]*)$/i;
const IPV4 = /^(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)$/;
const MAX_HOSTNAME_LENGTH = 253;
const PORT_DIGITS = /^\d{1,5}$/;
const MAX_PORT = 65535;
const IPV6_CHARACTERS = /^[0-9a-f:.]+$/i;

function refused(sentence: string, discardPaste?: true): ParsedConnection {
  return { type: "spark", refusal: sentence, ...(discardPaste === undefined ? {} : { discardPaste }) };
}

/** The parameter names after the first `;`, values never read. */
function parameterNames(text: string): string[] {
  const semicolon = text.indexOf(";");
  if (semicolon === -1) return [];
  return text
    .slice(semicolon + 1)
    .split(";")
    .map((part) => part.split("=")[0]);
}

/** The authority's host and port texts, or null where it is not `host`, `host:port`, `[v6]` or `[v6]:port`. */
function splitAuthority(authority: string): { readonly host: string; readonly port?: string } | null {
  if (authority.startsWith("[")) {
    const close = authority.indexOf("]");
    if (close === -1) return null;
    const host = authority.slice(0, close + 1);
    const after = authority.slice(close + 1);
    if (after === "") return { host };
    return after.startsWith(":") ? { host, port: after.slice(1) } : null;
  }
  const parts = authority.split(":");
  if (parts.length === 1) return { host: authority };
  return parts.length === 2 ? { host: parts[0], port: parts[1] } : null;
}

function isHostname(name: string): boolean {
  if (name.length === 0 || name.length > MAX_HOSTNAME_LENGTH) return false;
  const labels = name.split(".");
  return labels.every((label) => LABEL.test(label)) && !NUMERIC_LABEL.test(labels[labels.length - 1]);
}

/** A bracketed IPv6 literal in lower case, brackets kept, or null. */
function ipv6Of(address: string): string | null {
  if (!IPV6_CHARACTERS.test(address) || !URL.canParse(`http://[${address}]/`)) return null;
  return `[${address.toLowerCase()}]`;
}

function hostOf(text: string): string | null {
  if (text.startsWith("[")) return ipv6Of(text.slice(1, -1));
  if (IPV4.test(text)) return text;
  return isHostname(text) ? text.toLowerCase() : null;
}

function portOf(text: string): string | null {
  const value = PORT_DIGITS.test(text) ? Number(text) : 0;
  return value >= 1 && value <= MAX_PORT ? String(value) : null;
}

function notAppliedNotice(names: readonly string[], unnamed: number): string {
  const counted = `${unnamed} ${names.length > 0 ? "other " : ""}parameter${unnamed === 1 ? "" : "s"}`;
  const listed = [names.join(", "), unnamed > 0 ? counted : ""].filter(Boolean).join(" and ");
  return `Not applied: ${listed}. Studio's Apache Spark connection takes the host, the port, token and use_ssl from an sc:// address: it sends one fixed user id, opens a Spark session per request and sets its own message size limit.`;
}

/** An `sc://` address as the dialog's fields, or a refusal that fills nothing. */
export function parseSparkConnectUri(text: string): ParsedConnection {
  const trimmed = text.trim();
  // First, and with the box cleared: a Databricks Connect address can carry a workspace token.
  if (parameterNames(trimmed).includes(DATABRICKS_PARAMETER)) return refused(SPARK_PASTE_SENTENCES.databricks, true);
  if (trimmed.startsWith(JDBC_SCHEME)) return refused(SPARK_PASTE_SENTENCES.jdbcPrefix);
  if (!trimmed.startsWith(SCHEME)) return refused(SPARK_PASTE_SENTENCES.scheme);
  const rest = trimmed.slice(SCHEME.length);
  const semicolon = rest.indexOf(";");
  const head = semicolon === -1 ? rest : rest.slice(0, semicolon);
  if (head.includes("@")) return refused(SPARK_PASTE_SENTENCES.userInfo);
  const slash = head.indexOf("/");
  if (slash !== -1 && head.slice(slash) !== "/") return refused(SPARK_PASTE_SENTENCES.pathNotEmpty);
  const split = splitAuthority(slash === -1 ? head : head.slice(0, slash));
  const host = split === null ? null : hostOf(split.host);
  if (split === null || host === null) return refused(SPARK_PASTE_SENTENCES.host);
  const port = split.port === undefined ? DEFAULT_PORT : portOf(split.port);
  if (port === null) return refused(SPARK_PASTE_SENTENCES.port);

  let token: string | undefined;
  let useSsl: boolean | undefined;
  const named: string[] = [];
  let unnamed = 0;
  const parameters = semicolon === -1 ? [] : rest.slice(semicolon + 1).split(";");
  for (const part of parameters) {
    if (part === "") continue;
    const equals = part.indexOf("=");
    const key = equals === -1 ? "" : part.slice(0, equals);
    const value = part.slice(equals + 1);
    if (key === "token") {
      token = value === "" ? undefined : value;
    } else if (key === "use_ssl" && (value === "true" || value === "false")) {
      useSsl = value === "true";
    } else if (key === "use_ssl" || NAMED_NOT_APPLIED.has(key)) {
      if (!named.includes(key)) named.push(key);
    } else {
      // A text with no name=, or a name Spark does not define: counted, never named, since its value may be a secret.
      unnamed += 1;
    }
  }
  // Spark's 4.2.0 Python and Scala clients turn TLS on with a token unless the host is literally localhost.
  const sslMode: ParsedConnection["sslMode"] =
    useSsl !== undefined
      ? useSsl
        ? "verify-system"
        : "disable"
      : token !== undefined && host !== "localhost"
        ? "verify-system"
        : "disable";
  return {
    type: "spark",
    host,
    port,
    ...(token === undefined ? {} : { password: token }),
    sslMode,
    ...(named.length > 0 || unnamed > 0 ? { notice: notAppliedNotice(named, unnamed) } : {}),
  };
}
