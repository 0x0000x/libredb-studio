/**
 * The evidence scrub of the Snowflake harness: the only path from a raw SQL API exchange with a Snowflake account to a
 * file under tests/fixtures/snowflake/.
 *
 * `exchange` keeps only the allow-listed headers and body fields, recording the names of the fields it dropped, and
 * puts stable placeholders in place of what identifies an account or a person: every bearer token, the PATs it is
 * given and every JWT-shaped string (`<token-N>`, so the Authorization header reads `Bearer <token-N>` and a later
 * token of the same run reads as a different one), every UUID, which covers statement handles, query ids, request ids
 * and login-failure ids (`00000000-0000-0000-0000-00000000000N`, still UUID-shaped, so a capture replays through the
 * provider's handle check), the host, the account locator, the organisation and account names, user names
 * (`<user-N>`, numbered by their place in the secrets, matched in any case since Snowflake folds a login name to
 * upper case) and IP addresses (`<ip-N>`). A placeholder is stable for the life of the scrubber, so one handle reads
 * the same in every capture of a run. Replacement runs over the whole exchange, a failure's message beside its
 * `sqlState` included, so a text that names the account names a placeholder instead.
 *
 * `render` turns a set of files into text and refuses the whole set, writing nothing, while any file holds a PAT or a
 * passphrase (raw, percent-encoded, base64 in either alphabet, or escaped inside a JSON text), a JWT-shaped string, a
 * private key's armour, the host or any Snowflake host, the account locator, the organisation or account name, the
 * egress IP in dotted or dashed form, or an email address. It replaces nothing itself: every capture reaches it
 * through `exchange`, so what it finds is what `exchange` could not replace. Keys are scanned as values are, and every
 * string that reads as base64, and every one of up to 64 KiB that parses as JSON, is also scanned decoded, up to
 * four layers deep. The error names the file and what it holds, never the value.
 */

/** What a run sends or meets that must never reach a capture. Only `users` is required. */
export interface SnowflakeEvidenceSecrets {
  /** Login names, as configured. */
  readonly users: readonly string[];
  /**
   * Names no capture may hold anywhere, the operator's user name among `users`: replaced in exchanges like every user
   * name, and refused wherever else they appear. The three principals are not here, because their setup names are
   * the fixture's own (each scenario's `connection` block names the user the replay signs as).
   */
  readonly privateNames?: readonly string[];
  readonly pats?: readonly string[];
  readonly passphrases?: readonly string[];
  readonly accountLocator?: string;
  readonly organization?: string;
  readonly accountName?: string;
  /** The account's host, such as `<org>-<account>.snowflakecomputing.com`. */
  readonly host?: string;
  readonly egressIp?: string;
}

/** One request and its answer, as the harness saw them; a partition's body is given decoded. */
export interface SnowflakeRawExchange {
  readonly request: {
    readonly method: string;
    /** The path and the query string. */
    readonly path: string;
    readonly headers: Readonly<Record<string, string>>;
    readonly body?: unknown;
  };
  readonly response: {
    readonly status: number;
    readonly headers: Readonly<Record<string, string>>;
    readonly body: string;
  };
}

/** An exchange as it may be written: allow-listed, with placeholders, and the names of the fields it dropped. */
export interface SnowflakeScrubbedExchange {
  readonly request: {
    readonly method: string;
    readonly path: string;
    readonly headers: Record<string, string>;
    readonly body?: unknown;
    readonly dropped: readonly string[];
  };
  readonly response: {
    readonly status: number;
    readonly headers: Record<string, string>;
    readonly body: unknown;
    readonly dropped: readonly string[];
  };
}

export const SNOWFLAKE_EVIDENCE_LEAK_SENTENCE =
  "The evidence holds a credential, a private key, the host, the account, the egress IP or an email address: nothing was written.";

export class SnowflakeEvidenceLeakError extends Error {
  constructor(readonly findings: readonly string[]) {
    super(SNOWFLAKE_EVIDENCE_LEAK_SENTENCE);
    this.name = "SnowflakeEvidenceLeakError";
  }
}

const REQUEST_HEADERS = [
  "authorization",
  "x-snowflake-authorization-token-type",
  "content-type",
  "accept",
  "user-agent",
];
const RESPONSE_HEADERS = ["content-type", "content-encoding", "retry-after"];

/** An allow-list: `true` keeps the value whole, an object keeps only its own fields of an object value. */
type Shape = { readonly [field: string]: true | Shape };

const REQUEST_BODY: Shape = {
  statement: true,
  timeout: true,
  database: true,
  schema: true,
  warehouse: true,
  role: true,
  parameters: {
    multi_statement_count: true,
    query_tag: true,
    client_result_chunk_size: true,
    rows_per_resultset: true,
    timezone: true,
  },
};
const ANSWER: Shape = {
  code: true,
  sqlState: true,
  message: true,
  statementHandle: true,
  statementHandles: true,
  statementStatusUrl: true,
  createdOn: true,
  resultSetMetaData: { numRows: true, format: true, rowType: true, partitionInfo: true },
  data: true,
  stats: true,
};

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
const UUID_PLACEHOLDER = /^00000000-0000-0000-0000-\d{12}$/;
const JWT = /eyJ[A-Za-z0-9_-]{2,}\.eyJ[A-Za-z0-9_-]{2,}\.[A-Za-z0-9_-]+/g;
const BEARER = /^Bearer (.+)$/;
const IPV4 = /(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])/g;
const IPV6 =
  /(?<![\w:])(?:(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}|(?:[0-9a-f]{1,4}:){1,6}:(?:[0-9a-f]{1,4}(?::[0-9a-f]{1,4}){0,5})?)(?![\w:])/gi;
const EMAIL = /[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}/;
const SNOWFLAKE_HOST = /[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*\.snowflakecomputing\.(?:com|cn)/i;
const PRIVATE_KEY = /-----BEGIN [A-Z ]*PRIVATE KEY-----/;
const BASE64 = /^[A-Za-z0-9+/_-]{8,}={0,2}$/;
const NESTED_TEXT_LIMIT = 64 * 1024;
const NESTED_DEPTH_LIMIT = 4;
const JSON_TEXT = /^\s*[[{"]/;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const escapeRegExp = (text: string): string => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/** A name matched in any case where no letter, digit or `_` stands right before or after it. */
/**
 * A user or account name in any case, in its underscore and its hyphen spelling, as a whole name or joined to other
 * words by `_`: Snowflake writes an account name with `_` and its host with `-`, and an answer can carry a name inside
 * a longer identifier (`<user>_ROLE`), which a word boundary that counts `_` as a letter would miss.
 */
const name = (value: string): RegExp => {
  const spellings = [...new Set([value, value.replaceAll("_", "-"), value.replaceAll("-", "_")])];
  return new RegExp(`(?<![A-Za-z0-9])(?:${spellings.map(escapeRegExp).join("|")})(?![A-Za-z0-9])`, "gi");
};

/** Each spelling of a secret a text may carry: raw, percent-encoded, base64 in both alphabets, and JSON-escaped. */
function secretForms(secret: string): string[] {
  const bytes = Buffer.from(secret, "utf8");
  const escaped = JSON.stringify(secret).slice(1, -1);
  return [
    secret,
    encodeURIComponent(secret),
    bytes.toString("base64").replace(/=+$/, ""),
    bytes.toString("base64url"),
    ...(escaped === secret ? [] : [escaped]),
  ];
}

/** The value restricted to the shape's fields, and the dotted names of those it dropped. */
function pick(value: unknown, shape: Shape, prefix: string, dropped: string[]): unknown {
  if (!isRecord(value)) return value;
  const kept: Record<string, unknown> = {};
  for (const [field, inner] of Object.entries(value)) {
    const rule = shape[field];
    if (rule === undefined) dropped.push(`${prefix}${field}`);
    else kept[field] = rule === true ? inner : pick(inner, rule, `${prefix}${field}.`, dropped);
  }
  return kept;
}

function nestedJson(text: string): unknown {
  if (text.length > NESTED_TEXT_LIMIT || !JSON_TEXT.test(text)) return undefined;
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}

/** Every string and key of a JSON value, each base64-looking or JSON text also read decoded, a few layers deep. */
function stringsOf(value: unknown, out: string[] = [], depth = 0): string[] {
  if (typeof value === "string") textsOf(value, out, depth);
  else if (Array.isArray(value)) for (const item of value) stringsOf(item, out, depth);
  else if (isRecord(value))
    for (const [key, item] of Object.entries(value)) {
      textsOf(key, out, depth);
      stringsOf(item, out, depth);
    }
  return out;
}

function textsOf(text: string, out: string[], depth: number): void {
  out.push(text);
  if (depth >= NESTED_DEPTH_LIMIT) return;
  if (BASE64.test(text)) textsOf(Buffer.from(text, "base64").toString("utf8"), out, depth + 1);
  const nested = nestedJson(text);
  if (nested !== undefined) stringsOf(nested, out, depth + 1);
}

export class SnowflakeEvidenceScrubber {
  private readonly tokens = new Map<string, string>();
  private readonly uuids = new Map<string, string>();
  private readonly ips = new Map<string, string>();

  constructor(private readonly secrets: SnowflakeEvidenceSecrets) {}

  /** The exchange with only allow-listed parts and every identifier replaced. */
  exchange(raw: SnowflakeRawExchange): SnowflakeScrubbedExchange {
    const requestDropped: string[] = [];
    const responseDropped: string[] = [];
    let answer: unknown;
    try {
      answer = pick(JSON.parse(raw.response.body), ANSWER, "", responseDropped);
    } catch {
      answer = { text: raw.response.body };
    }
    const request = {
      method: raw.request.method,
      path: raw.request.path,
      headers: this.headers(raw.request.headers, REQUEST_HEADERS),
      ...(raw.request.body === undefined ? {} : { body: pick(raw.request.body, REQUEST_BODY, "", requestDropped) }),
      dropped: requestDropped.sort(),
    };
    const response = {
      status: raw.response.status,
      headers: this.headers(raw.response.headers, RESPONSE_HEADERS),
      body: answer,
      dropped: responseDropped.sort(),
    };
    const text = JSON.stringify({ request, response });
    this.collect(text);
    return JSON.parse(this.replace(text)) as SnowflakeScrubbedExchange;
  }

  /** Each file as indented JSON with a final newline, or a {@link SnowflakeEvidenceLeakError} and no text at all. */
  render(files: Readonly<Record<string, unknown>>): Record<string, string> {
    const rendered: Record<string, string> = {};
    const findings: string[] = [];
    for (const [name, value] of Object.entries(files)) {
      const text = `${JSON.stringify(value, null, 2)}\n`;
      const corpus = stringsOf(JSON.parse(text)).join("\n");
      for (const label of this.leaks(corpus)) findings.push(`${name} holds the ${label}`);
      rendered[name] = text;
    }
    if (findings.length > 0) throw new SnowflakeEvidenceLeakError(findings);
    return rendered;
  }

  private headers(headers: Readonly<Record<string, string>>, allowed: readonly string[]): Record<string, string> {
    const kept: Record<string, string> = {};
    for (const [name, value] of Object.entries(headers)) {
      const key = name.toLowerCase();
      if (!allowed.includes(key)) continue;
      if (key === "authorization") {
        const token = BEARER.exec(value)?.[1];
        kept[key] = token === undefined ? "<authorization>" : `Bearer ${this.token(token)}`;
      } else kept[key] = value;
    }
    return kept;
  }

  private token(value: string): string {
    let placeholder = this.tokens.get(value);
    if (placeholder === undefined) {
      placeholder = `<token-${this.tokens.size + 1}>`;
      this.tokens.set(value, placeholder);
    }
    return placeholder;
  }

  /** Registers every token and UUID of the text in the order a reader meets them. */
  private collect(text: string): void {
    const found: { readonly at: number; readonly kind: "token" | "uuid"; readonly value: string }[] = [];
    for (const match of text.matchAll(JWT)) found.push({ at: match.index, kind: "token", value: match[0] });
    for (const pat of this.secrets.pats ?? []) {
      if (pat === "") continue;
      for (let at = text.indexOf(pat); at !== -1; at = text.indexOf(pat, at + pat.length)) {
        found.push({ at, kind: "token", value: pat });
      }
    }
    for (const match of text.matchAll(UUID)) found.push({ at: match.index, kind: "uuid", value: match[0] });
    found.sort((a, b) => a.at - b.at);
    for (const { kind, value } of found) {
      if (kind === "token") this.token(value);
      else if (!UUID_PLACEHOLDER.test(value) && !this.uuids.has(value.toLowerCase())) {
        this.uuids.set(value.toLowerCase(), `00000000-0000-0000-0000-${String(this.uuids.size + 1).padStart(12, "0")}`);
      }
    }
  }

  private ip(address: string): string {
    let placeholder = this.ips.get(address);
    if (placeholder === undefined) {
      placeholder = `<ip-${this.ips.size + 1}>`;
      this.ips.set(address, placeholder);
    }
    return placeholder;
  }

  /** The text with every registered token and UUID, the account's names, user names and IP addresses replaced. */
  private replace(text: string): string {
    let out = text;
    for (const [value, placeholder] of [...this.tokens].sort((a, b) => b[0].length - a[0].length)) {
      out = out.split(value).join(placeholder);
    }
    out = out.replace(UUID, (id) => this.uuids.get(id.toLowerCase()) ?? id);
    const { host, accountLocator, organization, accountName } = this.secrets;
    if (host !== undefined && host !== "") out = out.replace(new RegExp(escapeRegExp(host), "gi"), "<host>");
    const names: [string | undefined, string][] = [
      [accountLocator, "<account-locator>"],
      [organization, "<organization>"],
      [accountName, "<account>"],
    ];
    for (const [value, placeholder] of names)
      if (value !== undefined && value !== "") out = out.replace(name(value), placeholder);
    this.secrets.users.forEach((user, index) => {
      if (user !== "") out = out.replace(name(user), `<user-${index + 1}>`);
    });
    return out.replace(IPV4, (address) => this.ip(address)).replace(IPV6, (address) => this.ip(address));
  }

  /** The labels of what the text holds that no capture may. */
  private leaks(text: string): string[] {
    const labels: string[] = [];
    const { pats = [], passphrases = [], host, accountLocator, organization, accountName, egressIp } = this.secrets;
    const holds = (secrets: readonly string[]): boolean =>
      secrets.some((secret) => secret !== "" && secretForms(secret).some((form) => text.includes(form)));
    const named = (value: string | undefined): boolean => value !== undefined && value !== "" && name(value).test(text);
    if (holds(pats)) labels.push("PAT");
    if (holds(passphrases)) labels.push("passphrase");
    if (new RegExp(JWT.source).test(text)) labels.push("JWT");
    if (PRIVATE_KEY.test(text)) labels.push("private key");
    if (
      (host !== undefined && host !== "" && text.toLowerCase().includes(host.toLowerCase())) ||
      SNOWFLAKE_HOST.test(text)
    )
      labels.push("host");
    if (named(accountLocator)) labels.push("account locator");
    if (named(organization)) labels.push("organization");
    if (named(accountName)) labels.push("account name");
    if ((this.secrets.privateNames ?? []).some(named)) labels.push("private name");
    if (egressIp !== undefined && [egressIp, egressIp.replaceAll(".", "-")].some((form) => text.includes(form)))
      labels.push("egress IP");
    if (EMAIL.test(text)) labels.push("email");
    return labels;
  }
}
