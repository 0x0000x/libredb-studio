/**
 * The Snowflake Account field: the reading of an account identifier, an account URL or a Snowsight URL into the one
 * host Studio connects to and the account part of the key-pair JWT claim, the exact suffix test, and the sentences the
 * dialog and `connect()` both show.
 *
 * Pure, and it imports nothing at all, not even a type: the connection dialog and the paste parser import it in the
 * browser, while `account.ts`, which builds the origin from this reading, reaches `node:net` through the shared
 * endpoint module. `tests/unit/db/snowflake/browser-modules.test.ts` holds the empty import set.
 */

/** The most labels the identifier may have before `.snowflakecomputing.com` or `.snowflakecomputing.cn`. */
export const SNOWFLAKE_MAX_HOST_LABELS = 5;

const SUFFIX_COM = ".snowflakecomputing.com";
const SUFFIX_CN = ".snowflakecomputing.cn";
const SNOWSIGHT_HOST = "app.snowflake.com";
const LABEL = /^[a-z0-9_-]+$/;
const SNOWSIGHT_SEGMENT = /^[A-Za-z0-9_-]+$/;

/** The sentences of the Snowflake fields, shown by the dialog's rules and again by `connect()` before any socket. */
export const SNOWFLAKE_FIELD_SENTENCES = {
  accountEmpty: "Enter Account: an account identifier such as myorg-myaccount, or its URL. Nothing was sent.",
  accountFormat:
    "Enter an account identifier such as myorg-myaccount, or its https URL; a jdbc: or snowflake:// URL goes into Paste URL instead. Nothing was sent.",
  privateLinkRegion:
    "A PrivateLink account locator needs its region, such as xy12345.us-east-1.privatelink. Nothing was sent.",
  userEmpty: "Enter the user's login name. Nothing was sent.",
  roleEmpty: "Enter the role every statement runs as. Nothing was sent.",
  keyEmpty: "Paste the private key for key-pair sign-in. Nothing was sent.",
  tokenEmpty: "Enter the programmatic access token. Nothing was sent.",
  method: "Choose Key pair or Programmatic access token for Sign-in. Nothing was sent.",
} as const;

/**
 * The dialog's `fieldRules.host.format.pattern`: the shapes `readSnowflakeAccount` reads, loosely; connect() is exact.
 * It refuses a `jdbc:` or `snowflake://` text, and every text it accepts is then read exactly by connect().
 */
export const SNOWFLAKE_ACCOUNT_PATTERN =
  /^(?:[a-z0-9_-]+(?:\.[a-z0-9_-]+)*\.?|https:\/\/[a-z0-9_.-]+\.snowflakecomputing\.(?:com|cn)\.?(?::443)?\/?|https:\/\/app\.snowflake\.com\/[a-z0-9_-]+\/[a-z0-9_-]+(?:\/[^\s]*)?)$/i;

/** What an account URL carried besides its host, for `account.ts` to refuse; empty strings when absent. */
export interface SnowflakeUrlParts {
  readonly port: string;
  readonly path: string;
  readonly search: string;
  readonly hash: string;
  readonly userinfo: boolean;
}

export type SnowflakeAccountReading =
  | { readonly ok: true; readonly host: string; readonly accountClaim: string; readonly url: SnowflakeUrlParts | null }
  | { readonly ok: false; readonly refusal: string };

function refusal(sentence: string): SnowflakeAccountReading {
  return { ok: false, refusal: sentence };
}

/** The name lower-cased with one trailing dot removed. */
function canonical(name: string): string {
  const lower = name.toLowerCase();
  return lower.endsWith(".") ? lower.slice(0, -1) : lower;
}

/** The suffix `name` (already canonical) ends with at a label boundary, or undefined. */
function suffixOf(name: string): typeof SUFFIX_COM | typeof SUFFIX_CN | undefined {
  if (name.endsWith(SUFFIX_COM)) return SUFFIX_COM;
  if (name.endsWith(SUFFIX_CN)) return SUFFIX_CN;
  return undefined;
}

/**
 * Whether `host` is a Snowflake API host: lower-cased with one trailing dot removed, it ends with
 * `.snowflakecomputing.com` or `.snowflakecomputing.cn` at a label boundary, and the part before the suffix has 1 to
 * `SNOWFLAKE_MAX_HOST_LABELS` non-empty labels.
 */
export function isSnowflakeHost(host: string): boolean {
  const name = canonical(host);
  const suffix = suffixOf(name);
  if (suffix === undefined) return false;
  const labels = name.slice(0, -suffix.length).split(".");
  return labels.length <= SNOWFLAKE_MAX_HOST_LABELS && labels.every((label) => label !== "");
}

/**
 * The identifier (lower-cased, its suffix off), the suffix the host takes and the URL parts, read from an `https://`
 * text; null when refused.
 */
function readUrl(text: string): { identifier: string; cn: boolean; url: SnowflakeUrlParts | null } | null {
  let parsed: URL;
  try {
    parsed = new URL(text);
  } catch {
    return null;
  }
  if (parsed.hostname === SNOWSIGHT_HOST) {
    // A Snowsight URL's path, query and fragment are its own; Studio never sends a request to app.snowflake.com.
    const [, org, account] = parsed.pathname.split("/");
    if (org === undefined || account === undefined) return null;
    if (!SNOWSIGHT_SEGMENT.test(org) || !SNOWSIGHT_SEGMENT.test(account)) return null;
    return { identifier: `${org}-${account}`.toLowerCase(), cn: false, url: null };
  }
  if (!isSnowflakeHost(parsed.hostname)) return null;
  const name = canonical(parsed.hostname);
  const suffix = suffixOf(name) as string;
  return {
    identifier: name.slice(0, -suffix.length),
    cn: suffix === SUFFIX_CN,
    url: {
      port: parsed.port,
      path: parsed.pathname,
      search: parsed.search,
      hash: parsed.hash,
      userinfo: parsed.username !== "" || parsed.password !== "",
    },
  };
}

/**
 * Whether the identifier's labels still name a Snowflake domain once the suffix is off: a label holding
 * `snowflakecomputing`, even fused into a longer one, or the two labels `snowflake` and `com` in a row.
 */
function namesSnowflakeDomain(labels: readonly string[]): boolean {
  return labels.some(
    (label, i) => label.includes("snowflakecomputing") || (label === "snowflake" && labels[i + 1] === "com"),
  );
}

/**
 * The Account text read into the host Studio connects to and the JWT account claim (the identifier's first label,
 * upper-cased, as typed with `_` or `-`), or the sentence that refuses it.
 */
export function readSnowflakeAccount(text: string): SnowflakeAccountReading {
  const trimmed = text.trim();
  if (trimmed === "") return refusal(SNOWFLAKE_FIELD_SENTENCES.accountEmpty);

  const isHttps = trimmed.toLowerCase().startsWith("https://");
  if (trimmed.toLowerCase().startsWith("jdbc:") || (!isHttps && trimmed.includes("://"))) {
    return refusal(SNOWFLAKE_FIELD_SENTENCES.accountFormat);
  }

  let identifier: string;
  let cn = false;
  let url: SnowflakeUrlParts | null = null;
  if (isHttps) {
    const read = readUrl(trimmed);
    if (read === null) return refusal(SNOWFLAKE_FIELD_SENTENCES.accountFormat);
    ({ identifier, cn, url } = read);
  } else {
    // The suffix comes off once, here or in readUrl, so a doubled suffix leaves one behind to refuse.
    identifier = canonical(trimmed);
    const suffix = suffixOf(identifier);
    if (suffix !== undefined) {
      identifier = identifier.slice(0, -suffix.length);
      cn = suffix === SUFFIX_CN;
    }
  }

  const labels = identifier.split(".");
  if (namesSnowflakeDomain(labels)) return refusal(SNOWFLAKE_FIELD_SENTENCES.accountFormat);
  if (labels.length > SNOWFLAKE_MAX_HOST_LABELS || !labels.every((label) => LABEL.test(label))) {
    return refusal(SNOWFLAKE_FIELD_SENTENCES.accountFormat);
  }
  if (labels.length === 2 && labels[1] === "privatelink") {
    return refusal(SNOWFLAKE_FIELD_SENTENCES.privateLinkRegion);
  }

  return {
    ok: true,
    host: `${identifier}${cn ? SUFFIX_CN : SUFFIX_COM}`,
    accountClaim: labels[0].toUpperCase(),
    url,
  };
}

/** The Account field's hint once the text reads: "Studio connects to <host>.", or undefined. */
export function snowflakeHostHint(text: string): string | undefined {
  const reading = readSnowflakeAccount(text);
  return reading.ok ? `Studio connects to ${reading.host}.` : undefined;
}
