/**
 * The Snowflake API origin: `https://<host>:443`, built by the provider itself from the Account field's reading, so a
 * connection or a paste can never point the bearer token at another host.
 *
 * On top of the pure reading of `account-host.ts` this refuses what needs the shared host rules: an account URL with a
 * port other than 443, a path, a query, a fragment or userinfo, an IPv4 literal as the identifier, and a host
 * `validateHost` refuses. Every refusal is the Account format sentence, raised before any socket, and never repeats
 * the text, since a pasted URL can carry a token.
 */
import { type HttpOrigin, httpOrigin, validateHost } from "@/lib/db/http/endpoint";
import { readSnowflakeAccount, SNOWFLAKE_FIELD_SENTENCES } from "./account-host";
import { SnowflakeError } from "./errors";

export interface SnowflakeEndpoint {
  readonly origin: HttpOrigin;
  readonly host: string;
  /** The JWT account claim: the identifier's first label, upper-cased. */
  readonly accountClaim: string;
}

const IPV4_LITERAL = /^\d{1,3}(\.\d{1,3}){3}$/;

function formatRefusal(): SnowflakeError {
  return new SnowflakeError("config", SNOWFLAKE_FIELD_SENTENCES.accountFormat);
}

/** The origin, host and account claim of the Account field, or a `config` refusal before any socket. */
export function snowflakeEndpoint(account: unknown): SnowflakeEndpoint {
  if (typeof account !== "string") throw new SnowflakeError("config", SNOWFLAKE_FIELD_SENTENCES.accountEmpty);
  const reading = readSnowflakeAccount(account);
  if (!reading.ok) throw new SnowflakeError("config", reading.refusal);

  const { url, host, accountClaim } = reading;
  if (url !== null) {
    const extra =
      (url.port !== "" && url.port !== "443") ||
      (url.path !== "" && url.path !== "/") ||
      url.search !== "" ||
      url.hash !== "" ||
      url.userinfo;
    if (extra) throw formatRefusal();
  }

  const identifier = host.slice(0, host.lastIndexOf(".snowflakecomputing."));
  if (IPV4_LITERAL.test(identifier)) throw formatRefusal();

  try {
    validateHost(host);
  } catch {
    throw formatRefusal();
  }
  return { origin: httpOrigin("https", host, 443), host, accountClaim };
}
