/**
 * The evidence scrub of the Snowflake harness: what tests/live/snowflake-evidence.ts may write under
 * tests/fixtures/snowflake/. Only allow-listed headers and body fields are kept; the Authorization header, every JWT
 * and PAT, the account locator, the organisation and account names, the host, user names, IP addresses, statement
 * handles and query ids become stable placeholders, the same one for the same value across every file of a run; and
 * no file is written while any of them, a passphrase, a private key or an email address is still there. Every
 * committed fixture re-renders unchanged through the scrub.
 */
import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import {
  SNOWFLAKE_EVIDENCE_LEAK_SENTENCE,
  SnowflakeEvidenceLeakError,
  SnowflakeEvidenceScrubber,
  type SnowflakeEvidenceSecrets,
  type SnowflakeRawExchange,
} from "../../../helpers/snowflake-evidence-scrub";

const HANDLE = "01b8a3c2-0000-4f1e-0000-00d5000a1b2e";
const HANDLE_2 = "01b8a3c2-0000-4f1e-0000-00d5000a1c41";
const REQUEST_ID = "3f2b6c1e-9d4a-4e2b-8c71-5a0e9b7d3f10";
const LOGIN_FAILURE = "8c1d2e3f-4a5b-4c6d-8e7f-901a2b3c4d5e";
const PAT = "pat-sentinel-0123456789abcdefghijklmnop";
const PASSPHRASE = "passphrase-sentinel-42";
// A JWT's shape, signed by nobody: the scrub reads only its form.
const JWT = [
  Buffer.from(JSON.stringify({ alg: "RS256", typ: "JWT" })).toString("base64url"),
  Buffer.from(JSON.stringify({ iss: "XY12345.STUDIO_SVC.SHA256:abc", sub: "XY12345.STUDIO_SVC" })).toString(
    "base64url",
  ),
  "c2lnbmF0dXJlLXNlbnRpbmVs",
].join(".");
const JWT_2 = `${JWT.slice(0, -4)}QUJD`;

const SECRETS: SnowflakeEvidenceSecrets = {
  users: ["studio_svc", "studio_reader", "trial_admin"],
  privateNames: ["trial_admin"],
  pats: [PAT],
  passphrases: [PASSPHRASE],
  accountLocator: "XY12345",
  organization: "ACMEORG",
  accountName: "PROBE_ACCT",
  host: "acmeorg-probe_acct.snowflakecomputing.com",
  egressIp: "203.0.113.7",
};

function answer(overrides: Record<string, unknown> = {}): string {
  return JSON.stringify({
    code: "090001",
    sqlState: "00000",
    message: "Statement executed successfully.",
    statementHandle: HANDLE,
    statementStatusUrl: `/api/v2/statements/${HANDLE}?requestId=${REQUEST_ID}`,
    createdOn: 1791800000000,
    resultSetMetaData: {
      numRows: 1,
      format: "jsonv2",
      rowType: [{ name: "N", type: "fixed", length: null, precision: 38, scale: 0, nullable: true }],
      partitionInfo: [{ rowCount: 1, uncompressedSize: 8 }],
      futureMeta: "dropped",
    },
    data: [["1"]],
    unknownTopLevel: 1,
    ...overrides,
  });
}

function exchange(body: string = answer(), extra: Partial<SnowflakeRawExchange> = {}): SnowflakeRawExchange {
  return {
    request: {
      method: "POST",
      path: `/api/v2/statements?async=true&requestId=${REQUEST_ID}`,
      headers: {
        Authorization: `Bearer ${JWT}`,
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        "Content-Type": "application/json",
        Accept: "application/json",
        "User-Agent": "LibreDB-Studio/0.18.3",
        Cookie: "dropped=1",
      },
      body: {
        statement: "SELECT 1",
        timeout: 60,
        warehouse: "STUDIO_PROBE_WH",
        role: "STUDIO_RW",
        parameters: { multi_statement_count: "1", query_tag: "libredb-studio:editor", extra: "dropped" },
        bindings: { dropped: true },
      },
    },
    response: {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Content-Encoding": "gzip",
        "Set-Cookie": "x=1",
        "X-Amz-Request-Id": "dropped",
      },
      body,
    },
    ...extra,
  };
}

function leakOf(action: () => unknown): SnowflakeEvidenceLeakError {
  try {
    action();
  } catch (error) {
    if (error instanceof SnowflakeEvidenceLeakError) return error;
    throw error;
  }
  throw new Error("no leak was reported");
}

describe("the allow-list", () => {
  test("keeps the named request and response headers, lower-cased, and drops the rest", () => {
    const scrubbed = new SnowflakeEvidenceScrubber(SECRETS).exchange(exchange());
    expect(Object.keys(scrubbed.request.headers).sort()).toEqual([
      "accept",
      "authorization",
      "content-type",
      "user-agent",
      "x-snowflake-authorization-token-type",
    ]);
    expect(Object.keys(scrubbed.response.headers).sort()).toEqual(["content-encoding", "content-type"]);
  });

  test("keeps the named body fields, nested ones included, and lists the names it dropped", () => {
    const scrubbed = new SnowflakeEvidenceScrubber(SECRETS).exchange(exchange());
    const body = scrubbed.response.body as Record<string, Record<string, unknown>>;
    expect(body.unknownTopLevel).toBeUndefined();
    expect(body.resultSetMetaData.futureMeta).toBeUndefined();
    expect(body.resultSetMetaData.rowType).toEqual([
      { name: "N", type: "fixed", length: null, precision: 38, scale: 0, nullable: true },
    ]);
    expect(scrubbed.response.dropped).toEqual(["resultSetMetaData.futureMeta", "unknownTopLevel"]);
    expect(scrubbed.request.body).toEqual({
      statement: "SELECT 1",
      timeout: 60,
      warehouse: "STUDIO_PROBE_WH",
      role: "STUDIO_RW",
      parameters: { multi_statement_count: "1", query_tag: "libredb-studio:editor" },
    });
    expect(scrubbed.request.dropped).toEqual(["bindings", "parameters.extra"]);
  });

  test("keeps a failure's code, sqlState and message", () => {
    const body = JSON.stringify({ code: "002003", sqlState: "42S02", message: "Object does not exist." });
    const scrubbed = new SnowflakeEvidenceScrubber(SECRETS).exchange(exchange(body));
    expect(scrubbed.response.body).toEqual({ code: "002003", sqlState: "42S02", message: "Object does not exist." });
  });

  test("keeps a body that is not JSON as text, through the same placeholders", () => {
    const scrubbed = new SnowflakeEvidenceScrubber(SECRETS).exchange(exchange("gateway 10.0.0.12 says no"));
    expect(scrubbed.response.body).toEqual({ text: "gateway <ip-1> says no" });
  });

  test("keeps a request with no body without one", () => {
    const scrubbed = new SnowflakeEvidenceScrubber(SECRETS).exchange({
      ...exchange(),
      request: { method: "GET", path: `/api/v2/statements/${HANDLE}`, headers: {} },
    });
    expect(scrubbed.request).toEqual({
      method: "GET",
      path: "/api/v2/statements/00000000-0000-0000-0000-000000000001",
      headers: {},
      dropped: [],
    });
  });
});

describe("the placeholders", () => {
  test("the Authorization header keeps its scheme and names its token by a placeholder, stable across files", () => {
    const scrubber = new SnowflakeEvidenceScrubber(SECRETS);
    const first = scrubber.exchange(exchange());
    const raw = exchange();
    const second = scrubber.exchange({
      ...raw,
      request: { ...raw.request, headers: { authorization: `Bearer ${JWT_2}` } },
    });
    const third = scrubber.exchange({
      ...raw,
      request: { ...raw.request, headers: { authorization: `Bearer ${JWT}` } },
    });
    expect(first.request.headers.authorization).toBe("Bearer <token-1>");
    expect(second.request.headers.authorization).toBe("Bearer <token-2>");
    expect(third.request.headers.authorization).toBe("Bearer <token-1>");
  });

  test("a PAT and a JWT anywhere in a body become the token's placeholder", () => {
    const scrubber = new SnowflakeEvidenceScrubber(SECRETS);
    const message = `bad token ${PAT}, then ${JWT}`;
    const scrubbed = scrubber.exchange(exchange(JSON.stringify({ code: "390303", message })));
    const text = (scrubbed.response.body as { message: string }).message;
    expect(text).toBe("bad token <token-2>, then <token-1>");
  });

  test("statement handles, request ids and query ids become stable UUID-shaped placeholders", () => {
    const scrubber = new SnowflakeEvidenceScrubber(SECRETS);
    const first = JSON.stringify(scrubber.exchange(exchange()));
    const second = scrubber.exchange(
      exchange(answer({ statementHandle: HANDLE_2, message: `see ${HANDLE}` }), {
        request: { method: "GET", path: `/api/v2/statements/${HANDLE_2}?partition=1`, headers: {} },
      }),
    );
    for (const id of [HANDLE, HANDLE_2, REQUEST_ID]) {
      expect(first).not.toContain(id);
      expect(JSON.stringify(second)).not.toContain(id);
    }
    expect(first).toContain('"statementHandle":"00000000-0000-0000-0000-000000000002"');
    expect(first).toContain("/api/v2/statements?async=true&requestId=00000000-0000-0000-0000-000000000001");
    expect(second.request.path).toBe("/api/v2/statements/00000000-0000-0000-0000-000000000003?partition=1");
    expect((second.response.body as { message: string }).message).toBe("see 00000000-0000-0000-0000-000000000002");
  });

  test("a placeholder already in a text is not numbered again", () => {
    const scrubber = new SnowflakeEvidenceScrubber(SECRETS);
    const placeholder = "00000000-0000-0000-0000-000000000009";
    const scrubbed = scrubber.exchange(exchange(answer({ statementHandle: placeholder })));
    expect((scrubbed.response.body as { statementHandle: string }).statementHandle).toBe(placeholder);
  });

  test("the account locator, organisation, account name and host become placeholders in a message, in any case", () => {
    const message = `Login failed for user STUDIO_SVC on account xy12345 (ACMEORG-probe_acct), host ACMEORG-PROBE_ACCT.snowflakecomputing.com [${LOGIN_FAILURE}]`;
    const scrubbed = new SnowflakeEvidenceScrubber(SECRETS).exchange(
      exchange(JSON.stringify({ code: "390144", sqlState: "08004", message })),
    );
    expect((scrubbed.response.body as { message: string }).message).toBe(
      "Login failed for user <user-1> on account <account-locator> (<organization>-<account>), host <host> [00000000-0000-0000-0000-000000000002]",
    );
  });

  test("each user name is replaced, inside a longer identifier too, so a database named after one carries no name", () => {
    const message = "User 'STUDIO_READER' cannot read STUDIO_READER_DB as studio_svc, nor STUDIO_READERS";
    const scrubbed = new SnowflakeEvidenceScrubber(SECRETS).exchange(
      exchange(JSON.stringify({ code: "003001", message })),
    );
    expect((scrubbed.response.body as { message: string }).message).toBe(
      "User '<user-2>' cannot read <user-2>_DB as <user-1>, nor STUDIO_READERS",
    );
  });

  test("the operator user, named after the three principals, is replaced in a SHOW USER KEY PAIRS row", () => {
    const scrubber = new SnowflakeEvidenceScrubber({
      ...SECRETS,
      users: ["studio_svc", "studio_reader", "studio_pat", "trial_admin"],
    });
    const scrubbed = scrubber.exchange(
      exchange(answer({ data: [["LIBREDB_STUDIO_1A2B3C4D", "TRIAL_ADMIN", "true"]] })),
    );
    expect((scrubbed.response.body as { data: string[][] }).data).toEqual([
      ["LIBREDB_STUDIO_1A2B3C4D", "<user-4>", "true"],
    ]);
  });

  test("IPv4 and IPv6 addresses are replaced, and versions and times are left alone", () => {
    const message = "from 203.0.113.7 and 2001:db8::7 at 12:34:56.789 on 9.30.1";
    const scrubbed = new SnowflakeEvidenceScrubber(SECRETS).exchange(exchange(JSON.stringify({ message })));
    expect((scrubbed.response.body as { message: string }).message).toBe(
      "from <ip-1> and <ip-2> at 12:34:56.789 on 9.30.1",
    );
  });

  test("a capture holding a sentinel of each kind re-renders with none of them", () => {
    const scrubber = new SnowflakeEvidenceScrubber(SECRETS);
    const message = `user studio_svc, account XY12345 of ACMEORG.PROBE_ACCT, ${SECRETS.host}, from 203.0.113.7, token ${PAT} ${JWT} [${LOGIN_FAILURE}]`;
    const scrubbed = scrubber.exchange(exchange(answer({ message })));
    const rendered = scrubber.render({ "a.json": scrubbed })["a.json"];
    for (const value of [
      "studio_svc",
      "XY12345",
      "ACMEORG",
      "PROBE_ACCT",
      SECRETS.host as string,
      "203.0.113.7",
      PAT,
      JWT,
      HANDLE,
      REQUEST_ID,
      LOGIN_FAILURE,
    ]) {
      expect(rendered.toLowerCase()).not.toContain(value.toLowerCase());
    }
  });
});

describe("nothing is written on a leak", () => {
  const forms: readonly [string, string][] = [
    ["PAT", PAT],
    ["PAT", Buffer.from(PAT).toString("base64")],
    ["passphrase", PASSPHRASE],
    ["passphrase", JSON.stringify({ p: PASSPHRASE })],
    ["JWT", JWT],
    ["private key", "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----"],
    ["host", "other-acct.snowflakecomputing.com"],
    ["host", (SECRETS.host as string).toUpperCase()],
    ["account locator", "xy12345"],
    ["organization", "acmeorg"],
    ["account name", "probe_acct"],
    ["account name", "probe-acct"],
    ["private name", "TRIAL_ADMIN"],
    ["private name", "trial_admin_role"],
    ["egress IP", "203-0-113-7"],
    ["email", "someone@example.com"],
  ];

  for (const [label, value] of forms) {
    test(`a ${label} form stops every file, and the error never repeats it (${JSON.stringify(value).slice(0, 24)})`, () => {
      const scrubber = new SnowflakeEvidenceScrubber(SECRETS);
      const clean = scrubber.exchange(exchange());
      const error = leakOf(() => scrubber.render({ "a.json": clean, "b.json": { message: `seen ${value} here` } }));
      expect(error.message).toBe(SNOWFLAKE_EVIDENCE_LEAK_SENTENCE);
      expect(error.findings).toContain(`b.json holds the ${label}`);
      expect(error.findings.every((finding) => finding.startsWith("b.json "))).toBe(true);
      expect(JSON.stringify(error.findings)).not.toContain(value);
    });
  }

  test("a user or account name joined to other words by an underscore or a hyphen is replaced, not written", () => {
    const scrubber = new SnowflakeEvidenceScrubber(SECRETS);
    const scrubbed = scrubber.exchange(
      exchange(answer({ data: [["STUDIO_SVC_ROLE", "probe-acct_db", "acmeorg.PROBE-ACCT"]] })),
    );
    const rendered = scrubber.render({ "a.json": scrubbed })["a.json"];
    expect(rendered).toContain('"<user-1>_ROLE"');
    expect(rendered).toContain('"<account>_db"');
    expect(rendered).toContain('"<organization>.<account>"');
  });

  test("a principal's setup name in a scenario's connection block is written, the operator's is refused there", () => {
    const scrubber = new SnowflakeEvidenceScrubber(SECRETS);
    const principal = { connection: { user: "studio_svc", authMethod: "keypair", role: "STUDIO_RW" } };
    expect(JSON.parse(scrubber.render({ "s.json": principal })["s.json"])).toEqual(principal);
    expect(leakOf(() => scrubber.render({ "s.json": { connection: { user: "trial_admin" } } })).findings).toEqual([
      "s.json holds the private name",
    ]);
  });

  test("a secret the allow-list keeps inside a whole field is found", () => {
    const scrubber = new SnowflakeEvidenceScrubber(SECRETS);
    const scrubbed = scrubber.exchange(exchange(answer({ data: [[`k ${PASSPHRASE}`]] })));
    expect(JSON.stringify(scrubbed)).toContain(PASSPHRASE);
    expect(leakOf(() => scrubber.render({ "a.json": scrubbed })).findings).toEqual(["a.json holds the passphrase"]);
  });

  test("a secret inside base64 or a JSON text a string carries is found", () => {
    const scrubber = new SnowflakeEvidenceScrubber(SECRETS);
    const layered = Buffer.from(JSON.stringify({ note: `x ${PASSPHRASE}` })).toString("base64");
    expect(leakOf(() => scrubber.render({ "d.json": { header: layered } })).findings).toEqual([
      "d.json holds the passphrase",
    ]);
  });

  test("a clean set renders every file as indented JSON with a final newline", () => {
    const scrubber = new SnowflakeEvidenceScrubber(SECRETS);
    const rendered = scrubber.render({ "a.json": scrubber.exchange(exchange()), "manifest.json": { ok: true } });
    expect(Object.keys(rendered)).toEqual(["a.json", "manifest.json"]);
    expect(rendered["manifest.json"]).toBe('{\n  "ok": true\n}\n');
  });
});

const FIXTURES = join(import.meta.dir, "..", "..", "..", "fixtures", "snowflake");

function jsonFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) return jsonFiles(path);
    return entry.name.endsWith(".json") ? [path] : [];
  });
}

describe("every committed fixture", () => {
  const files = jsonFiles(FIXTURES);

  test("there are fixtures to read", () => {
    expect(files.length).toBeGreaterThan(0);
  });

  test.each(files.map((file) => [relative(FIXTURES, file), file]))("%s re-renders unchanged", (name, file) => {
    const text = readFileSync(file, "utf8");
    const rendered = new SnowflakeEvidenceScrubber({ users: [] }).render({ [name]: JSON.parse(text) });
    expect(rendered[name]).toBe(text);
  });
});
