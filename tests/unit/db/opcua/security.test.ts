/**
 * What is refused before a socket exists (#1677 O-S2, O-S3, O-S7, O-S8).
 *
 * EVERY ENDPOINT SHAPE BELOW WAS READ OFF A REAL SERVER on 2026-10-11/12, because the rule the issue
 * proposed does not survive the shapes that actually occur. It said to refuse "a username on a
 * server whose UserName token policy is itself None". Measured: on secured endpoints OPC PLC and
 * open62541 leave the token's policy NULL, which Part 4 7.41 says means the channel's policy, so
 * that rule would refuse a perfectly protected connection; and stock asyncua declares the token's
 * policy as None on its only endpoint, where the password really would cross in the clear.
 */
import { describe, expect, test } from "bun:test";
import { DatabaseConfigError } from "@/lib/db/errors";
import type { OpcuaEndpoint } from "@/lib/db/providers/industrial/opcua/client";
import {
  effectiveTokenPolicy,
  normaliseThumbprint,
  pinRefusal,
  plaintextIdentityRefusal,
  readEndpointUrl,
} from "@/lib/db/providers/industrial/opcua/security";

const NONE = "http://opcfoundation.org/UA/SecurityPolicy#None";
const B256 = "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256";

/** OPC PLC's None endpoint: the UserName token carries its own Basic256Sha256 (policyId "5"). */
const PLC_NONE: OpcuaEndpoint = {
  advertisedUrl: "opc.tcp://9b9677b09793:50000/",
  securityMode: "None",
  securityPolicyUri: NONE,
  userIdentityTokens: [
    { tokenType: "Anonymous", securityPolicyUri: NONE },
    { tokenType: "UserName", securityPolicyUri: B256 },
  ],
};

/** OPC PLC's SignAndEncrypt endpoint: the token's policy is null, meaning the channel's. */
const PLC_SECURED: OpcuaEndpoint = {
  advertisedUrl: "opc.tcp://9b9677b09793:50000/",
  securityMode: "SignAndEncrypt",
  securityPolicyUri: B256,
  userIdentityTokens: [{ tokenType: "UserName", securityPolicyUri: null }],
};

/** Stock asyncua, with no certificate: the UserName token's policy is None. */
const ASYNCUA_STOCK: OpcuaEndpoint = {
  advertisedUrl: "opc.tcp://127.0.0.1:4840/freeopcua/server/",
  securityMode: "None",
  securityPolicyUri: NONE,
  userIdentityTokens: [
    { tokenType: "Anonymous", securityPolicyUri: NONE },
    { tokenType: "UserName", securityPolicyUri: NONE },
  ],
};

describe("the effective identity-token policy (Part 4 7.41)", () => {
  test("a token's own policy is used where it has one", () => {
    expect(effectiveTokenPolicy(PLC_NONE, "username")).toBe(B256);
  });

  test("a null token policy means the endpoint's, which is what secured endpoints publish", () => {
    expect(effectiveTokenPolicy(PLC_SECURED, "username")).toBe(B256);
  });

  test("an explicit None really is None, which stock asyncua publishes", () => {
    expect(effectiveTokenPolicy(ASYNCUA_STOCK, "username")).toBe(NONE);
  });

  test("an anonymous identity has no token policy to read", () => {
    expect(effectiveTokenPolicy(PLC_NONE, "anonymous")).toBeNull();
  });
});

describe("a credential that would cross the network unprotected (O-S3)", () => {
  const remote = "plc.ornek.net";

  test("stock asyncua over None to a remote host is refused, and the sentence names the remedy", () => {
    const refusal = plaintextIdentityRefusal({
      host: remote,
      tunnelled: false,
      mode: "None",
      kind: "username",
      effectivePolicy: effectiveTokenPolicy(ASYNCUA_STOCK, "username"),
    });
    expect(refusal).toBeDefined();
    expect(refusal).toMatch(/Sign and Encrypt|SSH tunnel/);
    // O-S8: no credential in the sentence.
    expect(refusal).not.toMatch(/password is|username is/i);
  });

  /**
   * The case the issue's rule would have got wrong. OPC PLC over None protects the password with
   * the token's own policy: captured on the wire, 2,428 bytes written, the plaintext absent, the
   * token's encryptionAlgorithm rsa-oaep and the encrypted password 256 bytes.
   */
  test("OPC PLC over None is allowed, because the token encrypts the password itself", () => {
    expect(
      plaintextIdentityRefusal({
        host: remote,
        tunnelled: false,
        mode: "None",
        kind: "username",
        effectivePolicy: effectiveTokenPolicy(PLC_NONE, "username"),
      }),
    ).toBeUndefined();
  });

  test("a null token policy on a secured endpoint is allowed, not refused", () => {
    expect(
      plaintextIdentityRefusal({
        host: remote,
        tunnelled: false,
        mode: "SignAndEncrypt",
        kind: "username",
        effectivePolicy: effectiveTokenPolicy(PLC_SECURED, "username"),
      }),
    ).toBeUndefined();
  });

  /**
   * THE MIDDLE ROW OF A THREE-VALUED DECISION, which had no test at all: the suite covered None
   * (refuse) and SignAndEncrypt (allow) and never `Sign`. Found as a surviving mutation - making
   * `channelProtects` accept `"Sign"` as well passed all 341 tests - and this is the decision that
   * governs whether a password crosses the wire readable, so it is the last line that should be
   * invisible to a test.
   *
   * Sign is a SIGNATURE AND NOT AN ENVELOPE: Part 4 6.1.2 signs the message and leaves the body in
   * the clear, so a UserName token with policy None under Sign puts the password on the wire for
   * anyone watching. It is refused, in the same words as None.
   */
  test("Sign is not encryption: a password under it is refused as plainly as under None", () => {
    const refusal = plaintextIdentityRefusal({
      host: remote,
      tunnelled: false,
      mode: "Sign",
      kind: "username",
      effectivePolicy: NONE,
    });
    expect(refusal).toBeDefined();
    expect(refusal).toMatch(/Sign and Encrypt|SSH tunnel/);
    // And the control, which is what makes the line above about the MODE: the same Sign channel
    // with a token policy of its own encrypts the password, so it is allowed.
    expect(
      plaintextIdentityRefusal({
        host: remote,
        tunnelled: false,
        mode: "Sign",
        kind: "username",
        effectivePolicy: effectiveTokenPolicy(PLC_NONE, "username"),
      }),
    ).toBeUndefined();
  });

  test("this machine, a tunnel, and no credential are each exempt", () => {
    const base = {
      mode: "None",
      kind: "username",
      effectivePolicy: NONE,
    } as const;
    expect(
      plaintextIdentityRefusal({
        ...base,
        host: "127.0.0.1",
        tunnelled: false,
      }),
    ).toBeUndefined();
    expect(
      plaintextIdentityRefusal({
        ...base,
        host: "localhost",
        tunnelled: false,
      }),
    ).toBeUndefined();
    expect(plaintextIdentityRefusal({ ...base, host: remote, tunnelled: true })).toBeUndefined();
    expect(
      plaintextIdentityRefusal({
        host: remote,
        tunnelled: false,
        mode: "None",
        kind: "anonymous",
        effectivePolicy: null,
      }),
    ).toBeUndefined();
  });
});

describe("the endpoint URL is what the reader typed (O-S7)", () => {
  test("host, port and path are read from it", () => {
    expect(readEndpointUrl("opc.tcp://plc.ornek.net:4841/UA/Server")).toEqual({
      host: "plc.ornek.net",
      port: 4841,
      path: "/UA/Server",
    });
  });

  test("an absent port is the IANA one", () => {
    expect(readEndpointUrl("opc.tcp://plc.ornek.net/").port).toBe(4840);
  });

  test("another scheme is refused by name, because this version has no other transport", () => {
    for (const url of ["http://plc/", "opc.https://plc/", "ws://plc/"]) {
      expect(() => readEndpointUrl(url)).toThrow(DatabaseConfigError);
    }
  });

  /** A credential in the URL would be written into a saved connection's address. */
  test("a credential in the URL is refused and the reader is told where it goes instead", () => {
    expect(() => readEndpointUrl("opc.tcp://user:pw@plc:4840/")).toThrow(/own fields/);
  });

  test("an empty, unparseable or portless-but-broken URL each name what was wrong", () => {
    expect(() => readEndpointUrl("")).toThrow(/needs an endpoint URL/);
    expect(() => readEndpointUrl("not a url")).toThrow(/could not be read/);
    expect(() => readEndpointUrl("opc.tcp://plc:0/")).toThrow(/between 1 and 65,535/);
  });
});

describe("the server certificate pin (O-S2)", () => {
  /** SHA-1 over the DER, 40 lowercase hex: the thumbprint measured on the fixture. */
  const PLC = "4a25cd3ba6a2ddef16c8c88d33973cdabea3d8b2";

  test("an operator's paste is read however their tool printed it", () => {
    expect(normaliseThumbprint("4A:25:CD:3B:A6:A2:DD:EF:16:C8:C8:8D:33:97:3C:DA:BE:A3:D8:B2")).toBe(PLC);
    expect(normaliseThumbprint(" 4a25cd3b a6a2ddef 16c8c88d 33973cda bea3d8b2 ")).toBe(PLC);
  });

  test("the pinned server is allowed", () => {
    expect(pinRefusal(PLC, PLC)).toBeUndefined();
    expect(pinRefusal(undefined, PLC)).toBeUndefined();
  });

  /** The case O-S2 exists for: the server was recreated with a new certificate. */
  test("a different certificate is refused, and the refusal names both thumbprints", () => {
    const other = "0000000000000000000000000000000000000000";
    const refusal = pinRefusal(PLC, other);
    expect(refusal).toContain(PLC);
    expect(refusal).toContain(other);
    expect(refusal).toMatch(/Nothing was sent/);
  });

  /** In None mode checkCertificate is never called at all, so a pin cannot be checked there. */
  test("no certificate presented is refused, and the sentence says why None cannot be pinned", () => {
    expect(pinRefusal(PLC, undefined)).toMatch(/None security mode/);
  });

  test("a pin that is not a SHA-1 thumbprint is refused as a mistake in the pin", () => {
    expect(pinRefusal("deadbeef", PLC)).toMatch(/40 hexadecimal/);
  });
});
