/**
 * Status codes to sentences, written from four servers' answers (#1677, Part 4 7.38).
 *
 * THE SAME FAILURE IS REPORTED FOUR WAYS, which is why this table is not keyed on one server.
 * Measured on a wrong password: OPC PLC, open62541 and asyncua-with-a-user-manager answer
 * BadUserAccessDenied; Milo answers BadIdentityTokenInvalid; and STOCK asyncua accepted every
 * username with every password, including an invented one. A table built against one server would
 * have been wrong on the next.
 */
import { describe, expect, test } from "bun:test";
import { DatabaseConfigError, QueryError } from "@/lib/db/errors";
import { errorForStatus, messageOf, statusSentence } from "@/lib/db/providers/industrial/opcua/errors";

describe("the codes real servers answered with", () => {
  /** Each row is a code measured against at least one of the four stacks. */
  const MEASURED: readonly [string, RegExp][] = [
    ["BadUserAccessDenied", /username or password is wrong|may not open a session/],
    ["BadIdentityTokenInvalid", /Milo/],
    ["BadIdentityTokenRejected", /empty password/],
    // The remedy is MEASURED, which is why the pattern is this specific: copying the rejected
    // certificate into trusted/certs while a copy stays in rejected/ keeps failing, and moving it
    // connects. The sentence that said "Pin the certificate ... or add it to the trust list" named
    // the useless action first, on the dialog's default security mode.
    ["BadCertificateUntrusted", /MOVE that file into the PKI root\/trusted\/certs/],
    ["BadSecurityChecksFailed", /our own certificate/],
    ["BadTooManyOperations", /more nodes than the server will take/],
    ["BadNodeIdUnknown", /namespace index/],
    ["BadNotReadable", /Milo/],
    ["BadHistoryOperationUnsupported", /keeps no history/],
    ["BadNotSupported", /open62541/],
    ["BadConnectionClosed", /closes the channel/],
  ];

  /** And the one thing the sentence must NOT say: that pinning makes an untrusted certificate work. */
  test("BadCertificateUntrusted does not offer the pin as the remedy", () => {
    const sentence = statusSentence("BadCertificateUntrusted");
    expect(sentence).toMatch(/Pinning the thumbprint does NOT make it trusted/);
    expect(sentence).toMatch(/moving, not copying/);
  });

  /*
   * AND IT NAMES THE DIRECTORY. This is the only answer a Sign or SignAndEncrypt connect gives, the
   * browser offers no way to move a file, and the sentence used to say "rejected/" and
   * "trusted/certs" without ever saying where those are - found by driving both secure modes in a
   * browser. The root is this connection's own, so it travels from the dial.
   */
  test("BadCertificateUntrusted names this connection's own PKI root, both times", () => {
    const sentence = statusSentence("BadCertificateUntrusted", "/app/data/opcua-pki");

    expect(sentence).toContain("/app/data/opcua-pki/rejected");
    expect(sentence).toContain("/app/data/opcua-pki/trusted/certs");
    expect(sentence).not.toContain("{pkiRoot}");
  });

  test("with no root in hand the sentence keeps the words it had", () => {
    expect(statusSentence("BadCertificateUntrusted")).toContain("the PKI root/trusted/certs");
  });

  for (const [code, expected] of MEASURED) {
    test(`${code} reads as something a reader can act on`, () => {
      const sentence = statusSentence(code);
      expect(sentence).toMatch(expected);
      // The code itself always travels, because a reader searching for it finds the server's docs.
      expect(sentence).toContain(code);
    });
  }
});

describe("a code this table does not know", () => {
  test("it travels as itself rather than as a guess", () => {
    expect(statusSentence("BadSomethingNobodyHasSeen")).toBe("The server answered BadSomethingNobodyHasSeen.");
  });

  test("and it is a query error, not a config error, because nothing in the dialog fixes it", () => {
    expect(errorForStatus("BadSomethingNobodyHasSeen")).toBeInstanceOf(QueryError);
  });
});

describe("which surface shows the failure", () => {
  /** A code about what the reader typed belongs in the dialog; everything else in the results. */
  test("identity, certificate and node-id codes are config errors", () => {
    for (const code of [
      "BadUserAccessDenied",
      "BadIdentityTokenInvalid",
      "BadCertificateUntrusted",
      "BadSecurityChecksFailed",
      "BadNodeIdUnknown",
    ]) {
      expect(errorForStatus(code)).toBeInstanceOf(DatabaseConfigError);
    }
  });

  test("transport and service codes are query errors", () => {
    for (const code of [
      "BadConnectionClosed",
      "BadTimeout",
      "BadTooManyOperations",
      "BadHistoryOperationUnsupported",
    ]) {
      expect(errorForStatus(code)).toBeInstanceOf(QueryError);
    }
  });
});

describe("no sentence leaks a credential (O-S8)", () => {
  test("not one sentence in the table echoes a username or a password", () => {
    for (const code of [
      "BadUserAccessDenied",
      "BadIdentityTokenInvalid",
      "BadIdentityTokenRejected",
      "BadSecurityChecksFailed",
    ]) {
      const sentence = statusSentence(code);
      expect(sentence).not.toMatch(/\bokuyucu\b|\byonetici\b|password=|user=/i);
    }
  });

  /**
   * THE RULE IS ABOUT THE ERROR OBJECT, NOT ONLY ITS MESSAGE. Measured: `node-opcua-client` throws
   * an Error carrying `request` and `response`, so `JSON.stringify(error)` emits the
   * ActivateSessionRequest including `userName`. `messageOf` is what crosses out of the provider.
   */
  test("only the message crosses out, so a carried request cannot be logged with it", () => {
    const leaky = Object.assign(new Error(" serviceResult = BadUserAccessDenied (0x801f0000)"), {
      request: { userName: "okuyucu", password: "Okuyucu.154515" },
      response: { serviceResult: "BadUserAccessDenied" },
    });
    const message = messageOf(leaky);
    expect(message).toBe("serviceResult = BadUserAccessDenied (0x801f0000)");
    expect(message).not.toContain("okuyucu");
    expect(message).not.toContain("Okuyucu.154515");
    // And the proof that the object itself would have leaked, so the rule is not theoretical.
    expect(JSON.stringify(leaky)).toContain("okuyucu");
  });

  test("something thrown that is not an Error still reads as text", () => {
    expect(messageOf("plain string")).toBe("plain string");
    expect(messageOf(undefined)).toBe("undefined");
  });
});
