/**
 * Where the client's key pair lives, and why there is no fallback (#1677 O-S4, section 6 item 5).
 *
 * MEASURED ON THE PRODUCT IMAGE: `ghcr.io/libredb/libredb-studio:0.18.2` runs as uid 1001 whose
 * passwd entry is `/nonexistent`; `os.homedir()` answers `/nonexistent`, the directory does not
 * exist, and a `mkdir` there fails EACCES. So the library's default PKI location does not merely
 * misbehave in production, it cannot be created.
 */
import { describe, expect, test } from "bun:test";
import { DatabaseConfigError } from "@/lib/db/errors";
import { resolve } from "node:path";
import { getDataDir } from "@/lib/data-dir";
import {
  clientIdentitySource,
  OPCUA_PKI_DIRECTORY,
  OPCUA_PKI_ROOT_ENV,
  pkiRoot,
} from "@/lib/db/providers/industrial/opcua/pki";

describe("the PKI root", () => {
  test("an absolute path is taken as given", () => {
    expect(pkiRoot({ [OPCUA_PKI_ROOT_ENV]: "/app/data/opcua-pki" })).toBe("/app/data/opcua-pki");
  });

  /**
   * NO VARIABLE IS STUDIO'S OWN DATA DIRECTORY, and that default is here because of what refusing
   * cost: measured end to end in a real browser against the real server, a reader who installed
   * Studio, picked OPC UA and pressed Test Connection was told the engine needs a directory nobody
   * had told them about - so the engine could not be used at all until they found an environment
   * variable. The LIBRARY's default still cannot be used (in the image `os.homedir()` is
   * `/nonexistent`), which is why the default is a directory Studio already owns and writes to.
   */
  test("no root set is Studio's own data directory, not a refusal", () => {
    const expected = resolve(getDataDir(), OPCUA_PKI_DIRECTORY);
    expect(pkiRoot({})).toBe(expected);
    expect(pkiRoot({ [OPCUA_PKI_ROOT_ENV]: "   " })).toBe(expected);
    // Absolute by construction, because the data directory is: the rule below still holds for it.
    expect(pkiRoot({}).startsWith("/")).toBe(true);
  });

  test("the variable still overrides the default, which is what a shared identity needs", () => {
    expect(pkiRoot({ [OPCUA_PKI_ROOT_ENV]: "/mnt/opcua" })).toBe("/mnt/opcua");
    expect(pkiRoot({ [OPCUA_PKI_ROOT_ENV]: "/mnt/opcua" })).not.toContain(OPCUA_PKI_DIRECTORY);
  });

  test("a relative path is refused, because it resolves against the working directory", () => {
    expect(() => pkiRoot({ [OPCUA_PKI_ROOT_ENV]: "pki" })).toThrow(/absolute path/);
    expect(() => pkiRoot({ [OPCUA_PKI_ROOT_ENV]: "./pki" })).toThrow(/absolute path/);
  });

  /** A relative override is still refused by a sentence that names the variable. */
  test("the refusal for a relative path names the variable", () => {
    expect(() => pkiRoot({ [OPCUA_PKI_ROOT_ENV]: "pki" })).toThrow(new RegExp(OPCUA_PKI_ROOT_ENV));
    expect(() => pkiRoot({ [OPCUA_PKI_ROOT_ENV]: "pki" })).toThrow(DatabaseConfigError);
  });
});

describe("the client's own identity", () => {
  test("a supplied certificate and key are the supplied case", () => {
    expect(
      clientIdentitySource({
        certificatePath: "/run/secrets/c.pem",
        privateKeyPath: "/run/secrets/k.pem",
      }),
    ).toBe("supplied");
  });

  test("neither supplied is the generated case", () => {
    expect(clientIdentitySource({})).toBe("generated");
    expect(clientIdentitySource({ certificatePath: "", privateKeyPath: "  " })).toBe("generated");
  });

  /** One without the other cannot open a secured channel, so it is a mistake rather than a default. */
  test("one without the other is refused", () => {
    expect(() => clientIdentitySource({ certificatePath: "/c.pem" })).toThrow(DatabaseConfigError);
    expect(() => clientIdentitySource({ privateKeyPath: "/k.pem" })).toThrow(/together or not at all/);
  });
});
