/**
 * Where the client's own key pair and trust store live (#1677 O-S4).
 *
 * THE LIBRARY'S DEFAULT CANNOT WORK IN THE PRODUCT IMAGE, AND THAT IS MEASURED, NOT FEARED. On a
 * first connect, even in None mode, `node-opcua-client` generates a private key and a self-signed
 * client certificate under the platform's own config directory, with `trusted`, `rejected` and
 * `issuers` beside them. Measured paths:
 *
 * | Platform | Default root |
 * |---|---|
 * | Linux (the image) | `$HOME/.config/node-opcua-default-nodejs/PKI` |
 * | macOS (a developer) | `$HOME/Library/Preferences/node-opcua-default-nodejs/PKI` |
 *
 * The issue named the Linux path as THE path; it is one of several, because `env-paths` answers per
 * platform. Either way the product image cannot use it: measured on
 * `ghcr.io/libredb/libredb-studio:0.18.2`, the process runs as uid 1001 whose passwd entry is
 * `/nonexistent`, `os.homedir()` is `/nonexistent`, the directory does not exist, and a `mkdir`
 * there fails `EACCES`. The issue's O-S4 describes a "read-only or ephemeral home"; the measured
 * state is neither. It is a home that cannot be created at all, so the default would not be awkward
 * in production, it would simply fail.
 *
 * Hence the root never falls back to a setting that depends on `HOME`. It falls back to a directory
 * Studio ALREADY owns and writes - `<data directory>/opcua-pki`, beside the storage database - and
 * the variable overrides it with an absolute path. Requiring the variable was the first answer here,
 * and a browser measured what it cost: a reader who installed Studio, picked OPC UA and pressed Test
 * Connection was told the engine needs a directory nobody had mentioned, so the engine could not be
 * used at all until they found an environment variable. A connection may also supply the certificate
 * and key itself, which is what a mounted secret is for. Two replicas sharing one mounted key present
 * one identity to the server's administrator, rather than one per pod to be trusted one by one.
 */
import { resolve } from "node:path";
import { getDataDir } from "@/lib/data-dir";
import { DatabaseConfigError } from "@/lib/db/errors";
import { OPCUA_TYPE } from "./constants";

/**
 * The environment variable naming the PKI root.
 *
 * Read from the server's own environment rather than from a connection, because a private key is
 * the installation's identity and not one reader's setting: every connection from this Studio
 * presents the same client certificate, which is what a server administrator adds to a trust list
 * once.
 */
export const OPCUA_PKI_ROOT_ENV = "LIBREDB_OPCUA_PKI_ROOT";

/** The directory under Studio's own data directory that holds this client's PKI by default. */
export const OPCUA_PKI_DIRECTORY = "opcua-pki";

/**
 * Where a connection's PKI lives: the variable, or Studio's own data directory.
 *
 * THE DEFAULT IS STUDIO'S DATA DIRECTORY, AND IT IS THERE BECAUSE A BROWSER RUN SHOWED WHAT NO
 * DEFAULT COST. This function used to refuse when the variable was unset, deliberately - a fallback
 * to the LIBRARY's default would have failed in the container image, where the application's home
 * directory is `/nonexistent`. That reasoning was right about the library's default and wrong about
 * the remedy: measured end to end in a real browser against the real server, a reader who installs
 * Studio, picks OPC UA and presses Test Connection was told the engine needs a directory nobody had
 * told them about, so the engine could not be used at all until they found an environment variable.
 *
 * `getDataDir()` is the directory Studio already owns and already writes to - it holds the storage
 * database - so it exists and is writable in every deployment, and the deployments that mount it
 * (the chart's PVC, a Docker volume) get exactly what the guide asks an operator to arrange: a
 * client identity that survives a restart. The variable still overrides it, which is what a
 * deployment sharing one identity between replicas sets.
 */
export function pkiRoot(environment: Readonly<Record<string, string | undefined>>): string {
  const root = environment[OPCUA_PKI_ROOT_ENV]?.trim();
  /*
   * RESOLVED, because `getDataDir()` is relative by default (`./data`, from
   * `DEFAULT_STORAGE_SQLITE_PATH`) and a relative PKI root is the one thing the rule below refuses:
   * it would resolve against whatever directory the server happened to start in. Resolving it here
   * against the process's own working directory is what the storage database already does, so the
   * PKI lands beside it - in the image, under the mounted `/app/data`.
   */
  if (root === undefined || root === "") return resolve(getDataDir(), OPCUA_PKI_DIRECTORY);
  if (!root.startsWith("/")) {
    throw new DatabaseConfigError(
      `${OPCUA_PKI_ROOT_ENV} is an absolute path, and this one is relative: ${root}. A relative path would ` +
        "resolve against whatever directory the server happened to start in.",
      OPCUA_TYPE,
    );
  }
  return root;
}

/**
 * Whether the client's own certificate was supplied rather than generated.
 *
 * Supplied is the better answer for a deployment with more than one replica, because the server's
 * administrator then trusts one certificate instead of one per pod. Generated is the answer for a
 * single instance with a mounted directory, where the key survives a restart.
 */
export function clientIdentitySource(options: {
  readonly certificatePath?: string;
  readonly privateKeyPath?: string;
}): "supplied" | "generated" {
  const hasCertificate = (options.certificatePath ?? "").trim() !== "";
  const hasKey = (options.privateKeyPath ?? "").trim() !== "";
  if (hasCertificate !== hasKey) {
    throw new DatabaseConfigError(
      "A client certificate and its private key are supplied together or not at all: one without the other " +
        "cannot open a secured channel.",
      OPCUA_TYPE,
    );
  }
  return hasCertificate ? "supplied" : "generated";
}
