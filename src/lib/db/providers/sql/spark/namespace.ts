/**
 * Spark's spelling of names and of multi-part namespaces (spec 4.1, 4.11, 7.4; L08-C17, C21).
 *
 * Pure and browser-safe: the connection dialog cannot import a provider module, so src/lib/db-ui-config.ts copies the
 * three field patterns below literally, and tests/unit/lib/db-ui-config.test.ts holds each copy equal to these.
 * `parseNamespaceSpelling` accepts exactly the texts SPARK_NAMESPACE_PATTERN accepts, so the dialog and the provider
 * refuse the same Namespace values.
 */
import { SPARK_NAMESPACE_MAX_PARTS } from "./constants";

/** One catalog name: 1 to 255 characters, no dot and no control character (spec 3.1). */
export const SPARK_CATALOG_PATTERN = /^[^.\p{Cc}]{1,255}$/u;
/** 1 to 8 parts joined by `.`, each bare as quoteIfNeeded leaves it or backticked with a backtick doubled (spec 3.1). */
export const SPARK_NAMESPACE_PATTERN =
  /^(?:[A-Za-z_][A-Za-z0-9_]*|`(?:[^`]|``)+`)(?:\.(?:[A-Za-z_][A-Za-z0-9_]*|`(?:[^`]|``)+`)){0,7}$/;
/** A token a gRPC header can carry: no CR, LF or NUL (spec 3.1). */
export const SPARK_TOKEN_PATTERN = /^[^\r\n\0]*$/;

/** A name Spark's own quoteIfNeeded leaves bare (L08-C21). */
const BARE_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
const BARE_PREFIX = /^[A-Za-z_][A-Za-z0-9_]*/;

/** Backticks, a backtick inside doubled (L08-C17). */
export function quoteSparkName(name: string): string {
  return `\`${name.replaceAll("`", "``")}\``;
}

/** Bare when Spark would leave it bare, else quoted. */
export function spellSparkName(name: string): string {
  return BARE_NAME.test(name) ? name : quoteSparkName(name);
}

/** The parts of a namespace spelling, or undefined for any text SPARK_NAMESPACE_PATTERN refuses. */
export function parseNamespaceSpelling(text: string): readonly string[] | undefined {
  const parts: string[] = [];
  let index = 0;
  for (;;) {
    if (parts.length === SPARK_NAMESPACE_MAX_PARTS) return undefined;
    let part = "";
    if (text[index] === "`") {
      index += 1;
      for (;;) {
        if (index >= text.length) return undefined;
        const char = text[index] as string;
        if (char === "`") {
          if (text[index + 1] === "`") {
            part += "`";
            index += 2;
            continue;
          }
          index += 1;
          break;
        }
        part += char;
        index += 1;
      }
      if (part === "") return undefined;
    } else {
      const bare = BARE_PREFIX.exec(text.slice(index));
      if (bare === null) return undefined;
      part = bare[0];
      index += part.length;
    }
    parts.push(part);
    if (index === text.length) return parts;
    if (text[index] !== ".") return undefined;
    index += 1;
  }
}

/** The parts joined by `.`, each spelled as Spark spells it. */
export function spellNamespace(parts: readonly string[]): string {
  return parts.map(spellSparkName).join(".");
}

/** `catalog.namespace[.name]` as a statement writes it (spec 4.11): the catalog and the name always quoted, the namespace as given. */
export function qualifiedName(catalog: string, namespaceSpelling: string, name?: string): string {
  return [quoteSparkName(catalog), namespaceSpelling, ...(name === undefined ? [] : [quoteSparkName(name)])].join(".");
}
