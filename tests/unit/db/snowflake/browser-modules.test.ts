/**
 * The Snowflake module shipped to the browser: `account-host.ts` imports nothing at all, not even a type, and names no
 * `Buffer`. The connection dialog and the paste parser import it in the browser, while `account.ts`, which builds the
 * origin from its reading, reaches `node:net` through `endpoint.ts`, so the reading and the host test live apart from
 * it. Proven both ways: the real source passes, and each planted import fails by name.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import ts from "typescript";

const ROOT = join(import.meta.dir, "..", "..", "..", "..");
const SNOWFLAKE = "src/lib/db/providers/sql/snowflake";

/** Each module and the imports it may make: none, for the one browser module. */
const BROWSER_MODULES: ReadonlyArray<readonly [string, readonly string[]]> = [[`${SNOWFLAKE}/account-host.ts`, []]];

/** Every import or re-export specifier of `source`, type-only ones included. */
function importSpecifiers(source: string): string[] {
  const file = ts.createSourceFile("module.ts", source, ts.ScriptTarget.Latest, true);
  const found: string[] = [];
  for (const statement of file.statements) {
    if (!ts.isImportDeclaration(statement) && !ts.isExportDeclaration(statement)) continue;
    const specifier = statement.moduleSpecifier;
    if (specifier !== undefined && ts.isStringLiteral(specifier)) found.push(specifier.text);
  }
  return found;
}

/**
 * Whether the code of `source` names `Buffer`, as an identifier or a string literal; comments and docblocks are not
 * code, and ts.forEachChild never visits them, so a docblock stating this very rule is not a finding.
 */
function namesBuffer(source: string): boolean {
  const file = ts.createSourceFile("module.ts", source, ts.ScriptTarget.Latest, true);
  const visit = (node: ts.Node): boolean =>
    ((ts.isIdentifier(node) || ts.isStringLiteralLike(node)) && node.text === "Buffer") ||
    ts.forEachChild(node, visit) === true;
  return visit(file);
}

function findings(file: string, source: string, allowed: readonly string[]): string[] {
  const found = importSpecifiers(source)
    .filter((specifier) => !allowed.includes(specifier))
    .map((specifier) => `${file} imports ${specifier}, which is not browser-safe`);
  if (namesBuffer(source)) found.push(`${file} names Buffer, which the browser does not have`);
  return found;
}

describe("the browser-shipped Snowflake module", () => {
  test.each(BROWSER_MODULES.map(([module, allowed]) => [module, allowed] as const))(
    "%s imports only what its row allows",
    (module, allowed) => {
      expect(findings(module, readFileSync(join(ROOT, module), "utf8"), allowed)).toEqual([]);
    },
  );

  const file = `${SNOWFLAKE}/account-host.ts`;
  test.each([
    ['import { httpOrigin } from "@/lib/db/http/endpoint";\n', "@/lib/db/http/endpoint"],
    ['import { SnowflakeError } from "./errors";\n', "./errors"],
    ['import { isIP } from "node:net";\n', "node:net"],
    ['import type { HttpOrigin } from "@/lib/db/http/endpoint";\n', "@/lib/db/http/endpoint"],
    ['export { snowflakeEndpoint } from "./account";\n', "./account"],
  ])("a planted %s fails by name", (planted, specifier) => {
    expect(findings(file, planted, [])).toEqual([`${file} imports ${specifier}, which is not browser-safe`]);
  });

  test("Buffer in code is a finding, and Buffer in a comment is not", () => {
    expect(findings(file, "const bytes = Buffer.from('x');\n", [])).toEqual([
      `${file} names Buffer, which the browser does not have`,
    ]);
    expect(findings(file, "/** No `Buffer` here. */\n// Buffer\nexport const a = 1;\n", [])).toEqual([]);
  });
});
