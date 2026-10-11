import { describe, expect, test } from "bun:test";
import { isDangerousQuery } from "@/components/QuerySafetyDialog";
import { resolveSqlGrammar } from "@/lib/sql/grammar";
import { readLeadingKeyword } from "@/lib/sql/leading-keyword";
import { hasUnterminatedSpan, readSqlSpan } from "@/lib/sql/spans";
import { splitStatements } from "@/lib/sql/statement-splitter";

// Every fact below was measured on 2026-10-11 through jobs.query dry runs (see BIGQUERY_GRAMMAR).
const grammar = resolveSqlGrammar("bigquery");
const texts = (sql: string) => splitStatements(sql, grammar).map((statement) => statement.sql.trim());

describe("the BigQuery grammar", () => {
  test("a triple-quoted string holds lone quotes, a semicolon and a line feed", () => {
    for (const sql of ["SELECT '''a'b;c'''", 'SELECT """a"b;c"""', "SELECT '''a\nb; DROP TABLE x'''"]) {
      expect(texts(`${sql}; SELECT 2`)).toHaveLength(2);
      expect(hasUnterminatedSpan(sql, grammar)).toBe(false);
    }
  });

  test("a triple-quoted string closes at the first unescaped triple only", () => {
    // The escaped quote is text, the next triple closes the literal, and what follows is outside it.
    expect(readSqlSpan("'''a\\''''b'''", 0, grammar)).toEqual({ kind: "string", end: 9, terminated: true });
    expect(readSqlSpan('"""x"""', 0, grammar)).toEqual({ kind: "quoted-identifier", end: 7, terminated: true });
  });

  test("an unterminated triple is reported as unterminated", () => {
    expect(hasUnterminatedSpan("SELECT '''a''", grammar)).toBe(true);
    expect(hasUnterminatedSpan("SELECT '''a\\'''", grammar)).toBe(true);
  });

  test("an empty string is not a triple opener", () => {
    expect(texts("SELECT ''; SELECT ''")).toEqual(["SELECT ''", "SELECT ''"]);
  });

  test("a backslash escapes a quote in either single-line form", () => {
    expect(texts("SELECT 'it\\'s; x'; SELECT \"a\\\"b\"")).toHaveLength(2);
  });

  test("a # comment hides the rest of its line, including a semicolon", () => {
    expect(texts("SELECT 1 # a; DROP TABLE x\n; SELECT 2")).toHaveLength(2);
    expect(readLeadingKeyword("# note\nDELETE FROM t WHERE true", grammar)?.keyword).toBe("DELETE");
  });

  test("[…] is a subscript, so a bracket holding a quote does not swallow the statement", () => {
    expect(texts("SELECT m['a]b'] FROM t; SELECT 2")).toHaveLength(2);
  });

  test("block comments are flat", () => {
    expect(texts("SELECT /* a /* b */ 1; SELECT 2")).toHaveLength(2);
  });

  test("the unsafe-statement check no longer fires on text inside a triple-quoted string or a # comment", () => {
    expect(isDangerousQuery("SELECT '''a ' DROP '''", "bigquery")).toBe(false);
    expect(isDangerousQuery("# drop table x\nSELECT 1", "bigquery")).toBe(false);
  });

  test("it still fires on a real write after those", () => {
    expect(isDangerousQuery("SELECT '''x'''; DROP TABLE `p.d.t`", "bigquery")).toBe(true);
    expect(isDangerousQuery("# note\nDROP TABLE `p.d.t`", "bigquery")).toBe(true);
    expect(isDangerousQuery("SELECT '''never closed", "bigquery")).toBe(true);
  });

  test("a backslash escapes a backtick, so the exploit splits where BigQuery runs it", () => {
    const sql = "SELECT * FROM `a\\` WHERE x = '`; DROP TABLE t; --'";
    expect(hasUnterminatedSpan(sql, grammar)).toBe(false);
    expect(texts(sql)).toEqual(["SELECT * FROM `a\\` WHERE x = '`", "DROP TABLE t", "--'"]);
    expect(isDangerousQuery(sql, "bigquery")).toBe(true);
    expect(readSqlSpan("`a\\`; DROP` x", 0, grammar)).toEqual({ kind: "quoted-identifier", end: 11, terminated: true });
  });

  test("an unterminated escaped backtick is reported unterminated", () => {
    expect(hasUnterminatedSpan("SELECT `a\\`", grammar)).toBe(true);
    expect(hasUnterminatedSpan("SELECT `a\\\n`", grammar)).toBe(true);
  });

  test("other dialects keep the doubling rule for backticks", () => {
    const mysql = resolveSqlGrammar("mysql");
    expect(readSqlSpan("`a\\` x", 0, mysql)).toEqual({ kind: "quoted-identifier", end: 4, terminated: true });
    expect(readSqlSpan("`a``b`", 0, mysql)).toEqual({ kind: "quoted-identifier", end: 6, terminated: true });
  });

  test("other dialects keep the single-quote reading of a triple", () => {
    // Doubling reads `'''a'` as one literal, so it ends at 5 where BigQuery's triple runs to 9.
    expect(readSqlSpan("'''a'b'''", 0, resolveSqlGrammar("postgres"))).toEqual({
      kind: "string",
      end: 5,
      terminated: true,
    });
    expect(readSqlSpan("'''a'b'''", 0, grammar)).toEqual({ kind: "string", end: 9, terminated: true });
  });
});
