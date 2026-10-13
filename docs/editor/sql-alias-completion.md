# SQL Alias-Based Code Completion

This document describes the intelligent SQL code completion feature that provides context-aware autocompletion with alias support in the Monaco Editor.

## Features

### 1. Alias-Based Column Completion

When you define a table alias in your SQL query, typing the alias followed by a dot (`.`) will suggest columns from the referenced table.

**Supported Patterns:**

| Pattern | Example | Result |
|---------|---------|--------|
| `FROM table AS alias` | `FROM employee AS e WHERE e.` | employee columns |
| `FROM table alias` | `FROM employee e WHERE e.` | employee columns |
| `JOIN table AS alias` | `JOIN department AS d ON d.` | department columns |
| `JOIN table alias` | `LEFT JOIN salary s ON s.` | salary columns |
| `schema.table alias` | `FROM employees.employee e WHERE e.` | employee columns |
| Multiple aliases | `FROM a AS x JOIN b AS y` | Both `x.` and `y.` work |
| CTE references | `WITH cte AS (...) SELECT cte.` | CTE columns |
| Direct table | `employee.` | employee columns |

### 2. Context-Aware Completions

The clause the cursor sits in decides what is offered, on every SQL engine.
It is read across lines, back to the statement's `;`, with strings, comments and quoted identifiers blanked, so a keyword, a `;` or a paren written inside one is no code.
A clause inside a group that closed before the cursor (a finished subquery) governs nothing there.
A `FROM` inside a call (`EXTRACT(YEAR FROM`, `TRIM(BOTH FROM`) is an argument, and a table function's open argument list (`FROM generate_series(1, `) is no table position.
`UPDATE` after `ON`, `FOR` or `KEY` (`ON UPDATE CASCADE`, `FOR UPDATE`, `ON DUPLICATE KEY UPDATE`) is no clause, nor is `ON` after `DISTINCT`, nor a keyword that is a qualified name's segment (`t.table`).

| Clause before the cursor                                                                                | Offered                                                                        |
|---------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| `SELECT`                                                                                                | Columns and qualifiers; tables and CTEs too until the statement names a source |
| `WHERE`, any other `ON` (a join's, `ON CONFLICT`, `ON DELETE`), `SET`, `HAVING`, `ORDER BY`, `GROUP BY` | Columns and qualifiers, no tables                                              |
| `FROM`, `JOIN`, `INTO`, `UPDATE`, `TABLE`, an object's `ON` (`CREATE INDEX i ON `, `GRANT SELECT ON `)  | Tables and CTEs, no columns                                                    |
| `INTO`, `TABLE` or `USING` with an unclosed `(` (`INSERT INTO t (a, `)                                  | Columns, no tables                                                             |
| Anything else (`LIMIT`, `USING` without `(`, none)                                                      | Tables, or columns instead after `AND`, `OR` or `,`                            |

Right after `FROM`, `JOIN`, `INTO`, `UPDATE` or `TABLE`, where only a name can stand, no keyword, function or snippet is offered.
Once a name is written, keywords return (`WHERE`, `JOIN`); functions stay out of table positions.
`JOIN … USING (` offers only the columns the joined source shares with an earlier source of its query.

Once the statement names a source, tables are not offered in a `SELECT` list: only a column can stand there.
The statement's own qualifiers are offered instead: each alias (`Alias of users`), each unaliased table (`Source orders`) and each CTE, so `u` then `.` reaches a column.
While the statement names no source (`SELECT us`, typed left to right), nothing replaces the tables, so they stay; the whole statement is read, so `SELECT | FROM users` already names one.
An object's `ON` is one at statement level after no clause, `SELECT` or `UPDATE` (`CREATE INDEX i ON`, `GRANT SELECT ON`, `CREATE TRIGGER trg AFTER UPDATE ON`), and only while one name follows it: `CREATE INDEX i ON t (` is a column position, scoped to `t`.

**Columns are scoped to the sources in scope.** [`src/lib/editor/sql-scope.ts`](../../src/lib/editor/sql-scope.ts) reads the sources after `FROM` (each item of a comma list), `JOIN`, `INTO`, `UPDATE`, `USING` and an object's `ON`.
It reads them in the cursor's own query and in every query enclosing it, so a correlated subquery sees the outer tables and the outer query does not see a subquery's.
A CTE body is a query of its own: only its own sources are in scope there.
A `WITH` is read at every query level, not only at the statement's head: `CREATE VIEW v AS WITH …`, `INSERT INTO t WITH …` and a subquery's own `WITH` all define CTEs, each seen by its own query and the queries inside it.
A `WITH` that defines nothing (`WITH (NOLOCK)`, `WITH ROLLUP`) is no CTE list.

| Source                                                                            | Its columns                                                             |
|-----------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| A table: `t`, `schema.table`, `"s"."t"`, `[s].[t]`, `` `project.dataset.table` `` | The schema's, looked up by its full path, never by a bare-name fallback |
| A CTE: `WITH c AS (SELECT …)` or `WITH c (a, b) AS (…)`                           | Its declared columns, else its select list's names                      |
| Any source with an alias column list: `(SELECT …) AS s(a, b)`, `f(x) AS g(n)`     | The listed names                                                        |
| A subquery: `FROM (SELECT …) s`                                                   | Its select list's names                                                 |
| A table function or a parenthesised join: `FROM generate_series(1, 3) g`          | Unknown                                                                 |

A select list yields each item's alias, or a bare column's own name (also through a PostgreSQL cast: `id::text` is `id`); an unaliased expression yields none, and `*` or `t.*` passes its sources' columns through, once per name.
A derived source whose select list names nothing is unknown.
A yielded column is typed where it matches a column of its own sources, else `expression`.
A set operation's columns are named by its first branch, which is also how a recursive CTE is read without looping.
A bare table name resolves to the default container's table, otherwise the first one listed.
In scope, columns rank above keywords, and a column several sources hold lists them: `Column (integer) · users, orders`.
If any source in scope is unknown (a table the schema does not hold, a table function), every known column is offered instead, ranked last as before.

The dot path reads the same scope first: `s.` after `FROM (SELECT id, total AS t FROM orders) s` offers `id` and `t`, and an alias of a qualified table reads that table.

### 3. Prioritized Suggestions

Suggestions are sorted by relevance:

| Priority | Type       | Description                                                       |
|----------|------------|-------------------------------------------------------------------|
| 1st      | Columns    | Columns in scope, then the statement's qualifiers                 |
| 2nd      | Keywords   | SQL keywords (SELECT, FROM, WHERE, etc.)                          |
| 3rd      | Functions  | SQL functions (COUNT, SUM, AVG, etc.)                             |
| 4th      | Tables     | Database tables and the statement's CTEs                          |
| 5th      | Snippets   | Query templates                                                   |
| 6th      | Columns    | Every known column, where the scope has an unknown source         |

## Architecture

### Module Structure

```
src/lib/sql/
├── alias-extractor.ts   # Core alias extraction logic
├── types.ts             # TypeScript interfaces
└── index.ts             # Module exports

src/lib/editor/
├── sql-completions.ts   # Monaco completion provider (consumes the alias extractor)
└── sql-scope.ts         # Sources, CTEs and derived columns in scope at the cursor
```

### Key Components

#### 1. Alias Extractor (`src/lib/sql/alias-extractor.ts`)

Lightweight regex-based SQL parser that extracts table aliases without external dependencies.

**Functions:**
- `extractAliases(sql: string)` - Extract all table aliases from a SQL query
- `resolveAlias(identifier: string, aliases: Map)` - Resolve an alias to its table name

**How it works:**
1. Preprocesses SQL to remove comments and string literals
2. Extracts FROM clause aliases
3. Extracts JOIN clause aliases
4. Extracts CTE (WITH clause) aliases
5. Returns a Map of alias → table name

#### 2. Completion Provider (`src/lib/editor/sql-completions.ts`)

Monaco Editor completion provider that integrates with the alias extractor. It is implemented as
`registerSQLCompletionProvider(monaco, schemaCompletionCache)`, which calls
`registerCompletionItemProvider('sql', …)` with trigger characters `.` and space. It is registered
from `src/components/QueryEditor.tsx` in a `useEffect` that returns the disposable for cleanup.

**Dot-triggered completion flow:**
```
User types: "e."
     ↓
1. Try direct table lookup: columnMap.get("e")
     ↓ (not found)
2. Extract aliases from query text
     ↓
3. Resolve "e" → "employee"
     ↓
4. Find columns: columnMap.get("employee")
     ↓
5. Return column suggestions
```

### Edge Cases Handled

1. **SQL Keywords as Aliases**: Filters out `ON`, `WHERE`, `AND`, etc.
2. **Comments**: Removes `--` and `/* */` before parsing
3. **String Literals**: Replaces with placeholder to avoid false matches
4. **Case Insensitivity**: Alias lookup is case-insensitive
5. **Schema Prefixes**: Handles `schema.table` format (e.g., `employees.employee`)

## Performance Considerations

- **No external SQL parser**: Keeps bundle size small
- **Regex-based parsing**: Fast execution (<10ms for typical queries)
- **Blanked once per edit**: literals and comments are blanked over the whole document once per model version, so the requests that filter a trigger's list reuse it
- **Bounded nesting**: a subquery nested more than 32 levels deep is read as opaque
- **Statement-limited parsing**: Parses the text from the start to the cursor plus the rest of
  the cursor's statement, up to its `;`. The part after the cursor is read because an alias is
  often defined there: in `SELECT c. FROM e2e.customers c`, `c.` offered nothing while only the
  text before the cursor was read (#1397). It stops at the terminator, so a later statement's
  alias of the same name is not taken.

## Examples

### Basic Alias Usage
```sql
SELECT e.first_name, e.last_name
FROM employee e
WHERE e.hire_date > '2020-01-01'
```
Typing `e.` after defining `FROM employee e` will suggest all employee columns.

### Multiple Table Aliases
```sql
SELECT
  e.first_name,
  d.dept_name,
  s.amount
FROM employee e
JOIN department_employee de ON de.employee_id = e.id
JOIN department d ON d.id = de.department_id
JOIN salary s ON s.employee_id = e.id
```
Each alias (`e`, `de`, `d`, `s`) resolves to its respective table.

### CTE Support
```sql
WITH active_employees AS (
  SELECT * FROM employee WHERE status = 'active'
)
SELECT ae.first_name
FROM active_employees ae
WHERE ae.department_id = 1
```
The `ae` alias resolves to the CTE `active_employees`.

## Containers and qualified names

The editor holds each table's address as well as its name, and the session's default container
as the object inventory reported it (`defaultContainer`). Three rules follow (#1397):

- **A table outside the default container inserts its qualified address.** On PostgreSQL 18.6, a
  `sales.regions` with `sales` off the search path used to insert `regions`, which runs as
  `relation "regions" does not exist`; it now inserts `sales.regions`, each segment quoted the
  way the dialect needs. A table in the default container keeps its bare name. Where no default
  was reported (the embedded workspace), every table keeps its bare name.
- **A container qualifier offers that container's tables.** `sales.`, a MySQL `e2e_other.` or a
  ClickHouse `demo.` lists the tables of that schema or database, matched against the trailing
  segments of each table's container, and inserts each by its own name after the qualifier
  already typed. Columns of a table or alias with that name still come first.
- **A row count is shown only where the engine measured one.** A table whose count is absent
  reads `Table`, not `Table (0 rows)`; RisingWave 3.1.0 reports no count for a four-row table.

Qualified segments are quoted the way the dialect needs: PostgreSQL with its keyword-aware
rule, every other engine through the connection's own identifier quoting, so a MySQL database
named `e2e-other` is inserted as `` `e2e-other` ``.

### Known limits

- A `;` inside a string or comment after the cursor ends the statement early. The aliases
  defined after it are not read, which is the old before-the-cursor behaviour, not a wrong one.
- A quoted qualifier such as `"Sales".` is not matched against containers; only bare
  identifiers are.
- Reading the clause blanks the whole document once per edit: about 3 ms for 100 KB, 45 ms for 1 MB.
- `SELECT … INTO new_table` (SQL Server, PostgreSQL) names a table that does not exist yet, which widens the columns to every known one.
- A column a derived source renames (`email AS mail`) is typed `expression`: the select-list reader does not follow the expression back to its column.
- A backticked name is split at every `.` inside it, the BigQuery reading; a MySQL `` `a.b` `` table is unknown, so every column is offered.
- Matching a qualifier is case-insensitive, so on an engine where two containers differ only
  in case, both containers' tables are offered.
- A qualifier longer than a table's container path matches nothing: a Trino
  `catalog.schema.` against a one-segment path offers no tables.

## Integration

The alias completion is automatically available in the SQL editor. No configuration required.

### API

```typescript
import { extractAliases, resolveAlias } from '@/lib/sql';

// Extract aliases from a query
const { aliases } = extractAliases('SELECT * FROM employee e WHERE e.id = 1');

// Resolve an alias
const tableName = resolveAlias('e', aliases); // Returns 'employee'
```

## Type Definitions

```typescript
interface TableAlias {
  alias: string;           // e.g., 'e'
  tableName: string;       // e.g., 'employee'
  schema?: string;         // e.g., 'employees'
  source: 'from' | 'join' | 'cte';
}

interface AliasExtractionResult {
  aliases: Map<string, TableAlias>;
  hasTableReferences: boolean;
}
```

## Related Files

| File | Description |
|------|-------------|
| `src/lib/sql/alias-extractor.ts` | Core alias parsing logic (`extractAliases`, `resolveAlias`) |
| `src/lib/sql/types.ts` | Type definitions (`TableAlias`, `AliasExtractionResult`) |
| `src/lib/sql/index.ts` | Module exports |
| `src/lib/editor/sql-completions.ts` | Monaco completion provider (dot/space-triggered; consumes the alias extractor) |
| `src/lib/editor/sql-scope.ts` | Statement scope reader: the sources, CTEs and derived columns in scope at the cursor |
| `src/components/QueryEditor.tsx` | Hosts the Monaco editor and registers the provider |
