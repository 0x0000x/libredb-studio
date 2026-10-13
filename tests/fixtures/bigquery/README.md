# BigQuery captures

What BigQuery's REST v2 API answered `tests/live/bigquery-evidence.ts`, one scenario per file; `tests/integration/db/bigquery-provider.test.ts` replays them and no test reaches a live project.

`2026-10-13-eu/` was recorded on 2026-10-13 against scratch datasets in region EU (`shop`, `archive`) and europe-west3 (`regional`), created for the run and dropped after it.
Datasets are listed through a per-run label, and `INFORMATION_SCHEMA` reads are filtered to the scratch datasets and the run's own jobs, so no other content of the project is in a capture; the `STARTS_WITH(table_schema, '')` in a recorded query is that filter with its run prefix scrubbed.

## The scrub

No request header is recorded.
The project becomes `demo-project`, the per-run dataset prefix and label key are removed, job and query ids become `<job-N>`, page tokens `<page-N>` and email addresses `<user-N>`, numbered across the run; `etag` and `destinationTable` (the anonymous result dataset, a hash of the principal) are dropped.
A scrubbed file that still names the project, any `--refuse` name, a token, a key field or an email address stops the run before anything is written.

## The scenarios

| Scenario                        | What it shows                                                                                             |
| ------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `datasets-list`                 | `datasets.list` of the two EU datasets                                                                    |
| `datasets-list-mixed-locations` | The same with the europe-west3 dataset                                                                    |
| `select-1`                      | The connect probe                                                                                         |
| `tables-list`                   | `tables.list` of `shop` (two tables, a view, a materialized view) and `archive`                           |
| `tables-get`                    | `tables.get` of every relation, then the 404 of a missing table                                           |
| `catalog`, `catalog-after-ddl`  | Region `INFORMATION_SCHEMA.TABLES` and `.COLUMNS`, before and after `CREATE TABLE shop.refunds`           |
| `ddl`                           | The `CREATE TABLE` answer                                                                                 |
| `dry-run`                       | A dry run: bytes processed, no job id                                                                     |
| `typed-pages`                   | INT64 beyond 2^53, TIMESTAMP micros, STRUCT, ARRAY, FLOAT, BOOL over two `getQueryResults` pages          |
| `cache-hit`                     | The same query twice, the second answered from cache                                                      |
| `dml`                           | A `DELETE` of seven rows: `numDmlAffectedRows` with the table's schema and no rows                        |
| `syntax-error`, `auth-401`      | A 400 `invalidQuery` and a 401 for an invalid token                                                       |
| `slow-job`                      | A job still running after `jobs.query`, its poll, its `JOBS_BY_PROJECT` row, and its `jobs.cancel` answer |
| `monitoring`                    | The `JOBS_BY_PROJECT` reads of the performance and slow-query panels                                      |
| `storage`                       | The `TABLE_STORAGE` reads of the overview, storage and table panels, filtered to the scratch datasets     |
| `access-denied`                 | A 403 `accessDenied`: `jobs.query` in `bigquery-public-data`, where no one may create jobs                |
| `token-rejected`                | The token endpoint refusing a service-account assertion signed by an unknown key                          |

Not captured: a successful token answer, which carries a live access token, and transport timeouts, which have no answer to record.
