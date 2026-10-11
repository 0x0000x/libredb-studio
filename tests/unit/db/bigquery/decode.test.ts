import { describe, expect, test } from "bun:test";
import { jobLocation } from "@/lib/db/providers/sql/bigquery/index";
import { decodeCell } from "@/lib/db/providers/sql/bigquery/transport";
import { BIGQUERY_DRY_RUN_MARKER, bigqueryDryRunStrategy } from "@/lib/explain/bigquery-dryrun";

describe("bigquery decodeCell", () => {
  test("scalars keep INT64 as string and convert FLOAT, BOOL, TIMESTAMP", () => {
    expect(decodeCell({ name: "i", type: "INTEGER" }, "9007199254740993")).toBe("9007199254740993");
    expect(decodeCell({ name: "f", type: "FLOAT" }, "1.5")).toBe(1.5);
    expect(decodeCell({ name: "b", type: "BOOLEAN" }, "true")).toBe(true);
    expect(decodeCell({ name: "t", type: "TIMESTAMP" }, "1767323045678000")).toBe("2026-01-02T03:04:05.678Z");
    expect(decodeCell({ name: "n", type: "STRING" }, null)).toBeNull();
  });

  test("repeated and nested records", () => {
    const field = {
      name: "st",
      type: "RECORD",
      mode: "REPEATED" as const,
      fields: [
        { name: "s", type: "STRING" },
        { name: "xs", type: "INTEGER", mode: "REPEATED" as const },
      ],
    };
    const wire = [{ v: { f: [{ v: "a" }, { v: [{ v: "1" }, { v: "2" }] }] } }];
    expect(decodeCell(field, wire)).toEqual([{ s: "a", xs: ["1", "2"] }]);
  });
});

describe("bigquery dry-run explain", () => {
  test("marks the statement and renders the byte estimate", () => {
    expect(bigqueryDryRunStrategy.buildSql("UPDATE t SET a = 1", "analyze")).toBe(
      `${BIGQUERY_DRY_RUN_MARKER}UPDATE t SET a = 1`,
    );
    const raw = bigqueryDryRunStrategy.extractPlan({
      rows: [{ bytes_processed: 1073741824, bytes_processed_human: "1 GB" }],
    });
    const model = bigqueryDryRunStrategy.toRenderModel(raw);
    expect(model?.kind === "tree" && model.root.label).toBe("Dry run: 1 GB would be processed");
    expect(bigqueryDryRunStrategy.toRenderModel({ nope: 1 })).toBeNull();
  });
});

describe("bigquery jobLocation", () => {
  const datasets = [
    { id: "a", location: "EU" },
    { id: "b", location: "EU" },
    { id: "c", location: "US" },
  ];
  test("the default dataset's location wins, else the most common one", () => {
    expect(jobLocation(datasets, "c")).toBe("US");
    expect(jobLocation(datasets)).toBe("EU");
    expect(jobLocation([])).toBeUndefined();
  });

  test("a dataset listed with no location counts for none", () => {
    expect(jobLocation([{ id: "x" }, { id: "y" }, { id: "c", location: "US" }], "x")).toBe("US");
    expect(jobLocation([{ id: "x" }])).toBeUndefined();
  });
});
