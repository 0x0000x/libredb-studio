/**
 * The level above the deepest, as every provider that declares a grounding walk scope marks it.
 *
 * The grounding walk reads one fact off that level: which parent is the session's. `scopeReadsUnder` in
 * `src/lib/db/container-walk.ts` reads below a parent marked `isSessionDefault`, so a failed listing below it ends the
 * capture rather than being named as not read, and `whenNoDefault` reads the children of that same parent. A scoped
 * provider that marked its deepest default and not the parent above it would lose both with no failing test of its own:
 * its session's catalog, failing to list, would be named as not read, and the run grounded on nothing of the session's.
 *
 * So each scoped provider with a level above the deepest is driven here through the listing seam its provider class's
 * `listContainers` delegates to and its own introspection tests drive: Snowflake's `listDatabases` and `listSchemas`
 * over a scripted statement runner, and Apache Spark's `listCatalogs` and `listNamespaces` over a stub of its metadata
 * statements. Kubernetes and RabbitMQ declare one container level, so no parent is ever listed and `scopeReadsUnder` is
 * never asked; their depth is held instead. The census builds every shipped provider unconnected, as the capability
 * censuses do, and fails on a provider that declares a scope and is not held here, so a new scoped provider arrives
 * with its proof. The check itself is proven both ways: each provider's listing passes it, and a planted listing that
 * stops marking the parent fails it by name.
 */
import { describe, expect, test } from "bun:test";
import { SHIPPED_DATABASE_TYPES } from "@/lib/db/compatibility";
import { scopeReadsUnder, sessionDefaultContainer } from "@/lib/db/container-walk";
import { createDatabaseProvider } from "@/lib/db/factory";
import { containerDepth } from "@/lib/db/object-kinds";
import { listDatabases, listSchemas } from "@/lib/db/providers/sql/snowflake/introspect";
import type { SparkStatementResult } from "@/lib/db/providers/sql/spark/execute";
import { listCatalogs, listNamespaces } from "@/lib/db/providers/sql/spark/introspect";
import type { Container, ProviderCapabilities } from "@/lib/db/types";
import type { DatabaseType } from "@/lib/types";
import { CENSUS_CONNECTION } from "../../../helpers/census-connection";
import { fakeRunner, showResult } from "../../../helpers/snowflake-runner";

/** A provider's `listContainers` on one session, through its seam: the top level, or the level below `parent`. */
type Listing = (parent?: readonly string[]) => Promise<readonly Container[]>;

/** A session's deepest container, and the listing of a provider on that session. */
interface ParentProof {
  readonly sessionDefault: readonly string[];
  readonly listing: () => Listing;
}

/** Spark's metadata statements answered from a table, each keyed exactly as `introspect.ts` sends it. */
function sparkListing(
  answers: Readonly<Record<string, readonly Record<string, unknown>[]>>,
  configuredCatalogs: readonly string[],
): Listing {
  const deps = {
    runSql: async (sql: string): Promise<SparkStatementResult> => {
      const rows = answers[sql];
      if (rows === undefined) throw new Error(`No answer is scripted for: ${sql}`);
      return { fields: Object.keys(rows[0] ?? {}), rows: [...rows], columnTypes: {}, cut: null };
    },
    confKeys: async () => configuredCatalogs,
  };
  return (parent) => (parent === undefined ? listCatalogs(deps) : listNamespaces(deps, parent[0]));
}

/** Every shipped provider that declares `agentWalkScope`: one container level, or the proof of its parent's mark. */
const HELD: Readonly<Partial<Record<DatabaseType, "one level" | ParentProof>>> = {
  kubernetes: "one level",
  rabbitmq: "one level",
  // A connection whose Database is `sales` and whose Schema is `public`, which Snowflake folds to upper case.
  snowflake: {
    sessionDefault: ["SALES", "PUBLIC"],
    listing: () => {
      const { run } = fakeRunner([
        [/^SHOW DATABASES /, () => showResult(["name"], [["ANALYTICS"], ["OTHER"], ["SALES"]])],
        [
          /^SHOW SCHEMAS IN DATABASE "SALES" /,
          () => showResult(["name"], [["INFORMATION_SCHEMA"], ["PUBLIC"], ["RAW"]]),
        ],
      ]);
      return (parent) => (parent === undefined ? listDatabases(run, "sales") : listSchemas(run, parent[0], "public"));
    },
  },
  // A session in spark_catalog.studio, beside a catalog SHOW CATALOGS lists and one that is only configured.
  spark: {
    sessionDefault: ["spark_catalog", "studio"],
    listing: () =>
      sparkListing(
        {
          "SHOW CURRENT NAMESPACE": [{ catalog: "spark_catalog", namespace: "studio" }],
          "SHOW CATALOGS": [{ catalog: "lake" }, { catalog: "spark_catalog" }],
          "SHOW NAMESPACES IN `spark_catalog`": [{ namespace: "default" }, { namespace: "studio" }],
        },
        ["studio_unloaded"],
      ),
  },
};

const ONE_LEVEL = Object.entries(HELD).flatMap(([type, held]) => (held === "one level" ? [type] : []));
const PROVEN = Object.entries(HELD).flatMap(([type, held]) =>
  held === undefined || held === "one level" ? [] : [[type, held] as const],
);

/** A shipped provider's declarations, read off it unconnected. */
async function declarations(type: DatabaseType): Promise<ProviderCapabilities> {
  return (await createDatabaseProvider(CENSUS_CONNECTION[type])).getCapabilities();
}

/**
 * What the walk needs of a scoped provider's listing and did not get, empty when it got all of it: one container of
 * the top level marked as the session's, the parent of `sessionDefault`; the scope reading below it; and
 * `sessionDefault` marked below it. The second holds for every marked parent while `scopeReadsUnder` reads the mark,
 * so it is the finding a `scopeReadsUnder` that stopped reading it would meet.
 */
async function parentMarkFindings(
  scope: ProviderCapabilities["agentWalkScope"],
  listing: Listing,
  sessionDefault: readonly string[],
): Promise<string[]> {
  const parent = sessionDefault.slice(0, -1);
  const marked = (await listing()).filter((container) => container.isSessionDefault === true);
  if (marked.length !== 1 || JSON.stringify(marked[0].path) !== JSON.stringify(parent)) {
    return [
      `the top level marks ${JSON.stringify(marked.map((container) => container.path))} as the session's, not ${JSON.stringify(parent)}`,
    ];
  }
  const below = sessionDefaultContainer(await listing(parent));
  return [
    ...(scopeReadsUnder(scope, marked[0]) ? [] : [`the scope reads nothing below ${JSON.stringify(parent)}`]),
    ...(JSON.stringify(below) === JSON.stringify(sessionDefault)
      ? []
      : [`below ${JSON.stringify(parent)} the session's container is not ${JSON.stringify(sessionDefault)}`]),
  ];
}

describe("the shipped providers that declare a grounding walk scope", () => {
  test("are exactly the ones held here, so a new one arrives with its proof", async () => {
    const declared = await Promise.all(SHIPPED_DATABASE_TYPES.map(declarations));
    const scoped: string[] = SHIPPED_DATABASE_TYPES.filter(
      (_type, index) => declared[index].agentWalkScope !== undefined,
    );

    expect(scoped.sort()).toStrictEqual(Object.keys(HELD).sort());
  });

  test.each(ONE_LEVEL)("%s declares one container level, so the walk lists no parent", async (type) => {
    expect(containerDepth(await declarations(type as DatabaseType))).toBe(1);
  });

  test.each(PROVEN)(
    "%s marks the session's parent, the one its scope reads below and its deepest default sits in",
    async (type, proof) => {
      const capabilities = await declarations(type as DatabaseType);

      expect(containerDepth(capabilities)).toBe(2);
      expect(
        await parentMarkFindings(capabilities.agentWalkScope, proof.listing(), proof.sessionDefault),
      ).toStrictEqual([]);
    },
  );
});

describe("the check refuses a listing that stops marking the session's parent", () => {
  const scope: ProviderCapabilities["agentWalkScope"] = {
    mode: "session-default",
    whenNoDefault: { walk: "parent-default-children" },
  };
  const SESSION = ["spark_catalog", "studio"];
  /** A two-level listing: the top level as given, and below any parent its `studio` marked as the session's. */
  const planted =
    (top: readonly Container[], marksBelow = true): Listing =>
    async (parent) =>
      parent === undefined
        ? top
        : [
            { path: [parent[0], "default"], name: "default", level: 1 },
            marksBelow
              ? { path: [parent[0], "studio"], name: "studio", level: 1, isSessionDefault: true }
              : { path: [parent[0], "studio"], name: "studio", level: 1 },
          ];
  const catalog = (name: string, isSessionDefault?: true): Container =>
    isSessionDefault === undefined
      ? { path: [name], name, level: 0 }
      : { path: [name], name, level: 0, isSessionDefault };

  test("the deepest default marked and its parent not", async () => {
    expect(
      await parentMarkFindings(scope, planted([catalog("spark_catalog"), catalog("lake")]), SESSION),
    ).toStrictEqual(['the top level marks [] as the session\'s, not ["spark_catalog"]']);
  });

  test("two parents marked", async () => {
    expect(
      await parentMarkFindings(scope, planted([catalog("spark_catalog", true), catalog("lake", true)]), SESSION),
    ).toStrictEqual(['the top level marks [["spark_catalog"],["lake"]] as the session\'s, not ["spark_catalog"]']);
  });

  test("another parent marked", async () => {
    expect(
      await parentMarkFindings(scope, planted([catalog("spark_catalog"), catalog("lake", true)]), SESSION),
    ).toStrictEqual(['the top level marks [["lake"]] as the session\'s, not ["spark_catalog"]']);
  });

  test("the parent marked and nothing below it", async () => {
    expect(
      await parentMarkFindings(scope, planted([catalog("spark_catalog", true), catalog("lake")], false), SESSION),
    ).toStrictEqual(['below ["spark_catalog"] the session\'s container is not ["spark_catalog","studio"]']);
  });
});
