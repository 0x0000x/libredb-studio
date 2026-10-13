import { afterEach, describe, expect, setSystemTime, test } from "bun:test";
import {
  readHealth,
  readOverview,
  SPARK_MONITORING_LABELS,
  type SparkMonitoringDeps,
} from "@/lib/db/providers/sql/spark/monitoring";
import { formatDuration } from "@/lib/db/utils/pool-manager";

const NOW = new Date("2026-10-13T12:00:00Z");

afterEach(() => {
  setSystemTime();
});

function deps(app: Readonly<Record<string, string>>, calls: string[] = []): SparkMonitoringDeps {
  return {
    version: async () => {
      calls.push("version");
      return "4.2.0";
    },
    confAll: async (prefix) => {
      calls.push(`confAll ${prefix}`);
      return app;
    },
    currentTables: async () => {
      calls.push("currentTables");
      return { count: 7, catalog: "spark_catalog", namespace: "studio" };
    },
  };
}

describe("overview", () => {
  test("version, start time and uptime from spark.app., the current namespace's tables, and no size or connections", async () => {
    setSystemTime(NOW);
    const runFor = 3 * 3_600_000 + 5 * 60_000;
    const start = NOW.getTime() - runFor;
    const calls: string[] = [];
    expect(await readOverview(deps({ startTime: String(start), name: "Spark Connect server" }, calls))).toStrictEqual({
      version: "4.2.0",
      uptime: formatDuration(runFor),
      startTime: new Date(start),
      maxConnections: 0,
      databaseSize: "N/A",
      tableCount: 7,
      tableCountSampledFrom: "the tables of spark_catalog.studio, the connection's current namespace",
      indexCount: 0,
    });
    expect(calls).toStrictEqual(["version", "confAll spark.app.", "currentTables"]);
  });

  test.each<Record<string, string>>([{}, { startTime: "" }, { startTime: "soon" }, { startTime: "-5" }])(
    "without a readable start time the uptime is N/A and no start time is claimed (%j)",
    async (app) => {
      const overview = await readOverview(deps(app));
      expect(overview.uptime).toBe("N/A");
      expect(overview.startTime).toBeUndefined();
      expect("startTime" in overview).toBe(false);
      expect("activeConnections" in overview).toBe(false);
      expect("databaseSizeBytes" in overview).toBe(false);
    },
  );
});

describe("health", () => {
  test("one AnalyzePlan round trip, and no figure Spark does not report", async () => {
    const calls: string[] = [];
    expect(await readHealth(deps({}, calls))).toStrictEqual({
      databaseSize: "N/A",
      cacheHitRatio: "N/A",
      slowQueries: [],
      activeSessions: [],
    });
    expect(calls).toStrictEqual(["version"]);
  });

  test("a failed version read fails health", async () => {
    const failing: Pick<SparkMonitoringDeps, "version"> = {
      version: async () => {
        throw new Error("UNAVAILABLE: No connection established");
      },
    };
    await expect(readHealth(failing)).rejects.toThrow("UNAVAILABLE: No connection established");
  });
});

test("the two empty states and the two refusals are the panels' own sentences", () => {
  expect(SPARK_MONITORING_LABELS).toStrictEqual({
    slowQueriesEmptyState:
      "Spark keeps finished statements in the driver's web UI, which Studio does not read in this version.",
    sessionsEmptyState:
      "Spark Connect sessions are listed in the driver's web UI, which Studio does not read in this version. Studio's own sessions last one request each.",
    tablesRefused:
      "Spark keeps table statistics only where its catalog stores them, and Studio does not read them in this version.",
    storageRefused: "Spark reports no storage figure Studio can read in this version.",
  });
});
