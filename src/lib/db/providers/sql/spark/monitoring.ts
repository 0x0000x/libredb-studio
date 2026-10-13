/**
 * The overview and health panels, read on the provider's metadata session only. Spark has no connection count, size,
 * cache ratio or index Studio can read, so those are declared, never estimated.
 */
import type { DatabaseOverview, HealthInfo } from "@/lib/db/types";
import { formatDuration } from "@/lib/db/utils/pool-manager";

export interface SparkMonitoringDeps {
  /** AnalyzePlan `spark_version` on the metadata session: one round trip. */
  version(): Promise<string>;
  /** Config `get_all` under `prefix` on the metadata session, the prefix removed from every key, as Spark answers it. */
  confAll(prefix: string): Promise<Readonly<Record<string, string>>>;
  /** The table listing of the metadata session's current namespace. */
  currentTables(): Promise<{ count: number; catalog: string; namespace: string }>;
}

/** The monitoring panels' own sentences. */
export const SPARK_MONITORING_LABELS: Readonly<{
  slowQueriesEmptyState: string;
  sessionsEmptyState: string;
  tablesRefused: string;
  storageRefused: string;
}> = {
  slowQueriesEmptyState:
    "Spark keeps finished statements in the driver's web UI, which Studio does not read in this version.",
  sessionsEmptyState:
    "Spark Connect sessions are listed in the driver's web UI, which Studio does not read in this version. Studio's own sessions last one request each.",
  tablesRefused:
    "Spark keeps table statistics only where its catalog stores them, and Studio does not read them in this version.",
  storageRefused: "Spark reports no storage figure Studio can read in this version.",
};

const NOT_AVAILABLE = "N/A";

export async function readOverview(deps: SparkMonitoringDeps): Promise<DatabaseOverview> {
  const version = await deps.version();
  // Read in the provider's own session: a user session can override both keys for itself.
  const app = await deps.confAll("spark.app.");
  const tables = await deps.currentTables();
  const raw = app.startTime;
  const now = Date.now();
  // `spark.app.startTime` is epoch milliseconds. Any other text, and a start later than Studio's clock, claims no start
  // time, so the overview never shows a negative uptime. A start past the Date range is later than any clock, so it
  // never becomes an Invalid Date either.
  const read = raw !== undefined && /^\d+$/.test(raw) ? Number(raw) : Number.NaN;
  const startMs = read <= now ? read : undefined;
  return {
    version,
    uptime: startMs === undefined ? NOT_AVAILABLE : formatDuration(now - startMs),
    ...(startMs === undefined ? {} : { startTime: new Date(startMs) }),
    maxConnections: 0,
    databaseSize: NOT_AVAILABLE,
    tableCount: tables.count,
    tableCountSampledFrom: `the tables of ${tables.catalog}.${tables.namespace}, the connection's current namespace`,
    indexCount: 0,
  };
}

export async function readHealth(deps: Pick<SparkMonitoringDeps, "version">): Promise<HealthInfo> {
  await deps.version();
  return { databaseSize: NOT_AVAILABLE, cacheHitRatio: NOT_AVAILABLE, slowQueries: [], activeSessions: [] };
}
