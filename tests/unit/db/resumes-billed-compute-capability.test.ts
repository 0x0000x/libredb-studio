import { describe, expect, test } from "bun:test";
import { SHIPPED_DATABASE_TYPES } from "@/lib/db/compatibility";
import { createDatabaseProvider } from "@/lib/db/factory";
import type { DatabaseType } from "@/lib/types";
import { CENSUS_CONNECTION } from "../../helpers/census-connection";

/**
 * `resumesBilledCompute` across every shipped provider.
 *
 * The capability withholds the connection pulse and the admin fleet health check from a connection, and labels the
 * monitoring auto-refresh toggle with the billing sentence. Declared by an engine whose suspended compute a request
 * resumes and bills, it keeps Studio from holding that compute awake; declared anywhere else it would silently drop
 * the health of a connection that costs nothing to check. So the census holds every shipped provider to the record
 * below, read from the census connection:
 *
 * - Snowflake declares it from configuration alone, because every statement that needs a warehouse can resume it and
 *   bills at least a minute.
 * - Databend declares it when its connection names a Warehouse or its host is Databend Cloud's, whose older form
 *   reaches a warehouse with Warehouse empty (section 4.4 of its provider doc); its own case is below, since the
 *   census connection names neither.
 * - Every other provider keeps its pulse.
 *
 * Nothing here connects: `createDatabaseProvider` builds each provider over `CENSUS_CONNECTION`'s unconnected
 * configuration, and `getCapabilities()` is a declaration.
 */
const EXPECTED: Readonly<Record<DatabaseType, boolean>> = {
  ...Object.fromEntries(SHIPPED_DATABASE_TYPES.map((type) => [type, false])),
  snowflake: true,
} as Record<DatabaseType, boolean>;

describe("resumesBilledCompute across the shipped providers", () => {
  test.each([...SHIPPED_DATABASE_TYPES])("%s declares it exactly where the record says", async (type) => {
    const provider = await createDatabaseProvider(CENSUS_CONNECTION[type]);
    expect(provider.getCapabilities().resumesBilledCompute === true).toBe(EXPECTED[type]);
    // Only the literal true is declared, so a provider that does not declare it leaves the field absent.
    if (!EXPECTED[type]) expect(provider.getCapabilities().resumesBilledCompute).toBeUndefined();
  });

  test("the record names only shipped types, and snowflake is the one declarer of the census connection", () => {
    expect(Object.keys(EXPECTED).sort()).toEqual([...SHIPPED_DATABASE_TYPES].sort());
    expect(SHIPPED_DATABASE_TYPES.filter((type) => EXPECTED[type])).toEqual(["snowflake"]);
  });

  test("databend declares it when its connection names a Warehouse, read from an unconnected provider", async () => {
    const provider = await createDatabaseProvider({ ...CENSUS_CONNECTION.databend, warehouse: "census-wh" });
    expect(provider.getCapabilities().resumesBilledCompute).toBe(true);
    expect(provider.isConnected()).toBe(false);
  });
});
