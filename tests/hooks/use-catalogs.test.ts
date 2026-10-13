import "../setup-dom";

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { act, renderHook, waitFor } from "@testing-library/react";
import { mockGlobalFetch, restoreGlobalFetch } from "../helpers/mock-fetch";
import { useCatalogs } from "@/hooks/use-catalogs";
import { storage } from "@/lib/storage";
import type { ProviderCapabilities } from "@/lib/db/types";
import type { DatabaseConnection } from "@/lib/types";

/** The databases the monitoring and operations pages offer on a server-level connection (#1530). */
const SERVER: DatabaseConnection = {
  id: "srv-1",
  name: "Server",
  type: "postgres",
  host: "db.internal",
  database: "",
  createdAt: new Date("2026-01-01"),
};
const SERVER_LEVEL = {
  catalogSessions: true,
  containerLevels: [
    { id: "catalog", label: "Database", labelPlural: "Databases" },
    { id: "schema", label: "Schema", labelPlural: "Schemas" },
  ],
} as unknown as ProviderCapabilities;
const PINNED = {
  containerLevels: [{ id: "schema", label: "Schema", labelPlural: "Schemas" }],
} as unknown as ProviderCapabilities;

const databases = (...names: string[]) => ({ json: names.map((name) => ({ path: [name], name, level: 0 })) });

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => {
  restoreGlobalFetch();
});

describe("useCatalogs", () => {
  test("starts on the database the studio chose last, and remembers a new choice", async () => {
    mockGlobalFetch({ "/api/db/objects/containers": databases("analytics", "shop") });
    storage.setActiveCatalog(SERVER.id, "shop");
    const { result } = renderHook(() => useCatalogs(SERVER, SERVER_LEVEL));
    await waitFor(() => expect(result.current.catalogs).toEqual(["analytics", "shop"]));
    expect(result.current.catalog).toBe("shop");

    act(() => result.current.setCatalog("analytics"));
    expect(result.current.catalog).toBe("analytics");
    expect(storage.getActiveCatalog(SERVER.id)).toBe("analytics");
  });

  test("starts on the first database when nothing was chosen, or the choice is gone", async () => {
    mockGlobalFetch({ "/api/db/objects/containers": databases("analytics", "shop") });
    storage.setActiveCatalog(SERVER.id, "dropped_since");
    const { result } = renderHook(() => useCatalogs(SERVER, SERVER_LEVEL));
    await waitFor(() => expect(result.current.catalog).toBe("analytics"));
  });

  test("a refused or unreachable list offers no database and says why, and a retry reads it again", async () => {
    mockGlobalFetch({ "/api/db/objects/containers": { ok: false, status: 503, json: { error: "down" } } });
    const refused = renderHook(() => useCatalogs(SERVER, SERVER_LEVEL));
    await waitFor(() => expect(refused.result.current.error).toBe("down"));
    expect(refused.result.current.catalogs).toEqual([]);
    expect(refused.result.current.catalog).toBeUndefined();

    // Without words of its own, the refusal says what the studio's schema read says.
    mockGlobalFetch({ "/api/db/objects/containers": { ok: false, status: 500, text: "not json" } });
    act(() => refused.result.current.retry());
    await waitFor(() => expect(refused.result.current.error).toBe("Failed to list the server's databases"));

    mockGlobalFetch({ "/api/db/objects/containers": databases("analytics") });
    act(() => refused.result.current.retry());
    await waitFor(() => expect(refused.result.current.catalogs).toEqual(["analytics"]));
    expect(refused.result.current.error).toBeNull();

    globalThis.fetch = (async () => {
      throw new TypeError("Failed to fetch");
    }) as unknown as typeof fetch;
    const unreachable = renderHook(() => useCatalogs({ ...SERVER, id: "srv-2" }, SERVER_LEVEL));
    await waitFor(() => expect(unreachable.result.current.error).toBe("Failed to list the server's databases"));
    expect(unreachable.result.current.catalogs).toEqual([]);
  });

  test("a list naming no database says so, rather than leaving the page waiting for one", async () => {
    mockGlobalFetch({ "/api/db/objects/containers": databases() });
    const empty = renderHook(() => useCatalogs(SERVER, SERVER_LEVEL));
    await waitFor(() => expect(empty.result.current.error).toBe("This role can connect to no database on this server"));
    expect(empty.result.current.catalog).toBeUndefined();
  });

  test("a connection that names its own database lists none and asks nothing", () => {
    const fetchMock = mockGlobalFetch({});
    const pinned = renderHook(() => useCatalogs({ ...SERVER, database: "sales" }, PINNED));
    expect(pinned.result.current.catalogs).toEqual([]);
    act(() => pinned.result.current.setCatalog("shop"));
    expect(storage.getActiveCatalog(SERVER.id)).toBeNull();
    const none = renderHook(() => useCatalogs(null, undefined));
    expect(none.result.current.catalog).toBeUndefined();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
