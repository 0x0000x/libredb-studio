"use client";

import { useEffect, useState } from "react";
import { appFetch } from "@/lib/config/base-path";
import { declaresCatalogSessions } from "@/lib/db/object-kinds";
import type { Container, ProviderCapabilities } from "@/lib/db/types";
import { storage } from "@/lib/storage";
import type { DatabaseConnection } from "@/lib/types";
import { buildConnectionPayload } from "./use-connection-payload";

/** Shown when the list fails without words of its own, as the studio's schema read says it. */
const LIST_FAILED = "Failed to list the server's databases";
/** An empty list is said too: the pages wait for a database, and none would ever come. */
const NONE_LISTED = "This role can connect to no database on this server";

/**
 * The databases a server-level connection lists, and the one a page reads (#1530), for the pages
 * outside the studio shell: the monitoring page and the admin operations panel. They start on the
 * database the studio last chose for this connection and remember a new choice the same way, so
 * all three surfaces agree on which database is active. On every other connection there are none.
 */
export function useCatalogs(connection: DatabaseConnection | null, capabilities: ProviderCapabilities | undefined) {
  const serverLevel = connection !== null && capabilities !== undefined && declaresCatalogSessions(capabilities);
  const connectionId = serverLevel ? connection.id : null;
  const [listed, setListed] = useState<{
    readonly readKey: string;
    readonly catalogs: readonly string[];
    readonly error: string | null;
  } | null>(null);
  const [picked, setPicked] = useState<{ readonly connectionId: string; readonly catalog: string } | null>(null);
  // One read per connection and attempt, so `retry` reads a failed list again.
  const [attempt, setAttempt] = useState(0);
  const readKey = connectionId === null ? null : `${attempt}:${connectionId}`;

  useEffect(() => {
    if (readKey === null || connection === null) return;
    let current = true;
    appFetch("/api/db/objects/containers", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildConnectionPayload(connection)),
    })
      .then(async (res) => {
        if (res.ok) {
          const catalogs = ((await res.json()) as Container[]).map((container) => container.name);
          return catalogs.length === 0 ? { catalogs, error: NONE_LISTED } : { catalogs };
        }
        // A refusal is shown in the route's own words, never as an empty list to wait on.
        const body = (await res.json().catch(() => ({}))) as { error?: string };
        return { catalogs: [], error: body.error || LIST_FAILED };
      })
      .catch(() => ({ catalogs: [], error: LIST_FAILED }))
      .then((read) => {
        if (current) setListed({ readKey, catalogs: read.catalogs, error: read.error ?? null });
      });
    return () => {
      current = false;
    };
  }, [connection, readKey]);

  const own = listed !== null && listed.readKey === readKey ? listed : null;
  const catalogs = own === null ? [] : own.catalogs;
  const wanted =
    picked !== null && picked.connectionId === connectionId
      ? picked.catalog
      : connectionId === null
        ? null
        : storage.getActiveCatalog(connectionId);
  const catalog = wanted !== null && catalogs.includes(wanted) ? wanted : catalogs[0];

  const setCatalog = (next: string) => {
    if (connectionId === null) return;
    storage.setActiveCatalog(connectionId, next);
    setPicked({ connectionId, catalog: next });
  };

  const retry = () => setAttempt((count) => count + 1);

  return { catalogs, catalog, setCatalog, error: own === null ? null : own.error, retry };
}
