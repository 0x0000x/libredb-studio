import { useEffect, useState } from "react";
import { appFetch } from "@/lib/config/base-path";
import type { DatabaseConnection } from "@/lib/types";
import { buildConnectionPayload } from "./use-connection-payload";

const DEBOUNCE_MS = 800;

export type DryRunEstimate =
  | { readonly state: "idle" }
  | { readonly state: "pending" }
  | { readonly state: "ok"; readonly bytesHuman: string }
  | { readonly state: "error"; readonly message: string };

/**
 * Live "this query will process N" for engines whose EXPLAIN is a free dry run, asked while the person types.
 */
export function useDryRunEstimate(
  connection: DatabaseConnection | null | undefined,
  sql: string,
  enabled: boolean,
): DryRunEstimate {
  // Keyed by the connection and statement it answers, so idle and pending are derived rather than set in the effect.
  const [answer, setAnswer] = useState<{ id: string; sql: string; estimate: DryRunEstimate } | null>(null);
  const text = sql.trim();
  const active = enabled && !!connection && text !== "";

  useEffect(() => {
    if (!active || !connection) return;
    const setEstimate = (estimate: DryRunEstimate) => setAnswer({ id: connection.id, sql: text, estimate });
    const controller = new AbortController();
    const timer = setTimeout(async () => {
      try {
        const response = await appFetch("/api/db/query", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            ...buildConnectionPayload(connection),
            sql: text,
            options: {},
            explain: { mode: "estimate" },
          }),
          signal: controller.signal,
        });
        // A proxy's HTML error page is not JSON; the status is the useful part.
        const body = (await response.json().catch(() => ({}))) as {
          rows?: { bytes_processed_human?: unknown }[];
          error?: unknown;
        };
        const human = body.rows?.[0]?.bytes_processed_human;
        if (response.ok && typeof human === "string") setEstimate({ state: "ok", bytesHuman: human });
        else setEstimate({ state: "error", message: errorMessage(body.error) ?? `HTTP ${response.status}` });
      } catch (error) {
        if (!controller.signal.aborted) {
          setEstimate({ state: "error", message: error instanceof Error ? error.message : String(error) });
        }
      }
    }, DEBOUNCE_MS);

    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [active, connection, text]);

  if (!active) return { state: "idle" };
  return answer?.sql === text && answer.id === connection?.id ? answer.estimate : { state: "pending" };
}

/** The API answers `error` as a sentence or as an object carrying one; anything else names no reason. */
function errorMessage(error: unknown): string | undefined {
  if (typeof error === "string") return error;
  const message = (error as { message?: unknown } | null | undefined)?.message;
  return typeof message === "string" ? message : undefined;
}
