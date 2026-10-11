import "../setup-dom";

import { afterEach, describe, expect, mock, test } from "bun:test";
import { renderHook, waitFor } from "@testing-library/react";
import { mockGlobalFetch, restoreGlobalFetch } from "../helpers/mock-fetch";

import { useDryRunEstimate } from "@/hooks/use-dry-run-estimate";
import type { DatabaseConnection } from "@/lib/types";

const connection = { id: "c1", name: "bq", type: "bigquery" } as unknown as DatabaseConnection;
const WAIT = { timeout: 2000 };

describe("useDryRunEstimate", () => {
  afterEach(() => restoreGlobalFetch());

  test("idle when disabled", () => {
    const fetchMock = mockGlobalFetch({});
    const { result } = renderHook(() => useDryRunEstimate(connection, "SELECT 1", false));
    expect(result.current).toEqual({ state: "idle" });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test("idle without a connection or with blank SQL", () => {
    expect(renderHook(() => useDryRunEstimate(null, "SELECT 1", true)).result.current).toEqual({ state: "idle" });
    expect(renderHook(() => useDryRunEstimate(connection, "   ", true)).result.current).toEqual({ state: "idle" });
  });

  test("pending, then ok after the debounce with the estimate request", async () => {
    const fetchMock = mockGlobalFetch({
      "/api/db/query": { ok: true, json: { rows: [{ bytes_processed_human: "1.2 GB" }] } },
    });
    const { result } = renderHook(() => useDryRunEstimate(connection, " SELECT 1 ", true));
    expect(result.current).toEqual({ state: "pending" });
    await waitFor(() => expect(result.current).toEqual({ state: "ok", bytesHuman: "1.2 GB" }), WAIT);
    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect(init.method).toBe("POST");
    const body = JSON.parse(init.body as string);
    expect(body.sql).toBe("SELECT 1");
    expect(body.explain).toEqual({ mode: "estimate" });
  });

  test("non-ok response reports body.error", async () => {
    mockGlobalFetch({ "/api/db/query": { status: 400, json: { error: "Syntax error" } } });
    const { result } = renderHook(() => useDryRunEstimate(connection, "SELEC", true));
    await waitFor(() => expect(result.current).toEqual({ state: "error", message: "Syntax error" }), WAIT);
  });

  test("an error object reports its message, never [object Object]", async () => {
    mockGlobalFetch({ "/api/db/query": { status: 400, json: { error: { message: "Bad dataset", code: 404 } } } });
    const { result } = renderHook(() => useDryRunEstimate(connection, "SELECT 1", true));
    await waitFor(() => expect(result.current).toEqual({ state: "error", message: "Bad dataset" }), WAIT);
  });

  test("an error object without a string message reports the HTTP status", async () => {
    mockGlobalFetch({ "/api/db/query": { status: 403, json: { error: { code: 403 } } } });
    const { result } = renderHook(() => useDryRunEstimate(connection, "SELECT 1", true));
    await waitFor(() => expect(result.current).toEqual({ state: "error", message: "HTTP 403" }), WAIT);
  });

  test("ok response without bytes_processed_human reports the HTTP status", async () => {
    mockGlobalFetch({ "/api/db/query": { status: 200, json: { rows: [{}] } } });
    const { result } = renderHook(() => useDryRunEstimate(connection, "SELECT 1", true));
    await waitFor(() => expect(result.current).toEqual({ state: "error", message: "HTTP 200" }), WAIT);
  });

  test("fetch rejection reports its message", async () => {
    globalThis.fetch = mock(async () => {
      throw new Error("network down");
    }) as unknown as typeof fetch;
    const { result } = renderHook(() => useDryRunEstimate(connection, "SELECT 1", true));
    await waitFor(() => expect(result.current).toEqual({ state: "error", message: "network down" }), WAIT);
  });

  test("non-Error rejection is stringified", async () => {
    globalThis.fetch = mock(() => Promise.reject("plain")) as unknown as typeof fetch;
    const { result } = renderHook(() => useDryRunEstimate(connection, "SELECT 1", true));
    await waitFor(() => expect(result.current).toEqual({ state: "error", message: "plain" }), WAIT);
  });

  test("changing the SQL aborts the in-flight request without reporting it", async () => {
    const signals: AbortSignal[] = [];
    globalThis.fetch = mock((_input: RequestInfo | URL, init?: RequestInit) => {
      const signal = init?.signal as AbortSignal;
      signals.push(signal);
      if (signals.length === 1) {
        return new Promise<Response>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(new Error("aborted")));
        });
      }
      return Promise.resolve(new Response(JSON.stringify({ rows: [{ bytes_processed_human: "5 MB" }] })));
    }) as unknown as typeof fetch;

    const { result, rerender } = renderHook(({ sql }) => useDryRunEstimate(connection, sql, true), {
      initialProps: { sql: "SELECT 1" },
    });
    await waitFor(() => expect(signals.length).toBe(1), WAIT);
    rerender({ sql: "SELECT 2" });
    expect(signals[0].aborted).toBe(true);
    expect(result.current).toEqual({ state: "pending" });
    await waitFor(() => expect(result.current).toEqual({ state: "ok", bytesHuman: "5 MB" }), WAIT);
    expect(signals.length).toBe(2);
  });

  test("a non-JSON error page reports the HTTP status, not the parse message", async () => {
    globalThis.fetch = mock(
      async () => new Response("<html>Bad Gateway</html>", { status: 502 }),
    ) as unknown as typeof fetch;
    const { result } = renderHook(() => useDryRunEstimate(connection, "SELECT 1", true));
    await waitFor(() => expect(result.current).toEqual({ state: "error", message: "HTTP 502" }), WAIT);
  });

  test("another connection with the same SQL is pending, not the previous connection's estimate", async () => {
    let calls = 0;
    globalThis.fetch = mock(() => {
      calls++;
      if (calls === 1)
        return Promise.resolve(new Response(JSON.stringify({ rows: [{ bytes_processed_human: "1 GB" }] })));
      return new Promise<Response>(() => {});
    }) as unknown as typeof fetch;
    const other = { ...connection, id: "c2" } as DatabaseConnection;
    const { result, rerender } = renderHook(({ c }) => useDryRunEstimate(c, "SELECT 1", true), {
      initialProps: { c: connection },
    });
    await waitFor(() => expect(result.current).toEqual({ state: "ok", bytesHuman: "1 GB" }), WAIT);
    rerender({ c: other });
    expect(result.current).toEqual({ state: "pending" });
  });

  test("unmount clears the pending timer", async () => {
    const fetchMock = mockGlobalFetch({});
    const { unmount } = renderHook(() => useDryRunEstimate(connection, "SELECT 1", true));
    unmount();
    await new Promise((resolve) => setTimeout(resolve, 900));
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
