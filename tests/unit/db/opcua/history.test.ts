/**
 * Raw history paging, bounded, against the two behaviours real servers have (#1677 section 6 item 4).
 *
 * TWO PAGING STRATEGIES, BECAUSE ONLY SOME SERVERS OFFER THE FIRST. OPC PLC hands back a
 * continuation point and token paging works. asyncua NEVER hands one back: measured, a read with
 * `numValuesPerNode: 5` returned 5 values and no token, leaving the rest unreachable that way. The
 * fallback advances the start time past the last source timestamp.
 *
 * AND EVERY CONTINUATION POINT IS CONSUMED OR RELEASED. They are scarce session state shared with
 * the plant's own clients, so a read that stops at the cap hands the slot back.
 */
import { describe, expect, test } from "bun:test";
import type {
  OpcuaHistoryPage,
  OpcuaHistoryValue,
  OpcuaReadOnlyClient,
} from "@/lib/db/providers/industrial/opcua/client";
import { historyEndSentence, readHistory } from "@/lib/db/providers/industrial/opcua/history";

/** A value at one second past the epoch plus `at` seconds, which is how a step is spelled here. */
function value(at: number): OpcuaHistoryValue {
  return {
    value: at,
    sourceTimestamp: new Date(1_000_000 + at * 1_000).toISOString(),
    statusCode: "Good",
  };
}

/** What a fake client recorded, so a test can assert the release as well as the reads. */
interface Recorded {
  readonly reads: { valuesPerPage: number; startTime?: Date; endTime?: Date }[];
  readonly nexts: number;
  readonly released: number;
  /** The window every continuation and release carried, which a server validates the point against. */
  readonly continued: {
    startTime?: Date;
    endTime?: Date;
    valuesPerPage: number;
  }[];
}

/**
 * A server that pages by continuation point, like OPC PLC and the in-process node-opcua server.
 */
function tokenPaging(
  total: number,
  record: {
    reads: Recorded["reads"];
    nexts: number;
    released: number;
    continued: Recorded["continued"];
  },
): OpcuaReadOnlyClient {
  let served = 0;
  const page = (perPage: number): OpcuaHistoryPage => {
    const values = Array.from({ length: Math.min(perPage, total - served) }, (_, at) => value(served + at));
    served += values.length;
    return {
      nodeId: "ns=3;s=H",
      values,
      ...(served < total ? { continuationPoint: new Uint8Array([1, served & 0xff]) } : {}),
      statusCode: "Good",
    };
  };
  return {
    endpoints: async () => [],
    limits: async () => ({}),
    browse: async () => [],
    browseNext: async () => [],
    releaseBrowseContinuationPoints: async () => {},
    read: async () => [],
    historyRead: async (options) => {
      record.reads.push({
        valuesPerPage: options.valuesPerPage,
        ...(options.startTime ? { startTime: options.startTime } : {}),
        ...(options.endTime ? { endTime: options.endTime } : {}),
      });
      return page(options.valuesPerPage);
    },
    historyReadNext: async (options) => {
      record.nexts += 1;
      record.continued.push({
        startTime: options.startTime,
        endTime: options.endTime,
        valuesPerPage: options.valuesPerPage,
      });
      return page(options.valuesPerPage);
    },
    releaseHistoryContinuationPoint: async (options) => {
      record.released += 1;
      record.continued.push({
        startTime: options.startTime,
        endTime: options.endTime,
        valuesPerPage: options.valuesPerPage,
      });
    },
    close: async () => {},
  };
}

/** A server that fills a page and hands back no token, which is what asyncua does. */
function timestampPaging(
  total: number,
  record: {
    reads: Recorded["reads"];
    nexts: number;
    released: number;
    continued: Recorded["continued"];
  },
  /** What the server will return at most, however many were asked for: Part 11 5.3.2 allows fewer. */
  serverCap = Number.POSITIVE_INFINITY,
): OpcuaReadOnlyClient {
  /*
   * A REAL HISTORIAN, WHICH MEANS IT ANSWERS FROM THE WINDOW AND NOT FROM A COUNTER. This fake used
   * to serve the next `perPage` values from a running total and ignore `options.startTime`
   * altogether - it mocked away the very thing the tests below are about. With `startTime` ignored,
   * the `+ 1` that advances past the last value read could be `+ 0` and every test still passed;
   * measured against an inclusive-start historian, that mutant returns 1,198 DISTINCT values of
   * 1,200 because each page re-serves the value the page before it ended on.
   *
   * Part 11 5.3.2: `ReadRawModified` returns values at or after `startTime`, so the bound is
   * inclusive and advancing past a value means asking from one millisecond later.
   */
  const held = Array.from({ length: total }, (_, at) => value(at));
  return {
    endpoints: async () => [],
    limits: async () => ({}),
    browse: async () => [],
    browseNext: async () => [],
    releaseBrowseContinuationPoints: async () => {},
    read: async () => [],
    historyRead: async (options) => {
      record.reads.push({
        valuesPerPage: options.valuesPerPage,
        ...(options.startTime ? { startTime: options.startTime } : {}),
        ...(options.endTime ? { endTime: options.endTime } : {}),
      });
      const from = options.startTime?.getTime() ?? Number.NEGATIVE_INFINITY;
      const until = options.endTime?.getTime() ?? Number.POSITIVE_INFINITY;
      const inWindow = held.filter((row) => {
        const at = new Date(row.sourceTimestamp ?? 0).getTime();
        return at >= from && at <= until;
      });
      // No continuation point, ever: this is the asyncua behaviour the fallback exists for.
      return {
        nodeId: "ns=3;s=H",
        values: inWindow.slice(0, Math.min(options.valuesPerPage, serverCap)),
        statusCode: "Good",
      };
    },
    historyReadNext: async () => {
      record.nexts += 1;
      return { nodeId: "ns=3;s=H", values: [], statusCode: "Good" };
    },
    releaseHistoryContinuationPoint: async () => {
      record.released += 1;
    },
    close: async () => {},
  };
}

const fresh = (): {
  reads: Recorded["reads"];
  nexts: number;
  released: number;
  continued: Recorded["continued"];
} => ({
  reads: [],
  nexts: 0,
  released: 0,
  continued: [],
});

describe("paging by continuation point", () => {
  test("every value in the window is read, over as many pages as it takes", async () => {
    const record = fresh();
    const result = await readHistory(tokenPaging(1_250, record), {
      nodeId: "ns=3;s=H",
    });
    expect(result.values).toHaveLength(1_250);
    expect(result.end).toBe("exhausted");
    expect(result.paging).toBe("continuation-point");
    expect(record.nexts).toBeGreaterThan(0);
  });

  /** The page size is always set, never left to the server to choose (Part 11 6.5.3). */
  test("the first read names a page size rather than leaving it to the server", async () => {
    const record = fresh();
    await readHistory(tokenPaging(10, record), { nodeId: "ns=3;s=H" });
    expect(record.reads[0]?.valuesPerPage).toBeGreaterThan(0);
  });

  test("the cap stops the read, and says so rather than implying the history ended", async () => {
    const record = fresh();
    const result = await readHistory(tokenPaging(100_000, record), {
      nodeId: "ns=3;s=H",
      limit: 700,
    });
    expect(result.values).toHaveLength(700);
    expect(result.end).toBe("value-cap");
  });

  /** The slot is handed back, because a plant's own clients share the server's table. */
  test("stopping at the cap releases the server's continuation point", async () => {
    const record = fresh();
    await readHistory(tokenPaging(100_000, record), {
      nodeId: "ns=3;s=H",
      limit: 700,
    });
    expect(record.released).toBe(1);
  });

  test("a read that ends on its own releases nothing, because there is nothing to release", async () => {
    const record = fresh();
    await readHistory(tokenPaging(50, record), { nodeId: "ns=3;s=H" });
    expect(record.released).toBe(0);
  });
});

describe("paging by timestamp, for a server that hands back no token", () => {
  test("the read continues past the first page by advancing the start time", async () => {
    const record = fresh();
    const result = await readHistory(timestampPaging(1_200, record), {
      nodeId: "ns=3;s=H",
    });
    expect(result.values).toHaveLength(1_200);
    expect(result.paging).toBe("timestamp");
    expect(result.end).toBe("exhausted");
    // Every read after the first names a start time, which is how it advanced.
    expect(record.reads.length).toBeGreaterThan(1);
    expect(record.reads[1]?.startTime).toBeDefined();
    expect(record.nexts).toBe(0);
  });

  test("the advance is past the last value, so no value is read twice", async () => {
    const record = fresh();
    const result = await readHistory(timestampPaging(1_200, record), {
      nodeId: "ns=3;s=H",
    });
    const stamps = result.values.map((entry) => entry.sourceTimestamp);
    expect(new Set(stamps).size).toBe(stamps.length);
  });

  test("a partial page ends the read, because the window is spent", async () => {
    const record = fresh();
    const result = await readHistory(timestampPaging(10, record), {
      nodeId: "ns=3;s=H",
    });
    expect(result.values).toHaveLength(10);
    expect(result.end).toBe("exhausted");
  });
});

/**
 * EVERY END SENTENCE IS A WHOLE MESSAGE NOW, so every one of them starts with a capital. They are
 * the `message` of the result's one `warnings` entry, drawn on its own beside the grid; two of them
 * started lowercase, which is what a FRAGMENT looks like - they were written when this text was
 * appended to something else.
 */
describe("every end sentence reads as a sentence", () => {
  test("each one starts with a capital letter", () => {
    const ends = ["exhausted", "refused", "value-cap", "no-continuation", "empty-pages"] as const;
    for (const end of ends) {
      const sentence = historyEndSentence(end, 7);
      const first = sentence[0] ?? "";
      if (first !== first.toUpperCase()) throw new Error(`the ${end} sentence starts lowercase: ${sentence}`);
    }
  });

  /** And a refusal's sentence is the status table's, which already starts with a capital. */
  test("a refusal carrying a code reads as the error table writes it", () => {
    expect(historyEndSentence("refused", 0, "BadHistoryOperationUnsupported")).toMatch(/^The server answered Bad/);
  });
});

describe("what the reader is told", () => {
  test("each end reason is a sentence that says what to do, or that nothing can be", () => {
    expect(historyEndSentence("exhausted", 361)).toMatch(/no more values/);
    expect(historyEndSentence("value-cap", 10_000)).toMatch(/narrower window/);
    expect(historyEndSentence("no-continuation", 500)).toMatch(/unreachable/);
  });

  test("the cap's sentence says why a cap exists at all", () => {
    expect(historyEndSentence("value-cap", 10_000)).toMatch(/appends history while it is read/);
  });

  test("every sentence carries the count, because a reader asks how much they have", () => {
    for (const end of ["exhausted", "value-cap", "no-continuation"] as const) {
      expect(historyEndSentence(end, 1_234)).toContain("1,234");
    }
  });
});

/**
 * THE WINDOW IS FIXED ONCE, EVEN WHEN THE READER NAMED NEITHER END.
 *
 * A server validates a continued read against the details of the read that handed out the point.
 * Measured on OPC PLC 2.15.10: an open-ended window resolved per call - `new Date()` a few seconds
 * later each time - answered `BadContinuationPointInvalid` on the second page, so a reader saw the
 * first page and was told the history had ended. With the window fixed once, the same read walked
 * 800 values over 9 pages and ended exhausted.
 */
describe("every continuation carries the first page's window", () => {
  test("a read with no window of its own still sends one window, the same one, every time", async () => {
    const record = fresh();
    const result = await readHistory(tokenPaging(1_200, record), {
      nodeId: "ns=3;s=H",
      limit: 1_200,
    });
    expect(result.pages).toBeGreaterThan(1);
    expect(record.continued.length).toBe(result.pages - 1);

    const first = record.reads[0];
    expect(first?.startTime).toBeDefined();
    expect(first?.endTime).toBeDefined();
    for (const continued of record.continued) {
      expect(continued.startTime?.getTime()).toBe(first?.startTime?.getTime());
      expect(continued.endTime?.getTime()).toBe(first?.endTime?.getTime());
    }
  });

  test("a release carries that window too, because it is the same service with one field set", async () => {
    const record = fresh();
    // The cap stops the read with a point still outstanding, which is the release path.
    await readHistory(tokenPaging(5_000, record), {
      nodeId: "ns=3;s=H",
      limit: 600,
    });
    expect(record.released).toBe(1);
    const first = record.reads[0];
    const release = record.continued.at(-1);
    expect(release?.startTime?.getTime()).toBe(first?.startTime?.getTime());
    expect(release?.endTime?.getTime()).toBe(first?.endTime?.getTime());
  });

  test("a reader's own window is the one that travels, unchanged", async () => {
    const record = fresh();
    const start = new Date("2026-10-12T00:00:00.000Z");
    const end = new Date("2026-10-12T01:00:00.000Z");
    await readHistory(tokenPaging(1_200, record), {
      nodeId: "ns=3;s=H",
      start,
      end,
      limit: 1_200,
    });
    for (const sent of [...record.reads, ...record.continued]) {
      expect(sent.startTime?.getTime()).toBe(start.getTime());
      expect(sent.endTime?.getTime()).toBe(end.getTime());
    }
  });
});

/**
 * A FULL PAGE WITH NO TOKEN AND NO TIMESTAMP IS UNREACHABLE, and the read says so rather than
 * implying the history ended. Both fallbacks are gone at once: there is nothing to continue from
 * and nothing to advance past, so the page reports what it read and names what it cannot reach.
 */
describe("a server that pages by neither token nor timestamp", () => {
  test("the read stops and the reason is no-continuation, not exhausted", async () => {
    const timeless: OpcuaReadOnlyClient = {
      endpoints: async () => [],
      limits: async () => ({}),
      browse: async () => [],
      browseNext: async () => [],
      releaseBrowseContinuationPoints: async () => {},
      read: async () => [],
      // A full page: every value the request asked for, none of them carrying a source timestamp.
      historyRead: async (options) => ({
        nodeId: options.nodeId,
        values: Array.from({ length: options.valuesPerPage }, () => ({
          value: 1,
          statusCode: "Good",
        })),
        statusCode: "Good",
      }),
      historyReadNext: async () => ({
        nodeId: "ns=3;s=H",
        values: [],
        statusCode: "Good",
      }),
      releaseHistoryContinuationPoint: async () => {},
      close: async () => {},
    };
    // A cap ABOVE one page, so the page is full without reaching the cap: that is the only way to
    // the arm this test is for, because a page that fills the cap stops as `value-cap` first.
    const result = await readHistory(timeless, {
      nodeId: "ns=3;s=H",
      limit: 1_200,
    });
    expect(result.values).toHaveLength(500);
    expect(result.end).toBe("no-continuation");
    expect(historyEndSentence(result.end, result.values.length)).toMatch(/unreachable/);
  });

  /** A timestamp that is not an instant is no timestamp: the same answer, by the same route. */
  test("a page whose timestamps do not parse is reported the same way", async () => {
    const nonsense: OpcuaReadOnlyClient = {
      endpoints: async () => [],
      limits: async () => ({}),
      browse: async () => [],
      browseNext: async () => [],
      releaseBrowseContinuationPoints: async () => {},
      read: async () => [],
      historyRead: async (options) => ({
        nodeId: options.nodeId,
        values: Array.from({ length: options.valuesPerPage }, () => ({
          value: 1,
          sourceTimestamp: "not an instant",
          statusCode: "Good",
        })),
        statusCode: "Good",
      }),
      historyReadNext: async () => ({
        nodeId: "ns=3;s=H",
        values: [],
        statusCode: "Good",
      }),
      releaseHistoryContinuationPoint: async () => {},
      close: async () => {},
    };
    const result = await readHistory(nonsense, {
      nodeId: "ns=3;s=H",
      limit: 1_200,
    });
    expect(result.end).toBe("no-continuation");
  });
});

/**
 * ONE PAGE SIZE FOR THE WHOLE READ, and the cap is honoured by SLICING rather than by asking for
 * less.
 *
 * `numValuesPerNode` is one of the details a server validates a continuation against. Measured on
 * OPC PLC: shrinking it as the cap came closer - 500, 500, then 200 for a cap of 1,200 - made the
 * last continuation a different request, which answered `BadContinuationPointInvalid` with zero
 * values and no new point. The loop then read that as an empty window, so a reader who asked for
 * 1,200 values of a node holding 1,475 was handed 800 and told the server had no more.
 */
describe("the page size is fixed for the whole read", () => {
  test("every page and every release asks for the same number of values", async () => {
    const record = fresh();
    const result = await readHistory(tokenPaging(5_000, record), {
      nodeId: "ns=3;s=H",
      limit: 1_200,
    });
    expect(result.values).toHaveLength(1_200);
    const asked = [...record.reads.map((read) => read.valuesPerPage), ...record.continued.map((c) => c.valuesPerPage)];
    expect(new Set(asked).size).toBe(1);
    expect(asked[0]).toBe(500);
  });

  test("a cap that is not a multiple of a page is still exact, and still one page size", async () => {
    const record = fresh();
    const result = await readHistory(tokenPaging(5_000, record), {
      nodeId: "ns=3;s=H",
      limit: 700,
    });
    expect(result.values).toHaveLength(700);
    expect(result.end).toBe("value-cap");
    expect(new Set(record.continued.map((c) => c.valuesPerPage)).size).toBe(1);
  });

  test("a cap below one page asks for the cap, once", async () => {
    const record = fresh();
    const result = await readHistory(tokenPaging(5_000, record), {
      nodeId: "ns=3;s=H",
      limit: 40,
    });
    expect(result.values).toHaveLength(40);
    expect(record.reads[0]?.valuesPerPage).toBe(40);
  });
});

/**
 * A REFUSAL IS NOT AN EMPTY WINDOW, and the server's own code is the only thing that tells them
 * apart. Measured on OPC PLC: a node that keeps no history answers `BadHistoryOperationUnsupported`
 * with zero values, and a node that does not exist answers `BadNodeIdUnknown` with zero values.
 */
describe("a server that refuses the read", () => {
  function refusing(statusCode: string, values: number): OpcuaReadOnlyClient {
    let served = 0;
    return {
      endpoints: async () => [],
      limits: async () => ({}),
      browse: async () => [],
      browseNext: async () => [],
      releaseBrowseContinuationPoints: async () => {},
      read: async () => [],
      historyRead: async (options) => {
        // A server that refuses outright answers the code on the FIRST page; one that refuses a
        // continuation answers a good page first, with a point, and the code on the next call.
        if (values === 0) return { nodeId: options.nodeId, values: [], statusCode };
        if (served > 0) return { nodeId: options.nodeId, values: [], statusCode };
        served += 1;
        return {
          nodeId: options.nodeId,
          values: Array.from({ length: values }, (_, at) => value(at)),
          continuationPoint: new Uint8Array([1]),
          statusCode: "Good",
        };
      },
      historyReadNext: async (options) => ({
        nodeId: options.nodeId,
        values: [],
        statusCode,
      }),
      releaseHistoryContinuationPoint: async () => {},
      close: async () => {},
    };
  }

  test("a first page that is refused answers the code, not exhaustion", async () => {
    const result = await readHistory(refusing("BadHistoryOperationUnsupported", 0), {
      nodeId: "ns=3;s=SlowUInt1",
      limit: 100,
    });
    expect(result.end).toBe("refused");
    expect(result.statusCode).toBe("BadHistoryOperationUnsupported");
    expect(result.values).toEqual([]);
    expect(historyEndSentence(result.end, 0, result.statusCode)).toContain("that node keeps no history");
  });

  test("a continuation that is refused keeps what was read and names the refusal", async () => {
    const result = await readHistory(refusing("BadContinuationPointInvalid", 500), {
      nodeId: "ns=3;s=H",
      limit: 1_200,
    });
    expect(result.values).toHaveLength(500);
    expect(result.end).toBe("refused");
    expect(result.statusCode).toBe("BadContinuationPointInvalid");
    const sentence = historyEndSentence(result.end, result.values.length, result.statusCode);
    expect(sentence).toContain("BadContinuationPointInvalid");
    expect(sentence).toContain("500 values were read before it");
  });

  /**
   * A REFUSAL THAT ARRIVES WITH A POINT STILL HANDS THE SLOT BACK. A server may refuse a page and
   * hand back a continuation point in the same answer; holding it until the session closed would
   * keep one of the hundred OPC PLC shares with the plant's own clients.
   */
  test("a refusal carrying a continuation point releases it", async () => {
    const released: Uint8Array[] = [];
    const refusingWithPoint: OpcuaReadOnlyClient = {
      endpoints: async () => [],
      limits: async () => ({}),
      browse: async () => [],
      browseNext: async () => [],
      releaseBrowseContinuationPoints: async () => {},
      read: async () => [],
      historyRead: async (options) => ({
        nodeId: options.nodeId,
        values: [],
        continuationPoint: new Uint8Array([3]),
        statusCode: "BadUserAccessDenied",
      }),
      historyReadNext: async (options) => ({
        nodeId: options.nodeId,
        values: [],
        statusCode: "Good",
      }),
      releaseHistoryContinuationPoint: async (options) => {
        released.push(options.continuationPoint);
      },
      close: async () => {},
    };
    const result = await readHistory(refusingWithPoint, {
      nodeId: "ns=3;s=H",
      limit: 100,
    });
    expect(result.end).toBe("refused");
    expect(result.statusCode).toBe("BadUserAccessDenied");
    expect(released).toEqual([new Uint8Array([3])]);
    expect(historyEndSentence(result.end, 0, result.statusCode)).toContain("BadUserAccessDenied");
  });

  /** An Uncertain status is a reading, not a refusal: the values come with it. */
  test("an Uncertain page is read, not refused", async () => {
    // One value, and then the window really is spent: a fake that answered the same value to every
    // window would be a server ignoring `startTime`, which is not what this test is about. (It is
    // bounded even so - the value cap ends such a read - and a test below asserts that.)
    let served = 0;
    const uncertain: OpcuaReadOnlyClient = {
      ...refusing("Good", 0),
      historyRead: async (options) => {
        served += 1;
        return {
          nodeId: options.nodeId,
          values: served === 1 ? [value(1)] : [],
          statusCode: "UncertainSubNormal",
        };
      },
    };
    const result = await readHistory(uncertain, {
      nodeId: "ns=3;s=H",
      limit: 100,
    });
    expect(result.values).toHaveLength(1);
    expect(result.end).toBe("exhausted");
  });

  /**
   * A SHORT PAGE IS NOT THE END OF THE HISTORY, and this is the server that proves why it cannot be
   * treated as one: it caps every page at 50 however many are asked for, and hands back no token.
   * Read as "a page shorter than the ask means exhausted", its first 50 values were the whole of a
   * node holding a thousand, and nothing on screen said so.
   */
  test("a server that caps its pages below the ask and pages by nothing is still read to the end", async () => {
    const record = fresh();
    const capped = timestampPaging(1_000, record, 50);
    // A limit above what the node holds, so the end is the WINDOW's and not the cap's.
    const result = await readHistory(capped, {
      nodeId: "ns=3;s=H",
      limit: 2_000,
    });
    expect(result.values).toHaveLength(1_000);
    expect(result.end).toBe("exhausted");
    expect(result.paging).toBe("timestamp");
    expect(new Set(result.values.map((row) => row.sourceTimestamp)).size).toBe(1_000);
  });

  /**
   * A PAGE WITH A CONTINUATION POINT AND NO VALUES MOVES NOTHING, and following it was an unbounded
   * loop: the value cap ends a read only because each page carries values. Measured before this
   * bound existed, against a server that answers Good with zero values and a point: 50,000
   * HistoryReadNext calls and still going, one HTTP request that never answered, and the session
   * held for all of it.
   */
  test("a server that answers a continuation point with no values is followed a few times, then stopped", async () => {
    let calls = 0;
    let released = 0;
    const emptyButContinued: OpcuaReadOnlyClient = {
      ...refusing("Good", 0),
      historyRead: async (options) => {
        calls += 1;
        if (calls > 200) throw new Error("the read did not stop");
        return {
          nodeId: options.nodeId,
          values: [],
          continuationPoint: new Uint8Array([1]),
          statusCode: "Good",
        };
      },
      historyReadNext: async (options) => {
        calls += 1;
        if (calls > 200) throw new Error("the read did not stop");
        return {
          nodeId: options.nodeId,
          values: [],
          continuationPoint: new Uint8Array([1]),
          statusCode: "Good",
        };
      },
      releaseHistoryContinuationPoint: async () => {
        released += 1;
      },
    };
    const result = await readHistory(emptyButContinued, {
      nodeId: "ns=3;s=H",
      limit: 1_000,
    });
    expect(result.end).toBe("empty-pages");
    expect(result.values).toEqual([]);
    // Eleven requests: the first read, then continuations until the eleventh empty page crosses the
    // limit of ten. And the server's slot is handed back rather than left held.
    expect(calls).toBe(11);
    expect(released).toBe(1);
    // The sentence says what happened, and names no window the reader did not ask for.
    expect(historyEndSentence(result.end, 0)).toMatch(/held no values and a continuation point/);
  });

  /**
   * A SERVER THAT DRIPS MEETS NEITHER OTHER BOUND, which is the gap between them. The value cap ends
   * a read that makes progress and the empty-page limit ends one that makes none; ten empty pages
   * followed by one value, for ever, resets the empty-page counter every eleventh page and so runs
   * until the cap. Measured before the request budget existed: 109,990 requests inside one HTTP
   * request, which is bounded in theory and a storm at a plant in practice.
   */
  test("a server that drips one value every tenth page is stopped by the request budget", async () => {
    let nexts = 0;
    let released = 0;
    const drip: OpcuaReadOnlyClient = {
      ...refusing("Good", 0),
      historyRead: async (options) => ({
        nodeId: options.nodeId,
        values: [value(0)],
        continuationPoint: new Uint8Array([1]),
        statusCode: "Good",
      }),
      historyReadNext: async (options) => {
        nexts += 1;
        if (nexts > 5_000) throw new Error("the read did not stop");
        return {
          nodeId: options.nodeId,
          // One value every tenth page: the empty-page counter never reaches its limit.
          values: nexts % 10 === 0 ? [value(nexts)] : [],
          continuationPoint: new Uint8Array([1]),
          statusCode: "Good",
        };
      },
      releaseHistoryContinuationPoint: async () => {
        released += 1;
      },
    };
    const result = await readHistory(drip, { nodeId: "ns=3;s=H" });
    expect(result.end).toBe("request-budget");
    expect(result.pages).toBe(300);
    // The budget is the whole read's, not one page's, and the slot is handed back on the way out.
    expect(nexts).toBe(300);
    expect(released).toBe(1);
    expect(result.values.length).toBeGreaterThan(0);
    expect(historyEndSentence(result.end, result.values.length)).toMatch(/stopped after 300 requests/);
  });

  /** A page that carries values resets the count, so a sparse archive is still read to its end. */
  test("one empty continued page between full ones does not stop the read", async () => {
    let reads = 0;
    let nexts = 0;
    const sparse: OpcuaReadOnlyClient = {
      ...refusing("Good", 0),
      historyRead: async (options) => {
        reads += 1;
        // The first read opens the chain; a later one means the window really is spent.
        if (reads > 1) return { nodeId: options.nodeId, values: [], statusCode: "Good" };
        return {
          nodeId: options.nodeId,
          values: [value(0)],
          continuationPoint: new Uint8Array([1]),
          statusCode: "Good",
        };
      },
      historyReadNext: async (options) => {
        nexts += 1;
        if (nexts > 200) throw new Error("the read did not stop");
        // Every other continuation is empty, and no run of empties ever reaches ten.
        const empty = nexts % 2 === 0;
        const last = nexts >= 21;
        return {
          nodeId: options.nodeId,
          values: empty ? [] : [value(nexts)],
          ...(last ? {} : { continuationPoint: new Uint8Array([1]) }),
          statusCode: "Good",
        };
      },
    };
    const result = await readHistory(sparse, {
      nodeId: "ns=3;s=H",
      limit: 1_000,
    });
    expect(result.end).toBe("exhausted");
    expect(result.values.length).toBeGreaterThan(10);
    expect(nexts).toBe(21);
  });

  /**
   * And the bound that makes the loop above safe against a server that ignores the window
   * altogether: every page carries at least one value, so the value cap ends the read.
   */
  test("a server that ignores the window is stopped by the cap rather than read for ever", async () => {
    let pages = 0;
    const stuck: OpcuaReadOnlyClient = {
      ...refusing("Good", 0),
      historyRead: async (options) => {
        pages += 1;
        if (pages > 5_000) throw new Error("the read did not stop");
        return {
          nodeId: options.nodeId,
          values: [value(1)],
          statusCode: "Good",
        };
      },
    };
    const result = await readHistory(stuck, { nodeId: "ns=3;s=H", limit: 100 });
    expect(result.end).toBe("value-cap");
    expect(result.values).toHaveLength(100);
  });
});
