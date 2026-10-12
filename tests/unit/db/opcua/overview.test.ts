import { describe, expect, test } from "bun:test";
import { serverStatusCard } from "@/lib/db/providers/industrial/opcua/overview";

/**
 * THE OVERVIEW CARD READ WHAT THE SERVER ALREADY ANSWERS (#1677).
 *
 * `getOverview` said "unknown" for the version and the uptime while the server handed both over,
 * Good, on one node: found by opening the monitoring dashboard in a browser against the documented
 * fixture. The value below is the structure that fixture really answers for `i=2256`, copied from
 * a live read rather than imagined.
 */
const LIVE = {
  startTime: "2026-10-12T15:28:49.204Z",
  currentTime: "2026-10-12T19:06:57.320Z",
  state: "Running",
  buildInfo: {
    productUri: "https://github.com/Azure-Samples/iot-edge-opc-plc",
    manufacturerName: "Microsoft",
    productName: "IoT Edge OPC UA PLC",
    softwareVersion: "2.15.10 (OPC UA SDK 1.5.378.176)",
    buildNumber: "5674286a0e",
    buildDate: "2026-10-08T10:22:44.000Z",
  },
};

/** The caller's own spelling, passed in; these tests only need it to be recognisable. */
const hours = (ms: number): string => `${(ms / 3_600_000).toFixed(2)}h`;

describe("the Overview card, from ServerStatus", () => {
  test("names the product with its version, and the uptime by the server's own clock", () => {
    const card = serverStatusCard(LIVE, hours);

    expect(card.version).toBe("IoT Edge OPC UA PLC 2.15.10 (OPC UA SDK 1.5.378.176)");
    // 15:28:49 to 19:06:57 is 3.64 hours, measured by the SERVER's two instants rather than by this
    // machine's clock: a client whose clock is wrong must not turn a running server into one that
    // started in the future.
    expect(card.uptime).toBe("3.64h");
    expect(card.startTime?.toISOString()).toBe("2026-10-12T15:28:49.204Z");
  });

  test("a Date, which is what the library decodes, reads the same as the ISO a capture replays", () => {
    const decoded = serverStatusCard(
      { ...LIVE, startTime: new Date(LIVE.startTime), currentTime: new Date(LIVE.currentTime) },
      hours,
    );

    expect(decoded.uptime).toBe("3.64h");
    expect(decoded.startTime?.toISOString()).toBe("2026-10-12T15:28:49.204Z");
  });

  test("a version with no product name is still a version, and a name with no version is still a name", () => {
    const software = serverStatusCard({ buildInfo: { softwareVersion: "1.2.3" } }, hours);
    const named = serverStatusCard({ buildInfo: { productName: "open62541" } }, hours);

    expect(software.version).toBe("1.2.3");
    expect(named.version).toBe("open62541");
  });

  /*
   * WHAT IS MISSING STAYS MISSING, so the card falls back to the sentence it drew before rather
   * than to a fabricated figure. Every one of these shapes was reachable: the three other stacks
   * this provider was measured against answer `ServerStatus` differently, and a capture replays it
   * as plain JSON.
   */
  test.each([
    ["nothing at all", undefined],
    ["null", null],
    ["a scalar", 42],
    ["blank strings", { buildInfo: { productName: "  ", softwareVersion: "" } }],
    ["an unreadable instant", { startTime: "not a time", currentTime: LIVE.currentTime, buildInfo: {} }],
  ])("%s leaves the card's own fields absent", (_name, value) => {
    const card = serverStatusCard(value, hours);

    expect(card.version).toBeUndefined();
    expect(card.uptime).toBeUndefined();
  });

  /* And the halves are independent: a server that states its clock but not its build still has an uptime. */
  test("a structure with no build info keeps the uptime and leaves the version absent", () => {
    const card = serverStatusCard({ startTime: LIVE.startTime, currentTime: LIVE.currentTime }, hours);

    expect(card.version).toBeUndefined();
    expect(card.uptime).toBe("3.64h");
  });

  test("a server whose clock contradicts itself reports no uptime rather than a negative one", () => {
    const backwards = serverStatusCard({ ...LIVE, currentTime: "2026-10-12T15:00:00.000Z" }, hours);

    expect(backwards.uptime).toBeUndefined();
    // The version still reads: one unusable field does not take the others with it.
    expect(backwards.version).toBe("IoT Edge OPC UA PLC 2.15.10 (OPC UA SDK 1.5.378.176)");
  });
});
