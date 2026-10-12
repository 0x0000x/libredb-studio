/**
 * The guard over `dns.lookupService`, measured against the throw it exists for (#1677).
 *
 * THIS IS NOT A STYLE FIX. `node-opcua-hostname` resolves this host's name at import time, does not
 * await it, and calls `dns.lookupService` with whatever `dns.lookup` answered. An address carrying
 * a zone index makes `lookupService` THROW rather than call back, inside a callback where the
 * library's own try/catch cannot see it - and Node's default for an uncaught exception is to exit.
 * Measured: Node 24.14.0 on the development host resolved the name to `::1` and nothing happened;
 * Bun 1.4.2 resolved it to a zone-indexed link-local address and the error arrived uncaught.
 */
import { describe, expect, test } from "bun:test";
import dns from "node:dns";
import { lookupServiceIsGuarded } from "@/lib/db/providers/industrial/opcua/hostname-guard";

/** The address that throws: a link-local IPv6 with the zone index the kernel reports with it. */
const ZONED = "fe80::18aa:2794:76e0:ef24%15";

describe("the dns.lookupService guard", () => {
  test("is installed by importing the module, which is what `library.ts` relies on", () => {
    expect(lookupServiceIsGuarded()).toBe(true);
  });

  /**
   * The control AND the fix in one measurement: the raw call throws, the guarded one does not, and
   * the error arrives where the library already handles it.
   */
  test("a zone-indexed address answers as an error instead of throwing", async () => {
    const answer = await new Promise<{
      threw: unknown;
      error: unknown;
      called: boolean;
    }>((resolve) => {
      let called = false;
      try {
        dns.lookupService(ZONED, 0, (error) => {
          called = true;
          resolve({ threw: undefined, error, called });
        });
      } catch (threw) {
        resolve({ threw, error: undefined, called });
      }
    });

    expect(answer.threw).toBeUndefined();
    expect(answer.called).toBe(true);
    expect((answer.error as { code?: string } | undefined)?.code).toBe("ERR_INVALID_ARG_VALUE");
  });

  /** A valid address is untouched: the callback is reached, whatever the host's reverse DNS says. */
  test("a valid address still answers through the callback", async () => {
    const reached = await new Promise<boolean>((resolve) => {
      try {
        dns.lookupService("127.0.0.1", 0, () => resolve(true));
      } catch {
        resolve(false);
      }
    });
    expect(reached).toBe(true);
  });

  /**
   * Wrapping the wrapper would make every lookup one frame deeper per import, and an installer that
   * cannot tell its own work from the platform's would do exactly that.
   */
  test("a second import does not wrap the wrapper", async () => {
    const before = dns.lookupService;
    await import("@/lib/db/providers/industrial/opcua/hostname-guard");
    expect(dns.lookupService).toBe(before);
    expect(lookupServiceIsGuarded()).toBe(true);
  });
});
