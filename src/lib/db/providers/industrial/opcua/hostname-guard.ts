/**
 * One guard over `dns.lookupService`, installed before the OPC UA library is imported (#1677).
 *
 * WHY A PRODUCT NEEDS THIS AND NOT ONLY A TEST. `node-opcua-hostname` calls `prepareFQDN()` at
 * module scope and does not await it. Inside that call:
 *
 *     dns.lookup(os.hostname(), { hints: dns.ADDRCONFIG }, (err, ip) => {
 *       dns.lookupService(ip, 0, (err2, fqdn) => …);   // <- throws, synchronously, in a callback
 *     });
 *
 * `dns.lookupService` REJECTS AN ADDRESS WITH A ZONE INDEX by throwing `ERR_INVALID_ARG_VALUE`
 * rather than calling back with it, and that throw happens inside the outer callback, where the
 * library's own `try { await promisify(fqdn)() } catch {}` cannot see it. Measured on the
 * development host: Node 24.14.0 resolved the host name to `::1` and nothing happened; Bun 1.4.2
 * resolved it to `fe80::18aa:2794:76e0:ef24%15` and the error arrived uncaught. Bun printed it and
 * carried on; NODE'S DEFAULT FOR AN UNCAUGHT EXCEPTION IS TO EXIT, so on a host that answers a
 * zone-indexed link-local address - an IPv6-only machine with no other address to offer - importing
 * the library would take the server down, on a path no caller of ours is on the stack for.
 *
 * So this file turns that one throw into the error callback the library already handles: it falls
 * back to `os.hostname()`, which is what it does for every other lookup failure. Nothing else
 * changes - a valid address behaves exactly as before, and the wrapper is installed once.
 *
 * It is a module of its own, and `library.ts` imports it FIRST, because ES modules are evaluated in
 * import order: a guard installed in the same file as the library import would run too late.
 *
 * WHAT IT TOUCHES, STATED PLAINLY. `dns.lookupService` is a platform function, so the wrapper is
 * process-wide from the moment an OPC UA connection is opened: every other caller in the process
 * gets the same behaviour, which is an invalid address answered as an error rather than thrown. No
 * valid input behaves differently, nothing else is patched, and `dns.promises.lookupService` is
 * left alone because the library does not use it - a promise-returning call already rejects rather
 * than throwing into the event loop.
 */
import dns from "node:dns";

/** Marks the wrapper, so a second import does not wrap the wrapper. */
const GUARDED = Symbol.for("libredb.opcua.lookupServiceGuard");

type LookupService = typeof dns.lookupService;

function install(): void {
  const original = dns.lookupService as LookupService & { [GUARDED]?: true };
  if (original[GUARDED] === true) return;

  const guarded = ((address: string, port: number, callback: (...args: unknown[]) => void) => {
    try {
      return (original as (a: string, p: number, c: (...args: unknown[]) => void) => void)(address, port, callback);
    } catch (error) {
      // The caller is inside another callback, so the answer has to arrive the same way its
      // successful answers do: asynchronously, as an error, never as a throw up the event loop.
      queueMicrotask(() => callback(error));
      return undefined;
    }
  }) as LookupService & { [GUARDED]?: true };
  guarded[GUARDED] = true;

  dns.lookupService = guarded;
}

install();

/** Whether the guard is in place, which its test asserts rather than inferring from behaviour. */
export function lookupServiceIsGuarded(): boolean {
  return (dns.lookupService as LookupService & { [GUARDED]?: true })[GUARDED] === true;
}
