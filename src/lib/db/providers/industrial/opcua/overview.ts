/**
 * The Overview card's three readable figures, read from one node (#1677).
 *
 * WHY THIS MODULE EXISTS. `getOverview` answered "unknown" for the version, the uptime and the
 * start time, and a browser pass found all three sitting in the server's own `ServerStatus`: Part 5
 * makes that node mandatory, and measured on OPC PLC 2.15.10 one Read of `i=2256` answers Good with
 * `productName`, `softwareVersion`, `startTime` and the server's own `currentTime`.
 *
 * THE SERVER'S CLOCK, NOT OURS. Uptime is `currentTime - startTime` as the SERVER reports both, so
 * a client whose clock is wrong - which on a plant floor is the normal case, and the reason the
 * protocol carries a source and a server timestamp on every value - cannot turn a running server
 * into one that started in the future.
 *
 * ANYTHING MISSING STAYS MISSING. A field the structure does not carry, or carries in a shape this
 * does not recognise, is left out rather than guessed: the card then draws what it drew before.
 */

/** What a `ServerStatus` read contributes to the card, with every absent field left out. */
export interface OpcuaOverviewCard {
  readonly version?: string;
  readonly uptime?: string;
  readonly startTime?: Date;
}

/** A field of the structure, whatever the library decoded it into, or undefined. */
function text(source: Record<string, unknown>, key: string): string | undefined {
  const value = source[key];
  return typeof value === "string" && value.trim() !== "" ? value.trim() : undefined;
}

/** An instant, from the Date the library decodes or the ISO string a capture replays. */
function instant(value: unknown): Date | undefined {
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? undefined : value;
  if (typeof value !== "string") return undefined;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? undefined : parsed;
}

/**
 * The duration, in the spelling the rest of the product uses for an uptime.
 *
 * A negative span is dropped rather than drawn: the two instants come from the same read, so one
 * ahead of the other is a server contradicting itself, and "-3h" on a card reads as a defect in
 * Studio.
 */
function spanSince(start: Date, now: Date, format: (ms: number) => string): string | undefined {
  const ms = now.getTime() - start.getTime();
  return ms < 0 ? undefined : format(ms);
}

/**
 * The card's fields from one `ServerStatus` value, or `{}` where the server said nothing usable.
 *
 * `format` is the caller's own duration spelling (`BaseDatabaseProvider.formatDuration`), passed in
 * rather than imported so this module stays a pure reading of the structure.
 */
export function serverStatusCard(value: unknown, format: (ms: number) => string): OpcuaOverviewCard {
  if (typeof value !== "object" || value === null) return {};
  const status = value as Record<string, unknown>;
  const build =
    typeof status.buildInfo === "object" && status.buildInfo !== null
      ? (status.buildInfo as Record<string, unknown>)
      : {};

  // The product's name leads its version, because "2.15.10" alone names nothing on a plant floor
  // where four stacks answer the same services: measured, this reads
  // "IoT Edge OPC UA PLC 2.15.10 (OPC UA SDK 1.5.378.176)".
  const product = text(build, "productName");
  const software = text(build, "softwareVersion");
  const version = product !== undefined && software !== undefined ? `${product} ${software}` : (software ?? product);

  const startTime = instant(status.startTime);
  const currentTime = instant(status.currentTime);
  const uptime =
    startTime !== undefined && currentTime !== undefined ? spanSince(startTime, currentTime, format) : undefined;

  return {
    ...(version === undefined ? {} : { version }),
    ...(uptime === undefined ? {} : { uptime }),
    ...(startTime === undefined ? {} : { startTime }),
  };
}
