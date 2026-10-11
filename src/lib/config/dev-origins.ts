/**
 * Parse DEV_ALLOWED_ORIGINS for the dev origin list in next.config.ts: comma separated hostnames.
 * Next compares each entry with the request's hostname exactly, so a stray space, a scheme or a
 * port would silently never match. Import-free, because next.config.ts cannot resolve `@/`.
 */
export function readDevAllowedOrigins(value = ""): string[] {
  const origins = value
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean);
  for (const origin of origins) {
    if (origin.includes("/") || /:\d+$/.test(origin)) {
      throw new Error(
        `DEV_ALLOWED_ORIGINS takes hostnames only, such as devbox.local or *.devbox.local, without a scheme, port or path: "${origin}".`,
      );
    }
  }
  return origins;
}
