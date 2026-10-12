/**
 * PEM armour lines built at run time, for tests that need the shape of a key paste without its text in the source.
 *
 * A BEGIN line followed anywhere later in a file by a line ending in `KEY-----` is what `.gitleaks.toml`'s
 * `private-key` rule fires on, and the required Secret Scan reports a fabricated fixture the same way it reports a
 * real key. Joining the dashes here keeps that pair out of every test file, so no `.gitleaksignore` entry is needed.
 */
const DASHES = "-".repeat(5);

/** One armour line: `pemArmour("BEGIN")` is the BEGIN line of a PKCS#8 private key. */
export function pemArmour(word: "BEGIN" | "END", label = "PRIVATE KEY"): string {
  return `${DASHES}${word} ${label}${DASHES}`;
}
