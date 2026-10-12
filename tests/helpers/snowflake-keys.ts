/**
 * Private keys for the Snowflake key-intake tests, made at run time so no key, PEM text or passphrase is committed.
 *
 * The encrypted PKCS#8 forms are assembled by hand with a small DER writer, so any PBKDF2 iteration count or scrypt
 * cost is reachable without `openssl`: PBES2 with AES-256-CBC over the unencrypted PKCS#8 DER, the key derived with
 * `pbkdf2Sync` or `scryptSync`. For a cost the reader must refuse before decrypting, the `...Outer` builders write the
 * same structure around random bytes and derive nothing, and `outerOnlyPem` does so for any outer algorithm.
 *
 * Measured on 2026-10-12: Node 24 and 26 open the PBKDF2 and scrypt forms this file makes; Bun opens the PBKDF2 form
 * only, since its BoringSSL refuses scrypt PBES2.
 */
import {
  createCipheriv,
  createPrivateKey,
  generateKeyPairSync,
  type KeyObject,
  pbkdf2Sync,
  randomBytes,
  scryptSync,
} from "node:crypto";

export const KEY_TEST_PASSPHRASE = "stand-in-passphrase";

// BigInt(...) calls rather than literals: the build targets below ES2020.
const BIG_0 = BigInt(0);
const BIG_1 = BigInt(1);
const BIG_7 = BigInt(7);
const BIG_40 = BigInt(40);
const BIG_127 = BigInt(127);
const BIG_65537 = BigInt(65537);

const PBES2 = "1.2.840.113549.1.5.13";
const PBKDF2 = "1.2.840.113549.1.5.12";
const SCRYPT = "1.3.6.1.4.1.11591.4.11";
const AES_256_CBC = "2.16.840.1.101.3.4.1.42";
const PRF_OIDS = {
  sha1: "1.2.840.113549.2.7",
  sha256: "1.2.840.113549.2.9",
  sha384: "1.2.840.113549.2.10",
  sha512: "1.2.840.113549.2.11",
} as const;

export type Prf = keyof typeof PRF_OIDS;

// The DER writer: tag, definite length, body.

function tlv(tag: number, body: Buffer): Buffer {
  const length = body.length;
  let head: Buffer;
  if (length < 0x80) head = Buffer.from([tag, length]);
  else {
    const bytes: number[] = [];
    for (let rest = length; rest > 0; rest = Math.floor(rest / 256)) bytes.unshift(rest % 256);
    head = Buffer.from([tag, 0x80 | bytes.length, ...bytes]);
  }
  return Buffer.concat([head, body]);
}

export function derSequence(...parts: Buffer[]): Buffer {
  return tlv(0x30, Buffer.concat(parts));
}

export function derOctets(bytes: Buffer): Buffer {
  return tlv(0x04, bytes);
}

/** A non-negative INTEGER, with a leading zero byte when the top bit is set. */
export function derInteger(value: bigint | number): Buffer {
  let hex = BigInt(value).toString(16);
  if (hex.length % 2 === 1) hex = `0${hex}`;
  let bytes = Buffer.from(hex, "hex");
  if (bytes[0] >= 0x80) bytes = Buffer.concat([Buffer.from([0]), bytes]);
  return tlv(0x02, bytes);
}

export function derOid(dotted: string): Buffer {
  const [first, second, ...rest] = dotted.split(".").map((part) => BigInt(part));
  const bytes: number[] = [];
  for (const arc of [first * BIG_40 + second, ...rest]) {
    const groups: number[] = [Number(arc & BIG_127)];
    for (let value = arc >> BIG_7; value > BIG_0; value >>= BIG_7) groups.unshift(Number(value & BIG_127) | 0x80);
    bytes.push(...groups);
  }
  return tlv(0x06, Buffer.from(bytes));
}

export function derNull(): Buffer {
  return Buffer.from([0x05, 0x00]);
}

function pem(label: string, der: Buffer): string {
  const lines = der.toString("base64").match(/.{1,64}/g) ?? [];
  return `-----BEGIN ${label}-----\n${lines.join("\n")}\n-----END ${label}-----\n`;
}

function encryptedPem(algorithm: Buffer, payload: Buffer): string {
  return pem("ENCRYPTED PRIVATE KEY", derSequence(algorithm, derOctets(payload)));
}

function pbes2(kdf: Buffer, iv: Buffer): Buffer {
  return derSequence(derOid(PBES2), derSequence(kdf, derSequence(derOid(AES_256_CBC), derOctets(iv))));
}

function pbkdf2Kdf(salt: Buffer, iterations: number, prf: Prf | null): Buffer {
  const prfPart = prf === null ? [] : [derSequence(derOid(PRF_OIDS[prf]), derNull())];
  return derSequence(derOid(PBKDF2), derSequence(derOctets(salt), derInteger(iterations), ...prfPart));
}

function scryptKdf(salt: Buffer, n: number, r: number, p: number): Buffer {
  return derSequence(
    derOid(SCRYPT),
    derSequence(derOctets(salt), derInteger(n), derInteger(r), derInteger(p), derInteger(32)),
  );
}

function aesOver(key: KeyObject, derived: Buffer, iv: Buffer): Buffer {
  const plain = key.export({ type: "pkcs8", format: "der" });
  const cipher = createCipheriv("aes-256-cbc", derived, iv);
  return Buffer.concat([cipher.update(plain), cipher.final()]);
}

/** An RSA private key, 2048 bits unless `bits` says otherwise. */
export function rsaKey(bits?: number): KeyObject {
  return generateKeyPairSync("rsa", { modulusLength: bits ?? 2048 }).privateKey;
}

/** Uniform random bytes as a non-negative bigint. */
function randomBigInt(bytes: number, top: number): bigint {
  const buffer = randomBytes(bytes);
  buffer[0] |= top;
  buffer[buffer.length - 1] |= 1;
  return BigInt(`0x${buffer.toString("hex")}`);
}

function inverse(value: bigint, modulus: bigint): bigint {
  let [r0, r1] = [value % modulus, modulus];
  let [s0, s1] = [BIG_1, BIG_0];
  while (r1 !== BIG_0) {
    const quotient = r0 / r1;
    [r0, r1] = [r1, r0 - quotient * r1];
    [s0, s1] = [s1, s0 - quotient * s1];
  }
  if (r0 !== BIG_1) throw new Error("no inverse");
  return ((s0 % modulus) + modulus) % modulus;
}

function gcd(a: bigint, b: bigint): bigint {
  let [x, y] = [a, b];
  while (y !== BIG_0) [x, y] = [y, x % y];
  return x;
}

/**
 * An RSA private key of exactly `bits` bits (a multiple of 16) in milliseconds, for a modulus bound: generating a real
 * 16384-bit key takes about a minute. Its factors are random odd numbers, not primes, chosen so the key is consistent
 * as a parser checks it (n = pq, ed = 1 mod (p-1)(q-1), the CRT values), so every runtime opens it and reports its
 * modulus length; it is never used to sign.
 */
export function wideRsaKey(bits: number): KeyObject {
  const e = BIG_65537;
  let p: bigint;
  let q: bigint;
  do {
    p = randomBigInt(bits / 16, 0xc0);
    q = randomBigInt(bits / 16, 0xc0);
  } while (gcd(e, p - BIG_1) !== BIG_1 || gcd(e, q - BIG_1) !== BIG_1 || gcd(p, q) !== BIG_1);
  const d = inverse(e, (p - BIG_1) * (q - BIG_1));
  const der = derSequence(
    ...[BIG_0, p * q, e, d, p, q, d % (p - BIG_1), d % (q - BIG_1), inverse(q, p)].map((value) => derInteger(value)),
  );
  return createPrivateKey({ key: der, format: "der", type: "pkcs1" });
}

/** The unencrypted PKCS#8 PEM. */
export function pkcs8Pem(key: KeyObject): string {
  return key.export({ type: "pkcs8", format: "pem" }) as string;
}

/** The traditional PKCS#1 PEM; with a passphrase, the legacy form with Proc-Type and DEK-Info (AES-256-CBC). */
export function pkcs1Pem(key: KeyObject, passphrase?: string): string {
  return passphrase === undefined
    ? (key.export({ type: "pkcs1", format: "pem" }) as string)
    : (key.export({ type: "pkcs1", format: "pem", cipher: "aes-256-cbc", passphrase }) as string);
}

/** PBES2 with PBKDF2 and AES-256-CBC; `prf` null omits the PRF (the DER default, hmacWithSHA1), absent is SHA-256. */
export function pbes2Pbkdf2Pem(
  key: KeyObject,
  passphrase: string,
  iterations: number,
  prf: Prf | null = "sha256",
): string {
  const salt = randomBytes(16);
  const iv = randomBytes(16);
  const derived = pbkdf2Sync(passphrase, salt, iterations, 32, prf ?? "sha1");
  return encryptedPem(pbes2(pbkdf2Kdf(salt, iterations, prf), iv), aesOver(key, derived, iv));
}

/** PBES2 with scrypt and AES-256-CBC. */
export function pbes2ScryptPem(key: KeyObject, passphrase: string, n: number, r: number, p: number): string {
  const salt = randomBytes(16);
  const iv = randomBytes(16);
  const derived = scryptSync(passphrase, salt, 32, { N: n, r, p, maxmem: 128 * n * r * 2 });
  return encryptedPem(pbes2(scryptKdf(salt, n, r, p), iv), aesOver(key, derived, iv));
}

/** The PBKDF2 structure around random bytes, with no key derived: for a cost refused before decryption. */
export function pbes2Pbkdf2Outer(iterations: number, prf: Prf | null = "sha256"): string {
  return encryptedPem(pbes2(pbkdf2Kdf(randomBytes(16), iterations, prf), randomBytes(16)), randomBytes(1232));
}

/** The scrypt structure around random bytes, with no key derived: for a cost refused before decryption. */
export function pbes2ScryptOuter(n: number, r: number, p: number): string {
  return encryptedPem(pbes2(scryptKdf(randomBytes(16), n, r, p), randomBytes(16)), randomBytes(1232));
}

/**
 * An EncryptedPrivateKeyInfo under any outer algorithm, its payload random bytes, for the refusals read before
 * decryption (PBES1 1.2.840.113549.1.5.3, PKCS#12 1.2.840.113549.1.12.1.3). The parameters default to a salt and
 * an iteration count of 2,000,000, the PBES1 and PKCS#12 shape.
 */
export function outerOnlyPem(algorithmOid: string, params?: Buffer): string {
  const parameters = params ?? derSequence(derOctets(randomBytes(8)), derInteger(2_000_000));
  return encryptedPem(derSequence(derOid(algorithmOid), parameters), randomBytes(1232));
}

/** Any DER under any label, wrapped at 64 characters. */
export function pemOf(label: string, der: Buffer): string {
  return pem(label, der);
}

/** One line: the armour and the body lines joined by spaces, headers included. */
export function spaceForm(text: string): string {
  return text
    .split(/\r?\n/)
    .filter((line) => line.trim() !== "")
    .join(" ");
}

/** The armour kept on its own lines, and every whitespace inside the body removed (headers stay on their lines). */
export function strippedForm(text: string): string {
  const lines = text.split(/\r?\n/).filter((line) => line.trim() !== "");
  const begin = lines[0];
  const end = lines[lines.length - 1];
  const inner = lines.slice(1, -1);
  const headers = inner.filter((line) => line.includes(":"));
  const body = inner.filter((line) => !line.includes(":")).join("");
  return [begin, ...headers, ...(headers.length > 0 ? [""] : []), body, end].join("\n");
}

/** Windows line endings and a trailing blank line. */
export function crlfForm(text: string): string {
  return `${text.replace(/\r?\n/g, "\r\n")}\r\n`;
}
