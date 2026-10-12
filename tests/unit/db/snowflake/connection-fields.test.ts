/**
 * The four connection fields the Snowflake provider adds (authMethod, privateKey, privateKeyPassphrase, role), end to
 * end through every layer a field passes before a provider reads it: the seed schema, the seed filter, the credential
 * resolver, the storage classification, the cache key and the agent's connection identity.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { generateKeyPairSync } from "node:crypto";
import { connectionIdentity } from "@/lib/agent/context-snapshot";
import { providerCacheKey } from "@/lib/db/provider-cache-key";
import { filterByRoles } from "@/lib/seed/connection-filter";
import { resolveConnectionCredentials } from "@/lib/seed/credential-resolver";
import { SeedConnectionSchema, type SeedConnection } from "@/lib/seed/types";
import { CONNECTION_FIELDS } from "@/lib/storage/connection-secrets";
import type { DatabaseConnection } from "@/lib/types";

// The type is any shipped one: no layer below the dialog gates these fields on the type.
const base: DatabaseConnection = {
  id: "acct",
  name: "Account",
  type: "postgres",
  host: "myorg-myaccount",
  createdAt: new Date(0),
};
const seed: SeedConnection = { id: "s", name: "S", type: "postgres", host: "myorg-myaccount", roles: ["*"] };
const PEM = generateKeyPairSync("rsa", { modulusLength: 2048 }).privateKey.export({
  type: "pkcs8",
  format: "pem",
}) as string;
const FIELDS = {
  authMethod: "keypair",
  privateKey: PEM,
  privateKeyPassphrase: "stand-in-pass",
  role: "STUDIO_RO",
} as const;

afterEach(() => {
  delete process.env.SF_TEST_KEY;
  delete process.env.SF_TEST_PASS;
  delete process.env.SF_TEST_ROLE;
  delete process.env.SF_TEST_METHOD;
});

describe("the seed layer keeps all four", () => {
  test("the schema declares them, so zod does not strip them", () => {
    const parsed = SeedConnectionSchema.safeParse({ ...seed, ...FIELDS });
    expect(parsed.success).toBe(true);
    expect([
      parsed.data?.authMethod,
      parsed.data?.privateKey,
      parsed.data?.privateKeyPassphrase,
      parsed.data?.role,
    ]).toEqual([FIELDS.authMethod, FIELDS.privateKey, FIELDS.privateKeyPassphrase, FIELDS.role]);
  });

  test("a value that is not a string is refused, naming the field", () => {
    const parsed = SeedConnectionSchema.safeParse({ ...seed, authMethod: 7 });
    expect(parsed.success).toBe(false);
    expect(parsed.error?.issues.map((issue) => issue.path.join("."))).toEqual(["authMethod"]);
  });

  test("the role filter copies them onto the managed connection", () => {
    const [managed] = filterByRoles([{ ...seed, ...FIELDS }], ["user"]);
    expect([managed.authMethod, managed.privateKey, managed.privateKeyPassphrase, managed.role]).toEqual([
      FIELDS.authMethod,
      FIELDS.privateKey,
      FIELDS.privateKeyPassphrase,
      FIELDS.role,
    ]);
  });

  test("each resolves ${VAR}, and a multi-line key keeps its line breaks", () => {
    process.env.SF_TEST_KEY = PEM;
    process.env.SF_TEST_PASS = "from-env";
    process.env.SF_TEST_ROLE = '"Mixed Role"';
    process.env.SF_TEST_METHOD = "pat";
    const resolved = resolveConnectionCredentials({
      ...seed,
      privateKey: "${SF_TEST_KEY}",
      privateKeyPassphrase: "${SF_TEST_PASS}",
      role: "${SF_TEST_ROLE}",
      authMethod: "${SF_TEST_METHOD}" as never,
    });
    expect(resolved.privateKey).toBe(PEM);
    expect(resolved.privateKey?.split("\n").length).toBeGreaterThan(3);
    expect([resolved.privateKeyPassphrase, resolved.role, resolved.authMethod]).toEqual([
      "from-env",
      '"Mixed Role"',
      "pat",
    ]);
  });
});

describe("classification", () => {
  test("the key and its passphrase are secrets; the method and the role are public", () => {
    expect([CONNECTION_FIELDS.privateKey, CONNECTION_FIELDS.privateKeyPassphrase]).toEqual(["secret", "secret"]);
    expect([CONNECTION_FIELDS.authMethod, CONNECTION_FIELDS.role]).toEqual(["public", "public"]);
  });
});

/**
 * The identity of one connection per shipped type, with neither `role` nor `authMethod`, measured before either joined
 * `connectionIdentity`: a connection that sets neither must keep the identity it had, so no held inventory and no
 * agent conversation of an existing connection is declined as re-pointed. SHA-256 hex, which holds no secret.
 */
const IDENTITY_BEFORE: Record<string, string> = {
  postgres: "6c2440b2ad91d660d375a4e2bfd3957013e14655be653092ed0462536f7acaca",
  bigquery: "aea33baf3746b4d5c8f985256c1e435b6a3e9ca2da0a2cf005305979ece9e9aa",
  mysql: "226760daa09836b2c4f58a75c25f10e7fb3a2368c55efc52893a51707cef831b",
  sqlite: "41f31d3452e818e88ef2f417ff6bd9b2e8c31ae4ef1b74faab5f938b4d40e29f",
  libsql: "3f6f534a9eb200ced73005d603d0e21d0828d351ee6fde2c9a1dc01a533e82ab",
  duckdb: "b9a18c9efdc11db925bd3b0067e9f999232f3ec3dd3bfe5199f192372686f9cb",
  oracle: "ca7a1566e112d6b861ec3b0f3a945949eed5715aca8319d35263d0e3504fb028",
  db2: "cc9ce32b1b8f3079e2b62695614c8c3c1e35ff8c014d41f4d712836f58557731",
  mssql: "f7e11e8f96914a5e3cc7bcb456f64653ce0498c9c5c230128b2cca2772bed832",
  clickhouse: "c9f8b02301a350c055b10c0b50392828b83b6515d8be2017361b2b23f5b0f039",
  druid: "44e26acdaae974be26e6568b4474026729170be7f0a8b77cc0614338810f0e8f",
  trino: "0d218a5cd13bae890b65d9f60f666242e13f819182cff1b85a39822855482502",
  cassandra: "c69b0e10439b61ce65eb56f8186b412eb6facbaa4af8bc95cbc38ea5cdeee229",
  elasticsearch: "d6c0211b5149464efcae89eb10905f6d400ac1f5df69ee739fa0b5ff26f51354",
  opensearch: "5ea185e8b33fc2656329b00dda937d8eaa6c566c79e0c6da329cc0134fb93ec5",
  mongodb: "2a6cbd7c2d368520a644a652721a44db737eb31eb919f69e5a13773c2c54d81a",
  couchbase: "5856d500f58e9b0974f546779525155f4e255bf591b346e102546999d36a2310",
  redis: "6a074ce934877d301ebcc433a66b0240968c568377ede26a2c4c3dfd7626075c",
  prometheus: "f418e85667eedf6660ebc6083539aaf0f62c1bf68f8873f24d780aa37dcd6c58",
  kafka: "68daf15c64e14b0d0d7fd13f47f1b80010ef8e94347b86494046f12513c22ea8",
  etcd: "4b844de9d732b19b13f5a5e9a4e7a6922204995190b2824106a0a3730264a641",
  neo4j: "b4ff0f5723f47508e0514c8febfaafc8f86c78ae8c6528427eba140be2d84b6e",
  milvus: "db657555837ad1430d53574d2d4320b6864b324a1d46d117d5a9fbb819ad0e62",
  qdrant: "c1b5823713a119e337276e78e88a31472e3adfd7272727f388f226a16986dd53",
  influxdb: "7f5000854b170e3fff5a16b9a60636c387d697c210897a806eb898d992ef1cb9",
  influxdb3: "d3542d79824873229082828e4b1cf670fc7ebdd0b8f13fa862b9d7be345227f1",
  oxia: "d1d952af4b334ebb01fd73b77acedb701c3f71e64be6e94f5f6711727be1c663",
  databend: "a8976346d32f0c2b58b7f4df5027988d12d8a92738476ce51831cc7390ef8569",
  s3: "768b0be43814283202445350279179b4a256a4b01a40030ac9d18b30eaca10b4",
  kubernetes: "96c60e30398a83146e2b1ba7f276e814bfef265d570abdda9f96d3b79522826d",
  mqtt: "0bb7ec312b49f6e421af995fbacfe814b0a942a7bd68b5ffba0ec6ce96593719",
  libredb: "27c598300a682824969f08a5dff2bc27015188811d321fe4fe5bf462dd179f5e",
};

const pinned = (type: string): DatabaseConnection =>
  ({
    id: "pin",
    name: "Pin",
    type,
    host: "db.example",
    port: 5432,
    database: "app",
    user: "reader",
    warehouse: "wh",
    createdAt: new Date(0),
  }) as never;

describe("the cache key and the agent identity", () => {
  const moved: [string, Partial<DatabaseConnection>, Partial<DatabaseConnection>][] = [
    ["role", { role: "A" }, { role: "B" }],
    ["authMethod", { authMethod: "keypair" }, { authMethod: "pat" }],
  ];
  test.each(moved)("%s moves both the identity and the cache key", async (_field, one, two) => {
    expect(connectionIdentity({ ...base, ...one })).not.toBe(connectionIdentity({ ...base, ...two }));
    expect(await providerCacheKey({ ...base, ...one })).not.toBe(await providerCacheKey({ ...base, ...two }));
  });

  const keyOnly: [string, Partial<DatabaseConnection>, Partial<DatabaseConnection>][] = [
    ["privateKey", { privateKey: "KEY-A" }, { privateKey: "KEY-B" }],
    ["privateKeyPassphrase", { privateKeyPassphrase: "P-A" }, { privateKeyPassphrase: "P-B" }],
    ["the token in password", { password: "PAT-A" }, { password: "PAT-B" }],
    ["warehouse", { warehouse: "WH_A" }, { warehouse: "WH_B" }],
  ];
  test.each(keyOnly)("%s moves the cache key and not the identity", async (_field, one, two) => {
    expect(connectionIdentity({ ...base, ...one })).toBe(connectionIdentity({ ...base, ...two }));
    expect(await providerCacheKey({ ...base, ...one })).not.toBe(await providerCacheKey({ ...base, ...two }));
  });

  test("an empty role or method reads as unset", () => {
    expect(connectionIdentity({ ...base, role: "" })).toBe(connectionIdentity(base));
    expect(connectionIdentity({ ...base, authMethod: "" as never })).toBe(connectionIdentity(base));
  });

  test("a connection with neither role nor authMethod keeps the identity it had before these fields", () => {
    expect(Object.keys(IDENTITY_BEFORE)).toHaveLength(32);
    for (const [type, digest] of Object.entries(IDENTITY_BEFORE)) {
      expect(connectionIdentity(pinned(type))).toBe(digest);
    }
  });
});
