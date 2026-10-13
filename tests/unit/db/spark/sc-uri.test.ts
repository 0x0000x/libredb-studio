import { describe, expect, test } from "bun:test";
import { parseSparkConnectUri, SPARK_PASTE_SENTENCES } from "@/lib/db/providers/sql/spark/sc-uri";

/** The sc:// reader of spec 3.2: Spark's documented grammar sc://host[:port][/][;key=value]*, pure and type-only. */
describe("parseSparkConnectUri", () => {
  test("reads the host and the port, 15002 when absent, a bare / after the host accepted", () => {
    expect(parseSparkConnectUri("sc://Spark.Example.Internal:15003")).toEqual({
      type: "spark",
      host: "spark.example.internal",
      port: "15003",
      sslMode: "disable",
    });
    expect(parseSparkConnectUri("  sc://localhost/  ")).toEqual({
      type: "spark",
      host: "localhost",
      port: "15002",
      sslMode: "disable",
    });
    expect(parseSparkConnectUri("sc://10.0.0.7;")).toMatchObject({ host: "10.0.0.7", port: "15002" });
  });

  test("keeps a bracketed IPv6 literal with its brackets", () => {
    expect(parseSparkConnectUri("sc://[::1]:15002")).toMatchObject({ host: "[::1]", port: "15002" });
    expect(parseSparkConnectUri("sc://[FD00::7]")).toMatchObject({ host: "[fd00::7]", port: "15002" });
  });

  test("token fills Token; use_ssl=true is verify-system and use_ssl=false is disable", () => {
    expect(parseSparkConnectUri("sc://h:1/;use_ssl=true;token=abc")).toEqual({
      type: "spark",
      host: "h",
      port: "1",
      password: "abc",
      sslMode: "verify-system",
    });
    expect(parseSparkConnectUri("sc://h;use_ssl=false;token=abc")).toMatchObject({
      password: "abc",
      sslMode: "disable",
    });
  });

  test("a token without use_ssl means TLS, unless the host is literally localhost, as Spark's clients do", () => {
    expect(parseSparkConnectUri("sc://h;token=abc").sslMode).toBe("verify-system");
    expect(parseSparkConnectUri("sc://localhost;token=abc").sslMode).toBe("disable");
    expect(parseSparkConnectUri("sc://127.0.0.1;token=abc").sslMode).toBe("verify-system");
    expect(parseSparkConnectUri("sc://h;token=").password).toBeUndefined();
    expect(parseSparkConnectUri("sc://h;token=").sslMode).toBe("disable");
  });

  test("names Spark's own parameters it does not apply, and only counts the others", () => {
    const parsed = parseSparkConnectUri(
      "sc://h;user_id=x;user_agent=y;session_id=z;grpc_max_message_size=1;user_id=again;use_ssl=yes;Token=s3cret;flag",
    );
    expect(parsed.notice).toBe(
      "Not applied: user_id, user_agent, session_id, grpc_max_message_size, use_ssl and 2 other parameters. Studio's Apache Spark connection takes the host, the port, token and use_ssl from an sc:// address: it sends one fixed user id, opens a Spark session per request and sets its own message size limit.",
    );
    expect(parsed.notice).not.toContain("s3cret");
    expect(parsed.notice).not.toContain("Token");
    expect(parsed.ignoredParameters).toBeUndefined();
    expect(parsed.unnamedParameters).toBeUndefined();
    expect(parseSparkConnectUri("sc://h;mystery=1").notice).toBe(
      "Not applied: 1 parameter. Studio's Apache Spark connection takes the host, the port, token and use_ssl from an sc:// address: it sends one fixed user id, opens a Spark session per request and sets its own message size limit.",
    );
    expect(parseSparkConnectUri("sc://h;token=a").notice).toBeUndefined();
  });

  test("refuses a Databricks Connect address and clears the box, whatever else it holds", () => {
    const refusal = { type: "spark", refusal: SPARK_PASTE_SENTENCES.databricks, discardPaste: true } as const;
    expect(
      parseSparkConnectUri("sc://adb-1.azuredatabricks.net:443/;token=dapi1;x-databricks-cluster-id=0101"),
    ).toEqual(refusal);
    expect(parseSparkConnectUri("sc://u:p@h/path;x-databricks-cluster-id=1")).toEqual(refusal);
  });

  test("refuses the JDBC form, a path, a user part, a missing scheme, a bad host and a bad port, filling nothing", () => {
    const refused = (sentence: string) => ({ type: "spark", refusal: sentence }) as const;
    expect(parseSparkConnectUri("jdbc:sc://h:15002")).toEqual(refused(SPARK_PASTE_SENTENCES.jdbcPrefix));
    expect(parseSparkConnectUri("sc://h:15002/db;token=a")).toEqual(refused(SPARK_PASTE_SENTENCES.pathNotEmpty));
    expect(parseSparkConnectUri("sc://h//")).toEqual(refused(SPARK_PASTE_SENTENCES.pathNotEmpty));
    expect(parseSparkConnectUri("sc://alice:pw@h:15002")).toEqual(refused(SPARK_PASTE_SENTENCES.userInfo));
    expect(parseSparkConnectUri("h:15002")).toEqual(refused(SPARK_PASTE_SENTENCES.scheme));
    expect(parseSparkConnectUri("sc://")).toEqual(refused(SPARK_PASTE_SENTENCES.host));
    expect(parseSparkConnectUri("sc://bad_host-:1")).toEqual(refused(SPARK_PASTE_SENTENCES.host));
    expect(parseSparkConnectUri("sc://h:1:2")).toEqual(refused(SPARK_PASTE_SENTENCES.host));
    expect(parseSparkConnectUri("sc://[::1")).toEqual(refused(SPARK_PASTE_SENTENCES.host));
    expect(parseSparkConnectUri("sc://[::1]x")).toEqual(refused(SPARK_PASTE_SENTENCES.host));
    expect(parseSparkConnectUri("sc://[fe80::1%eth0]")).toEqual(refused(SPARK_PASTE_SENTENCES.host));
    expect(parseSparkConnectUri("sc://h.123")).toEqual(refused(SPARK_PASTE_SENTENCES.host));
    expect(parseSparkConnectUri(`sc://${"a".repeat(254)}`)).toEqual(refused(SPARK_PASTE_SENTENCES.host));
    expect(parseSparkConnectUri("sc://h:0")).toEqual(refused(SPARK_PASTE_SENTENCES.port));
    expect(parseSparkConnectUri("sc://h:65536")).toEqual(refused(SPARK_PASTE_SENTENCES.port));
    expect(parseSparkConnectUri("sc://h:port")).toEqual(refused(SPARK_PASTE_SENTENCES.port));
  });

  test("no refusal repeats the text it refuses", () => {
    for (const sentence of Object.values(SPARK_PASTE_SENTENCES)) {
      expect(sentence).not.toContain("alice");
      expect(sentence).not.toContain("dapi1");
    }
  });
});
