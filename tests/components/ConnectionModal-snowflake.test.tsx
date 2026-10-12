import "../setup-dom";

import { afterAll, afterEach, beforeAll, describe, expect, mock, test } from "bun:test";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";

import { ConnectionModal } from "@/components/ConnectionModal";
import { SNOWFLAKE_ERROR_SENTENCES } from "@/lib/db/providers/sql/snowflake/errors";
import type { DatabaseConnection } from "@/lib/types";

import { pemArmour } from "../helpers/pem-armour";

/**
 * The Snowflake connection form with the real dialog, the real form hook and the real field declarations, so a
 * refusal the dialog says before anything is sent is measured as the user meets it: by the requests that did not go.
 */

// As in ConnectionModal-test-answer.test.tsx: without Element.animate motion drives its animations from its own frame
// loop rather than through happy-dom's Web Animations.
const nativeAnimate = Element.prototype.animate;
beforeAll(() => {
  Reflect.deleteProperty(Element.prototype, "animate");
});
afterAll(() => {
  Element.prototype.animate = nativeAnimate;
});

afterEach(cleanup);

function field(id: string): HTMLInputElement {
  const element = document.getElementById(id);
  if (element === null) throw new Error(`no #${id} in the dialog`);
  return element as HTMLInputElement;
}

function openSnowflake() {
  const probe = mock(async (_connection: DatabaseConnection) => ({ success: true }));
  const onConnect = mock((_connection: DatabaseConnection) => {});
  render(<ConnectionModal isOpen onClose={() => {}} onConnect={onConnect} onTestConnection={probe} />);
  fireEvent.click(screen.getByRole("button", { name: "Snowflake" }));
  return { probe, onConnect };
}

async function click(name: string) {
  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name }));
  });
}

describe("the Snowflake form's own refusals", () => {
  test("an empty Account is refused in its sentence, and neither Test Connection nor Save sends a request", async () => {
    const { probe, onConnect } = openSnowflake();
    fireEvent.change(field("host"), { target: { value: "" } });
    fireEvent.change(field("user"), { target: { value: "studio_svc" } });
    fireEvent.change(field("role"), { target: { value: "STUDIO_RO" } });
    fireEvent.change(field("privateKey"), { target: { value: pemArmour("BEGIN") } });

    await click("Test Connection");
    expect(screen.getByTestId("connection-test-result").textContent).toContain(
      "Enter Account: an account identifier such as myorg-myaccount, or its URL. Nothing was sent.",
    );
    await click("Establish Connection");
    expect(probe).toHaveBeenCalledTimes(0);
    expect(onConnect).toHaveBeenCalledTimes(0);
  });

  test("a key pair with no key is refused, and so is a token sign-in with no token, each before any request", async () => {
    const { probe } = openSnowflake();
    fireEvent.change(field("host"), { target: { value: "myorg-myacct" } });
    fireEvent.change(field("user"), { target: { value: "studio_svc" } });
    fireEvent.change(field("role"), { target: { value: "STUDIO_RO" } });
    await click("Test Connection");
    expect(screen.getByTestId("connection-test-result").textContent).toContain(
      "Paste the private key for key-pair sign-in. Nothing was sent.",
    );

    fireEvent.change(field("authMethod"), { target: { value: "pat" } });
    await click("Test Connection");
    expect(screen.getByTestId("connection-test-result").textContent).toContain(
      "Enter the programmatic access token. Nothing was sent.",
    );
    expect(probe).toHaveBeenCalledTimes(0);

    // With the token typed, the connection is tested holding the token and no key.
    fireEvent.change(field("password"), { target: { value: "S-TOKEN" } });
    await click("Test Connection");
    expect(probe).toHaveBeenCalledTimes(1);
    const sent = probe.mock.calls[0][0];
    expect(sent).toMatchObject({ type: "snowflake", host: "myorg-myacct", authMethod: "pat", password: "S-TOKEN" });
    expect(sent).not.toHaveProperty("privateKey");
    expect(sent).not.toHaveProperty("port");
  });
});

/** Every text the dialog holds: its markup and the value of each box, which markup alone may not carry. */
function everythingDrawn(): string {
  const values = Array.from(document.querySelectorAll("input, textarea, select")).map(
    (element) => (element as HTMLInputElement).value,
  );
  return [document.body.innerHTML, ...values].join("\n");
}

describe("a Snowflake text in the dialog keeps nothing of its secrets", () => {
  function pasteText(text: string) {
    fireEvent.click(screen.getByRole("button", { name: "Paste URL" }));
    const box = document.querySelector('input[placeholder^="postgres://"]') as HTMLInputElement;
    fireEvent.change(box, { target: { value: text } });
    fireEvent.click(screen.getByRole("button", { name: "Parse" }));
  }

  test("a pasted inline key is refused by name, no field is set, and the key is nowhere in the dialog", () => {
    const probe = mock(async (_connection: DatabaseConnection) => ({ success: true }));
    render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} onTestConnection={probe} />);
    pasteText(
      "jdbc:snowflake://myorg-myacct.snowflakecomputing.com/?user=u-applied&token=T-SENTINEL&private_key_base64=K-SENTINEL",
    );
    expect(screen.getByTestId("connection-test-result").textContent).toContain(
      "Studio does not take private_key_base64 from a pasted URL. Paste the key itself into Private key, not inside a URL.",
    );
    const drawn = everythingDrawn();
    expect(drawn).not.toContain("K-SENTINEL");
    expect(drawn).not.toContain("T-SENTINEL");
    expect(drawn).not.toContain("u-applied");
    // The dialog shows Snowflake's form, so the Private key box the sentence names is there, and fills nothing else:
    // Account opens empty rather than holding the previous engine's localhost.
    expect(field("authMethod")).toBeDefined();
    expect(field("privateKey").value).toBe("");
    expect(field("host").value).toBe("");
    expect(field("user").value).toBe("");
    expect(probe).toHaveBeenCalledTimes(0);
  });

  test("a pasted key path is refused on Snowflake's form, which shows the Private key box and nothing of the path", () => {
    render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} />);
    pasteText(
      "jdbc:snowflake://myorg-myacct.snowflakecomputing.com/?user=u-applied&private_key_file=/home/u/P-SENTINEL.p8",
    );
    expect(screen.getByTestId("connection-test-result").textContent).toContain(
      "Studio does not take private_key_file from a pasted URL. Studio reads no file: paste the key's contents into Private key.",
    );
    expect(field("privateKey").value).toBe("");
    expect(field("host").value).toBe("");
    const drawn = everythingDrawn();
    expect(drawn).not.toContain("P-SENTINEL");
    expect(drawn).not.toContain("u-applied");
  });

  test("a jdbc: URL typed into Account is refused before any request, in a sentence holding nothing of it", async () => {
    const { probe } = openSnowflake();
    fireEvent.change(field("host"), {
      target: { value: "jdbc:snowflake://myorg-myacct.snowflakecomputing.com/?token=S-SENTINEL" },
    });
    fireEvent.change(field("user"), { target: { value: "studio_svc" } });
    fireEvent.change(field("role"), { target: { value: "STUDIO_RO" } });
    fireEvent.change(field("privateKey"), { target: { value: pemArmour("BEGIN") } });
    await click("Test Connection");
    const sentence = screen.getByTestId("connection-test-result").textContent ?? "";
    expect(sentence).toContain(
      "Enter an account identifier such as myorg-myaccount, or its https URL; a jdbc: or snowflake:// URL goes into Paste URL instead. Nothing was sent.",
    );
    expect(sentence).not.toContain("S-SENTINEL");
    expect(probe).toHaveBeenCalledTimes(0);
  });

  test("a pasted JDBC URL with a token fills the form and leaves the paste box empty", async () => {
    render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} />);
    pasteText("jdbc:snowflake://myorg-myacct.snowflakecomputing.com/?user=u&token=S-SENT&warehouse=wh");
    expect(field("host").value).toBe("myorg-myacct.snowflakecomputing.com");
    expect(field("user").value).toBe("u");
    expect(field("warehouse").value).toBe("wh");
    expect((field("authMethod") as unknown as HTMLSelectElement).value).toBe("pat");
    expect(field("password").value).toBe("S-SENT");
    expect(document.getElementById("privateKey")).toBeNull();
    // The form state holds no text at once; the box itself goes when its exit animation ends.
    await waitFor(() => expect(document.querySelector('input[placeholder^="postgres://"]')).toBeNull());
    expect(everythingDrawn()).not.toContain("jdbc:snowflake");
    expect(screen.getByTestId("host-hint").textContent).toBe("Studio connects to myorg-myacct.snowflakecomputing.com.");
  });
});

describe("choosing Snowflake", () => {
  const choose = (name: string) => fireEvent.click(screen.getByRole("button", { name }));

  test("opens Account empty after another engine's host, so Test meets the Account sentence, not a sign-in", async () => {
    const probe = mock(async (_connection: DatabaseConnection) => ({ success: true }));
    render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} onTestConnection={probe} />);
    expect(field("host").value).toBe("localhost");
    choose("Snowflake");
    expect(field("host").value).toBe("");
    expect(screen.queryByTestId("host-hint")?.textContent ?? "").not.toContain("localhost");

    fireEvent.change(field("user"), { target: { value: "studio_svc" } });
    fireEvent.change(field("role"), { target: { value: "STUDIO_RO" } });
    fireEvent.change(field("privateKey"), { target: { value: pemArmour("BEGIN") } });
    await click("Test Connection");
    expect(screen.getByTestId("connection-test-result").textContent).toContain(
      "Enter Account: an account identifier such as myorg-myaccount, or its URL. Nothing was sent.",
    );
    expect(probe).toHaveBeenCalledTimes(0);
  });

  test("drops a host typed for another engine, and keeps an account typed for Snowflake when it is chosen again", () => {
    render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} />);
    choose("MySQL");
    fireEvent.change(field("host"), { target: { value: "db.example.invalid" } });
    choose("Snowflake");
    expect(field("host").value).toBe("");
    fireEvent.change(field("host"), { target: { value: "myorg-myacct" } });
    choose("Snowflake");
    expect(field("host").value).toBe("myorg-myacct");
    // Another engine keeps the host as before.
    choose("PostgreSQL");
    expect(field("host").value).toBe("myorg-myacct");
  });
});

describe("the Snowflake key-pair refusal in the result banner", () => {
  const BODY = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA".repeat(9);
  const FINGERPRINT = "SHA256:Qm9vZ2xlR29vZ2xlR29vZ2xlR29vZ2xlR29vZ2xlR29vZ2xlJWZG3hSx=";

  test("wraps its long words, so the fingerprint and both key bodies are read whole at any width", async () => {
    const sentence = SNOWFLAKE_ERROR_SENTENCES.keyPairRefused({
      uuid: null,
      account: "MYORG-MYACCT",
      user: "STUDIO_SVC",
      fingerprint: FINGERPRINT,
      fp8: "0a1b2c3d",
      body: BODY,
    });
    const statement = `ALTER USER ADD KEY PAIR STUDIO_0a1b2c3d PUBLIC_KEY = '${BODY}'`;
    const probe = mock(async (_connection: DatabaseConnection) => ({
      success: false,
      error: sentence,
      fixStatement: statement,
    }));
    render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} onTestConnection={probe} />);
    fireEvent.click(screen.getByRole("button", { name: "Snowflake" }));
    fireEvent.change(field("host"), { target: { value: "myorg-myacct" } });
    fireEvent.change(field("user"), { target: { value: "studio_svc" } });
    fireEvent.change(field("role"), { target: { value: "STUDIO_RO" } });
    fireEvent.change(field("privateKey"), { target: { value: pemArmour("BEGIN") } });
    await click("Test Connection");

    const text = screen.getByTestId("connection-test-message");
    expect(text.textContent).toBe(sentence);
    expect(text.textContent).toContain(FINGERPRINT);
    // happy-dom lays nothing out, so the rule that lets a word longer than the line break is what is read: without
    // it the fingerprint and the key bodies run past the banner and its overflow-hidden ancestor clips them.
    expect(text.className.split(" ")).toContain("break-words");
    expect(text.tagName).toBe("P");
    expect(screen.getByTestId("connection-fix-statement").textContent).toBe(statement);
  });
});

describe("the Snowflake Schema, Database and User boxes", () => {
  test("draw Snowflake's hint alone under Schema, and no placeholder that could pass for a typed value", () => {
    render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} />);
    fireEvent.click(screen.getByRole("button", { name: "Snowflake" }));
    const schemaBox = field("schema").closest("div") as HTMLElement;
    expect(schemaBox.textContent).toContain("The default schema for unqualified names.");
    expect(schemaBox.textContent).not.toContain("SHOW SCHEMAS");
    expect(schemaBox.querySelectorAll("p")).toHaveLength(1);
    for (const id of ["user", "database", "schema"]) expect(field(id).placeholder).toBe("");
  });

  test("leave Trino's Schema box as it was, its sentence and placeholders included", () => {
    render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} />);
    fireEvent.click(screen.getByRole("button", { name: "Trino" }));
    expect((field("schema").closest("div") as HTMLElement).textContent).toContain(
      "Run SHOW SCHEMAS to list the catalog's schemas.",
    );
    expect(field("schema").placeholder).toBe("default");
    expect(field("database").placeholder).toBe("tpch");
    expect(field("user").placeholder).toBe("user");
  });

  test("read an engine's declared User example: Databend's root, declared before the box read one", () => {
    render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} />);
    fireEvent.click(screen.getByRole("button", { name: "Databend" }));
    expect(field("user").placeholder).toBe("root");
  });
});
