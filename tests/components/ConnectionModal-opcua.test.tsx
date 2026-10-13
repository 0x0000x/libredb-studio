import "../setup-dom";

import { afterEach, describe, expect, test } from "bun:test";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MotionGlobalConfig } from "framer-motion";

import { ConnectionModal } from "@/components/ConnectionModal";
import { getDBConfig } from "@/lib/db-ui-config";

/**
 * The OPC UA entry's own two fields, in the real dialog (#1677).
 *
 * WHY A RENDER AND NOT A DECLARATION TEST. `DB_UI_CONFIG` naming a field buys nothing on its own:
 * every field in this dialog is a block of JSX, and a declared field with no block renders NOTHING.
 * That is exactly what happened here first — `securityMode` and `serverCertificateThumbprint` were
 * declared, labelled and hinted, and the dialog drew neither — so this file renders the dialog and
 * reads the controls out of the DOM.
 *
 * The security mode is chosen rather than typed, because there are exactly three and a typo would
 * otherwise be refused at connect time; the pin is a text box, because a thumbprint is pasted.
 */

// The dialog's banners animate in; happy-dom cancels an animation still running when the test ends.
MotionGlobalConfig.skipAnimations = true;

afterEach(cleanup);

function openOpcua(): void {
  render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} />);
  fireEvent.click(screen.getByRole("button", { name: "OPC UA" }));
}

describe("the OPC UA entry in the connection dialog", () => {
  test("is offered as its own driver, with the endpoint URL in the Host box", () => {
    openOpcua();
    const host = document.getElementById("host") as HTMLInputElement;
    expect(host).not.toBeNull();
    expect(host.placeholder).toBe("opc.tcp://plc.example.net:4840/");
    // The label is the engine's own word, not "Host": the endpoint URL is the whole address.
    expect(screen.getByText("Endpoint URL")).toBeDefined();
    expect(getDBConfig("opcua").showConnectionStringToggle).toBe(false);
  });

  /*
   * THE WHOLE ADDRESS IS ONE URL, so neither of the two boxes a host engine gets belongs here.
   * Measured in a browser: the dialog drew a port box holding `4840` beside the endpoint - typing
   * 54840 into it changed nothing, because the dial takes the port from the URL - and the endpoint
   * box itself held `localhost`, which is not an endpoint and which Studio had put there, so a
   * reader who picked OPC UA and pressed Test Connection was refused for a value they never typed.
   */
  /* The chosen engine is said, not only tinted: a screen reader was told nothing about which of the
   * 33 cards is active. The security-mode buttons below already answer this way. */
  test("the engine grid says which card is chosen", () => {
    openOpcua();

    expect(screen.getByRole("button", { name: "OPC UA" }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByRole("button", { name: "PostgreSQL" }).getAttribute("aria-pressed")).toBe("false");
  });

  test("draws no port box, and does not pre-fill the endpoint with the generic host default", () => {
    openOpcua();

    expect(document.getElementById("port")).toBeNull();
    expect((document.getElementById("host") as HTMLInputElement).value).toBe("");
  });

  test("an engine addressed by host and port keeps both, and the host default comes back", () => {
    render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} />);
    fireEvent.click(screen.getByRole("button", { name: "OPC UA" }));
    expect((document.getElementById("host") as HTMLInputElement).value).toBe("");

    fireEvent.click(screen.getByRole("button", { name: "PostgreSQL" }));
    expect((document.getElementById("host") as HTMLInputElement).value).toBe("localhost");
    expect((document.getElementById("port") as HTMLInputElement).value).toBe("5432");
  });

  test("an endpoint the reader typed survives a change of engine", () => {
    render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} />);
    const host = () => document.getElementById("host") as HTMLInputElement;
    fireEvent.change(host(), { target: { value: "plc.example.net" } });

    fireEvent.click(screen.getByRole("button", { name: "OPC UA" }));
    expect(host().value).toBe("plc.example.net");
  });

  test("draws the security mode as three buttons, with SignAndEncrypt the standing choice", () => {
    openOpcua();
    for (const mode of ["None", "Sign", "SignAndEncrypt"]) {
      expect(screen.getByRole("button", { name: mode })).toBeDefined();
    }
    // Nothing typed yet, and the connection reader's own default is SignAndEncrypt, so that is the
    // one drawn as chosen: a dialog that showed none would read as "no mode decided".
    expect(screen.getByRole("button", { name: "SignAndEncrypt" }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByRole("button", { name: "None" }).getAttribute("aria-pressed")).toBe("false");
  });

  test("choosing None moves the choice, which is what a reader needs for an unsecured server", () => {
    openOpcua();
    fireEvent.click(screen.getByRole("button", { name: "None" }));
    expect(screen.getByRole("button", { name: "None" }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByRole("button", { name: "SignAndEncrypt" }).getAttribute("aria-pressed")).toBe("false");
    fireEvent.click(screen.getByRole("button", { name: "Sign" }));
    expect(screen.getByRole("button", { name: "Sign" }).getAttribute("aria-pressed")).toBe("true");
  });

  test("draws the certificate pin as a box, and keeps what is typed into it", () => {
    openOpcua();
    const pin = document.getElementById("serverCertificateThumbprint") as HTMLInputElement;
    expect(pin).not.toBeNull();
    const thumbprint = "b9432ed131e30092d2a0619284763a403c33fc3929546728864b27b475c9529a";
    fireEvent.change(pin, { target: { value: thumbprint } });
    expect((document.getElementById("serverCertificateThumbprint") as HTMLInputElement).value).toBe(thumbprint);
  });

  /**
   * THE HINT IS READ FROM THE DECLARATION AND THEN HELD TO WHAT IT HAS TO SAY. Searching the DOM for
   * whatever the declaration happens to hold proves the text was rendered and nothing about the
   * text: any wording passes, the empty string included. So this asserts three things - that the
   * declared hint is on screen, that the field POINTS at it (`aria-describedby`, which is what a
   * screen reader follows), and that each hint still names the fact a reader needs from it.
   */
  test("both fields carry the hint that says what they decide, and point at it", () => {
    openOpcua();
    const hints = getDBConfig("opcua").fieldHints ?? {};
    const mode = hints.securityMode ?? "";
    const pin = hints.serverCertificateThumbprint ?? "";
    expect(screen.getByText(mode)).toBeDefined();
    expect(screen.getByText(pin)).toBeDefined();
    expect(document.getElementById("securityMode")?.getAttribute("aria-describedby")).toBe("securityMode-hint");
    expect(document.getElementById("serverCertificateThumbprint")?.getAttribute("aria-describedby")).toBe(
      "serverCertificateThumbprint-hint",
    );
    // The three modes, and that there is no fallback: a mode the server lacks is refused.
    for (const word of ["None", "Sign", "SignAndEncrypt", "refused"]) expect(mode).toContain(word);
    // What a pin is, and what its absence means.
    for (const word of ["SHA-256", "64"]) expect(pin).toContain(word);
  });

  /** The control: another engine draws neither field, so these two belong to this entry alone. */
  test("PostgreSQL draws no security mode and no certificate pin", () => {
    render(<ConnectionModal isOpen onClose={() => {}} onConnect={() => {}} />);
    fireEvent.click(screen.getByRole("button", { name: "PostgreSQL" }));
    expect(document.getElementById("securityMode")).toBeNull();
    expect(document.getElementById("serverCertificateThumbprint")).toBeNull();
  });
});
