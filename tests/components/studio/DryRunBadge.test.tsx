import "../../setup-dom";

import { afterEach, describe, expect, test } from "bun:test";
import { cleanup, render } from "@testing-library/react";

import { DryRunBadge } from "@/components/studio/DryRunBadge";

describe("DryRunBadge", () => {
  afterEach(cleanup);

  test("renders nothing when idle", () => {
    const { container } = render(<DryRunBadge estimate={{ state: "idle" }} />);
    expect(container.innerHTML).toBe("");
  });

  test("pending shows a muted estimating line", () => {
    const { container } = render(<DryRunBadge estimate={{ state: "pending" }} />);
    const output = container.querySelector("output");
    expect(output?.textContent).toBe("Estimating bytes processed…");
    expect(output?.className).toContain("text-fg-muted");
  });

  test("ok shows the bytes it will process", () => {
    const { container } = render(<DryRunBadge estimate={{ state: "ok", bytesHuman: "1.2 GB" }} />);
    const output = container.querySelector("output");
    expect(output?.textContent).toBe("This query will process 1.2 GB when run.");
    expect(output?.getAttribute("title")).toBe("This query will process 1.2 GB when run.");
    expect(output?.className).toContain("text-success");
  });

  test("error shows the message", () => {
    const { container } = render(<DryRunBadge estimate={{ state: "error", message: "boom" }} />);
    const output = container.querySelector("output");
    expect(output?.textContent).toBe("boom");
    expect(output?.className).toContain("text-danger");
  });
});
