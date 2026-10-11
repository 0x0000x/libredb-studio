import type { DryRunEstimate } from "@/hooks/use-dry-run-estimate";

export function DryRunBadge({ estimate }: { estimate: DryRunEstimate }) {
  if (estimate.state === "idle") return null;
  const [tone, text] =
    estimate.state === "ok"
      ? ["text-success", `This query will process ${estimate.bytesHuman} when run.`]
      : estimate.state === "error"
        ? ["text-danger", estimate.message]
        : ["text-fg-muted", "Estimating bytes processed…"];
  return (
    <output
      aria-live="polite"
      title={text}
      className={`absolute bottom-2 right-4 z-10 max-w-[60%] truncate rounded border border-hairline bg-panel/90 px-2 py-1 text-xs ${tone}`}
    >
      {text}
    </output>
  );
}
