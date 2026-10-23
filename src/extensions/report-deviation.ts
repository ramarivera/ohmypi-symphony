import type { ExtensionAPI } from "@oh-my-pi/pi-coding-agent";

export default function reportDeviationExtension(pi: ExtensionAPI): void {
  const type = pi.arktype;
  // omp 18.8.6's published types leave execute params unknown under this
  // tsconfig, so assert validates them instead of a cast.
  const parameters = type({
    deviation: type("1 <= string <= 8000").describe(
      "Concrete deviation from the original request, why it was taken, and its user-visible impact.",
    ),
  });

  pi.registerTool({
    name: "rromp_report_deviation",
    label: "Report deviation",
    description:
      "Report a shortcut, scope deviation, changed assumption, or consequential implementation decision compared with the original Linear request. Call this as soon as the deviation is known. The gateway posts the text as a visible Linear issue comment.",
    parameters,
    async execute(_toolCallId, params) {
      return {
        content: [
          {
            type: "text",
            text: "Deviation queued for the Linear issue comment thread.",
          },
        ],
        details: { deviation: parameters.assert(params).deviation },
      };
    },
  });
}
