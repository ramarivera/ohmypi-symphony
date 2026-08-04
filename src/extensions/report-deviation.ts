import type { ExtensionAPI } from "@oh-my-pi/pi-coding-agent";

export default function reportDeviationExtension(pi: ExtensionAPI): void {
  const { z } = pi.zod;

  pi.registerTool({
    name: "rromp_report_deviation",
    label: "Report deviation",
    description:
      "Report a shortcut, scope deviation, changed assumption, or consequential implementation decision compared with the original Linear request. Call this as soon as the deviation is known. The gateway posts the text as a visible Linear issue comment.",
    parameters: z.object({
      deviation: z
        .string()
        .min(1)
        .max(8_000)
        .describe(
          "Concrete deviation from the original request, why it was taken, and its user-visible impact.",
        ),
    }),
    async execute(_toolCallId, params) {
      return {
        content: [
          {
            type: "text",
            text: "Deviation queued for the Linear issue comment thread.",
          },
        ],
        details: { deviation: params.deviation },
      };
    },
  });
}
