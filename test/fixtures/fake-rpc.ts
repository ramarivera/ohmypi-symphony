import { writeSync } from "node:fs";

function send(value: unknown): void {
  writeSync(1, `${JSON.stringify(value)}\n`);
}

function sendMany(values: readonly unknown[]): void {
  writeSync(1, `${values.map((value) => JSON.stringify(value)).join("\n")}\n`);
}

send({
  type: "ready",
  protocolVersion: 1,
  supportedProtocolVersions: [1, 2],
  maxFrameBytes: 1_048_576,
  maxReassembledFrameBytes: 67_108_864,
});

const reader = Bun.stdin.stream().getReader();
const decoder = new TextDecoder();
let buffer = "";
while (true) {
  const result = await reader.read();
  if (result.done) break;
  buffer += decoder.decode(result.value, { stream: true });
  let newline = buffer.indexOf("\n");
  while (newline >= 0) {
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (line) {
      const input: unknown = JSON.parse(line);
      if (
        typeof input === "object" &&
        input !== null &&
        "id" in input &&
        "type" in input &&
        typeof input.id === "string" &&
        typeof input.type === "string"
      ) {
        if (input.type === "get_state") {
          send({
            type: "response",
            id: input.id,
            command: input.type,
            success: true,
            data: { sessionId: "omp-test", sessionFile: "/safe/session.jsonl" },
          });
        } else {
          send({
            type: "response",
            id: input.id,
            command: input.type,
            success: true,
          });
          if (
            input.type === "prompt" &&
            "message" in input &&
            input.message === "late failure"
          ) {
            send({
              type: "response",
              id: input.id,
              command: input.type,
              success: false,
              error: "late scheduling failure",
            });
            continue;
          }
          if (input.type === "prompt") {
            sendMany([
              {
                type: "agent_start",
                sessionId: "omp-test",
                sessionFile: "/safe/session.jsonl",
                linearSecretPresent: Boolean(Bun.env.LINEAR_CLIENT_SECRET),
              },
              {
                type: "message_end",
                message: {
                  role: "assistant",
                  content: [{ type: "text", text: "fixture complete" }],
                },
              },
              { type: "agent_end" },
            ]);
          }
        }
      }
    }
    newline = buffer.indexOf("\n");
  }
}
