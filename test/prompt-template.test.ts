import { Effect, Layer, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";
import { OrganizationId } from "../src/domain/ids.js";
import {
  promptTemplateWarnings,
  substitutePromptTemplate,
} from "../src/services/prompt-templates.js";
import { PromptTemplateRepo } from "../src/services/store/prompt-template-repo.js";
import { SqliteClientLive } from "../src/services/store/sqlite-client.js";

const orgA = Schema.decodeUnknownSync(OrganizationId)("org-a");
const orgB = Schema.decodeUnknownSync(OrganizationId)("org-b");

describe("prompt template substitution", () => {
  it("substitutes every created placeholder and leaves unknown tokens literal", () => {
    expect(
      substitutePromptTemplate(
        "{{userRequest}}|{{issueContext}}|{{threadComment}}|{{previousComments}}|{{guidance}}|{{unknown}}",
        {
          userRequest: "request",
          issueContext: "issue",
          threadComment: "thread",
          previousComments: "previous",
          guidance: "guidance",
        },
      ),
    ).toBe("request|issue|thread|previous|guidance|{{unknown}}");
  });

  it("drops empty token-only sections and preserves unknown tokens", () => {
    expect(
      substitutePromptTemplate("before\n{{threadComment}}\nafter|{{unknown}}", {
        threadComment: "",
      }),
    ).toBe("before\nafter|{{unknown}}");
    expect(promptTemplateWarnings("created", "{{unknown}}")).toHaveLength(1);
    expect(promptTemplateWarnings("created", "{{guidance}}")).toEqual([]);
  });
});

describe("PromptTemplateRepo", () => {
  it("upserts, scopes, lists, and removes templates by organization and kind", async () => {
    const layer = PromptTemplateRepo.Default.pipe(
      Layer.provide(SqliteClientLive(":memory:")),
    );
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const repo = yield* PromptTemplateRepo;
          yield* repo.upsert({
            organizationId: orgA,
            kind: "created",
            body: "first",
            updatedAt: 1,
          });
          yield* repo.upsert({
            organizationId: orgA,
            kind: "created",
            body: "second",
            updatedAt: 2,
          });
          yield* repo.upsert({
            organizationId: orgB,
            kind: "created",
            body: "other",
            updatedAt: 3,
          });
          const current = yield* repo.get(orgA, "created");
          const listed = yield* repo.list(orgA);
          const removed = yield* repo.remove(orgA, "created");
          const missing = yield* repo.get(orgA, "created");
          return { current, listed, removed, missing };
        }).pipe(Effect.provide(layer)),
      ),
    );
    expect(Option.getOrThrow(result.current).body).toBe("second");
    expect(result.listed).toHaveLength(1);
    expect(result.removed).toBe(true);
    expect(Option.isNone(result.missing)).toBe(true);
  });
});
