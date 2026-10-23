import type { PromptTemplateKind } from "../domain/models.js";

export const LINEAR_WORKER_CONTRACT = `Linear integration:
- Use OMP todos for meaningful multi-step work; they are displayed as the Linear agent plan.
- Use OMP UI requests when human input, selection, confirmation, or authorization is required; they are displayed as Linear elicitations.
- Tool execution and lifecycle progress are projected automatically as Linear thought and action activities. Do not call Linear directly to report progress.
- Gateway-owned Linear tools are available: linear_get_issue(issueId) reads any issue visible to this installation; linear_create_comment(body) posts a comment to this run's issue; linear_update_issue(stateId?, delegateId?) updates this run's issue; linear_add_external_url(label, url) appends a URL to this run's agent session.
- Use linear_get_issue to inspect issue state/details, linear_create_comment for user-facing issue updates, linear_update_issue when changing workflow state or delegation, and linear_add_external_url for durable artifact links.
- Call linear_add_external_url immediately when you open or update a pull request, using label "Pull request" and the PR URL.
- Call rromp_report_deviation as soon as you take a shortcut, depart from the original request, change a material assumption, or make a consequential implementation decision. The report becomes a visible Linear issue comment.
- Write the final response for the Linear user: state the outcome, include relevant artifact URLs, and name any required user action.
- If the run is stopped, cease work immediately; the gateway handles the terminal Linear response.`;

const T3_CONTRACT_HEADER = `Linear integration:
- Your plan, tool activity, questions and replies are projected to the Linear agent session automatically. Do not call Linear directly to report progress.`;
const T3_CONTRACT_FOOTER = `- Write the final response for the Linear user: state the outcome, include relevant artifact URLs, and name any required user action.
- If the run is stopped, cease work immediately; the gateway handles the terminal Linear response.`;

/** T3 Code agents have no OMP extensions or inline Linear host tools. */
export const T3_WORKER_CONTRACT = `${T3_CONTRACT_HEADER}
- Work in the repository checkout that is your working directory. Git is preconfigured with a short-lived credential scoped to this repository: commit on a new branch and push it with \`git push -u origin <branch>\`.
- Open the pull request with the GitHub REST API (POST https://api.github.com/repos/<owner>/<repo>/pulls). Authenticate with the same short-lived credential Git uses: \`git config --get http.https://github.com/.extraheader\` holds it as HTTP Basic auth for the user x-access-token. Never commit it or write it to files.
- Include the pull request URL in your final response; the gateway links it to the Linear issue.
${T3_CONTRACT_FOOTER}`;

/** A shared remote daemon gets no repository credential. */
export const T3_SHARED_WORKER_CONTRACT = `${T3_CONTRACT_HEADER}
- No Git or GitHub credential is available in this workspace. Leave your changes in the working directory and describe them; don't try to push.
${T3_CONTRACT_FOOTER}`;

export type PromptTemplateValues = Readonly<Record<string, string>>;

export const substitutePromptTemplate = (
  template: string,
  values: PromptTemplateValues,
): string => {
  const emptyTokens = new Set(
    Object.entries(values)
      .filter(([, value]) => value === "")
      .map(([name]) => `{{${name}}}`),
  );
  const filtered = template
    .split("\n")
    .filter((line) => !emptyTokens.has(line.trim()))
    .join("\n");
  return filtered.replace(
    /\{\{([A-Za-z][A-Za-z0-9_]*)\}\}/gu,
    (token, name: string) =>
      Object.hasOwn(values, name) ? (values[name] ?? "") : token,
  );
};

export const promptTemplatePlaceholders = (
  kind: PromptTemplateKind,
): ReadonlyArray<string> =>
  kind === "created"
    ? [
        "{{userRequest}}",
        "{{issueContext}}",
        "{{threadComment}}",
        "{{previousComments}}",
        "{{guidance}}",
      ]
    : kind === "prompted"
      ? ["{{userRequest}}"]
      : [];

export const promptTemplateWarnings = (
  kind: PromptTemplateKind,
  body: string,
): ReadonlyArray<string> => {
  const placeholders = promptTemplatePlaceholders(kind);
  if (
    placeholders.length === 0 ||
    placeholders.some((placeholder) => body.includes(placeholder))
  ) {
    return [];
  }
  return [
    `Template does not contain any recognized ${kind} placeholders; dynamic sections may be omitted.`,
  ];
};
