import { THEME_CONTROLS } from "./theme";

export type RunDetailLevel = "debug" | "info" | "warn" | "result" | "error";

export interface RunDetailEvent {
  readonly sourceKey: string;
  readonly kind: string;
  readonly level: RunDetailLevel;
  readonly text: string | null;
  readonly payload: string;
  readonly status: string | null;
  readonly error: string | null;
  readonly createdAt: number;
  readonly updatedAt: number;
}

export interface RunDetailModel {
  readonly run: {
    readonly sessionId: string;
    readonly organizationId: string;
    readonly issueId: string | null;
    readonly repositoryId: string | null;
    readonly state: string;
    readonly desiredState: string;
    readonly ompSessionId: string | null;
    readonly ompSessionFile: string | null;
    readonly workspacePath: string | null;
    readonly teamId: string | null;
    readonly projectId: string | null;
    readonly attempt: number;
    readonly leaseOwner: string | null;
    readonly leaseExpiresAt: number | null;
    readonly lastActivityAt: number | null;
    readonly terminalReason: string | null;
    readonly nextAttemptAt: number | null;
    readonly createdAt: number;
    readonly updatedAt: number;
  };
  readonly issue: {
    readonly identifier: string | null;
    readonly title: string | null;
    readonly url: string | null;
  } | null;
  readonly events: readonly RunDetailEvent[];
}

const LEVELS: readonly RunDetailLevel[] = [
  "debug",
  "info",
  "warn",
  "result",
  "error",
];

const TERMINAL_STATES: Record<string, true> = {
  succeeded: true,
  failed: true,
  canceled: true,
};

/** Removes common credentials before a public run page or its JSON is emitted. */
export function redact(value: string): string {
  return value
    .replace(
      /\bAuthorization\s*:\s*Bearer\s+[^\s,;"'}\]]+/giu,
      "Authorization: Bearer redacted",
    )
    .replace(/\bBearer\s+[A-Za-z0-9._~+\-/=]+/gu, "Bearer redacted")
    .replace(
      /((?:["'](?:access[_-]?token|token|api[_-]?key|key|secret|signature|password)["']|(?:access[_-]?token|token|api[_-]?key|key|secret|signature|password))\s*[:=]\s*["']?)[^,"'\s}\]]+/giu,
      "$1redacted",
    )
    .replace(
      /([?&](?:token|key|secret|signature|password)=)[^&#\s"'<]*/giu,
      "$1redacted",
    );
}

function escapeHtml(value: string): string {
  return value.replace(
    /[&<>"']/g,
    (character) =>
      ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      })[character] ?? character,
  );
}

function present(value: string | number | null): string {
  if (value === null || value === "") return "—";
  return escapeHtml(String(value));
}

function timestamp(value: number | null): string {
  if (value === null) return "—";
  const instant = new Date(value);
  const absolute = `${instant.toLocaleString("en-US", {
    dateStyle: "medium",
    timeStyle: "medium",
    timeZone: "UTC",
  })} UTC`;
  return `<time datetime="${instant.toISOString()}" title="${instant.toISOString()}" data-relative-time="${value}">${escapeHtml(absolute)} · ${escapeHtml(relativeTime(value))}</time>`;
}

function relativeTime(value: number): string {
  const delta = Math.round((value - Date.now()) / 1_000);
  const units: readonly [Intl.RelativeTimeFormatUnit, number][] = [
    ["year", 31_536_000],
    ["month", 2_592_000],
    ["day", 86_400],
    ["hour", 3_600],
    ["minute", 60],
    ["second", 1],
  ];
  const [unit, seconds] = units.find(
    ([, seconds]) => Math.abs(delta) >= seconds,
  ) ?? ["second", 1];
  return new Intl.RelativeTimeFormat("en", { numeric: "auto" }).format(
    Math.round(delta / seconds),
    unit,
  );
}

function fact(label: string, value: string): string {
  return `<div><dt>${label}</dt><dd>${value}</dd></div>`;
}

function eventMarkup(event: RunDetailEvent): string {
  const message =
    event.text === null
      ? ""
      : `<pre class="run-event-text">${escapeHtml(redact(event.text))}</pre>`;
  const error =
    event.error === null
      ? ""
      : `<p class="run-event-error">${escapeHtml(redact(event.error))}</p>`;
  const payload =
    event.payload === "null"
      ? ""
      : `<details><summary>Rendered payload</summary><pre>${escapeHtml(redact(event.payload))}</pre></details>`;
  const status =
    event.status === null
      ? ""
      : `<span class="run-event-status">${escapeHtml(event.status)}</span>`;
  return `<article class="run-event" data-run-level="${event.level}">
  <div class="run-event-meta">
    <span class="run-event-level">${event.level}</span>
    <strong>${escapeHtml(event.kind)}</strong>
    ${status}
    <time datetime="${new Date(event.createdAt).toISOString()}" title="${new Date(event.createdAt).toISOString()}" data-relative-time="${event.createdAt}">${escapeHtml(relativeTime(event.createdAt))}</time>
  </div>
  <p class="run-event-source">source key: <code>${escapeHtml(event.sourceKey)}</code></p>
  ${message}
  ${error}
  ${payload}
</article>`;
}

export function renderRunDetailBody(model: RunDetailModel): string {
  const counts: Record<RunDetailLevel, number> = {
    debug: 0,
    info: 0,
    warn: 0,
    result: 0,
    error: 0,
  };
  for (const event of model.events) counts[event.level] += 1;
  const issue = model.issue;
  const issueMarkup = issue
    ? `<section class="run-issue" aria-labelledby="issue-heading">
      <p class="eyebrow">Linked Linear issue</p>
      <h2 id="issue-heading">${present(issue.identifier)}${issue.title === null ? "" : ` · ${present(issue.title)}`}</h2>
      ${issue.url === null ? "" : `<a href="${escapeHtml(issue.url)}" rel="noreferrer">Open in Linear</a>`}
    </section>`
    : "";
  const filters = LEVELS.map(
    (level) =>
      `<button type="button" class="run-filter" data-run-level-toggle="${level}" aria-pressed="true">${level} <span>${counts[level]}</span></button>`,
  ).join("\n");
  const terminal = Object.hasOwn(TERMINAL_STATES, model.run.state);

  return `<main class="run-detail" data-run-updated-at="${model.run.updatedAt}">
  <header class="run-header">
    <div>
      <p class="eyebrow">OhMyPi run</p>
      <h1>${present(model.run.sessionId)}</h1>
      <p class="run-status" data-state="${escapeHtml(model.run.state)}">${present(model.run.state)}</p>
    </div>
    ${THEME_CONTROLS}
  </header>

  ${issueMarkup}

  <section class="run-facts" aria-labelledby="run-facts-heading">
    <h2 id="run-facts-heading">Run details</h2>
    <dl>
      ${fact("Organization", present(model.run.organizationId))}
      ${fact("Issue ID", present(model.run.issueId))}
      ${fact("State", present(model.run.state))}
      ${fact("Desired state", present(model.run.desiredState))}
      ${fact("Attempt", present(model.run.attempt))}
      ${fact("Terminal reason", present(model.run.terminalReason))}
      ${fact("Repository", present(model.run.repositoryId))}
      ${fact("Workspace", present(model.run.workspacePath))}
      ${fact("OhMyPi session", present(model.run.ompSessionId))}
      ${fact("OhMyPi session file", present(model.run.ompSessionFile))}
      ${fact("Team", present(model.run.teamId))}
      ${fact("Project", present(model.run.projectId))}
      ${fact("Lease owner", present(model.run.leaseOwner))}
      ${fact("Lease expires", timestamp(model.run.leaseExpiresAt))}
      ${fact("Next attempt", timestamp(model.run.nextAttemptAt))}
      ${fact("Created", timestamp(model.run.createdAt))}
      ${fact("Updated", timestamp(model.run.updatedAt))}
      ${fact("Last activity", timestamp(model.run.lastActivityAt))}
    </dl>
  </section>

  <section class="run-timeline" aria-labelledby="timeline-heading">
    <div class="run-timeline-heading">
      <div>
        <p class="eyebrow">Run-scoped records</p>
        <h2 id="timeline-heading">Timeline</h2>
      </div>
      <div class="run-filters" aria-label="Filter timeline levels">${filters}</div>
    </div>
    <p class="run-timeline-empty"${model.events.length === 0 ? "" : " hidden"}>No run events have been recorded yet.</p>
    <div class="run-events">${model.events.map(eventMarkup).join("\n")}</div>
  </section>
</main>
<script>window.__OHMYPI_RUN_POLL__ = ${String(!terminal)};</script>`;
}

export const RUN_DETAIL_STYLES = `
  .run-detail { max-width: 1120px; margin: 0 auto; padding: 32px 28px 72px; }
  .run-header, .run-timeline-heading { display: flex; align-items: flex-start; justify-content: space-between; gap: 24px; }
  .run-header { border-bottom: 1px solid var(--rule); padding-bottom: 24px; }
  .run-header h1 { font-family: var(--mono); font-size: clamp(1.4rem, 3vw, 2.25rem); overflow-wrap: anywhere; }
  .eyebrow { margin: 0 0 4px; color: var(--ink-mute); font-family: var(--mono); font-size: 0.72rem; letter-spacing: 0.08em; text-transform: uppercase; }
  .run-status, .run-event-level, .run-event-status { display: inline-block; margin: 12px 0 0; padding: 2px 8px; border: 1px solid var(--rule); font-family: var(--mono); font-size: 0.72rem; text-transform: uppercase; }
  .run-status[data-state="failed"], .run-status[data-state="canceled"], .run-event[data-run-level="error"] .run-event-level { border-color: var(--danger); color: var(--danger); }
  .run-status[data-state="succeeded"], .run-event[data-run-level="result"] .run-event-level { border-color: var(--good); color: var(--good); }
  .run-issue, .run-facts, .run-timeline { margin-top: 28px; padding: 24px; border: 1px solid var(--rule); background: var(--paper); }
  .run-issue h2 { margin-bottom: 8px; }
  .run-facts dl { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); margin: 18px 0 0; border-top: 1px solid var(--rule-soft); }
  .run-facts dl > div { min-width: 0; padding: 12px 14px 12px 0; border-bottom: 1px solid var(--rule-soft); }
  .run-facts dt, .run-event-source { color: var(--ink-mute); font-family: var(--mono); font-size: 0.72rem; letter-spacing: 0.04em; text-transform: uppercase; }
  .run-facts dd { margin: 4px 0 0; overflow-wrap: anywhere; }
  .run-filters { display: flex; flex-wrap: wrap; gap: 6px; }
  .run-filter { padding: 6px 9px; border-color: var(--rule); font-family: var(--mono); font-size: 0.72rem; }
  .run-filter[aria-pressed="false"] { opacity: 0.45; }
  .run-event { padding: 16px 0; border-bottom: 1px solid var(--rule-soft); }
  .run-event[hidden] { display: none; }
  .run-event-meta { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; }
  .run-event-meta time { margin-left: auto; color: var(--ink-mute); font-family: var(--mono); font-size: 0.74rem; }
  .run-event-level, .run-event-status { margin: 0; }
  .run-event-source { margin: 8px 0; overflow-wrap: anywhere; }
  .run-event pre { max-height: 320px; margin: 8px 0 0; padding: 12px; overflow: auto; background: var(--paper-tint); border: 1px solid var(--rule-soft); font-family: var(--mono); font-size: 0.8rem; line-height: 1.5; white-space: pre-wrap; overflow-wrap: anywhere; }
  .run-event-text { color: var(--ink); }
  .run-event-error { color: var(--danger); }
  .run-event details { margin-top: 10px; }
  .run-event summary { cursor: pointer; color: var(--ink-soft); font-family: var(--mono); font-size: 0.78rem; }
  @media (max-width: 720px) { .run-detail { padding: 20px 16px 48px; } .run-header, .run-timeline-heading { flex-direction: column; } .run-issue, .run-facts, .run-timeline { padding: 18px; } .run-event-meta time { width: 100%; margin-left: 0; } }
`;

export const RUN_DETAIL_SCRIPT = `
(function () {
  "use strict";
  var active = new Set(["debug", "info", "warn", "result", "error"]);
  var toggles = document.querySelectorAll("[data-run-level-toggle]");
  var events = document.querySelectorAll("[data-run-level]");
  function applyFilters() {
    for (var index = 0; index < events.length; index += 1) {
      var event = events[index];
      event.hidden = !active.has(event.getAttribute("data-run-level"));
    }
  }
  for (var index = 0; index < toggles.length; index += 1) {
    toggles[index].addEventListener("click", function (clickEvent) {
      var toggle = clickEvent.currentTarget;
      var level = toggle.getAttribute("data-run-level-toggle");
      if (!level) return;
      if (active.has(level)) active.delete(level); else active.add(level);
      toggle.setAttribute("aria-pressed", active.has(level) ? "true" : "false");
      applyFilters();
    });
  }
  if (!window.__OHMYPI_RUN_POLL__) return;
  var root = document.querySelector("[data-run-updated-at]");
  if (!root) return;
  var previous = root.getAttribute("data-run-updated-at");
  var jsonUrl = window.location.pathname + ".json";
  window.setInterval(function () {
    window.fetch(jsonUrl, { headers: { Accept: "application/json" }, cache: "no-store" })
      .then(function (response) { return response.ok ? response.json() : null; })
      .then(function (payload) {
        if (payload && payload.run && String(payload.run.updatedAt) !== previous) window.location.reload();
      })
      .catch(function () { /* polling is best-effort */ });
  }, 15000);
})();
`;
