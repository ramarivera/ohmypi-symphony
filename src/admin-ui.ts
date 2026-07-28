/**
 * Admin UI presentation module — thin facade.
 *
 * Composes a complete, self-contained HTML document (inline CSS + JS, no
 * external font or asset dependencies) from three focused collaborators:
 *
 *   - `./admin-ui/styles`     — the inline `<style>` token + rule sheet
 *   - `./admin-ui/landing`    — the unauthenticated landing body
 *   - `./admin-ui/dashboard`  — the authenticated admin body + client script
 *
 * The two public renderers preserve the API contract the rest of the system
 * (and the test suite) depends on:
 *
 *   - renderLandingPage(): the pre-auth onboarding shell served from GET /.
 *     Carries one unmistakable "Connect Linear" anchor pointing at
 *     /oauth/start. No JavaScript is required.
 *
 *   - renderAdminPage(): the authenticated single-page configuration shell
 *     served from GET /admin. Loads bootstrap from /api/admin/bootstrap,
 *     drives repository CRUD, route preview, and logout. All mutations
 *     include the application/json body, an X-CSRF-Token header, and
 *     same-origin credentials.
 *
 * The dynamic client code that consumes the API renders every server-supplied
 * value through `textContent` or attribute assignment; no user or API payload
 * is ever interpolated into innerHTML.
 */

import { ADMIN_BODY, ADMIN_SCRIPT } from "./admin-ui/dashboard";
import { LANDING_BODY } from "./admin-ui/landing";
import { PAGE_STYLES } from "./admin-ui/styles";

const DOCTYPE = "<!doctype html>";

/**
 * Assemble a complete HTML document around the provided body and (optional)
 * inline script. The page uses the shared stylesheet and a single body class
 * so the same stylesheet can serve both shells without leaking each other's
 * selectors.
 */
function renderDocument(meta: {
  readonly title: string;
  readonly body: string;
  readonly script?: string;
  readonly extraHead?: string;
  readonly pageClass: string;
}): string {
  const head = [
    '<meta charset="utf-8">',
    '<meta name="viewport" content="width=device-width,initial-scale=1">',
    '<meta name="referrer" content="no-referrer">',
    `<title>${meta.title}</title>`,
    `<style>${PAGE_STYLES}</style>`,
    meta.extraHead ?? "",
  ]
    .filter((part) => part.length > 0)
    .join("\n");

  const script = meta.script ? `<script>${meta.script}</script>` : "";

  return [
    DOCTYPE,
    '<html lang="en">',
    "<head>",
    head,
    "</head>",
    `<body class="${meta.pageClass}">`,
    meta.body,
    script,
    "</body>",
    "</html>",
  ].join("\n");
}

/**
 * Render the unauthenticated landing / onboarding page served from GET /.
 * Contains exactly one anchor pointing at /oauth/start. All copy and styles
 * are inlined; the page requires no JavaScript.
 */
export function renderLandingPage(): string {
  return renderDocument({
    title: "OhMyPi ↔ Linear — Connect",
    body: LANDING_BODY,
    pageClass: "landing-shell",
  });
}

/**
 * Render the authenticated admin SPA served from GET /admin. The page
 * fetches /api/admin/bootstrap to populate installation status and the
 * repository list, then drives CRUD and preview operations against the
 * documented /api/admin/* endpoints.
 */
export function renderAdminPage(): string {
  return renderDocument({
    title: "OhMyPi ↔ Linear — Console",
    body: ADMIN_BODY,
    script: ADMIN_SCRIPT,
    pageClass: "admin-shell",
  });
}
