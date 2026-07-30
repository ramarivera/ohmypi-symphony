import { describe, expect, test } from "bun:test";
import { renderAdminPage, renderLandingPage } from "../src/admin-ui";

/**
 * Focused tests for the admin UI presentation module.
 *
 * Asserts:
 *   - key semantics (page structure, CTAs, forms, copy)
 *   - the documented API endpoints appear in the admin client script
 *   - accessibility landmarks and aria-live status regions exist
 *   - user/API payloads are never interpolated into innerHTML
 *
 * Does NOT touch the backend/store/index/package files and exercises the
 * renderers in isolation.
 */

describe("renderLandingPage", () => {
  test("returns a complete HTML document", () => {
    const html = renderLandingPage();
    const lower = html.toLowerCase();
    expect(html.startsWith("<!doctype html>")).toBe(true);
    expect(lower).toContain('<html lang="en">');
    expect(lower).toContain("</html>");
    expect(lower).toContain("<head>");
    expect(lower).toContain("</head>");
    expect(lower).toContain("<body");
    expect(lower).toContain("</body>");
  });

  test("renders accessible theme and mode controls with a pre-paint bootstrap", () => {
    const html = renderLandingPage();
    expect(html).toContain('data-theme-control="theme"');
    expect(html).toContain('data-theme-control="mode"');
    expect(html).toContain('aria-label="Theme family"');
    expect(html).toContain('aria-label="Color mode"');
    expect(html).toContain("ohmypi-admin-appearance");
    expect(html).toContain("prefers-color-scheme: dark");
    expect(html.indexOf("<script>")).toBeLessThan(html.indexOf("<style>"));
  });

  test("exposes exactly one 'Connect Linear' anchor pointing at /oauth/start", () => {
    const html = renderLandingPage();
    const ctaMatches = html.match(/<a\b[^>]*href="\/oauth\/start"/g) ?? [];
    expect(ctaMatches.length).toBe(1);
    expect(html).toMatch(
      /<a\b[^>]*href="\/oauth\/start"[^>]*>\s*Connect Linear/,
    );
  });

  test("does not put the start action on a button or JS handler", () => {
    const html = renderLandingPage();
    expect(html).not.toMatch(/<button\b[^>]*>\s*Connect Linear/);
    // The page must NOT need JS to navigate — no inline click handlers.
    expect(html).not.toMatch(/onclick="[^"]*oauth/);
  });

  test("explains the connection flow with the four scopes used by the gateway", () => {
    const html = renderLandingPage();
    const lower = html.toLowerCase();
    expect(lower).toContain("oauth");
    expect(lower).toContain("scope");
    expect(lower).toContain("read");
    expect(lower).toContain("write");
    expect(lower).toContain("app:assignable");
    expect(lower).toContain("app:mentionable");
  });

  test("includes trust copy stating credentials are encrypted on this server", () => {
    const html = renderLandingPage();
    const lower = html.toLowerCase();
    expect(lower).toContain("encrypt");
    expect(lower).toContain("at rest");
    expect(lower).toContain("this server");
  });

  test("links to the admin console in the footer for already-configured operators", () => {
    const html = renderLandingPage();
    expect(html).toMatch(/<a\b[^>]*href="\/admin"/);
  });

  test("emits inline styles only — no remote stylesheets, scripts, or fonts", () => {
    const html = renderLandingPage();
    const lower = html.toLowerCase();
    expect(lower).not.toMatch(/<link\b[^>]+href=["']https?:/);
    expect(lower).not.toMatch(/<script\b[^>]+src=/);
    expect(lower).not.toMatch(/@import\s+url\(["']?https?:/);
    expect(html).not.toMatch(
      /fonts\.googleapis|fonts\.gstatic|typekit|use\.typekit/,
    );
    expect(html).not.toMatch(
      /<link\b[^>]+rel=["']?preconnect["']?[^>]+href=["']?https?:/,
    );
  });

  test("honors prefers-reduced-motion and renders semantic landmarks", () => {
    const html = renderLandingPage();
    expect(html).toMatch(
      /@media\s*\(\s*prefers-reduced-motion\s*:\s*reduce\s*\)/,
    );
    expect(html).toMatch(/<main\b/);
    expect(html).toMatch(/<footer\b[^>]*role="contentinfo"/);
  });

  test("sets the page language and viewport", () => {
    const html = renderLandingPage();
    expect(html).toMatch(/<html lang="en">/);
    expect(html).toMatch(/<meta name="viewport"/);
  });
});

describe("renderAdminPage", () => {
  test("returns a complete HTML document with inline CSS+JS, no remote assets", () => {
    const html = renderAdminPage();
    const lower = html.toLowerCase();
    expect(html.startsWith("<!doctype html>")).toBe(true);
    expect(lower).toContain("</html>");
    expect(lower).toContain("<style>");
    expect(lower).toContain("<script>");
    expect(lower).not.toMatch(/<link\b[^>]+href=["']https?:/);
    expect(html).not.toMatch(/fonts\.googleapis|fonts\.gstatic|typekit/);
  });

  test("exposes the same orthogonal appearance controls on the dashboard", () => {
    const html = renderAdminPage();
    expect(html).toContain('data-theme-control="theme"');
    expect(html).toContain('data-theme-control="mode"');
    expect(html).toContain('value="editorial"');
    expect(html).toContain('value="linear"');
    expect(html).toContain('value="light"');
    expect(html).toContain('value="dark"');
    expect(html).toContain('value="system"');
    expect(html).toContain("localStorage.setItem(STORAGE_KEY");
    expect(html).toContain('addEventListener("change", handleSystemChange)');
  });

  test("declares every documented /api/admin/* endpoint in the client script", () => {
    const html = renderAdminPage();
    expect(html).toContain("/api/admin/bootstrap");
    expect(html).toContain("/api/admin/repositories");
    expect(html).toContain("/api/admin/preview");
    expect(html).toContain("/api/admin/logout");

    // compose a detail URL the right way
    expect(html).toMatch(
      /REPOSITORIES_BASE\s*\+\s*"\/"\s*\+\s*encodeURIComponent/,
    );

    // verb coverage for mutations: POST (create or logout), PUT (update),
    // DELETE (delete) all used against the repository detail URL.
    expect(html).toMatch(/method:\s*"POST"/);
    expect(html).toMatch(/isUpdate\s*\?\s*"PUT"\s*:\s*"POST"/);
    expect(html).toMatch(/method:\s*"DELETE"/);
    expect(html).toMatch(/method:\s*"GET"/);
  });

  test("sends application/json, X-CSRF-Token, and same-origin credentials on every mutation", () => {
    const html = renderAdminPage();
    expect(html).toContain("X-CSRF-Token");
    expect(html).toContain("application/json");
    expect(html).toMatch(/credentials["'\s:=]*same-origin/);
  });

  test("treats a 401 from the API as a return-to-root signal", () => {
    const html = renderAdminPage();
    // The page must check response.status === 401 and bounce to "/".
    expect(html).toMatch(/response\.status\s*===\s*401/);
    // Bounce must be to root, not some other path.
    expect(html).toMatch(
      /status\s*===\s*401[\s\S]{0,400}location\.assign\(\s*"\/"\s*\)/,
    );
  });

  test("never interpolates API payloads or user data into innerHTML", () => {
    const html = renderAdminPage();
    // Disallow innerHTML = `... ${something} ...`
    const badInterpolation = /innerHTML\s*=\s*`[^`]*\$\{/;
    expect(html).not.toMatch(badInterpolation);
    // Disallow innerHTML = "abc" + xyz where xyz holds user data
    const badConcat = /innerHTML\s*=\s*['"][^'"]*['"]\s*\+\s*(?!document)/;
    expect(html).not.toMatch(badConcat);
    // The script must do at least one textContent assignment for user-derived
    // values, proving the safe-by-default path is exercised.
    expect(html).toMatch(/\.textContent\s*=/);
  });

  test("renders semantic landmarks (header/nav/aside/main) and aria-live status regions", () => {
    const html = renderAdminPage();
    expect(html).toMatch(/<header\b/);
    expect(html).toMatch(/<main\b/);
    expect(html).toMatch(/<aside\b/);
    expect(html).toMatch(/<section\b[^>]*aria-labelledby=/);
    expect(html).toMatch(/aria-live="polite"/);
    expect(html).toMatch(/role="status"/);
    expect(html).toMatch(/role="alert"/);
  });

  test("exposes the routing rail segments in the documented precedence order", () => {
    const html = renderAdminPage();
    const segments = [
      "explicit",
      "issue-label",
      "project-label",
      "project",
      "team",
      "default",
    ];
    let cursor = 0;
    for (const segment of segments) {
      const idx = html.indexOf(`data-rail-segment="${segment}"`, cursor);
      expect(idx).toBeGreaterThanOrEqual(0);
      cursor = idx + 1;
    }
    const headerHint = html.toLowerCase().indexOf("explicit → issue-label");
    expect(headerHint).toBeGreaterThanOrEqual(0);
  });

  test("renders the repository form fields, default toggle, confirm dialog, and logout control", () => {
    const html = renderAdminPage();
    expect(html).toMatch(/<form\b[^>]*id="repo-form"/);
    expect(html).toMatch(/name="id"[^>]*type="text"[^>]*required/);
    expect(html).toMatch(/name="url"/);
    expect(html).toMatch(/name="ref"/);
    expect(html).toMatch(/name="teamIds"/);
    expect(html).toMatch(/name="projectIds"/);
    expect(html).toMatch(/name="labels"/);
    expect(html).toMatch(/<input\b[^>]*name="isDefault"[^>]*type="checkbox"/);
    expect(html).toMatch(/<form\b[^>]*id="preview-form"/);
    expect(html).toMatch(/<div\b[^>]*id="confirm-modal"[^>]*role="dialog"/);
    expect(html).toMatch(/<button\b[^>]*id="logout-btn"/);
    expect(html).toMatch(/<button\b[^>]*id="new-repo-btn"/);
  });

  test("maps the preview API kind and repository fields into the result UI", () => {
    const html = renderAdminPage();
    expect(html).toMatch(/result\.kind\s*===\s*"match"\s*\?\s*"matched"/);
    expect(html).toMatch(/result\.repository\s*\|\|\s*result\.matched/);
  });

  test("uses POST and same-origin credentials on logout so the session is destroyed", () => {
    const html = renderAdminPage();
    expect(html).toMatch(
      /fetchJSON\(\s*LOGOUT_URL\s*,\s*\{\s*method:\s*"POST"/,
    );
    expect(html).toMatch(/credentials\s*=\s*"same-origin"/);
  });

  test("respects prefers-reduced-motion and avoids decorative bounce easing", () => {
    const html = renderAdminPage();
    expect(html).toMatch(/prefers-reduced-motion/);
    // Avoid bouncy easings flagged as dated in the design direction.
    expect(html).not.toMatch(/cubic-bezier\([^)]*1\.7[^)]*\)/);
    expect(html).not.toMatch(/ease-elastic|easeInBack|easeOutBack/);
  });

  test("exposes loading, empty, and error states for the repository list", () => {
    const html = renderAdminPage();
    expect(html).toMatch(/setAriaBusy\(\s*["']repos-list["']/);
    expect(html).toMatch(/aria-busy/);
    // Empty-state message for the repository table.
    expect(html).toContain("No repositories yet.");
    // Error card for bootstrap failures.
    expect(html).toMatch(/className\s*=\s*"error-card"/);
    expect(html).toMatch(/Failed to load/);
  });

  test("confirms before deleting and only runs the delete on acceptance", () => {
    const html = renderAdminPage();
    expect(html).toMatch(/openConfirm\(\s*\{[^}]*title:\s*"Delete repository/);
    expect(html).toMatch(/deleteRepository\(\s*id\s*\)/);
    expect(html).toMatch(
      /yesBtn\.addEventListener\(\s*["']click["'],\s*function/,
    );
    expect(html).toMatch(
      /noBtn\.addEventListener\(\s*["']click["'],\s*function/,
    );
    // The delete must require explicit confirmation — not a single click path.
    expect(html).not.toMatch(
      /dataset\.action === "delete"[\s\S]*?\.then\(\s*deleteRepository/,
    );
  });

  test("does not depend on any external resources (CSP / network hardening)", () => {
    const html = renderAdminPage();
    const lower = html.toLowerCase();
    expect(lower).not.toMatch(/https?:\/\/[^"'\s)]+/);
    expect(lower).not.toMatch(/src=["']https?:/);
    expect(html).not.toMatch(/cdn\./);
    // Same-origin credentials are explicit, never wildcard.
    expect(html).not.toMatch(/credentials:\s*"include"/);
  });
});
