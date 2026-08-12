/**
 * Static HTML body and inline client script for the authenticated admin SPA
 * served from GET /admin.
 *
 * The body markup exposes the documented landmarks and form structures the
 * client script hooks into (installation aside, routing rail, repositories
 * table, repository form, preview form, confirm dialog, toast). The client
 * script performs the bootstrap fetch, renders dynamic state into the DOM
 * via `textContent` / `setAttribute` and never interpolates API payloads
 * into innerHTML. All mutations use X-CSRF-Token, same-origin credentials,
 * and an application/json body. A 401 on any response bounces back to "/".
 */

import { THEME_CONTROLS } from "./theme";
export const ADMIN_BODY = `
<header class="admin-header" role="banner">
  <div class="title"><strong>OhMyPi</strong> &nbsp;↔&nbsp; Linear Control Plane</div>
  <div class="header-actions">
    ${THEME_CONTROLS}
    <span class="status-pill" id="header-status" data-state="loading" aria-live="polite">loading…</span>
    <button id="logout-btn" type="button" class="btn" hidden>Log out</button>
  </div>
</header>

<div class="admin-body">

  <aside class="installation" role="complementary" aria-labelledby="inst-heading">
    <h2 id="inst-heading">Installation</h2>
    <div id="installation-card">
      <p class="loading">Loading installation…</p>
    </div>
  </aside>

  <main class="console" role="main">

    <section class="rail-block" aria-labelledby="rail-heading">
      <div class="rail-header">
        <h2 id="rail-heading">Routing rail</h2>
        <span class="hint">explicit → issue-label → project-label → project → team → default</span>
      </div>
      <div class="rail">
        <div class="rail-track" id="rail-track">
          <div class="rail-segment" data-state="active" data-rail-segment="explicit">
            <div class="seg-title">01 · Explicit</div>
            <div class="seg-detail" data-rail-detail>—</div>
            <div class="seg-id" data-rail-id></div>
          </div>
          <div class="rail-segment" data-state="active" data-rail-segment="issue-label">
            <div class="seg-title">02 · Issue label</div>
            <div class="seg-detail" data-rail-detail>—</div>
            <div class="seg-id" data-rail-id></div>
          </div>
          <div class="rail-segment" data-state="active" data-rail-segment="project-label">
            <div class="seg-title">03 · Project label</div>
            <div class="seg-detail" data-rail-detail>—</div>
            <div class="seg-id" data-rail-id></div>
          </div>
          <div class="rail-segment" data-state="active" data-rail-segment="project">
            <div class="seg-title">04 · Project</div>
            <div class="seg-detail" data-rail-detail>—</div>
            <div class="seg-id" data-rail-id></div>
          </div>
          <div class="rail-segment" data-state="active" data-rail-segment="team">
            <div class="seg-title">05 · Team</div>
            <div class="seg-detail" data-rail-detail>—</div>
            <div class="seg-id" data-rail-id></div>
          </div>
          <div class="rail-segment" data-state="active" data-rail-segment="default">
            <div class="seg-title">06 · Default</div>
            <div class="seg-detail" data-rail-detail>—</div>
            <div class="seg-id" data-rail-id></div>
          </div>
        </div>
      </div>
    </section>

    <section class="panel" aria-labelledby="repos-heading">
      <div class="panel-header">
        <h2 id="repos-heading">Repositories</h2>
        <button id="new-repo-btn" type="button" class="btn">+ Add repository</button>
      </div>
      <div class="panel-body">
        <div id="repos-status" role="status" aria-live="polite"></div>
        <div id="repos-list" aria-busy="true"></div>
      </div>
    </section>

    <section class="panel" aria-labelledby="form-heading" id="repo-form-panel" hidden>
      <div class="panel-header">
        <h2 id="form-heading">Repository</h2>
        <button type="button" class="btn" id="form-cancel-btn" aria-label="Cancel">Cancel</button>
      </div>
      <div class="panel-body">
        <form id="repo-form" class="repo-form" novalidate>
          <div class="field">
            <label for="repo-id">Repository ID</label>
            <input id="repo-id" name="id" type="text" required autocomplete="off"
              placeholder="backend-api">
          </div>
          <div class="field">
            <label for="repo-url">Repository URL</label>
            <input id="repo-url" name="url" type="url" required autocomplete="off"
              placeholder="git@github.com:org/repo.git">
          </div>
          <div class="field">
            <label for="repo-ref">Default ref</label>
            <input id="repo-ref" name="ref" type="text" required autocomplete="off"
              placeholder="main">
          </div>
          <div class="field">
            <label for="repo-teams">Team IDs (comma-separated)</label>
            <input id="repo-teams" name="teamIds" type="text" autocomplete="off"
              placeholder="team-1, team-2">
          </div>
          <div class="field">
            <label for="repo-projects">Project IDs (comma-separated)</label>
            <input id="repo-projects" name="projectIds" type="text" autocomplete="off"
              placeholder="proj-a">
          </div>
          <div class="field full">
            <label for="repo-labels">Labels (comma-separated)</label>
            <input id="repo-labels" name="labels" type="text" autocomplete="off"
              placeholder="backend, urgent">
          </div>
          <div class="field full">
            <label for="repo-nix-packages">Nix packages (comma-separated)</label>
            <input id="repo-nix-packages" name="nixPackages" type="text" autocomplete="off"
              placeholder="git, nodejs_22">
            <span class="hint">Nixpkgs attribute paths only; packages are installed for this repository's agent sessions.</span>
          </div>
          <div class="field full">
            <div class="field-row">
              <label class="checkbox" for="repo-default">
                <input id="repo-default" name="isDefault" type="checkbox">
                Default repository (catch-all when nothing else matches)
              </label>
            </div>
          </div>
          <div id="repo-form-error" class="error" role="alert" hidden></div>
          <div class="form-actions">
            <button type="button" class="btn" id="repo-form-cancel">Cancel</button>
            <button type="submit" class="btn btn-primary" id="repo-form-submit">Save repository</button>
          </div>
        </form>
      </div>
    </section>

    <section class="panel" aria-labelledby="mcp-heading">
      <div class="panel-header">
        <h2 id="mcp-heading">MCP servers</h2>
        <button id="new-mcp-btn" type="button" class="btn">+ Add MCP server</button>
      </div>
      <div class="panel-body">
        <div id="mcp-status" role="status" aria-live="polite"></div>
        <div id="mcp-list" aria-busy="true"></div>
        <form id="mcp-form" class="repo-form" novalidate hidden>
          <div class="field"><label for="mcp-id">Server ID</label><input id="mcp-id" name="id" required autocomplete="off" placeholder="github"></div>
          <div class="field"><label for="mcp-name">MCP name</label><input id="mcp-name" name="name" required autocomplete="off" placeholder="github"></div>
          <div class="field"><label for="mcp-transport">Transport</label><select id="mcp-transport" name="transport"><option value="stdio">stdio</option><option value="http">http</option><option value="sse">sse</option></select></div>
          <div class="field"><label for="mcp-command">Command</label><input id="mcp-command" name="command" autocomplete="off" placeholder="npx"></div>
          <div class="field"><label for="mcp-url">URL</label><input id="mcp-url" name="url" type="url" autocomplete="off" placeholder="https://mcp.example.com"></div>
          <div class="field"><label for="mcp-args">Arguments (JSON array or comma-separated)</label><input id="mcp-args" name="args" autocomplete="off"></div>
          <div class="field"><label for="mcp-env">Environment (KEY=value per line)</label><textarea id="mcp-env" name="env" rows="3" autocomplete="off"></textarea><span class="hint">Secret values are write-only; existing values show as •••.</span></div>
          <div class="field full" id="mcp-headers-field" hidden>
            <label>Headers</label>
            <div id="mcp-headers-editor" class="key-value-editor"></div>
            <button type="button" class="btn" id="mcp-add-header">+ Add header</button>
            <span class="hint">Secret values are write-only; existing values show as •••.</span>
          </div>
          <div class="field"><label for="mcp-repository">Repository ID (blank = installation-wide)</label><input id="mcp-repository" name="repositoryId" autocomplete="off"></div>
          <div class="field"><label class="checkbox"><input id="mcp-enabled" name="enabled" type="checkbox" checked> Enabled</label></div>
          <div id="mcp-form-error" class="error" role="alert" hidden></div>
          <div class="form-actions"><button type="button" class="btn" id="mcp-cancel">Cancel</button><button type="submit" class="btn btn-primary" id="mcp-submit">Save MCP server</button></div>
        </form>
      </div>
    </section>

    <section class="panel" aria-labelledby="nix-cache-heading">
      <div class="panel-header">
        <h2 id="nix-cache-heading">Nix package cache</h2>
      </div>
      <div class="panel-body">
        <div id="nix-cache-status" role="status" aria-live="polite"></div>
        <div id="nix-cache-list" aria-busy="true"></div>
      </div>
    </section>

    <section class="panel" aria-labelledby="prompt-templates-heading">
      <div class="panel-header">
        <h2 id="prompt-templates-heading">Worker prompt templates</h2>
        <span class="hint">Changes apply to new Linear agent sessions.</span>
      </div>
      <div class="panel-body">
        <div id="prompt-templates-status" role="status" aria-live="polite"></div>
        <div id="prompt-template-editors">
          <div class="field full"><label for="prompt-template-created">Created input</label><textarea id="prompt-template-created" rows="8" data-prompt-kind="created"></textarea></div>
          <div class="field full"><label for="prompt-template-prompted">Prompted input</label><textarea id="prompt-template-prompted" rows="4" data-prompt-kind="prompted"></textarea></div>
          <div class="field full"><label for="prompt-template-contract">Worker contract</label><textarea id="prompt-template-contract" rows="10" data-prompt-kind="contract"></textarea></div>
        </div>
        <div class="hint">Created placeholders: <code>{{userRequest}}</code> <code>{{issueContext}}</code> <code>{{threadComment}}</code> <code>{{previousComments}}</code> <code>{{guidance}}</code>. Unknown placeholders remain literal.</div>
        <div class="field full"><label for="prompt-template-preview">Live preview (sample payload)</label><pre id="prompt-template-preview" aria-live="polite"></pre></div>
        <div class="form-actions"><button type="button" class="btn btn-primary" id="prompt-templates-save">Save prompt templates</button></div>
      </div>
    </section>

    <section class="panel" aria-labelledby="preview-heading">
      <div class="panel-header">
        <h2 id="preview-heading">Route preview</h2>
        <span class="hint" style="font-family: var(--mono); font-size: 0.72rem; color: var(--ink-mute);">
          Simulate which repository a Linear event would resolve to.
        </span>
      </div>
      <div class="panel-body">
        <form id="preview-form" class="preview-form" novalidate>
          <div class="field">
            <label for="preview-repo">Explicit repository ID</label>
            <input id="preview-repo" name="repositoryId" type="text" autocomplete="off">
          </div>
          <div class="field">
            <label for="preview-team">Team ID</label>
            <input id="preview-team" name="teamId" type="text" autocomplete="off">
          </div>
          <div class="field">
            <label for="preview-project">Project ID</label>
            <input id="preview-project" name="projectId" type="text" autocomplete="off">
          </div>
          <div class="field">
            <label for="preview-issue-labels">Issue labels (comma-separated)</label>
            <input id="preview-issue-labels" name="issueLabels" type="text" autocomplete="off">
          </div>
          <div class="field">
            <label for="preview-project-labels">Project labels (comma-separated)</label>
            <input id="preview-project-labels" name="projectLabels" type="text" autocomplete="off">
          </div>
          <div class="field field-full">
            <button type="submit" class="btn">Resolve preview</button>
          </div>
        </form>
        <div id="preview-result" class="preview-result" role="status" aria-live="polite" data-state="idle"></div>
      </div>
    </section>

  </main>
</div>

<div id="toast" class="toast" role="status" aria-live="polite"></div>

<div id="confirm-modal" hidden role="dialog" aria-modal="true"
  aria-labelledby="confirm-title" aria-describedby="confirm-body">
  <div class="confirm-card">
    <h3 id="confirm-title">Confirm</h3>
    <p id="confirm-body">Are you sure?</p>
    <div class="confirm-actions">
      <button type="button" class="btn" id="confirm-no">Cancel</button>
      <button type="button" class="btn btn-danger" id="confirm-yes">Confirm</button>
    </div>
  </div>
</div>
`;

export const ADMIN_SCRIPT = `
(function () {
  "use strict";

  var BOOTSTRAP_URL = "/api/admin/bootstrap";
  var REPOSITORIES_BASE = "/api/admin/repositories";
  var REPOSITORY_DETAIL = function (id) { return REPOSITORIES_BASE + "/" + encodeURIComponent(id); };
  var PREVIEW_URL = "/api/admin/preview";
  var NIX_CACHE_URL = "/api/admin/nix-cache";
  var MCP_BASE = "/api/admin/mcp-servers";
  var MCP_DETAIL = function (id) { return MCP_BASE + "/" + encodeURIComponent(id); };
  var LOGOUT_URL = "/api/admin/logout";
  var PROMPT_TEMPLATES_URL = "/api/admin/prompt-templates";

  var state = {
    csrfToken: "",
    installation: null,
    repositories: [],
    mcpServers: [],
    editingMcp: null,
    nixCache: [],
    editing: null,
    pendingDelete: null,
    promptTemplates: {},
    pendingConfirm: null,
  };

  function el(id) { return document.getElementById(id); }

  function setStatus(stateText, label) {
    var pill = el("header-status");
    if (!pill) return;
    pill.setAttribute("data-state", stateText);
    pill.textContent = label;
  }

  function setAriaBusy(rootId, busy) {
    var root = el(rootId);
    if (!root) return;
    root.setAttribute("aria-busy", busy ? "true" : "false");
  }

  function showToast(message, tone) {
    var toast = el("toast");
    if (!toast) return;
    toast.textContent = message;
    if (tone) toast.setAttribute("data-tone", tone); else toast.removeAttribute("data-tone");
    toast.setAttribute("data-show", "true");
    window.setTimeout(function () {
      toast.setAttribute("data-show", "false");
    }, TOAST_DURATION_MS);
  }

  function announceStatus(regionId, message) {
    var node = el(regionId);
    if (!node) return;
    node.textContent = message;
  }

  function csrfHeaders() {
    return {
      "content-type": "application/json",
      "X-CSRF-Token": state.csrfToken,
    };
  }

  function parseErrorBody(bodyText, fallback) {
    if (!bodyText) return fallback;
    try {
      var data = JSON.parse(bodyText);
      if (data && typeof data === "object") {
        if (typeof data.error === "string") return data.error;
        if (typeof data.message === "string") return data.message;
      }
    } catch (err) { /* swallow */ }
    return bodyText || fallback;
  }

  async function handleUnauthorized(response) {
    if (response && response.status === 401) {
      window.location.assign("/");
      return true;
    }
    return false;
  }

  async function fetchJSON(url, init) {
    init = init || {};
    init.credentials = "same-origin";
    if (init.body && typeof init.body !== "string") {
      init.body = JSON.stringify(init.body);
    }
    if (init.body && !init.headers) init.headers = {};
    if (init.body) {
      init.headers = Object.assign(
        { "content-type": "application/json" },
        csrfHeaders(),
        init.headers || {}
      );
    } else if (!init.method || init.method === "GET") {
      init.headers = Object.assign({ accept: "application/json" }, init.headers || {});
    }
    var response = await fetch(url, init);
    if (await handleUnauthorized(response)) {
      return { redirecting: true, response: response };
    }
    var text = await response.text();
    var data = null;
    if (text) {
      try { data = JSON.parse(text); } catch (err) { data = null; }
    }
    if (!response.ok) {
      var message = parseErrorBody(text, "Request failed (" + response.status + ")");
      var error = new Error(message);
      error.status = response.status;
      error.data = data;
      throw error;
    }
    return { ok: true, status: response.status, data: data, raw: text };
  }

  // ---- rendering -----------------------------------------------------------

  function renderInstallation(installation) {
    var card = el("installation-card");
    if (!card) return;
    card.textContent = "";

    var rows = [
      ["Organization", installation && installation.organizationId ? installation.organizationId : "—"],
      ["App user", installation && installation.appUserId ? installation.appUserId : "—"],
      ["Scopes", installation && Array.isArray(installation.scopes) && installation.scopes.length
        ? installation.scopes.join(", ")
        : "—"],
      ["Accessible teams", installation && installation.canAccessAllPublicTeams
        ? "All public teams"
        : (Array.isArray(installation.accessibleTeamIds)
            ? installation.accessibleTeamIds.length + " teams"
            : "Restricted — see Linear")],
      ["Status", installation && installation.revokedAt
        ? "Revoked"
        : "Active"],
    ];

    rows.forEach(function (row) {
      var wrap = document.createElement("div");
      wrap.className = "install-row";
      var label = document.createElement("div");
      label.className = "label";
      label.textContent = row[0];
      var value = document.createElement("div");
      value.className = "value";
      value.textContent = row[1];
      if (row[0] === "Status") {
        value.setAttribute("data-state", row[1] === "Revoked" ? "danger" : "good");
      }
      wrap.appendChild(label);
      wrap.appendChild(value);
      card.appendChild(wrap);
    });
  }

  function computeAccessibilityBadge(installation) {
    if (!installation) return { state: "warn", label: "no installation" };
    if (installation.revokedAt) return { state: "danger", label: "revoked" };
    if (installation.canAccessAllPublicTeams) return { state: "ok", label: "all public teams" };
    if (Array.isArray(installation.accessibleTeamIds) && installation.accessibleTeamIds.length > 0) {
      return { state: "ok", label: installation.accessibleTeamIds.length + " teams" };
    }
    return { state: "warn", label: "restricted" };
  }

  function renderRepositories(repositories) {
    var list = el("repos-list");
    var status = el("repos-status");
    list.textContent = "";
    status.textContent = "";
    if (!Array.isArray(repositories) || repositories.length === 0) {
      list.removeAttribute("aria-busy");
      setAriaBusy("repos-list", false);
      status.textContent = "No repositories yet.";
      var empty = document.createElement("div");
      empty.className = "empty";
      empty.textContent =
        "Add the first repository the gateway is allowed to route issues into.";
      list.appendChild(empty);
      return;
    }
    var table = document.createElement("table");
    table.className = "repos";
    var thead = document.createElement("thead");
    var headRow = document.createElement("tr");
    ["URL", "Default ref", "Teams", "Projects", "Labels", "Nix packages", "Default", ""].forEach(function (text) {
      var th = document.createElement("th");
      th.scope = "col";
      th.textContent = text;
      headRow.appendChild(th);
    });
    thead.appendChild(headRow);
    table.appendChild(thead);

    var tbody = document.createElement("tbody");
    repositories.forEach(function (repo) {
      var tr = document.createElement("tr");
      tr.dataset.repoId = typeof repo.id === "string" ? repo.id : "";

      var urlCell = document.createElement("td");
      urlCell.appendChild(buildCopyCell(repo.url || "—"));
      tr.appendChild(urlCell);

      var refCell = document.createElement("td");
      var refCode = document.createElement("code");
      refCode.textContent = repo.ref || "—";
      refCell.appendChild(refCode);
      tr.appendChild(refCell);

      var teamsCell = document.createElement("td");
      teamsCell.appendChild(buildPillList(repo.teamIds, "no team scoping"));
      tr.appendChild(teamsCell);

      var projectsCell = document.createElement("td");
      projectsCell.appendChild(buildPillList(repo.projectIds, "no project scoping"));
      tr.appendChild(projectsCell);

      var labelsCell = document.createElement("td");
      labelsCell.appendChild(buildPillList(repo.labels, "no label matching"));
      tr.appendChild(labelsCell);

      var nixPackagesCell = document.createElement("td");
      nixPackagesCell.appendChild(buildPillList(repo.nixPackages, "no extra packages"));
      tr.appendChild(nixPackagesCell);

      var defaultCell = document.createElement("td");
      defaultCell.className = "default-cell";
      var badge = document.createElement("span");
      if (repo.isDefault) {
        badge.textContent = "✓ default";
        badge.setAttribute("data-pill", "good");
        badge.className = "pill good";
      } else {
        badge.textContent = "scoped";
        badge.className = "pill";
      }
      defaultCell.appendChild(badge);
      tr.appendChild(defaultCell);

      var actionsCell = document.createElement("td");
      actionsCell.className = "actions";
      var editBtn = document.createElement("button");
      editBtn.type = "button";
      editBtn.dataset.action = "edit";
      editBtn.dataset.repoId = typeof repo.id === "string" ? repo.id : "";
      editBtn.textContent = "Edit";
      actionsCell.appendChild(editBtn);

      var deleteBtn = document.createElement("button");
      deleteBtn.type = "button";
      deleteBtn.className = "btn-danger";
      deleteBtn.dataset.action = "delete";
      deleteBtn.dataset.repoId = typeof repo.id === "string" ? repo.id : "";
      deleteBtn.textContent = "Delete";
      actionsCell.appendChild(deleteBtn);

      tr.appendChild(actionsCell);
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    list.appendChild(table);
    setAriaBusy("repos-list", false);
    status.textContent = repositories.length + " repositories configured.";
  }

  function renderMcpServers(servers) {
    var list = el("mcp-list");
    var status = el("mcp-status");
    if (!list || !status) return;
    list.textContent = "";
    if (!Array.isArray(servers) || servers.length === 0) {
      status.textContent = "No MCP servers configured.";
      list.textContent = "Add an installation-wide or repository-scoped server.";
      setAriaBusy("mcp-list", false);
      return;
    }
    status.textContent = servers.length + " MCP servers configured.";
    var table = document.createElement("table");
    table.className = "repos";
    var head = document.createElement("thead");
    var row = document.createElement("tr");
    ["Name", "Transport", "Scope", "Environment", "Status", ""].forEach(function (label) {
      var th = document.createElement("th"); th.scope = "col"; th.textContent = label; row.appendChild(th);
    });
    head.appendChild(row); table.appendChild(head);
    var body = document.createElement("tbody");
    servers.forEach(function (server) {
      var tr = document.createElement("tr");
      var name = document.createElement("td"); name.textContent = server.name || server.id || "—"; tr.appendChild(name);
      var transport = document.createElement("td"); transport.textContent = server.transport || "—"; tr.appendChild(transport);
      var scope = document.createElement("td"); scope.textContent = server.repositoryId || "installation-wide"; tr.appendChild(scope);
      var env = document.createElement("td"); env.textContent = server.env && Object.keys(server.env).length ? Object.keys(server.env).map(function (key) { return key + "=•••"; }).join(", ") : "—"; tr.appendChild(env);
      var enabled = document.createElement("td"); enabled.textContent = server.enabled ? "enabled" : "disabled"; tr.appendChild(enabled);
      var actions = document.createElement("td"); actions.className = "actions";
      var edit = document.createElement("button"); edit.type = "button"; edit.dataset.action = "edit-mcp"; edit.dataset.mcpId = server.id || ""; edit.textContent = "Edit"; actions.appendChild(edit);
      var toggle = document.createElement("button"); toggle.type = "button"; toggle.dataset.action = "toggle-mcp"; toggle.dataset.mcpId = server.id || ""; toggle.textContent = server.enabled ? "Disable" : "Enable"; actions.appendChild(toggle);
      var remove = document.createElement("button"); remove.type = "button"; remove.className = "btn-danger"; remove.dataset.action = "delete-mcp"; remove.dataset.mcpId = server.id || ""; remove.textContent = "Delete"; actions.appendChild(remove);
      tr.appendChild(actions); body.appendChild(tr);
    });
    table.appendChild(body); list.appendChild(table); setAriaBusy("mcp-list", false);
  }
  function addMcpHeaderRow(key, value) {
    var editor = el("mcp-headers-editor"); if (!editor) return;
    var row = document.createElement("div"); row.className = "key-value-row"; row.setAttribute("data-header-row", "true");
    var keyInput = document.createElement("input"); keyInput.type = "text"; keyInput.placeholder = "Header name"; keyInput.autocomplete = "off"; keyInput.value = key || "";
    var valueInput = document.createElement("input"); valueInput.type = "text"; valueInput.placeholder = "Value"; valueInput.autocomplete = "off"; valueInput.value = value || "";
    var remove = document.createElement("button"); remove.type = "button"; remove.className = "btn btn-danger"; remove.textContent = "Remove"; remove.addEventListener("click", function () { row.remove(); });
    row.appendChild(keyInput); row.appendChild(valueInput); row.appendChild(remove); editor.appendChild(row);
  }
  function setMcpHeaderRows(headers) {
    var editor = el("mcp-headers-editor"); if (!editor) return;
    editor.textContent = "";
    Object.keys(headers || {}).forEach(function (key) { addMcpHeaderRow(key, headers[key]); });
  }
  function readMcpHeaderRows() {
    var headers = {};
    var editor = el("mcp-headers-editor"); if (!editor) return headers;
    editor.querySelectorAll("[data-header-row]").forEach(function (row) {
      var inputs = row.querySelectorAll("input"); if (inputs.length < 2) return;
      var key = inputs[0].value.trim(); if (!key) return;
      headers[key] = inputs[1].value;
    });
    return headers;
  }
  function syncMcpHeaderVisibility() {
    var transport = el("mcp-transport"); var field = el("mcp-headers-field");
    if (field) field.hidden = !transport || (transport.value !== "http" && transport.value !== "sse");
  }
  function openMcpForm(server) {
    state.editingMcp = server || null;
    var form = el("mcp-form"); if (!form) return;
    var fields = { id: server && server.id || "", name: server && server.name || "", transport: server && server.transport || "stdio", command: server && server.command || "", url: server && server.url || "", args: server && Array.isArray(server.args) ? JSON.stringify(server.args) : "", env: server && server.env ? Object.keys(server.env).map(function (key) { return key + "=" + server.env[key]; }).join("\\n") : "", repositoryId: server && server.repositoryId || "", enabled: !server || server.enabled !== false };
    Object.keys(fields).forEach(function (key) { var field = form.elements.namedItem(key); if (!field) return; if (field.type === "checkbox") field.checked = fields[key]; else field.value = fields[key]; });
    setMcpHeaderRows(server && server.headers ? server.headers : {});
    syncMcpHeaderVisibility();
    var idField = el("mcp-id"); if (idField) idField.readOnly = !!(server && server.id);
    var error = el("mcp-form-error"); if (error) { error.textContent = ""; error.hidden = true; }
    form.hidden = false; var focus = server ? el("mcp-name") : idField; if (focus) focus.focus();
  }
  function closeMcpForm() { var form = el("mcp-form"); if (form) form.hidden = true; state.editingMcp = null; }
  function serializeMcpForm() {
    var form = el("mcp-form"), fields = form.elements, env = {};
    (fields.namedItem("env").value || "").split("\\n").forEach(function (line) { var index = line.indexOf("="); if (index <= 0) return; env[line.slice(0, index).trim()] = line.slice(index + 1); });
    return { id: fields.namedItem("id").value.trim(), name: fields.namedItem("name").value.trim(), transport: fields.namedItem("transport").value, command: fields.namedItem("command").value.trim() || null, url: fields.namedItem("url").value.trim() || null, args: parseMcpArgs(fields.namedItem("args").value), env: env, headers: readMcpHeaderRows(), repositoryId: fields.namedItem("repositoryId").value.trim() || null, enabled: !!fields.namedItem("enabled").checked };
  }
  async function submitMcpForm(event) {
    event.preventDefault(); var payload = serializeMcpForm();
    if (!payload.id || !payload.name) { var invalid = el("mcp-form-error"); invalid.textContent = "Server ID and name are required."; invalid.hidden = false; return; }
    var button = el("mcp-submit"); if (button) button.disabled = true;
    try { var update = !!(state.editingMcp && state.editingMcp.id); var result = await fetchJSON(update ? MCP_DETAIL(state.editingMcp.id) : MCP_BASE, { method: update ? "PUT" : "POST", body: payload }); if (result && result.redirecting) return; closeMcpForm(); showToast(update ? "MCP server updated." : "MCP server added.", "ok"); await loadBootstrap({ announce: false }); }
    catch (err) { var box = el("mcp-form-error"); box.textContent = err && err.message ? err.message : "Could not save MCP server."; box.hidden = false; }
    finally { if (button) button.disabled = false; }
  }
  async function deleteMcpServer(id) { var result = await fetchJSON(MCP_DETAIL(id), { method: "DELETE", body: {} }); if (result && result.redirecting) return; showToast("MCP server removed.", "ok"); await loadBootstrap({ announce: false }); }
  async function toggleMcpServer(server) { var result = await fetchJSON(MCP_DETAIL(server.id), { method: "PUT", body: Object.assign({}, server, { enabled: !server.enabled }) }); if (result && result.redirecting) return; await loadBootstrap({ announce: false }); }
  function handleMcpClick(event) {
    var target = event.target; if (!(target instanceof HTMLElement)) return;
    var button = target.closest("button[data-action]"); if (!button) return;
    var id = button.getAttribute("data-mcp-id") || ""; var server = state.mcpServers.find(function (item) { return item && item.id === id; }); if (!server) return;
    var action = button.getAttribute("data-action");
    if (action === "edit-mcp") openMcpForm(server);
    if (action === "toggle-mcp") toggleMcpServer(server);
    if (action === "delete-mcp") openConfirm({ title: "Delete MCP server?", body: "Remove " + (server.name || id) + "?", onConfirm: function () { deleteMcpServer(id); } });
  }

  function buildCopyCell(text) {
    var span = document.createElement("span");
    span.textContent = text;
    return span;
  }

  function buildPillList(values, emptyLabel) {
    var frag = document.createDocumentFragment();
    if (!Array.isArray(values) || values.length === 0) {
      var empty = document.createElement("span");
      empty.className = "pill";
      empty.textContent = emptyLabel;
      frag.appendChild(empty);
      return frag;
    }
    values.forEach(function (value) {
      var pill = document.createElement("span");
      pill.className = "pill";
      pill.textContent = String(value);
      frag.appendChild(pill);
    });
    return frag;
  }

  function renderNixCache(entries) {
    var list = el("nix-cache-list");
    var status = el("nix-cache-status");
    if (!list || !status) return;
    list.textContent = "";
    status.textContent = "";
    if (!Array.isArray(entries) || entries.length === 0) {
      list.textContent = "No cached Nix environments.";
      setAriaBusy("nix-cache-list", false);
      return;
    }
    var table = document.createElement("table");
    table.className = "repos";
    var head = document.createElement("thead");
    var headRow = document.createElement("tr");
    ["Cache key", "Status", "Size (bytes)", "Last used", ""].forEach(function (label) {
      var th = document.createElement("th");
      th.scope = "col";
      th.textContent = label;
      headRow.appendChild(th);
    });
    head.appendChild(headRow);
    table.appendChild(head);
    var body = document.createElement("tbody");
    entries.forEach(function (entry) {
      var row = document.createElement("tr");
      [
        entry && entry.cacheKey,
        entry && entry.status,
        entry && entry.sizeBytes,
        entry && entry.lastUsedAt,
      ].forEach(function (value) {
        var cell = document.createElement("td");
        cell.textContent = value === undefined || value === null ? "—" : String(value);
        row.appendChild(cell);
      });
      var actions = document.createElement("td");
      var prune = document.createElement("button");
      prune.type = "button";
      prune.className = "btn-danger";
      prune.dataset.action = "prune-nix-cache";
      prune.dataset.cacheKey = entry && typeof entry.cacheKey === "string" ? entry.cacheKey : "";
      prune.textContent = "Prune";
      prune.disabled = !prune.dataset.cacheKey;
      actions.appendChild(prune);
      row.appendChild(actions);
      body.appendChild(row);
    });
    table.appendChild(body);
    list.appendChild(table);
    status.textContent = entries.length + " cached Nix environment" + (entries.length === 1 ? "." : "s.");
    setAriaBusy("nix-cache-list", false);
  }

  async function loadNixCache() {
    setAriaBusy("nix-cache-list", true);
    var result = await fetchJSON(NIX_CACHE_URL, { method: "GET" });
    if (result && result.redirecting) return;
    state.nixCache = result && result.data && Array.isArray(result.data.entries) ? result.data.entries : [];
    renderNixCache(state.nixCache);
  }

  async function pruneNixCache(cacheKey) {
    var result = await fetchJSON(NIX_CACHE_URL + "/" + encodeURIComponent(cacheKey) + "/prune", {
      method: "POST",
      body: {},
    });
    if (result && result.redirecting) return;
    showToast("Nix cache entry pruned.", "ok");
    await loadNixCache();
  }


  // ---- form ----------------------------------------------------------------

  function openForm(repository) {
    state.editing = repository || null;
    var panel = el("repo-form-panel");
    var form = el("repo-form");
    var heading = el("form-heading");
    var errorBox = el("repo-form-error");
    if (!panel || !form || !heading) return;

    heading.textContent = repository && repository.id ? "Edit repository" : "Add repository";
    var fields = {
      id: repository && repository.id ? repository.id : "",
      url: repository && repository.url ? repository.url : "",
      ref: repository && repository.ref ? repository.ref : "",
      teamIds: Array.isArray(repository && repository.teamIds)
        ? repository.teamIds.join(", ")
        : "",
      projectIds: Array.isArray(repository && repository.projectIds)
        ? repository.projectIds.join(", ")
        : "",
      labels: Array.isArray(repository && repository.labels)
        ? repository.labels.join(", ")
        : "",
      nixPackages: Array.isArray(repository && repository.nixPackages)
        ? repository.nixPackages.join(", ")
        : "",
      isDefault: !!(repository && repository.isDefault),
    };
    Object.keys(fields).forEach(function (name) {
      var field = form.elements.namedItem(name);
      if (!field) return;
      if (field.type === "checkbox") {
        field.checked = fields[name];
      } else {
        field.value = fields[name];
      }
    });
    var idField = el("repo-id");
    if (idField) idField.readOnly = !!(repository && repository.id);
    errorBox.textContent = "";
    errorBox.hidden = true;
    panel.hidden = false;
    var focusField = repository && repository.id ? el("repo-url") : idField;
    if (focusField) focusField.focus();
  }

  function closeForm() {
    var panel = el("repo-form-panel");
    if (panel) panel.hidden = true;
    state.editing = null;
  }

  function parseList(value) {
    if (typeof value !== "string") return [];
    return value
      .split(",")
      .map(function (entry) { return entry.trim(); })
      .filter(function (entry) { return entry.length > 0; });
  }
  function parseMcpArgs(value) {
    if (typeof value !== "string") return [];
    var trimmed = value.trim();
    if (trimmed.charAt(0) === "[") {
      try {
        var parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed) && parsed.every(function (entry) { return typeof entry === "string"; })) return parsed;
      } catch {}
    }
    return parseList(value);
  }

  function serializeForm(form) {
    var data = {};
    var elements = form.elements;
    data.url = (elements.namedItem("url").value || "").trim();
    data.ref = (elements.namedItem("ref").value || "").trim();
    data.teamIds = parseList(elements.namedItem("teamIds").value);
    data.projectIds = parseList(elements.namedItem("projectIds").value);
    data.labels = parseList(elements.namedItem("labels").value);
    data.nixPackages = parseList(elements.namedItem("nixPackages").value);
    data.isDefault = !!(elements.namedItem("isDefault") && elements.namedItem("isDefault").checked);
    var idField = elements.namedItem("id");
    if (idField && idField.value) data.id = idField.value;
    return data;
  }

  function showFormError(message) {
    var box = el("repo-form-error");
    if (!box) return;
    box.textContent = message;
    box.hidden = false;
  }

  function validateRepoPayload(payload) {
    if (!payload.id) return "Repository ID is required.";
    if (!payload.url) return "Repository URL is required.";
    if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:[\\/]+/.test(payload.url) && !/^https?:\\/\\//i.test(payload.url)) {
      // accept ssh-style git@... and https://
      if (!/^git@/.test(payload.url)) {
        return "Repository URL must be an ssh: or https:// URL.";
      }
    }
    if (!payload.ref) return "Default ref is required.";
    return null;
  }

  async function submitRepoForm(event) {
    event.preventDefault();
    var form = el("repo-form");
    if (!form) return;
    var payload = serializeForm(form);
    var validation = validateRepoPayload(payload);
    if (validation) {
      showFormError(validation);
      return;
    }
    var submitBtn = el("repo-form-submit");
    if (submitBtn) submitBtn.disabled = true;
    try {
      var isUpdate = !!(state.editing && state.editing.id);
      var url = isUpdate ? REPOSITORY_DETAIL(state.editing.id) : REPOSITORIES_BASE;
      var result = await fetchJSON(url, {
        method: isUpdate ? "PUT" : "POST",
        body: payload,
      });
      if (result && result.redirecting) return;
      showToast(isUpdate ? "Repository updated." : "Repository added.", "ok");
      closeForm();
      await loadBootstrap({ announce: false });
    } catch (err) {
      showFormError(err && err.message ? err.message : "Could not save repository.");
    } finally {
      if (submitBtn) submitBtn.disabled = false;
    }
  }

  function openConfirm(options) {
    var modal = el("confirm-modal");
    var title = el("confirm-title");
    var body = el("confirm-body");
    var yesBtn = el("confirm-yes");
    var noBtn = el("confirm-no");
    if (!modal || !title || !body || !yesBtn || !noBtn) return;
    title.textContent = options.title;
    body.textContent = options.body;
    modal.hidden = false;
    state.pendingConfirm = options.onConfirm || null;
    yesBtn.focus();
  }

  function closeConfirm(runCallback) {
    var modal = el("confirm-modal");
    var yesBtn = el("confirm-yes");
    var noBtn = el("confirm-no");
    if (!modal) return;
    var callback = state.pendingConfirm;
    state.pendingConfirm = null;
    if (modal) modal.hidden = true;
    if (yesBtn) yesBtn.onclick = null;
    if (noBtn) noBtn.onclick = null;
    if (runCallback && typeof callback === "function") callback();
  }

  async function deleteRepository(id) {
    var result = await fetchJSON(REPOSITORY_DETAIL(id), { method: "DELETE" });
    if (result && result.redirecting) return;
    showToast("Repository removed.", "ok");
    await loadBootstrap({ announce: false });
  }

  function handleRepoRowClick(event) {
    var target = event.target;
    if (!(target instanceof HTMLElement)) return;
    var button = target.closest("button[data-action]");
    if (!button) return;
    var id = button.getAttribute("data-repo-id") || "";
    var action = button.getAttribute("data-action");
    if (action === "prune-nix-cache") {
      var cacheKey = button.getAttribute("data-cache-key") || "";
      if (!cacheKey) return;
      openConfirm({
        title: "Prune Nix cache?",
        body: "Remove cached Nix environment " + cacheKey + "? Future sessions will rebuild it.",
        onConfirm: function () { pruneNixCache(cacheKey); },
      });
      return;
    }
    if (!id) return;
    if (action === "edit") {
      var found = state.repositories.find(function (r) { return r && r.id === id; });
      if (!found) return;
      openForm(found);
    } else if (action === "delete") {
      openConfirm({
        title: "Delete repository?",
        body: "Remove this repository from the gateway? Live sessions targeting this ref will be cancelled.",
        onConfirm: function () { deleteRepository(id); },
      });
    }
  }

  // ---- preview -------------------------------------------------------------

  function setPreviewIdle() {
    var node = el("preview-result");
    if (!node) return;
    node.setAttribute("data-state", "idle");
    node.textContent = "Run a preview to see which repository would be chosen.";
  }

  function renderPreview(result) {
    var node = el("preview-result");
    if (!node) return;
    node.textContent = "";
    if (!result) {
      setPreviewIdle();
      return;
    }

    var stateValue = result.state
      || (result.kind === "match" ? "matched" : result.kind)
      || (result.matched || result.match ? "matched" : "none");
    var match = result.repository || result.matched || result.match || null;
    var reason = result.reason || result.message || "";

    node.setAttribute("data-state", stateValue);

    if (stateValue === "matched" && match) {
      var row = document.createElement("div");
      row.className = "matched-row";
      row.textContent = "→ " + (match.url || match.id || "(repository)");
      node.appendChild(row);
      if (match.ref) {
        var refRow = document.createElement("div");
        refRow.textContent = "ref: " + match.ref;
        node.appendChild(refRow);
      }
    } else if (stateValue === "ambiguous") {
      var ambRow = document.createElement("div");
      ambRow.className = "matched-row";
      ambRow.textContent = "Ambiguous — multiple repositories match.";
      node.appendChild(ambRow);
    } else {
      var noneRow = document.createElement("div");
      noneRow.className = "matched-row";
      noneRow.textContent = "No repository matched.";
      node.appendChild(noneRow);
    }

    if (reason) {
      var reasonRow = document.createElement("div");
      reasonRow.textContent = reason;
      node.appendChild(reasonRow);
    }

    if (Array.isArray(result.trace)) {
      applyRailTrace(result.trace, match);
    }
  }

  function applyRailTrace(trace, matched) {
    var levels = {
      "explicit": "explicit",
      "issue-label": "issue-label",
      "issueLabel": "issue-label",
      "issue_label": "issue-label",
      "project-label": "project-label",
      "projectLabel": "project-label",
      "project_label": "project-label",
      "project": "project",
      "team": "team",
      "default": "default",
    };
    var stopAt = -1;
    var labels = [];
    trace.forEach(function (step) {
      var levelKey = step && step.level;
      var segmentKey = levels[levelKey] || levelKey;
      if (!levels[segmentKey]) return;
      labels.push(segmentKey);
      if (step && (step.matchedRepository || step.repositoryId)) {
        stopAt = labels.length - 1;
      }
    });

    var segments = document.querySelectorAll('[data-rail-segment]');
    segments.forEach(function (segment) {
      var key = segment.getAttribute("data-rail-segment");
      var index = labels.indexOf(key);
      var detail = segment.querySelector("[data-rail-detail]");
      var idNode = segment.querySelector("[data-rail-id]");
      if (!detail || !idNode) return;
      if (index === -1) {
        detail.textContent = "—";
        idNode.textContent = "";
        segment.setAttribute("data-state", "active");
      } else if (index < stopAt) {
        detail.textContent = "skipped";
        idNode.textContent = "";
        segment.setAttribute("data-state", "skipped");
      } else if (index === stopAt) {
        var matchedRepo = matched || {};
        detail.textContent = matchedRepo.url || matchedRepo.id || "matched";
        idNode.textContent = matchedRepo.id ? "id: " + matchedRepo.id : "";
        segment.setAttribute("data-state", "matched");
      } else {
        detail.textContent = "—";
        idNode.textContent = "";
        segment.setAttribute("data-state", "active");
      }
    });
  }

  function resetRail() {
    var segments = document.querySelectorAll('[data-rail-segment]');
    segments.forEach(function (segment) {
      var detail = segment.querySelector("[data-rail-detail]");
      var idNode = segment.querySelector("[data-rail-id]");
      if (detail) detail.textContent = "—";
      if (idNode) idNode.textContent = "";
      segment.setAttribute("data-state", "active");
    });
  }

  async function submitPreview(event) {
    event.preventDefault();
    var form = el("preview-form");
    if (!form) return;
    var elements = form.elements;
    var payload = {
      repositoryId: (elements.namedItem("repositoryId").value || "").trim() || null,
      teamId: (elements.namedItem("teamId").value || "").trim() || null,
      projectId: (elements.namedItem("projectId").value || "").trim() || null,
      issueLabels: parseList(elements.namedItem("issueLabels").value),
      projectLabels: parseList(elements.namedItem("projectLabels").value),
    };
    var node = el("preview-result");
    node.setAttribute("data-state", "idle");
    node.textContent = "Resolving…";
    try {
      var result = await fetchJSON(PREVIEW_URL, { method: "POST", body: payload });
      if (result && result.redirecting) return;
      renderPreview(result.data);
    } catch (err) {
      resetRail();
      var node = el("preview-result");
      node.setAttribute("data-state", "error");
      node.textContent = err && err.message ? err.message : "Preview failed.";
    }
  }

  var PROMPT_SAMPLE = {
    userRequest: "User request:\nSample task from Linear",
    issueContext: "Issue context:\nIssue: Sample issue (SYM-1)",
    threadComment: "Thread comment:\nPlease investigate this.",
    previousComments: "Previous comments:\n1. Earlier discussion",
    guidance: "Guidance:\n1. Keep the change focused.",
  };

  function substitutePromptPreview(template) {
    return String(template || "").replace(/{{([A-Za-z][A-Za-z0-9_]*)}}/g, function (token, name) {
      return Object.prototype.hasOwnProperty.call(PROMPT_SAMPLE, name) ? PROMPT_SAMPLE[name] : token;
    });
  }

  function renderPromptPreview() {
    var source = el("prompt-template-created");
    var preview = el("prompt-template-preview");
    if (source && preview) preview.textContent = substitutePromptPreview(source.value);
  }

  function renderPromptTemplates(templates) {
    state.promptTemplates = {};
    (Array.isArray(templates) ? templates : []).forEach(function (template) {
      state.promptTemplates[template.kind] = template.body;
      var field = el("prompt-template-" + template.kind);
      if (field) field.value = template.body;
    });
    renderPromptPreview();
  }

  async function loadPromptTemplates() {
    var result = await fetchJSON(PROMPT_TEMPLATES_URL, { method: "GET" });
    if (result && result.redirecting) return;
    renderPromptTemplates(result.data && result.data.templates);
  }

  async function savePromptTemplates() {
    var status = el("prompt-templates-status");
    try {
      var kinds = ["created", "prompted", "contract"];
      for (var i = 0; i < kinds.length; i += 1) {
        var kind = kinds[i];
        var field = el("prompt-template-" + kind);
        await fetchJSON(PROMPT_TEMPLATES_URL, {
          method: "PUT",
          body: { kind: kind, body: field ? field.value : "" },
        });
      }
      if (status) status.textContent = "Prompt templates saved.";
      await loadPromptTemplates();
    } catch (err) {
      if (status) status.textContent = err && err.message ? err.message : "Unable to save prompt templates.";
    }
  }

  // ---- bootstrap loader ----------------------------------------------------

  async function loadBootstrap(options) {
    options = options || {};
    var announce = options.announce !== false;
    if (announce) {
      setStatus("loading", "loading…");
      announceStatus("repos-status", "Loading repositories…");
      announceStatus("installation-card", "");
      var loadingNode = document.createElement("p");
      loadingNode.className = "loading";
      loadingNode.textContent = "Loading installation…";
      var card = el("installation-card");
      if (card) {
        card.textContent = "";
        card.appendChild(loadingNode);
      }
    }
    setAriaBusy("repos-list", true);
    try {
      var result = await fetchJSON(BOOTSTRAP_URL, { method: "GET" });
      if (result && result.redirecting) return;
      var data = result.data || {};
      state.csrfToken = typeof data.csrfToken === "string" ? data.csrfToken : "";
      state.installation = data.installation || null;
      state.repositories = Array.isArray(data.repositories) ? data.repositories : [];
      state.mcpServers = Array.isArray(data.mcpServers) ? data.mcpServers : [];
      renderInstallation(state.installation);
      renderRepositories(state.repositories);
      renderMcpServers(state.mcpServers);
      await loadNixCache();
      var badge = computeAccessibilityBadge(state.installation);
      setStatus(badge.state, badge.label);
      var logoutBtn = el("logout-btn");
      if (logoutBtn) logoutBtn.hidden = false;
      var installStatus = (state.installation && state.installation.revokedAt)
        ? "Installation revoked."
        : "Installation ready.";
      announceStatus("repos-status", installStatus);
    } catch (err) {
      var node = el("repos-status");
      if (node) {
        node.textContent = "";
        var errorCard = document.createElement("div");
        errorCard.className = "error-card";
        errorCard.textContent = (err && err.message)
          ? "Failed to load: " + err.message
          : "Failed to load bootstrap.";
        node.appendChild(errorCard);
      }
      setAriaBusy("repos-list", false);
      setStatus("warn", "load error");
    }
  }

  // ---- logout --------------------------------------------------------------

  async function logout() {
    try {
      var result = await fetchJSON(LOGOUT_URL, { method: "POST", body: {} });
      if (result && result.redirecting) return;
    } catch (err) {
      // fall through to redirect even on failure
    }
    window.location.assign("/");
  }

  // ---- wire-up -------------------------------------------------------------

  function init() {
    var newBtn = el("new-repo-btn");
    if (newBtn) newBtn.addEventListener("click", function () { openForm(null); });
    var newMcpBtn = el("new-mcp-btn");
    if (newMcpBtn) newMcpBtn.addEventListener("click", function () { openMcpForm(null); });
    var mcpCancel = el("mcp-cancel");
    if (mcpCancel) mcpCancel.addEventListener("click", closeMcpForm);
    var addHeader = el("mcp-add-header");
    if (addHeader) addHeader.addEventListener("click", function () { addMcpHeaderRow("", ""); });
    var transport = el("mcp-transport");
    if (transport) transport.addEventListener("change", syncMcpHeaderVisibility);

    var cancelBtn = el("form-cancel-btn");
    if (cancelBtn) cancelBtn.addEventListener("click", closeForm);

    var inlineCancel = el("repo-form-cancel");
    if (inlineCancel) inlineCancel.addEventListener("click", closeForm);

    var form = el("repo-form");
    if (form) form.addEventListener("submit", submitRepoForm);
    var mcpForm = el("mcp-form");
    if (mcpForm) mcpForm.addEventListener("submit", submitMcpForm);

    var previewForm = el("preview-form");
    if (previewForm) previewForm.addEventListener("submit", submitPreview);
    var promptCreated = el("prompt-template-created");
    if (promptCreated) promptCreated.addEventListener("input", renderPromptPreview);
    var promptSave = el("prompt-templates-save");
    if (promptSave) promptSave.addEventListener("click", savePromptTemplates);

    var list = el("repos-list");
    if (list) list.addEventListener("click", handleRepoRowClick);
    var mcpList = el("mcp-list");
    if (mcpList) mcpList.addEventListener("click", handleMcpClick);
    var nixCacheList = el("nix-cache-list");
    if (nixCacheList) nixCacheList.addEventListener("click", handleRepoRowClick);

    var yesBtn = el("confirm-yes");
    var noBtn = el("confirm-no");
    if (yesBtn) yesBtn.addEventListener("click", function () { closeConfirm(true); });
    if (noBtn) noBtn.addEventListener("click", function () { closeConfirm(false); });

    loadPromptTemplates().catch(function (err) {
      var status = el("prompt-templates-status");
      if (status) status.textContent = err && err.message ? err.message : "Unable to load prompt templates.";
    });
    if (logoutBtn) logoutBtn.addEventListener("click", logout);

    setPreviewIdle();
    loadBootstrap();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
`;
