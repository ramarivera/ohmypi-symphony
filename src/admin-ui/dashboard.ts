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
  var LOGOUT_URL = "/api/admin/logout";
  var TOAST_DURATION_MS = 3500;

  var state = {
    csrfToken: "",
    installation: null,
    repositories: [],
    editing: null,
    pendingDelete: null,
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
    ["Default ref", "URL", "Teams", "Projects", "Labels", "Default", ""].forEach(function (text) {
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

  function serializeForm(form) {
    var data = {};
    var elements = form.elements;
    data.url = (elements.namedItem("url").value || "").trim();
    data.ref = (elements.namedItem("ref").value || "").trim();
    data.teamIds = parseList(elements.namedItem("teamIds").value);
    data.projectIds = parseList(elements.namedItem("projectIds").value);
    data.labels = parseList(elements.namedItem("labels").value);
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
      renderInstallation(state.installation);
      renderRepositories(state.repositories);
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

    var cancelBtn = el("form-cancel-btn");
    if (cancelBtn) cancelBtn.addEventListener("click", closeForm);

    var inlineCancel = el("repo-form-cancel");
    if (inlineCancel) inlineCancel.addEventListener("click", closeForm);

    var form = el("repo-form");
    if (form) form.addEventListener("submit", submitRepoForm);

    var previewForm = el("preview-form");
    if (previewForm) previewForm.addEventListener("submit", submitPreview);

    var list = el("repos-list");
    if (list) list.addEventListener("click", handleRepoRowClick);

    var yesBtn = el("confirm-yes");
    var noBtn = el("confirm-no");
    if (yesBtn) yesBtn.addEventListener("click", function () { closeConfirm(true); });
    if (noBtn) noBtn.addEventListener("click", function () { closeConfirm(false); });

    var logoutBtn = el("logout-btn");
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
