/**
 * Inline CSS for the admin UI HTML documents.
 *
 * The full stylesheet is embedded into every page rendered by
 * `src/admin-ui.ts`. Tokens define a warm-paper / near-black ink / signal-orange
 * palette with crisp 1px rules, a subtle grid texture, and typography that
 * favors a system serif display stack against a local sans fallback. There
 * are no external font or asset dependencies.
 */
export const PAGE_STYLES = `
  :root {
    --paper: #f1e7d2;
    --paper-tint: #ece1c5;
    --ink: #1a1612;
    --ink-soft: #4a3e30;
    --ink-mute: #877355;
    --rule: #b8a886;
    --rule-soft: #d6c8a5;
    --signal: #c44a14;
    --signal-soft: #e88c5f;
    --warn: #8a6d00;
    --danger: #8a1f12;
    --good: #1f5a2e;
    --mono: ui-monospace, "SF Mono", "Menlo", "Consolas", monospace;
    --sans: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI",
      system-ui, sans-serif;
    --display: "Iowan Old Style", "Charter", "Source Serif Pro",
      "Cambria", Georgia, serif;
    --rail-segment-gap: 0;
    --grid-line: rgba(26, 22, 18, 0.04);
  }

  :root[data-theme="editorial"][data-resolved-mode="dark"] {
    --paper: #181512;
    --paper-tint: #241f19;
    --ink: #f5eee1;
    --ink-soft: #d4c7b4;
    --ink-mute: #aa997f;
    --rule: #695d4b;
    --rule-soft: #433b31;
    --signal: #f07b42;
    --signal-soft: #f7b38f;
    --warn: #d8b742;
    --danger: #ee8477;
    --good: #79c28b;
    --grid-line: rgba(245, 238, 225, 0.05);
  }

  :root[data-theme="linear"] {
    --paper: #f8f9fb;
    --paper-tint: #eef0f4;
    --ink: #20232d;
    --ink-soft: #586171;
    --ink-mute: #7b8494;
    --rule: #d3d8e1;
    --rule-soft: #e5e8ee;
    --signal: #5e6ad2;
    --signal-soft: #8b95e6;
    --warn: #956e14;
    --danger: #b33d46;
    --good: #2f7a4d;
    --grid-line: transparent;
    --display: var(--sans);
    --rail-segment-gap: 6px;
  }

  :root[data-theme="linear"][data-resolved-mode="dark"] {
    --paper: #15171b;
    --paper-tint: #1d2026;
    --ink: #f0f1f5;
    --ink-soft: #b8becb;
    --ink-mute: #858d9d;
    --rule: #3a404c;
    --rule-soft: #2a2f38;
    --signal: #8b95f5;
    --signal-soft: #aeb5ff;
    --warn: #e0b84e;
    --danger: #f08a8f;
    --good: #80cf9b;
  }

  *, *::before, *::after { box-sizing: border-box; }

  html { font-size: 16px; }
  body {
    margin: 0;
    background: var(--paper);
    color: var(--ink);
    font-family: var(--sans);
    line-height: 1.55;
    -webkit-font-smoothing: antialiased;
    background-image:
      linear-gradient(var(--grid-line) 1px, transparent 1px),
      linear-gradient(90deg, var(--grid-line) 1px, transparent 1px);
    background-size: 24px 24px;
    background-position: -1px -1px;
  }

  ::selection { background: var(--signal); color: var(--paper); }

  a {
    color: var(--ink);
    text-decoration: underline;
    text-decoration-color: var(--rule);
    text-underline-offset: 3px;
  }
  a:hover { text-decoration-color: var(--signal); }

  h1, h2, h3 {
    font-family: var(--display);
    font-weight: 600;
    letter-spacing: -0.01em;
    margin: 0;
  }

  hr {
    border: 0;
    border-top: 1px solid var(--rule);
    margin: 32px 0;
  }

  button, .btn {
    font-family: var(--sans);
    font-size: 1rem;
    line-height: 1.2;
    border: 1px solid var(--ink);
    background: var(--paper);
    color: var(--ink);
    padding: 10px 18px;
    cursor: pointer;
    transition: background 120ms ease-out, color 120ms ease-out,
      border-color 120ms ease-out;
  }
  button:hover, .btn:hover {
    background: var(--ink);
    color: var(--paper);
  }
  button:focus-visible,
  a:focus-visible,
  input:focus-visible,
  select:focus-visible,
  textarea:focus-visible {
    outline: 2px solid var(--signal);
    outline-offset: 2px;
  }
  button.btn-primary {
    background: var(--ink);
    color: var(--paper);
  }
  button.btn-primary:hover {
    background: var(--signal);
    border-color: var(--signal);
  }
  button.btn-danger {
    border-color: var(--danger);
    color: var(--danger);
  }
  button.btn-danger:hover {
    background: var(--danger);
    color: var(--paper);
  }
  button:disabled,
  .btn[aria-disabled="true"] {
    cursor: not-allowed;
    opacity: 0.55;
  }
  button:disabled:hover,
  .btn[aria-disabled="true"]:hover {
    background: var(--paper);
    color: var(--ink);
  }

  input[type="text"],
  input[type="url"],
  input[type="search"],
  textarea {
    font-family: var(--sans);
    font-size: 0.95rem;
    width: 100%;
    padding: 8px 10px;
    border: 1px solid var(--rule);
    background: var(--paper-tint);
    color: var(--ink);
    border-radius: 0;
  }
  input[type="text"]:focus,
  input[type="url"]:focus,
  textarea:focus {
    border-color: var(--ink);
  }
  textarea { font-family: var(--mono); resize: vertical; }

  select {
    font: inherit;
    color: var(--ink);
    background: var(--paper);
    border: 1px solid var(--rule);
    padding: 6px 26px 6px 8px;
    border-radius: 0;
    min-height: 32px;
  }

  .theme-switcher {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
  }
  .theme-switcher label {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    color: var(--ink-mute);
    font-family: var(--mono);
    font-size: 0.68rem;
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }
  .theme-switcher select {
    min-height: 30px;
    font-size: 0.75rem;
    padding: 4px 22px 4px 7px;
    color: var(--ink);
    background: var(--paper);
  }
  .theme-switcher select:hover {
    border-color: var(--ink);
  }

  input[type="checkbox"] {
    width: 16px;
    height: 16px;
    accent-color: var(--signal);
  }

  table {
    width: 100%;
    border-collapse: collapse;
  }
  th, td {
    text-align: left;
    padding: 10px 12px;
    border-bottom: 1px solid var(--rule-soft);
    vertical-align: top;
    font-size: 0.92rem;
  }
  th {
    border-bottom: 1px solid var(--rule);
    font-family: var(--mono);
    font-size: 0.72rem;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--ink-soft);
    background: var(--paper-tint);
    font-weight: 500;
  }
  td code {
    font-family: var(--mono);
    font-size: 0.85em;
    color: var(--ink-soft);
  }
  td .pill {
    display: inline-block;
    padding: 2px 8px;
    border: 1px solid var(--rule);
    background: var(--paper);
    font-family: var(--mono);
    font-size: 0.72rem;
    letter-spacing: 0.02em;
    color: var(--ink-soft);
    margin: 2px 4px 2px 0;
  }
  td .pill.signal {
    border-color: var(--signal);
    color: var(--signal);
  }
  td .pill.warn {
    border-color: var(--warn);
    color: var(--warn);
  }
  td .pill.danger {
    border-color: var(--danger);
    color: var(--danger);
  }
  td .pill.good {
    border-color: var(--good);
    color: var(--good);
  }

  /* Reduced motion */
  @media (prefers-reduced-motion: reduce) {
    *, *::before, *::after {
      animation-duration: 0.001ms !important;
      animation-iteration-count: 1 !important;
      transition-duration: 0.001ms !important;
      scroll-behavior: auto !important;
    }
  }

  /* ----- Landing ----- */
  .landing {
    max-width: 720px;
    margin: 0 auto;
    padding: 32px 24px 64px;
  }
  .landing-header {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    border-bottom: 1px solid var(--rule);
    padding-bottom: 16px;
    margin-bottom: 48px;
  }
  .brand {
    font-family: var(--mono);
    font-size: 0.78rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: var(--ink-soft);
  }
  .brand strong { color: var(--ink); font-weight: 600; }
  .version {
    font-family: var(--mono);
    font-size: 0.72rem;
    color: var(--ink-mute);
  }

  .landing-header-tools {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 16px;
    flex-wrap: wrap;
  }

  .landing-header .theme-switcher {
    justify-content: flex-end;
  }

  .hero h1 {
    font-size: clamp(2.2rem, 5vw, 3.4rem);
    line-height: 1.05;
    margin-bottom: 24px;
  }
  .hero h1 em {
    font-style: italic;
    color: var(--signal);
    font-weight: 600;
  }
  .hero p.lead {
    font-size: 1.1rem;
    color: var(--ink-soft);
    max-width: 56ch;
    margin: 0 0 32px;
  }

  .cta-row {
    display: flex;
    align-items: center;
    gap: 16px;
    margin-bottom: 16px;
  }
  .cta-row a.cta {
    font-family: var(--display);
    font-size: 1.2rem;
    padding: 14px 26px;
    border: 1px solid var(--ink);
    background: var(--ink);
    color: var(--paper);
    text-decoration: none;
    letter-spacing: 0.01em;
    transition: background 120ms ease-out, border-color 120ms ease-out,
      transform 120ms ease-out;
  }
  .cta-row a.cta:hover {
    background: var(--signal);
    border-color: var(--signal);
    transform: translate(-2px, -2px);
    box-shadow: 4px 4px 0 0 var(--ink);
    color: var(--paper);
  }
  .cta-row .cta-hint {
    font-family: var(--mono);
    font-size: 0.78rem;
    color: var(--ink-mute);
  }

  .trust {
    display: flex;
    gap: 12px;
    align-items: center;
    padding: 12px 16px;
    border: 1px solid var(--rule-soft);
    background: var(--paper-tint);
    font-family: var(--mono);
    font-size: 0.82rem;
    color: var(--ink-soft);
    margin: 32px 0 48px;
  }
  .trust .lock {
    width: 14px;
    height: 14px;
    background:
      linear-gradient(var(--ink), var(--ink)) center 60% / 60% 2px no-repeat,
      var(--paper);
    border: 1px solid var(--ink);
    flex: 0 0 14px;
    position: relative;
  }
  .trust .lock::before {
    content: "";
    position: absolute;
    left: 2px; right: 2px; top: -6px;
    height: 8px;
    border: 1px solid var(--ink);
    border-bottom: 0;
  }

  ol.steps {
    list-style: none;
    counter-reset: step;
    padding: 0;
    margin: 0;
    border-top: 1px solid var(--rule);
  }
  ol.steps li {
    counter-increment: step;
    display: grid;
    grid-template-columns: 64px 1fr;
    gap: 16px;
    padding: 18px 0;
    border-bottom: 1px solid var(--rule-soft);
  }
  ol.steps li::before {
    content: counter(step, decimal-leading-zero);
    font-family: var(--mono);
    font-size: 0.8rem;
    color: var(--ink-mute);
    align-self: start;
    padding-top: 4px;
  }
  ol.steps li h3 {
    font-size: 1.2rem;
    margin-bottom: 4px;
  }
  ol.steps li p {
    margin: 0;
    color: var(--ink-soft);
  }

  .landing-footer {
    border-top: 1px solid var(--rule);
    padding-top: 16px;
    margin-top: 48px;
    display: flex;
    justify-content: space-between;
    color: var(--ink-mute);
    font-size: 0.82rem;
  }

  /* ----- Admin ----- */
  .admin-shell {
    --aside-width: 280px;
    display: grid;
    grid-template-columns: 1fr;
    min-height: 100vh;
  }
  .admin-header {
    grid-column: 1 / -1;
    border-bottom: 1px solid var(--rule);
    padding: 14px 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    background: var(--paper);
  }
  .admin-header .title {
    font-family: var(--mono);
    font-size: 0.78rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: var(--ink-soft);
  }
  .admin-header .title strong {
    color: var(--ink);
    font-weight: 600;
  }
  .header-actions {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .header-actions .theme-switcher {
    justify-content: flex-end;
  }
  .status-pill {
    font-family: var(--mono);
    font-size: 0.72rem;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    padding: 4px 10px;
    border: 1px solid var(--rule);
    background: var(--paper-tint);
    color: var(--ink-soft);
  }
  .status-pill[data-state="ok"] {
    border-color: var(--good);
    color: var(--good);
  }
  .status-pill[data-state="warn"] {
    border-color: var(--warn);
    color: var(--warn);
  }
  .status-pill[data-state="danger"] {
    border-color: var(--danger);
    color: var(--danger);
  }

  .admin-body {
    display: grid;
    grid-template-columns: var(--aside-width) 1fr;
    gap: 0;
  }

  aside.installation {
    border-right: 1px solid var(--rule);
    padding: 20px;
    background: var(--paper-tint);
  }
  aside.installation h2 {
    font-family: var(--mono);
    font-size: 0.72rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--ink-mute);
    margin-bottom: 14px;
    border-bottom: 1px solid var(--rule-soft);
    padding-bottom: 6px;
  }
  .install-row {
    margin-bottom: 16px;
  }
  .install-row .label {
    font-family: var(--mono);
    font-size: 0.7rem;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--ink-mute);
    margin-bottom: 4px;
  }
  .install-row .value {
    font-family: var(--mono);
    font-size: 0.88rem;
    color: var(--ink);
    word-break: break-word;
  }

  main.console {
    padding: 24px;
    overflow-x: hidden;
  }

  section.rail-block {
    border: 1px solid var(--rule);
    margin-bottom: 32px;
    background: var(--paper);
  }
  .rail-header {
    padding: 12px 16px;
    border-bottom: 1px solid var(--rule-soft);
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 12px;
  }
  .rail-header h2 {
    font-family: var(--display);
    font-size: 1.1rem;
    font-weight: 600;
  }
  .rail-header .hint {
    font-family: var(--mono);
    font-size: 0.72rem;
    color: var(--ink-mute);
  }
  .rail {
    padding: 18px 16px;
    overflow-x: auto;
  }
  .rail-track {
    display: grid;
    grid-template-columns: repeat(6, minmax(120px, 1fr));
    gap: 0;
    align-items: stretch;
    min-width: 720px;
  }
  .rail-segment {
    position: relative;
    border: 1px solid var(--rule);
    border-right: 0;
    padding: 12px 14px;
    background: var(--paper-tint);
    transition: background 120ms ease-out, border-color 120ms ease-out;
  }
  .rail-segment:last-child {
    border-right: 1px solid var(--rule);
  }
  .rail-segment::after {
    content: "";
    position: absolute;
    right: -8px;
    top: 50%;
    transform: translateY(-50%);
    width: 16px;
    height: 16px;
    border-top: 1px solid var(--rule);
    border-right: 1px solid var(--rule);
    background: var(--paper-tint);
    z-index: 1;
    pointer-events: none;
  }
  .rail-segment[data-state="matched"] {
    background: var(--signal);
    color: var(--paper);
    border-color: var(--signal);
  }
  .rail-segment[data-state="matched"]::after {
    background: var(--signal);
    border-color: var(--signal);
  }
  .rail-segment[data-state="active"] {
    background: var(--paper);
    border-color: var(--ink);
  }
  .rail-segment[data-state="skipped"] {
    background: var(--paper);
    color: var(--ink-mute);
    border-color: var(--rule-soft);
  }
  .rail-segment .seg-title {
    font-family: var(--mono);
    font-size: 0.7rem;
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }
  .rail-segment .seg-detail {
    font-family: var(--display);
    font-size: 1rem;
    margin-top: 4px;
    word-break: break-word;
  }
  .rail-segment .seg-id {
    font-family: var(--mono);
    font-size: 0.72rem;
    margin-top: 4px;
    opacity: 0.85;
  }

  section.panel {
    border: 1px solid var(--rule);
    margin-bottom: 32px;
    background: var(--paper);
  }
  .panel-header {
    padding: 12px 16px;
    border-bottom: 1px solid var(--rule-soft);
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
  }
  .panel-header h2 {
    font-family: var(--display);
    font-size: 1.1rem;
    font-weight: 600;
  }
  .panel-body { padding: 16px; }

  table.repos {
    width: 100%;
  }
  table.repos td.actions {
    white-space: nowrap;
    text-align: right;
  }
  table.repos td.actions button {
    margin-left: 6px;
    padding: 4px 10px;
    font-size: 0.8rem;
  }
  .default-cell {
    font-family: var(--mono);
    font-size: 0.78rem;
    letter-spacing: 0.04em;
  }

  form.repo-form {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 16px;
  }
  form.repo-form .field {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  form.repo-form .field.full { grid-column: 1 / -1; }
  form.repo-form label {
    font-family: var(--mono);
    font-size: 0.72rem;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--ink-mute);
  }
  form.repo-form .field-row {
    display: flex;
    gap: 12px;
    align-items: center;
  }
  form.repo-form .field-row label.checkbox {
    font-family: var(--sans);
    font-size: 0.92rem;
    color: var(--ink);
    text-transform: none;
    letter-spacing: 0;
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }
  form.repo-form .form-actions {
    grid-column: 1 / -1;
    display: flex;
    justify-content: flex-end;
    gap: 12px;
    border-top: 1px solid var(--rule-soft);
    padding-top: 16px;
  }
  form .error {
    color: var(--danger);
    font-family: var(--mono);
    font-size: 0.78rem;
    margin-top: 4px;
  }

  form.preview-form {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 16px;
  }
  form.preview-form .field-full { grid-column: 1 / -1; }
  .preview-result {
    border-top: 1px solid var(--rule-soft);
    margin-top: 16px;
    padding: 12px 16px;
    font-family: var(--mono);
    font-size: 0.85rem;
    display: grid;
    gap: 4px;
  }
  .preview-result[data-state="matched"] { color: var(--good); }
  .preview-result[data-state="none"] { color: var(--ink-mute); }
  .preview-result[data-state="ambiguous"] { color: var(--warn); }
  .preview-result[data-state="error"] { color: var(--danger); }
  .preview-result .matched-row {
    font-family: var(--display);
    font-size: 1rem;
    color: var(--ink);
  }

  .empty {
    border: 1px dashed var(--rule);
    padding: 28px 16px;
    text-align: center;
    color: var(--ink-mute);
    font-family: var(--mono);
    font-size: 0.88rem;
  }
  .loading {
    padding: 16px;
    color: var(--ink-mute);
    font-family: var(--mono);
    font-size: 0.85rem;
  }
  .error-card {
    border: 1px solid var(--danger);
    padding: 14px 16px;
    color: var(--danger);
    font-family: var(--mono);
    font-size: 0.85rem;
    background: var(--paper-tint);
  }

  .toast {
    position: fixed;
    bottom: 16px;
    right: 16px;
    padding: 12px 16px;
    border: 1px solid var(--ink);
    background: var(--ink);
    color: var(--paper);
    font-family: var(--mono);
    font-size: 0.82rem;
    max-width: 360px;
    opacity: 0;
    transform: translateY(8px);
    transition: opacity 160ms ease-out, transform 160ms ease-out;
    pointer-events: none;
  }
  .toast[data-show="true"] {
    opacity: 1;
    transform: translateY(0);
    pointer-events: auto;
  }
  .toast[data-tone="error"] {
    background: var(--danger);
    border-color: var(--danger);
  }
  .toast[data-tone="warn"] {
    background: var(--warn);
    border-color: var(--warn);
  }

  /* ----- Linear-inspired alternate system ----- */
  :root[data-theme="linear"] body {
    background-image: none;
  }
  :root[data-theme="linear"] h1,
  :root[data-theme="linear"] h2,
  :root[data-theme="linear"] h3 {
    font-family: var(--sans);
    font-weight: 650;
    letter-spacing: -0.025em;
  }
  :root[data-theme="linear"] button,
  :root[data-theme="linear"] .btn,
  :root[data-theme="linear"] input[type="text"],
  :root[data-theme="linear"] input[type="url"],
  :root[data-theme="linear"] input[type="search"],
  :root[data-theme="linear"] textarea,
  :root[data-theme="linear"] select {
    border-radius: 6px;
  }
  :root[data-theme="linear"] button,
  :root[data-theme="linear"] .btn {
    font-size: 0.88rem;
    padding: 9px 14px;
  }
  :root[data-theme="linear"] button.btn-primary {
    background: var(--signal);
    border-color: var(--signal);
    color: #fff;
  }
  :root[data-theme="linear"] button.btn-primary:hover {
    background: var(--signal-soft);
    border-color: var(--signal-soft);
    color: #fff;
  }
  :root[data-theme="linear"] .theme-switcher select {
    border-radius: 6px;
  }
  :root[data-theme="linear"] .landing {
    max-width: 880px;
  }
  :root[data-theme="linear"] .hero h1 {
    max-width: 18ch;
    line-height: 1.08;
  }
  :root[data-theme="linear"] .hero h1 em {
    font-style: normal;
    color: var(--signal);
  }
  :root[data-theme="linear"] .cta-row a.cta {
    border-radius: 6px;
    background: var(--signal);
    border-color: var(--signal);
    font-family: var(--sans);
    font-size: 0.95rem;
    font-weight: 600;
  }
  :root[data-theme="linear"] .cta-row a.cta:hover {
    background: var(--signal-soft);
    border-color: var(--signal-soft);
    transform: translateY(-1px);
    box-shadow: none;
  }
  :root[data-theme="linear"] .trust {
    border-radius: 8px;
  }
  :root[data-theme="linear"] ol.steps li {
    grid-template-columns: 32px 1fr;
    gap: 12px;
  }
  :root[data-theme="linear"] ol.steps li::before {
    content: "•";
    color: var(--signal);
    font-size: 1.2rem;
    line-height: 1;
  }
  :root[data-theme="linear"] .admin-header {
    background: var(--paper-tint);
  }
  :root[data-theme="linear"] section.rail-block,
  :root[data-theme="linear"] section.panel {
    border-radius: 10px;
    box-shadow: 0 1px 2px rgba(31, 35, 45, 0.08);
  }
  :root[data-theme="linear"] .rail-track {
    gap: var(--rail-segment-gap);
  }
  :root[data-theme="linear"] .rail-segment {
    border-right: 1px solid var(--rule);
    border-radius: 8px;
  }
  :root[data-theme="linear"] .rail-segment::after {
    display: none;
  }
  :root[data-theme="linear"] .status-pill,
  :root[data-theme="linear"] td .pill {
    border-radius: 999px;
  }
  :root[data-theme="linear"] section.panel,
  :root[data-theme="linear"] section.rail-block {
    margin-bottom: 20px;
  }

  /* Responsive */
  @media (max-width: 1024px) {
    .admin-body {
      grid-template-columns: 1fr;
    }
    aside.installation {
      border-right: 0;
      border-bottom: 1px solid var(--rule);
    }
  }
  @media (max-width: 720px) {
    .landing-header {
      align-items: flex-start;
      flex-wrap: wrap;
      gap: 16px;
    }
    .landing-header-tools {
      flex: 1 1 280px;
    }
    .header-actions {
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    main.console { padding: 16px; }
    .rail-track {
      grid-template-columns: 1fr;
      gap: 16px;
    }
    .rail-segment { border-right: 1px solid var(--rule) !important; }
    .rail-segment::after { display: none; }
    form.repo-form, form.preview-form {
      grid-template-columns: 1fr;
    }
    .panel-body { overflow-x: auto; }
  }
  @media (max-width: 360px) {
    .landing { padding: 24px 16px 48px; }
    .admin-header { flex-wrap: wrap; }
  }
`;
