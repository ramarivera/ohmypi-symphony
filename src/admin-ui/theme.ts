/**
 * Shared appearance controls and runtime for the landing and admin pages.
 *
 * The preference is deliberately a small, validated string rather than JSON so
 * the head bootstrap can apply it before the stylesheet paints. Theme family
 * and color mode stay independent: `editorial|dark` and `linear|system` are
 * both valid preferences.
 */

export const THEME_CONTROLS = `
<div class="theme-switcher" data-theme-switcher>
  <label>
    <span>Theme</span>
    <select data-theme-control="theme" aria-label="Theme family">
      <option value="editorial">Editorial grid</option>
      <option value="linear">Linear-inspired</option>
    </select>
  </label>
  <label>
    <span>Mode</span>
    <select data-theme-control="mode" aria-label="Color mode">
      <option value="light">Light</option>
      <option value="dark">Dark</option>
      <option value="system">System</option>
    </select>
  </label>
</div>
`;

/**
 * Runs in the document head before PAGE_STYLES. It only reads a validated,
 * author-defined local preference and writes known data attributes to the root.
 */
export const THEME_BOOTSTRAP_SCRIPT = `
(function () {
  "use strict";

  var root = document.documentElement;
  var STORAGE_KEY = "ohmypi-admin-appearance";

  function validTheme(value) {
    return value === "editorial" || value === "linear";
  }

  function validMode(value) {
    return value === "light" || value === "dark" || value === "system";
  }

  function readPreference() {
    var raw = "";
    try { raw = window.localStorage.getItem(STORAGE_KEY) || ""; } catch (err) { /* storage unavailable */ }
    var parts = raw.split("|");
    return {
      theme: validTheme(parts[0]) ? parts[0] : "editorial",
      mode: validMode(parts[1]) ? parts[1] : "system",
    };
  }

  function isDark(mode) {
    return mode === "dark" || (
      mode === "system" &&
      window.matchMedia &&
      window.matchMedia("(prefers-color-scheme: dark)").matches
    );
  }

  var preference = readPreference();
  root.setAttribute("data-theme", preference.theme);
  root.setAttribute("data-color-mode", preference.mode);
  root.setAttribute("data-resolved-mode", isDark(preference.mode) ? "dark" : "light");
  root.style.colorScheme = preference.mode === "system" ? "light dark" : preference.mode;
})();
`;

/**
 * Runs after the body is available to wire both compact switchers. The media
 * listener keeps the resolved mode current while the user is in System mode.
 */
export const THEME_SCRIPT = `
(function () {
  "use strict";

  var root = document.documentElement;
  var STORAGE_KEY = "ohmypi-admin-appearance";
  var media = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;
  var controls = document.querySelectorAll("[data-theme-control]");

  function validTheme(value) {
    return value === "editorial" || value === "linear";
  }

  function validMode(value) {
    return value === "light" || value === "dark" || value === "system";
  }

  function readPreference() {
    var raw = "";
    try { raw = window.localStorage.getItem(STORAGE_KEY) || ""; } catch (err) { /* storage unavailable */ }
    var parts = raw.split("|");
    return {
      theme: validTheme(parts[0]) ? parts[0] : "editorial",
      mode: validMode(parts[1]) ? parts[1] : "system",
    };
  }

  function applyPreference(preference, persist) {
    var dark = preference.mode === "dark" || (preference.mode === "system" && media && media.matches);
    root.setAttribute("data-theme", preference.theme);
    root.setAttribute("data-color-mode", preference.mode);
    root.setAttribute("data-resolved-mode", dark ? "dark" : "light");
    root.style.colorScheme = preference.mode === "system" ? "light dark" : preference.mode;

    for (var i = 0; i < controls.length; i += 1) {
      if (controls[i].getAttribute("data-theme-control") === "theme") controls[i].value = preference.theme;
      if (controls[i].getAttribute("data-theme-control") === "mode") controls[i].value = preference.mode;
    }

    if (persist) {
      try { window.localStorage.setItem(STORAGE_KEY, preference.theme + "|" + preference.mode); }
      catch (err) { /* preference remains active for this page */ }
    }
  }

  var preference = readPreference();
  applyPreference(preference, false);

  for (var i = 0; i < controls.length; i += 1) {
    controls[i].addEventListener("change", function (event) {
      var target = event.currentTarget;
      var next = readPreference();
      if (target.getAttribute("data-theme-control") === "theme") next.theme = target.value;
      if (target.getAttribute("data-theme-control") === "mode") next.mode = target.value;
      if (!validTheme(next.theme)) next.theme = "editorial";
      if (!validMode(next.mode)) next.mode = "system";
      applyPreference(next, true);
      preference = next;
    });
  }

  function handleSystemChange() {
    if (preference.mode === "system") applyPreference(preference, false);
  }

  if (media && media.addEventListener) media.addEventListener("change", handleSystemChange);
  else if (media && media.addListener) media.addListener(handleSystemChange);
})();
`;
