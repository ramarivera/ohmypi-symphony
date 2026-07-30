/**
 * Static HTML body for the unauthenticated landing / onboarding page.
 *
 * The single CTA is an ordinary `<a href="/oauth/start">` anchor; no
 * JavaScript is required. All copy is author-controlled and is safe to embed
 * as a constant string — no API or user data is interpolated here.
 */
import { THEME_CONTROLS } from "./theme";
export const LANDING_BODY = `
<header class="landing-header">
  <div class="brand"><strong>OhMyPi</strong> &nbsp;↔&nbsp; Linear Control Plane</div>
  <div class="landing-header-tools">
    <div class="version">v1 · operator console</div>
    ${THEME_CONTROLS}
  </div>
</header>

<main class="landing" role="main">
  <section class="hero" aria-labelledby="hero-heading">
    <h1 id="hero-heading">Wire every Linear issue<br>to a <em>real</em> workspace.</h1>
    <p class="lead">
      This gateway sits between your Linear workspace and the agent that actually does the work.
      Connect Linear once, point it at the repositories the gateway should route to, and every
      issue you assign ends up in a checked-out branch with a live session attached.
    </p>

    <div class="cta-row">
      <a class="cta" href="/oauth/start" data-testid="connect-linear">
        Connect Linear <span aria-hidden="true">→</span>
      </a>
      <span class="cta-hint">scopes: read · write · app:assignable · app:mentionable</span>
    </div>

    <div class="trust" role="note">
      <span class="lock" aria-hidden="true"></span>
      <span>Your Linear OAuth credentials are encrypted at rest with AES-GCM
        and never leave this server.</span>
    </div>
  </section>

  <hr>

  <section aria-labelledby="what-happens">
    <h2 id="what-happens" class="visually-hidden-soft">What happens when you connect</h2>
    <ol class="steps">
      <li>
        <div>
          <h3>Hand-off to Linear</h3>
          <p>
            We open a Linear OAuth handshake using the official
            <code>linear.app/oauth/authorize</code> endpoint with the four scopes needed
            for assignment, mention, and read access. No password ever touches this server.
          </p>
        </div>
      </li>
      <li>
        <div>
          <h3>Encrypt and store</h3>
          <p>
            The rotating access and refresh tokens come back to the gateway, get sealed with
            the server-side key, and stay here until you remove the installation. Revocation
            is one click and immediately retires every live session.
          </p>
        </div>
      </li>
      <li>
        <div>
          <h3>Route the first issue</h3>
          <p>
            Pick the repositories the gateway may route to, set a default, and the next
            assigned issue spins up an OhMyPi session in the right worktree with the right
            ref — automatically.
          </p>
        </div>
      </li>
    </ol>
  </section>
</main>

<footer class="landing-footer" role="contentinfo">
  <span>Self-hosted operator console · single-user</span>
  <span>Already configured? <a href="/admin">Open the console</a></span>
</footer>
`;
