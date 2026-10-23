# Optional Tailscale sidecar

Use [deploy/tailscale/compose.yaml](../deploy/tailscale/compose.yaml) when a Docker
gateway needs a private tailnet address and workers need ordinary outbound TCP to
tailnet hosts. Tailscale lives only in the sidecar. The gateway image, its child
processes, and SSH client don't need a Tailscale binary, socket, or auth key.
This is an opt-in, standalone Compose project. It doesn't change the default image.

The gateway shares the sidecar's network namespace. A kernel `tailscale0` interface
provides both incoming HTTP on port 3000 and outbound worker traffic. The optional
Serve overlay terminates tailnet HTTPS on port 443 and proxies to localhost:3000.
No host ports are published. Gateway admin authentication and webhook signature
checks still apply.

## Important webhook limitation

Tailscale Serve is private to your tailnet. Linear's hosted webhook sender and
agent callbacks can't reach a private `*.ts.net` address. This example alone is
therefore insufficient for a working hosted Linear integration. Keep a separately
reachable public HTTPS ingress for Linear's required webhook/callback routes and
configure `PUBLIC_URL` to that canonical public URL when using those integrations.
An ingress container can join the same namespace and proxy to localhost:3000;
publish its ports on the **tailscale service**, or route to it on a Docker network
attached to that service. Restrict public routes and preserve signature/auth checks.
The private admin UI and outbound Mac access can still use the tailnet.
This example doesn't enable Funnel or configure a public ingress.

## Setup

1. Use a Linux Docker engine with `/dev/net/tun` and support for `NET_ADMIN` and
   `NET_RAW`. On Docker Desktop these requirements apply inside its Linux VM.
   Rootless engines may not support the required device/capabilities.
2. Enable MagicDNS in the tailnet. Add the policy below and create a pre-approved,
   short-lived, single-use auth key tagged `tag:symphony`. For an always-on gateway,
   use a non-ephemeral node and keep its state volume. Ephemeral keys suit disposable
   deployments: the node disappears after disconnecting, and its next registration
   needs a usable key. Auth-key expiry and node-key expiry are separate settings.
3. From this directory, create local configuration and a secrets directory:

   ```nu
   cd deploy/tailscale
   cp .env.example .env
   mkdir secrets
   chmod 700 secrets
   ```

   Have your secret manager write these files, with no quotes or variable names:
   `secrets/ts_authkey`, `secrets/linear_client_id`,
   `secrets/linear_client_secret`, `secrets/linear_webhook_secret`, and
   `secrets/token_encryption_key`. Follow the gateway's normal credential setup;
   the encryption key must be a valid 32-byte key encoded as base64.
   Paths can instead point outside the checkout through the `.env` `*_FILE` values.
   Don't put actual credentials in `.env`, Compose arguments, or shell history.

   Local Compose secrets are read-only file mounts, not an encrypted secret store.
   Gateway source files must be root-owned with mode 0400, including the four
   gateway files above. Startup rejects insecure `_FILE` mounts. Protect their
   parent directory from worker writes. Compose file secrets don't implement
   ownership remapping with `uid`/`gid`/`mode`; set ownership and permissions on
   the Docker host. See [gateway security](security.md) for the complete policy.
   Only the sidecar receives the Tailscale key. Its native
   `TS_AUTHKEY=file:/run/secrets/ts_authkey` support avoids exporting the key value.
   `TS_AUTHKEY_FILE` in `.env` selects the **host file**, not a containerboot option.
   The gateway reads `TOKEN_ENCRYPTION_KEY_FILE` directly. Preserve the image's
   entrypoint and default command; no shell export of the encryption key is needed.
4. Set `TS_HOSTNAME`, `PUBLIC_URL` and `LINEAR_ALLOWED_ORGANIZATION_IDS` in `.env`.
   Production startup requires the allowed organization list. Configure
   `GATEWAY_ADMIN_ORGANIZATION_IDS` and `GITHUB_APP_ALLOWED_OWNERS_JSON` as described
   in [gateway security](security.md) before using those administrative features.
   With only private HTTPS, use the
   node's actual full `https://symphony.<tailnet>.ts.net` name. For Linear delivery,
   use the public URL described above. Optionally add your actual tailnet search
   suffix to `resolv.conf` for short hostnames.
5. For HTTPS, enable HTTPS certificates in the Tailscale admin console, then run:

   ```nu
   docker compose -f compose.yaml -f compose.serve.yaml config --quiet
   docker compose -f compose.yaml -f compose.serve.yaml up -d --build
   docker compose -f compose.yaml -f compose.serve.yaml ps
   ```

   Omit `-f compose.serve.yaml` for plain tailnet HTTP on port 3000. The checked-in
   Serve JSON uses containerboot's `${TS_CERT_DOMAIN}` substitution, not Compose
   environment interpolation. It mounts the config directory so updates are watched.
   If HTTPS isn't configured for the tailnet, Serve won't become usable.

Keep `tailscale-state` across recreations. `TS_AUTH_ONCE=true` reuses its identity.
After enrollment, replace the local auth-key file with an empty file to remove the
spent credential while retaining the required mount. Never delete the state volume
as routine maintenance, and don't clone it into another running deployment.

## Minimal tailnet policy example

Merge this fragment into your existing policy and replace the example admin email
and builder tag. Existing broad allow rules remain additive; remove overlapping
allow-all rules if these restrictions are intended to enforce isolation.

```json
{
  "tagOwners": {
    "tag:symphony": ["autogroup:admin"],
    "tag:mac-builder": ["autogroup:admin"]
  },
  "acls": [
    {
      "action": "accept",
      "src": ["admin@example.com"],
      "dst": ["tag:symphony:443", "tag:symphony:3000"]
    },
    {
      "action": "accept",
      "src": ["tag:symphony"],
      "dst": ["tag:mac-builder:22"]
    }
  ]
}
```

Tag the Mac appropriately, or replace its tag with a narrowly scoped host alias.
Remove the port 3000 grant if clients should use only HTTPS. Serve doesn't firewall
the gateway's direct port 3000 listener. No exit node, subnet routing, or Tailscale
SSH server is enabled on the gateway.

## Worker access to a Mac build host

Enable Remote Login on the Mac and authorize a dedicated build account and SSH
key. Provision a read-only SSH configuration, private key and verified `known_hosts`
entry for the **worker's** home/UID using your existing worker credential mechanism.
Don't disable host-key checking or give the gateway the Docker socket. A worker
can then run an ordinary SSH command, including from a tool invocation:

```nu
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes builder@mac-builder.your-tailnet.ts.net 'xcodebuild -version'
```

The full MagicDNS name works without a DNS search suffix. Repository workflow
commands can use the same SSH destination for builds, signing and artifact transfer.
No credentials, test build, or tailnet registration are included in this example.

Tailscale transport and Tailscale SSH are separate. macOS normally uses Apple's
OpenSSH Remote Login server; a Tailscale network grant to port 22 doesn't replace
its user/key checks. Tailscale SSH on a supported target intercepts port 22 and
requires a separate `ssh` policy, including a suitable `accept` rule for unattended
workers. macOS Tailscale SSH requires the open-source `tailscaled` variant;
the normal GUI variants use OpenSSH Remote Login. Don't enable `--ssh` on this
gateway just to make outbound SSH work. Match
the target's actual server and port, and provision the matching credentials.

## DNS and networking choices

The sidecar deliberately uses `TS_ACCEPT_DNS=false`, preserving Docker DNS for
bootstrap/control-plane lookups. The gateway bind-mounts its own `resolv.conf` with
only `nameserver 100.100.100.100`, Tailscale's Quad100 resolver. Kernel TUN routes
this address to tailscaled. MagicDNS answers and ordinary DNS forwarding follow
the tailnet's DNS policy. Workers inherit the gateway's mount and network namespaces,
including after a UID drop. DNS stops working if tailscaled is unavailable; no
public fallback resolver silently bypasses tailnet DNS policy.

`TS_ACCEPT_DNS=true` lets tailscaled manage the **sidecar's** resolver. It doesn't
guarantee that another container's independently mounted `/etc/resolv.conf` is
updated. Keep the explicit gateway resolver even if enabling it. Don't set `dns`,
`hostname`, `networks`, or `ports` on the gateway when using `network_mode: service:`;
put network attachments and port publication on the sidecar. A private Docker
service name isn't a MagicDNS name and may need a separate DNS forwarding design.

`TS_USERSPACE=false`, `/dev/net/tun`, `NET_ADMIN` and `NET_RAW` belong only to the
sidecar. Userspace mode removes those requirements, and can handle Serve ingress,
but doesn't install transparent kernel routes for ordinary gateway/worker sockets.
Simply setting `TS_USERSPACE=true` breaks this example's outbound SSH and Quad100
resolver. A userspace alternative needs `TS_SOCKS5_SERVER` or an HTTP proxy plus
explicit client proxy configuration, such as SSH `ProxyCommand` with a SOCKS-aware
connector and remote DNS resolution. That isn't equivalent to this transparent
deployment, so no userspace override is supplied.

## Hardening and worker identities

Both containers use `no-new-privileges`. The gateway keeps the image's root
supervisor and entrypoint, with exactly CHOWN, DAC_OVERRIDE, KILL, SETUID, SETGID
and SETPCAP after `cap_drop: ALL`. Workers drop to uid/gid 1001 through `setpriv`,
with no supplementary groups, capabilities or privilege escalation. See
[gateway security](security.md) for the authoritative worker and workspace policy.
The entrypoint locks `/app/data` and `/app/logs` to root-owned mode 0700;
`/app/workspaces` stays a separate writable volume. Never force `user: bun` or
remove DAC_OVERRIDE or SETPCAP. The gateway receives no TUN device, Tailscale
state, LocalAPI socket or auth key. Only the sidecar receives network capabilities.

No network capability is needed by the supervisor or UID 1001 worker. The DNS file
is world-readable, so dropping UID doesn't affect resolution. `no-new-privileges`
allows a supervisor with existing identity-changing capabilities to drop UID; it
prevents regaining privileges via setuid executables. Network namespace sharing
doesn't share process, user or mount namespaces and doesn't undo a worker UID drop.

Gateway and workers do share the same tailnet identity, so tailnet policy can't
distinguish them by Unix UID. All peer containers in the namespace can also reach
loopback listeners, including the sidecar health endpoint. Never mount the sidecar
LocalAPI socket into workers. Separate identities require separate namespaces.

## Troubleshooting and lifecycle

- `tailscale` unhealthy: inspect `docker compose logs tailscale` and
  `docker compose exec tailscale tailscale status`. `/healthz` checks that a tailnet
  IP exists, not SSH reachability or gateway health. A logged-out node is correctly
  unhealthy, and the gateway waits for it. Check key/tag permissions and TUN access.
- Gateway unhealthy: inspect its logs and `/health`. Its reconciler can return 503;
  a healthy Tailscale daemon doesn't prove the application is ready.
- Names fail: run `docker compose exec gateway cat /etc/resolv.conf`, check MagicDNS
  is enabled, and try the full `*.ts.net` name. A numeric tailnet IP working while
  the name fails points to DNS. `tailscale ping` inside the sidecar alone doesn't
  prove that a worker can resolve names or open SSH sockets.
- SSH fails: distinguish DNS, tailnet port grants, target Remote Login, Unix user,
  SSH key readability by the worker UID, and `known_hosts`. Test from an actual
  worker; the supervisor's credentials may differ.
- TUN/iptables errors: check the Linux VM/host's TUN and netfilter support. This
  example uses upstream Tailscale, without Toolbox's host-specific iptables/MTU
  patches. For stalled large transfers check path MTU and ICMP handling first.
- Recreate the gateway whenever replacing the sidecar container. An existing
  gateway can stay attached to the old namespace after an independent sidecar
  replacement. Run the same full Compose invocation with `up -d --force-recreate`
  for both services. `depends_on.restart: true` helps with explicit Compose updates;
  Docker restart policies don't provide general dependency recovery.

## Image provenance and verification

On 2026-10-23, `docker buildx imagetools inspect tailscale/tailscale:stable` and
`docker buildx imagetools inspect tailscale/tailscale:v1.104.2` both returned the
multi-platform index digest pinned in Compose. GitHub's
[latest stable release](https://github.com/tailscale/tailscale/releases/tag/v1.104.2)
was v1.104.2, published 2026-10-21. Refresh both tag and digest together after
reviewing upstream releases; a floating `stable` tag alone isn't reproducible.

Validate either configuration without reading secret contents or joining a tailnet:

```nu
docker compose -f deploy/tailscale/compose.yaml config --quiet
docker compose -f deploy/tailscale/compose.yaml -f deploy/tailscale/compose.serve.yaml config --quiet
```

For a logged-out runtime probe, use an isolated scratch Compose override with an
internal Docker network, fresh volumes, an empty auth-key file, dummy gateway
credentials and `PUBLIC_URL=http://localhost:3000`. Temporarily use
`condition: service_started` for the gateway and omit Serve. Assert `NeedsLogin`,
an unhealthy sidecar, equal `/proc/self/ns/net` links, a responding gateway health
endpoint through sidecar localhost, a root gateway with the six-capability policy,
and a worker launched through `setpriv` as uid/gid 1001 with no capabilities. Tear
down only that probe's containers and volumes. This tests local wiring; it cannot
prove authenticated MagicDNS, tailnet HTTPS, policy or remote Mac builds.

Primary references: [Docker parameters](https://tailscale.com/docs/features/containers/docker/docker-params),
[pinned containerboot source](https://github.com/tailscale/tailscale/blob/v1.104.2/cmd/containerboot/main.go),
[MagicDNS](https://tailscale.com/kb/1081/magicdns),
[userspace networking](https://tailscale.com/kb/1112/userspace-networking),
[Tailscale SSH](https://tailscale.com/kb/1193/tailscale-ssh).
