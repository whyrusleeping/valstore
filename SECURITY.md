# Security

## Reporting a vulnerability

Please don't open a public issue for a security problem. Report it
privately through GitHub's "Report a vulnerability" button on this
repository's Security tab. Include what you found, how to reproduce it, and
which commit you tested.

vals has no releases yet, so fixes land on `master`.

## What vals does and doesn't protect

vals assumes it runs on a **trusted private network**. Read this before
exposing any port.

- **Nothing on the wire is encrypted.** That includes data traffic, PD Raft
  traffic, and the cluster secret itself. Run vals on a private network or a
  VPN such as WireGuard or Tailscale.
- **Data reads and writes are not authenticated.** Any process that can reach
  a data node's port can read, write, and delete any key.
- **The cluster secret protects only control operations.** With `secret_file`
  set on every node and PD, nodes refuse membership, catalog, and range-map
  changes, and the PD refuses Raft peers, unless they present the secret.
  The secret is sent as a plain token, so anyone who can observe the traffic
  can learn it. Without `secret_file`, control operations are open too.
- **The admin sockets are local Unix sockets, and access to one is powerful.**
  Anyone who can connect can change cluster membership and zones through the
  PD socket, and can make a node write a dump file to any path the daemon
  user can write (`dump <zone> <out-path>` overwrites existing files). SPDK
  nodes usually run as root. `valsd` does not set the socket's mode, so it
  follows the process umask: put the sockets in a directory only the daemon
  user can open (mode 0700), or set `UMask=0077` in the systemd unit.
- **The metrics endpoint is unauthenticated** HTTP. Bind it to a private
  address.

Reports about problems inside this model are welcome. Examples: a crafted
frame that crashes a node or corrupts data, a way around the control-op
check, or memory-safety bugs. Reports that only show the documented gaps
above, such as "data traffic is unencrypted", are known limitations rather
than vulnerabilities.
