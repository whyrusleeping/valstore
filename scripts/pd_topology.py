"""PD replica config generation and checks for box deployments (docs/deploy.md#PD config reference).

Two modes:
  three        the reference deployment: three voting PD replicas, one per host, each with
               the identical full `pd_peer` voter list (including itself at its advertised
               address), a separate `listen` bind address and exactly one `campaign = true`.
  single-perf  the explicitly non-HA single-PD performance-test mode: one replica, no raft peers.

pd_confs() renders the configs; check_pd_confs() validates rendered or deployed text and returns
a list of problems (empty when the set is consistent). deployed_mode() and provision_mode() decide
which mode existing PD state belongs to and whether a provision may keep it.
"""
import ipaddress

MODES = ("three", "single-perf")
RAFT_PORT = 7100
SINGLE_PERF_MARKER = "# single-PD performance-test mode: not HA (docs/deploy.md#PD config reference)"
PER_REPLICA_KEYS = {"pd_id", "data_dir", "listen", "pd_peer", "campaign", "join", "admin_socket", "metrics"}


def pd_confs(mode, hosts, shared_lines, data_dir, admin_socket, metrics=None, raft_port=RAFT_PORT, campaigner=None):
    """Render PD configs.

    hosts: ordered {host: {"id": pd_id, "ip": advertised_ip}}; mode "three" needs exactly three
    and uses each host's id as its pd_id; "single-perf" uses the first host only, as pd_id 1.
    shared_lines: config lines every replica must carry identically (data_node, rf, vnodes, zone,
    secret_file, ...). campaigner: the host whose replica campaigns at boot (default: first).
    """
    if mode not in MODES:
        raise ValueError("unknown PD mode %r (expected one of %s)" % (mode, ", ".join(MODES)))
    names = list(hosts)
    if mode == "single-perf":
        host = names[0]
        lines = [SINGLE_PERF_MARKER, "pd_id = 1", "data_dir = %s" % data_dir, "admin_socket = %s" % admin_socket]
        if metrics:
            lines.append("metrics = %s" % metrics)
        return {host: "\n".join(lines + list(shared_lines)) + "\n"}
    if len(names) != 3:
        raise ValueError("three-PD mode needs exactly three hosts, got %d" % len(names))
    campaigner = campaigner or names[0]
    if campaigner not in hosts:
        raise ValueError("campaigner %r is not a PD host" % campaigner)
    peers = ["pd_peer = %d @ %s:%d" % (hosts[h]["id"], hosts[h]["ip"], raft_port) for h in names]
    out = {}
    for h in names:
        lines = ["pd_id = %d" % hosts[h]["id"], "data_dir = %s" % data_dir, "listen = 0.0.0.0:%d" % raft_port]
        lines += peers
        lines += ["campaign = %s" % ("true" if h == campaigner else "false"), "admin_socket = %s" % admin_socket]
        if metrics:
            lines.append("metrics = %s" % metrics)
        out[h] = "\n".join(lines + list(shared_lines)) + "\n"
    return out


def parse(text):
    """Return [(key, value)] in file order, dropping blank lines and `#` comments."""
    out = []
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        key, sep, value = line.partition("=")
        out.append((key.strip(), value.strip() if sep else None))
    return out


def _addr(value):
    host, _, port = value.rpartition(":")
    return host.strip("[]"), int(port)


def _peer(value):
    pid, _, addr = value.partition("@")
    return int(pid.strip()), addr.strip()


def check_pd_confs(mode, confs, advertised):
    """Check a PD config set. confs: {host: text}; advertised: {host: ip other hosts reach it at}."""
    errors = []
    if mode not in MODES:
        return ["unknown PD mode %r" % mode]
    parsed = {h: parse(t) for h, t in confs.items()}
    if mode == "single-perf":
        if len(confs) != 1:
            errors.append("single-perf mode needs exactly one PD config, got %d" % len(confs))
        for h, kv in parsed.items():
            if SINGLE_PERF_MARKER not in confs[h]:
                errors.append("%s: single-PD config lacks the performance-test marker line" % h)
            for k in ("pd_peer", "listen", "campaign", "join"):
                if any(key == k for key, _ in kv):
                    errors.append("%s: single-PD config sets `%s`" % (h, k))
        return errors
    if len(confs) != 3:
        errors.append("three-PD mode needs three PD configs, got %d" % len(confs))
    peer_lists, ids, campaigners, shared = {}, {}, [], {}
    for h, kv in parsed.items():
        vals = {}
        for k, v in kv:
            vals.setdefault(k, []).append(v)
        missing = [k for k in ("pd_id", "data_dir", "listen") if len(vals.get(k, [])) != 1]
        errors += ["%s: needs exactly one `%s`" % (h, k) for k in missing]
        if missing:
            continue
        pd_id = int(vals["pd_id"][0])
        ids[h] = pd_id
        peers = sorted(_peer(v) for v in vals.get("pd_peer", []))
        peer_lists[h] = peers
        if len({p for p, _ in peers}) != len(peers):
            errors.append("%s: duplicate pd_peer id" % h)
        own = [a for p, a in peers if p == pd_id]
        listen_host, listen_port = _addr(vals["listen"][0])
        if len(own) != 1:
            errors.append("%s: pd_peer list has no entry for its own pd_id %d" % (h, pd_id))
        else:
            own_host, own_port = _addr(own[0])
            if own_host != advertised.get(h):
                errors.append("%s: own pd_peer address %s is not its advertised address %s" % (h, own[0], advertised.get(h)))
            if own_port != listen_port:
                errors.append("%s: own pd_peer port %d differs from listen port %d" % (h, own_port, listen_port))
            if listen_host not in ("0.0.0.0", "::", own_host):
                errors.append("%s: listen %s binds neither a wildcard nor the advertised address" % (h, vals["listen"][0]))
        for p, a in peers:
            try:
                if ipaddress.ip_address(_addr(a)[0]).is_unspecified:
                    errors.append("%s: pd_peer %d uses wildcard address %s" % (h, p, a))
            except ValueError:
                pass
        if vals.get("join") and vals["join"] != ["false"]:
            errors.append("%s: `join` is only for replacing a lost voter, not a reference bootstrap" % h)
        if vals.get("campaign") == ["true"]:
            campaigners.append(h)
        shared[h] = sorted((k, v) for k, v in kv if k not in PER_REPLICA_KEYS)
    if len(set(ids.values())) != len(ids):
        errors.append("pd_id values are not distinct: %s" % ids)
    if len({advertised.get(h) for h in confs}) != len(confs):
        errors.append("PD replicas share a host address, so a host loss takes more than one voter: %s"
                      % {h: advertised.get(h) for h in confs})
    lists = {tuple(v) for v in peer_lists.values()}
    if len(lists) > 1:
        errors.append("pd_peer voter lists differ: %s" % peer_lists)
    for h, peers in peer_lists.items():
        if sorted(p for p, _ in peers) != sorted(ids.values()):
            errors.append("%s: pd_peer ids %s are not the PD ids %s" % (h, [p for p, _ in peers], sorted(ids.values())))
        for p, a in peers:
            owner = [o for o, i in ids.items() if i == p]
            if owner and _addr(a)[0] != advertised.get(owner[0]):
                errors.append("%s: pd_peer %d -> %s, but pd %d's advertised address is %s" % (h, p, a, p, advertised.get(owner[0])))
    if len(campaigners) != 1:
        errors.append("exactly one replica must set `campaign = true`, got %s" % campaigners)
    if len({tuple(v) for v in shared.values()}) > 1:
        errors.append("shared PD settings (data_node, rf, vnodes, zone, secret_file, ...) differ between replicas")
    return errors


def deployed_mode(conf_hosts, all_hosts, single_host):
    """The mode existing PD configs describe: None (no PD config anywhere), "single-perf" (only
    single_host has one) or "three" (every host has one). Anything else raises ValueError."""
    conf_hosts = set(conf_hosts)
    if not conf_hosts:
        return None
    if conf_hosts == {single_host}:
        return "single-perf"
    if conf_hosts == set(all_hosts):
        return "three"
    raise ValueError("PD configs on %s match no known mode" % sorted(conf_hosts))


def provision_mode(recorded, deployed, state_hosts, target, reset):
    """Mode a provision should write, or raise ValueError when that would reuse PD state of
    another (or an unknown) mode. recorded: the harness's mode marker or None; deployed: what
    deployed_mode() found; state_hosts: hosts holding a PD data_dir; target: the requested mode or
    None (keep the current one); reset: the existing PD data_dirs are moved aside."""
    current = recorded or deployed
    if recorded and deployed and recorded != deployed:
        raise ValueError("recorded PD mode %s disagrees with the deployed configs (%s)" % (recorded, deployed))
    mode = target or current or "three"
    if mode not in MODES:
        raise ValueError("unknown PD mode %r" % mode)
    if state_hosts and not reset:
        if current is None:
            raise ValueError("PD state exists on %s but its mode is unknown: pass --reset-pd" % sorted(state_hosts))
        if mode != current:
            raise ValueError("changing the PD mode from %s to %s starts a new PD group: pass --reset-pd" % (current, mode))
    return mode
