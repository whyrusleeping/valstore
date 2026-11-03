#!/usr/bin/env python3
"""Launch, inspect and tear down a vals benchmark fleet on AWS (plans/cloud-aws-runbook.md).

Everything goes through the `aws` CLI (credentials from the environment:
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION). Every
instance carries the tag `vals-run=<run>` and a self-destruct timer: user-data
starts a systemd timer that powers the machine off at the deadline, and
instances are launched with shutdown behaviour `terminate`, so a lost session
cannot leave a fleet billing. `down` terminates by tag; `sweep` lists anything
still running with a vals-run tag in the region.

  fleet.py plan  --profile c1-3node-i4i [--hours 3]
  fleet.py build [--rev HEAD] [--type c6i.8xlarge] [--spot]
  fleet.py up    --profile c1-3node-i4i --run c1-YYYYmmdd [--hours 3] [--az us-east-1a] [--yes]
  fleet.py status --run RUN
  fleet.py extend --run RUN --hours 1
  fleet.py down  --run RUN
  fleet.py sweep
  fleet.py local --run NAME            # dry-run fleet on this machine (file backend)
"""
import argparse
import datetime
import json
import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cloudlib as cl  # noqa: E402

RUNS = os.path.join(cl.REPO, "bench", "cloud", "runs")
ARTIFACTS = os.path.join(cl.REPO, "bench", "cloud", "artifacts")
PROFILES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "profiles")
SSH_KEY = os.path.expanduser("~/.ssh/vals-cloud")
KEY_NAME = "vals-cloud"
SG_NAME = "vals-cloud"
UBUNTU_AMI_PARAM = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
REMOTE_ROOT = "/home/ubuntu/vals"
PACKAGES = ("fio iperf3 nvme-cli numactl jq ethtool sysstat zstd rsync chrony pciutils hwloc-nox "
            "linux-tools-common libnuma1 libaio1t64 libuuid1 python3 gdb")
OPTIONAL_PACKAGES = "netperf linux-tools-aws"
QUOTA_CODES = {"on-demand": "L-1216C47A", "spot": "L-34B43A08"}


REGION = None


def resolve_region(fleet=None):
    """Pin every AWS call to one region: the fleet's recorded one, else the CLI's configured one."""
    global REGION
    REGION = (fleet or {}).get("region") or os.environ.get("AWS_DEFAULT_REGION") or subprocess.run(
        ["aws", "configure", "get", "region"], capture_output=True, text=True).stdout.strip()
    if not REGION:
        raise SystemExit("no AWS region: set AWS_DEFAULT_REGION")
    return REGION


def aws(*args, check=True):
    """Run `aws <args> --output json` in REGION; returns parsed JSON (or None for empty output)."""
    argv = ["aws"] + list(args) + ["--output", "json"] + (["--region", REGION] if REGION else [])
    res = subprocess.run(argv, capture_output=True, text=True, timeout=300)
    if check and res.returncode != 0:
        raise SystemExit("aws %s failed: %s" % (" ".join(args[:2]), res.stderr.strip()))
    if res.returncode != 0:
        return None
    return json.loads(res.stdout) if res.stdout.strip() else None


def load_profile(name):
    path = name if name.endswith(".json") else os.path.join(PROFILES, name + ".json")
    with open(path) as f:
        return json.load(f)


def roles_of(profile):
    return [{"role": r, **spec} for r, spec in profile["fleet"].items()]


# ---------------------------------------------------------------- plan

def cmd_plan(args):
    try:
        resolve_region()
    except SystemExit:
        pass
    p = load_profile(args.profile)
    roles = roles_of(p)
    print("profile %s: %s" % (args.profile, p.get("description", "")))
    print("prices: Linux on-demand, us-east-1/us-east-2, %s; verify before launch" % cl.PRICES_DATE)
    for r in roles:
        print("  %-7s %d x %-16s %-9s $%7.3f/h each  %4d vCPU each"
              % (r["role"], r["count"], r["type"], r.get("market", "on-demand"),
                 cl.hourly(r["type"], r.get("market", "on-demand")), cl.VCPUS[r["type"]]))
    rate = cl.fleet_hourly(roles)
    hours = args.hours or p.get("hours", 3)
    print("fleet: $%.2f/h; %.1f h => $%.0f (plus ~$1 build, a few $ EBS/egress)" % (rate, hours, rate * hours))
    for market in ("on-demand", "spot"):
        need = cl.fleet_vcpus(roles, market)
        if not need:
            continue
        q = aws("service-quotas", "get-service-quota", "--service-code", "ec2",
                "--quota-code", QUOTA_CODES[market], check=False)
        have = q["Quota"]["Value"] if q else None
        print("  %s vCPU needed %d, quota %s%s" % (market, need, have if have is not None else "unknown (no credentials?)",
                                                   "  <-- REQUEST AN INCREASE" if have is not None and have < need else ""))


# ---------------------------------------------------------------- shared AWS setup

def ensure_key():
    if not os.path.exists(SSH_KEY):
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "vals-cloud", "-f", SSH_KEY], check=True)
    have = aws("ec2", "describe-key-pairs", "--key-names", KEY_NAME, check=False)
    if not have:
        aws("ec2", "import-key-pair", "--key-name", KEY_NAME,
            "--public-key-material", "fileb://%s.pub" % SSH_KEY)


def my_ip():
    out = subprocess.run(["curl", "-s", "--max-time", "10", "https://checkip.amazonaws.com"],
                         capture_output=True, text=True).stdout.strip()
    if not out:
        raise SystemExit("could not determine this machine's public IP")
    return out


def default_vpc():
    vpcs = aws("ec2", "describe-vpcs", "--filters", "Name=isDefault,Values=true")["Vpcs"]
    if not vpcs:
        raise SystemExit("no default VPC in this region; create one (aws ec2 create-default-vpc)")
    return vpcs[0]["VpcId"]


def ensure_sg(vpc):
    """SG: ssh from this machine only; all traffic between members of the SG."""
    found = aws("ec2", "describe-security-groups", "--filters", "Name=group-name,Values=%s" % SG_NAME,
                "Name=vpc-id,Values=%s" % vpc)["SecurityGroups"]
    if found:
        sg = found[0]["GroupId"]
    else:
        sg = aws("ec2", "create-security-group", "--group-name", SG_NAME, "--vpc-id", vpc,
                 "--description", "vals cloud benchmark fleet")["GroupId"]
        aws("ec2", "authorize-security-group-ingress", "--group-id", sg,
            "--ip-permissions", json.dumps([{"IpProtocol": "-1", "UserIdGroupPairs": [{"GroupId": sg}]}]))
    cidr = my_ip() + "/32"
    aws("ec2", "authorize-security-group-ingress", "--group-id", sg, "--protocol", "tcp",
        "--port", "22", "--cidr", cidr, check=False)
    return sg


def subnet_for(vpc, az):
    subs = aws("ec2", "describe-subnets", "--filters", "Name=vpc-id,Values=%s" % vpc,
               "Name=default-for-az,Values=true")["Subnets"]
    if az:
        subs = [s for s in subs if s["AvailabilityZone"] == az]
    if not subs:
        raise SystemExit("no default subnet%s" % (" in " + az if az else ""))
    subs.sort(key=lambda s: s["AvailabilityZone"])
    return subs[0]["SubnetId"], subs[0]["AvailabilityZone"]


def ubuntu_ami():
    return aws("ssm", "get-parameters", "--names", UBUNTU_AMI_PARAM)["Parameters"][0]["Value"]


DEADLINE_UNITS = r"""
cat > /usr/local/sbin/vals-deadline-check <<'VALS_UNIT'
#!/bin/sh
exp=$(cat /var/lib/vals-deadline-epoch 2>/dev/null) || exp=0
[ "$exp" -gt 0 ] && [ "$(date +%s)" -ge "$exp" ] && exec /usr/bin/systemctl poweroff
exit 0
VALS_UNIT
chmod 755 /usr/local/sbin/vals-deadline-check
cat > /etc/systemd/system/vals-deadline.service <<'VALS_UNIT'
[Unit]
Description=vals cloud fleet self-destruct check (poweroff = terminate)
[Service]
Type=oneshot
ExecStart=/usr/local/sbin/vals-deadline-check
VALS_UNIT
cat > /etc/systemd/system/vals-deadline.timer <<'VALS_UNIT'
[Unit]
Description=vals cloud fleet self-destruct check every minute
[Timer]
OnBootSec=30s
OnUnitActiveSec=60s
AccuracySec=10s
[Install]
WantedBy=timers.target
VALS_UNIT
systemctl daemon-reload
systemctl enable --now vals-deadline.timer
"""


def user_data(deadline_secs):
    """cloud-init script: arm a reboot-persistent self-destruct deadline first, then install packages."""
    return "\n".join([
        "#!/bin/bash",
        "echo $(( $(date +%%s) + %d )) > /var/lib/vals-deadline-epoch" % deadline_secs,
        DEADLINE_UNITS.strip(),
        "export DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a",
        "apt-get update -q",
        "apt-get install -y -q %s || true" % PACKAGES,
        "for p in %s linux-tools-$(uname -r) linux-modules-extra-$(uname -r); do apt-get install -y -q $p || true; done"
        % OPTIONAL_PACKAGES,
        "mkdir -p %s && chown ubuntu:ubuntu %s" % (REMOTE_ROOT, REMOTE_ROOT),
        "touch /var/lib/vals-userdata-done",
        "",
    ])


def launch(run, role, spec, ami, sg, subnet, pg, deadline_secs, name):
    """One run-instances call for one instance, fully tagged at launch; returns its id."""
    market = spec.get("market", "on-demand")
    tags = [{"Key": "vals-run", "Value": run}, {"Key": "vals-role", "Value": role}, {"Key": "Name", "Value": name}]
    argv = ["ec2", "run-instances", "--image-id", ami, "--instance-type", spec["type"],
            "--count", "1", "--key-name", KEY_NAME, "--security-group-ids", sg,
            "--subnet-id", subnet, "--instance-initiated-shutdown-behavior", "terminate",
            "--metadata-options", "HttpTokens=required,HttpEndpoint=enabled",
            "--block-device-mappings", json.dumps([{"DeviceName": "/dev/sda1", "Ebs": {
                "VolumeSize": spec.get("root_gb", 100), "VolumeType": "gp3", "DeleteOnTermination": True}}]),
            "--tag-specifications", json.dumps([{"ResourceType": "instance", "Tags": tags},
                                                {"ResourceType": "volume", "Tags": tags}]),
            "--user-data", user_data(deadline_secs)]
    if pg:
        argv += ["--placement", "GroupName=%s" % pg]
    if market == "spot":
        argv += ["--instance-market-options", json.dumps({"MarketType": "spot", "SpotOptions": {
            "SpotInstanceType": "one-time", "InstanceInterruptionBehavior": "terminate"}})]
    return aws(*argv)["Instances"][0]["InstanceId"]


def terminate_run(run, ids=()):
    """Terminate everything tagged vals-run=run plus any ids we know of; returns the ids."""
    found = {i["InstanceId"] for i in describe(run=run)} | set(ids)
    if found:
        aws("ec2", "terminate-instances", "--instance-ids", *sorted(found), check=False)
    return sorted(found)


def describe(ids=None, run=None):
    filters = ["Name=instance-state-name,Values=pending,running,stopping,shutting-down"]
    if run:
        filters.append("Name=tag:vals-run,Values=%s" % run)
    else:
        filters.append("Name=tag-key,Values=vals-run")
    argv = ["ec2", "describe-instances", "--filters"] + filters
    if ids:
        argv += ["--instance-ids"] + ids
    out = []
    for r in aws(*argv)["Reservations"]:
        out += r["Instances"]
    return out


def wait_running(ids, timeout=2400):
    """Poll until every instance is running with a public IP (metal can take 10+ minutes)."""
    start = time.time()
    deadline = start + timeout
    while True:
        try:
            insts = describe(ids=ids)
        except SystemExit:
            if time.time() - start < 300:
                time.sleep(10)
                continue
            raise
        if len(insts) < len(ids) and time.time() - start < 300:
            time.sleep(10)
            continue
        ready = [i for i in insts if i["State"]["Name"] == "running" and i.get("PublicIpAddress")]
        if len(ready) == len(ids):
            return insts
        bad = [i["InstanceId"] for i in insts if i["State"]["Name"] in ("shutting-down", "terminated", "stopping")]
        if bad or len(insts) < len(ids):
            raise SystemExit("instances failed to start: %s" % (bad or sorted(set(ids) - {i["InstanceId"] for i in insts})))
        if time.time() > deadline:
            raise SystemExit("instances not running after %ds; `fleet.py down` terminates them by tag" % timeout)
        print("  %d/%d running" % (len(ready), len(ids)), flush=True)
        time.sleep(20)


def tag(inst, key):
    return next((t["Value"] for t in inst.get("Tags", []) if t["Key"] == key), None)


def wait_ssh(hosts, timeout=1800):
    """Wait until every host answers ssh and its user-data finished."""
    deadline = time.time() + timeout
    pending = list(hosts)
    while pending and time.time() < deadline:
        still = []
        for h in pending:
            out = cl.try_sh(h, "test -e /var/lib/vals-userdata-done && systemctl is-active --quiet vals-deadline.timer "
                               "&& test -s /var/lib/vals-deadline-epoch && echo ready", timeout=20)
            if "ready" not in out:
                still.append(h)
        if still:
            print("  waiting for %s" % ", ".join(h.name for h in still), flush=True)
            time.sleep(20)
        pending = still
    if pending:
        raise SystemExit("hosts never became ready: %s" % ", ".join(h.name for h in pending))


def fleet_hosts_json(insts, price_of):
    hosts = []
    counters = {}
    for inst in sorted(insts, key=lambda i: (tag(i, "vals-role"), tag(i, "Name") or "")):
        role = tag(inst, "vals-role")
        counters[role] = counters.get(role, 0) + 1
        h = {"name": tag(inst, "Name"), "role": role, "instance_id": inst["InstanceId"],
             "instance_type": inst["InstanceType"], "private_ip": inst.get("PrivateIpAddress"),
             "public_ip": inst.get("PublicIpAddress"), "ssh": "ubuntu@%s" % inst.get("PublicIpAddress"),
             "root": REMOTE_ROOT, "port_base": 7000,
             "market": "spot" if inst.get("InstanceLifecycle") == "spot" else "on-demand",
             "launch_time": inst["LaunchTime"]}
        h["price_per_hour"] = price_of(h)
        if role == "node":
            h["node_id"] = counters[role]
        hosts.append(h)
    return hosts


def save_fleet(run_dir, fleet):
    os.makedirs(run_dir, exist_ok=True)
    tmp = os.path.join(run_dir, ".fleet.json.tmp")
    with open(tmp, "w") as f:
        json.dump(fleet, f, indent=2)
    os.replace(tmp, os.path.join(run_dir, "fleet.json"))


# ---------------------------------------------------------------- up / down

def cmd_up(args):
    p = load_profile(args.profile)
    roles = roles_of(p)
    hours = args.hours or p.get("hours", 3)
    rate = cl.fleet_hourly(roles)
    resolve_region()
    run_dir = os.path.join(RUNS, args.run)
    if os.path.exists(os.path.join(run_dir, "fleet.json")):
        raise SystemExit("%s already has a fleet.json; pick a new run name" % run_dir)
    if describe(run=args.run):
        raise SystemExit("instances tagged vals-run=%s already exist" % args.run)
    print("launching %s: $%.2f/h, self-destruct after %.1f h (worst case $%.0f)" % (args.run, rate, hours, rate * hours))
    if not args.yes and input("type the run name to confirm: ").strip() != args.run:
        raise SystemExit("not confirmed")
    ensure_key()
    vpc = default_vpc()
    sg = ensure_sg(vpc)
    subnet, az = subnet_for(vpc, args.az)
    pg = "vals-%s" % args.run
    aws("ec2", "create-placement-group", "--group-name", pg, "--strategy", "cluster", check=False)
    ami = ubuntu_ami()
    deadline_secs = int(hours * 3600)
    deadline = cl.utcnow() + datetime.timedelta(seconds=deadline_secs)
    ids = []
    try:
        for r in roles:
            for i in range(r["count"]):
                ids.append(launch(args.run, r["role"], r, ami, sg, subnet, pg, deadline_secs,
                                  "%s-%s%d" % (args.run, r["role"], i + 1)))
    except BaseException:
        print("launch failed part-way; terminating run %s: %s" % (args.run, " ".join(terminate_run(args.run, ids))))
        raise
    fleet = {"run": args.run, "profile": args.profile, "profile_body": p,
             "region": REGION, "az": az, "placement_group": pg,
             "ami": ami, "security_group": sg, "ssh_key": SSH_KEY, "deadline": deadline.isoformat(),
             "created": cl.utcnow().isoformat(), "instance_ids": ids, "hosts": []}
    save_fleet(run_dir, fleet)
    print("launched %s; waiting for running state" % " ".join(ids))
    insts = wait_running(ids)
    fleet["hosts"] = fleet_hosts_json(insts, lambda h: cl.hourly(h["instance_type"], h["market"]))
    save_fleet(run_dir, fleet)
    _, hosts = cl.load_fleet(run_dir)
    wait_ssh(hosts)
    print("fleet %s ready; %s" % (args.run, os.path.join(run_dir, "fleet.json")))


def cmd_status(args):
    run_dir = os.path.join(RUNS, args.run)
    fleet, _ = cl.load_fleet(run_dir)
    resolve_region(fleet)
    live = {i["InstanceId"]: i for i in describe(run=args.run)}
    for h in fleet["hosts"]:
        inst = live.get(h["instance_id"])
        print("%-22s %-9s %-16s %-14s %-15s %s" % (h["name"], h["role"], h["instance_type"],
                                                  inst["State"]["Name"] if inst else "gone",
                                                  h.get("public_ip"), h["instance_id"]))
    print("accrued ~$%.2f; deadline %s" % (cl.accrued([h for h in fleet["hosts"]]), fleet["deadline"]))


def cmd_extend(args):
    run_dir = os.path.join(RUNS, args.run)
    fleet, hosts = cl.load_fleet(run_dir)
    secs = int(args.hours * 3600)

    def rearm(h):
        cl.sh(h, "echo $(( $(date +%%s) + %d )) | sudo tee /var/lib/vals-deadline-epoch.new >/dev/null && "
                 "sudo mv /var/lib/vals-deadline-epoch.new /var/lib/vals-deadline-epoch && "
                 "systemctl is-active --quiet vals-deadline.timer" % secs)

    cl.parallel(rearm, hosts)
    fleet["deadline"] = (cl.utcnow() + datetime.timedelta(seconds=secs)).isoformat()
    save_fleet(run_dir, fleet)
    print("deadline now %s" % fleet["deadline"])


def cmd_down(args):
    run_dir = os.path.join(RUNS, args.run)
    fleet = None
    if os.path.exists(os.path.join(run_dir, "fleet.json")):
        fleet, _ = cl.load_fleet(run_dir)
    resolve_region(fleet)
    known = (fleet or {}).get("instance_ids", []) + [h["instance_id"] for h in (fleet or {}).get("hosts", [])
                                                     if h.get("instance_id")]
    ids = terminate_run(args.run, known)
    if ids:
        print("terminating %s in %s" % (" ".join(ids), REGION))
        aws("ec2", "wait", "instance-terminated", "--instance-ids", *ids, check=False)
    aws("ec2", "delete-placement-group", "--group-name", "vals-%s" % args.run, check=False)
    if os.path.exists(os.path.join(run_dir, "fleet.json")):
        fleet, _ = cl.load_fleet(run_dir)
        end = cl.utcnow().isoformat()
        for h in fleet["hosts"]:
            h.setdefault("end_time", end)
        fleet["terminated"] = end
        save_fleet(run_dir, fleet)
        print("run %s down; accrued ~$%.2f" % (args.run, cl.accrued(fleet["hosts"])))
    left = describe(run=args.run)
    if left:
        raise SystemExit("STILL RUNNING: %s" % " ".join(i["InstanceId"] for i in left))


def cmd_sweep(args):
    resolve_region()
    insts = describe()
    if not insts:
        print("no vals instances running in %s" % os.environ.get("AWS_DEFAULT_REGION", "the default region"))
        return
    for i in insts:
        print("%-20s %-16s %-14s run=%s launched %s" % (i["InstanceId"], i["InstanceType"], i["State"]["Name"],
                                                        tag(i, "vals-run"), i["LaunchTime"]))
    sys.exit(1)


# ---------------------------------------------------------------- build

def cmd_build(args):
    """Build valsd + vals-bench with SPDK on a short-lived builder; download the artifact."""
    rev = subprocess.run(["git", "-C", cl.REPO, "rev-parse", args.rev], capture_output=True, text=True,
                         check=True).stdout.strip()
    os.makedirs(ARTIFACTS, exist_ok=True)
    art = os.path.join(ARTIFACTS, "vals-%s.tar.gz" % rev[:12])
    if os.path.exists(art) and not args.force:
        print("artifact exists: %s" % art)
        return
    run = "build-%s-%s" % (rev[:12], cl.stamp().lower())
    run_dir = os.path.join(RUNS, run)
    os.makedirs(run_dir, exist_ok=True)
    src = os.path.join(run_dir, "src.tar.gz")
    subprocess.run(["git", "-C", cl.REPO, "archive", "--format=tar.gz", "--prefix=valstore/", "-o", src, rev],
                   check=True)
    resolve_region()
    ensure_key()
    vpc = default_vpc()
    sg = ensure_sg(vpc)
    subnet, az = subnet_for(vpc, args.az)
    spec = {"type": args.type, "count": 1, "market": "spot" if args.spot else "on-demand", "root_gb": 80}
    ids = []
    try:
        ids.append(launch(run, "builder", spec, ubuntu_ami(), sg, subnet, None, 2 * 3600, run))
        insts = wait_running(ids)
        fleet = {"run": run, "ssh_key": SSH_KEY, "deadline": None,
                 "hosts": fleet_hosts_json(insts, lambda h: cl.hourly(h["instance_type"], h["market"]))}
        save_fleet(run_dir, fleet)
        _, hosts = cl.load_fleet(run_dir)
        b = hosts[0]
        wait_ssh(hosts)
        cl.sh(b, "mkdir -p /home/ubuntu/build")
        cl.push(b, src, "/home/ubuntu/build/src.tar.gz")
        cl.push(b, os.path.join(os.path.dirname(os.path.abspath(__file__)), "remote", "build.sh"),
                "/home/ubuntu/build/build.sh")
        job = cl.RemoteJob(b, "build", "bash /home/ubuntu/build/build.sh /home/ubuntu/build %s" % rev,
                           cwd="/home/ubuntu/build")
        job.start()
        rc = job.wait(timeout=5400, poll=30, on_poll=lambda: print("  building...", flush=True))
        log = job.output()
        with open(os.path.join(run_dir, "build.log"), "w") as f:
            f.write(log)
        if rc != 0:
            raise SystemExit("build failed (rc=%s); log in %s/build.log" % (rc, run_dir))
        tmp = os.path.join(run_dir, "dl")
        if cl.pull(b, "/home/ubuntu/build/artifact/vals-%s.tar.gz" % rev[:12], tmp + "/") != 0:
            raise SystemExit("artifact download failed")
        got = os.path.join(tmp, os.path.basename(art))
        names = subprocess.run(["tar", "-tzf", got], capture_output=True, text=True, check=True).stdout.split()
        for need in ("bin/valsd", "bin/vals-bench", "artifact.json", "spdk/scripts/setup.sh"):
            if "vals-%s/%s" % (rev[:12], need) not in names:
                raise SystemExit("artifact is missing %s" % need)
        os.replace(got, art)
        print("artifact: %s" % art)
    finally:
        print("builder terminating: %s" % " ".join(terminate_run(run, ids)))


# ---------------------------------------------------------------- local dry run

def cmd_local(args):
    """A fake fleet on this machine: three nodes and one client, file backend."""
    run_dir = os.path.join(RUNS, args.run)
    base = os.path.join(tempfile.gettempdir(), "vals-cloud-%s" % args.run)
    hosts = []
    for i in range(3):
        hosts.append({"name": "%s-node%d" % (args.run, i + 1), "role": "node", "node_id": i + 1,
                      "ssh": cl.LOCAL, "private_ip": "127.0.0.1", "root": "%s/node%d" % (base, i + 1),
                      "port_base": 27000 + 1000 * i, "instance_type": "local", "price_per_hour": 0.0})
    hosts.append({"name": "%s-client1" % args.run, "role": "client", "ssh": cl.LOCAL,
                  "private_ip": "127.0.0.1", "root": "%s/client1" % base, "port_base": 31000,
                  "instance_type": "local", "price_per_hour": 0.0})
    save_fleet(run_dir, {"run": args.run, "profile": args.profile, "profile_body": load_profile(args.profile),
                         "local": True, "deadline": None, "created": cl.utcnow().isoformat(), "hosts": hosts})
    print("local fleet in %s (roots under %s)" % (run_dir, base))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("plan")
    s.add_argument("--profile", required=True)
    s.add_argument("--hours", type=float)
    s = sub.add_parser("build")
    s.add_argument("--rev", default="HEAD")
    s.add_argument("--type", default="c6i.8xlarge")
    s.add_argument("--spot", action="store_true")
    s.add_argument("--az")
    s.add_argument("--force", action="store_true")
    s = sub.add_parser("up")
    s.add_argument("--profile", required=True)
    s.add_argument("--run", required=True)
    s.add_argument("--hours", type=float)
    s.add_argument("--az")
    s.add_argument("--yes", action="store_true")
    for name in ("status", "down"):
        s = sub.add_parser(name)
        s.add_argument("--run", required=True)
    s = sub.add_parser("extend")
    s.add_argument("--run", required=True)
    s.add_argument("--hours", type=float, required=True)
    sub.add_parser("sweep")
    s = sub.add_parser("local")
    s.add_argument("--run", required=True)
    s.add_argument("--profile", default="local-dry-run")
    args = ap.parse_args()
    globals()["cmd_" + args.cmd](args)


if __name__ == "__main__":
    main()
