#!/usr/bin/env python3
# Copyright 2026 Democratized Data Foundation
#
# This file is part of the DefraDB test suite.
#
# The DefraDB test suite is licensed under either:
#
#   (1) GNU Affero General Public License v3
#   (2) Business Source License 1.1
#
# See tests/LICENSE for details.
"""Cross-device range-based set-reconciliation benchmark orchestrator.

Drives N instrumented `benchnode` processes (cmd/benchnode) wired into a chosen
topology, runs the default-sync vs reconciliation ("ranges") contrast over a
scenario, and records per-node control bytes / payload / wall-clock to CSV.

It is dependency-free (standard library only), matching tests/bench/reconcile/
plot.py. Three orthogonal inputs select what runs:

  placement  --devices local:N | ssh:hostA,hostB,...   (where the nodes run)
  topology   --topology pair|line|ring|star|mesh|tree:<fanout> | --topology-file
  scenario   --scenario cold-start|tiny-diff|multi-writer

The node count comes from the topology; placement supplies that many endpoints.
Convergence is detected by polling /blockstats until every node reports an
identical (blocks, blockBytes) — after full convergence every node holds the same
block set, so equality is an identity-free correctness + fixpoint signal.

Example (the committed smoke run):
  python3 run.py --devices local:3 --topology line --scenario multi-writer --docs 30

See README.md for the metrics rationale and the SSH recipe for real devices.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

COLLECTION = "Item"
SCHEMA_SDL = "type Item {\n\tname: String\n\tvalue: Int\n}\n"

REPO_BENCHNODE_PKG = "./tests/bench/crossdevice/cmd/benchnode"


# --------------------------------------------------------------------------
# HTTP control-API client
# --------------------------------------------------------------------------

def _req(url, obj=None, timeout=60):
    data = None if obj is None else json.dumps(obj).encode()
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = urllib.request.Request(url, data=data, headers=headers,
                                 method="POST" if data is not None else "GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    return json.loads(body) if body else {}


class Node:
    """A handle to one running benchnode: its control URL and identity."""

    def __init__(self, index, control_url, proc=None, ssh_host=None):
        self.index = index
        self.control_url = control_url
        self.proc = proc
        self.ssh_host = ssh_host
        self.peer_id = None
        self.addrs = None

    def get(self, path, timeout=60):
        return _req(self.control_url + path, None, timeout)

    def post(self, path, obj, timeout=120):
        return _req(self.control_url + path, obj, timeout)

    def wait_ready(self, timeout=20):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                info = self.get("/peerinfo", timeout=3)
                self.peer_id = info["peerID"]
                self.addrs = info["addrs"]
                return
            except (urllib.error.URLError, OSError, KeyError) as e:
                last = e
                time.sleep(0.25)
        raise RuntimeError(f"node {self.index} not ready at {self.control_url}: {last}")


# --------------------------------------------------------------------------
# Placement: where the nodes run
# --------------------------------------------------------------------------

def build_benchnode():
    """Build the benchnode binary natively; return its path."""
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".bin", "benchnode")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    print(f"building benchnode -> {out}")
    subprocess.run(["go", "build", "-o", out, REPO_BENCHNODE_PKG], check=True,
                   cwd=_repo_root())
    return out


def _repo_root():
    # this file lives at <repo>/tests/bench/crossdevice/run.py
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


# Control port base avoids 7000/7100 (macOS AirPlay Receiver / Control Center).
def provision_local(n, reconciliation, binary, control_base=17700, p2p_base=9300):
    """Spawn n benchnode subprocesses locally on distinct fixed ports."""
    nodes = []
    for i in range(n):
        control = f"127.0.0.1:{control_base + i}"
        p2p = f"/ip4/127.0.0.1/tcp/{p2p_base + i}"
        args = [binary, "--control", control, "--p2p", p2p, "--store", "memory"]
        if reconciliation:
            args.append("--reconciliation")
        proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        nodes.append(Node(i, f"http://{control}", proc=proc))
    return nodes


def provision_ssh(hosts, reconciliation, binary, control_port=17700, p2p_base=9300):
    """Launch benchnode on each ssh host. The binary must already exist on the
    remote at `binary` (cross-compiled for the remote GOOS/GOARCH and copied over,
    e.g. via scp) — see README. Each remote node binds its control API and p2p on
    the host's reachable interface."""
    nodes = []
    for i, host in enumerate(hosts):
        control = f"0.0.0.0:{control_port}"
        p2p = f"/ip4/0.0.0.0/tcp/{p2p_base}"
        remote = [binary, "--control", control, "--p2p", p2p, "--store", "memory"]
        if reconciliation:
            remote.append("--reconciliation")
        # nohup so the node outlives the ssh channel; orchestrator reaches it via host:control_port.
        cmd = ["ssh", host, "nohup", *remote, ">/tmp/benchnode.log", "2>&1", "&"]
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        hostname = host.split("@")[-1]
        nodes.append(Node(i, f"http://{hostname}:{control_port}", ssh_host=host))
    return nodes


def provision_node(specs, reconciliation):
    """Connect to bench-nodes already running at the given (host, port) control
    endpoints — no launch, no teardown (the operator manages their lifecycle).
    The operator must have started them with the matching --reconciliation flag for
    the mode being run (a reconciliation node auto-reconciles on connect, which
    would contaminate a 'default' measurement)."""
    return [Node(i, f"http://{h}:{p}") for i, (h, p) in enumerate(specs)]


def teardown(nodes):
    for nd in nodes:
        if nd.proc is not None:
            nd.proc.terminate()
    for nd in nodes:
        if nd.proc is not None:
            try:
                nd.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                nd.proc.kill()
        if nd.ssh_host is not None:
            # '[b]enchnode' so the remote pkill pattern does not match its own shell.
            subprocess.run(["ssh", nd.ssh_host, "pkill", "-f", "[b]enchnode"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# --------------------------------------------------------------------------
# Topology: the graph the nodes form
# --------------------------------------------------------------------------

def topology(name, n):
    """Return (node_count, edges) for a named generator. edges are undirected
    [i, j] pairs. n is the requested node count (ignored by `pair`)."""
    if name == "pair":
        return 2, [[0, 1]]
    if name == "line":
        return n, [[i, i + 1] for i in range(n - 1)]
    if name == "ring":
        return n, [[i, (i + 1) % n] for i in range(n)]
    if name == "star":
        return n, [[0, i] for i in range(1, n)]
    if name == "mesh":
        return n, [[i, j] for i in range(n) for j in range(i + 1, n)]
    if name.startswith("tree:"):
        fanout = int(name.split(":", 1)[1])
        edges = [[(i - 1) // fanout, i] for i in range(1, n)]
        return n, edges
    raise ValueError(f"unknown topology {name!r}")


def topology_from_file(path):
    with open(path) as f:
        spec = json.load(f)
    return spec["nodes"], spec["edges"], spec.get("seeds")


# --------------------------------------------------------------------------
# Scenario + convergence engine
# --------------------------------------------------------------------------

def blockstats(nodes):
    return [nd.get("/blockstats") for nd in nodes]


def all_equal(stats):
    first = (stats[0]["blocks"], stats[0]["blockBytes"])
    return all((s["blocks"], s["blockBytes"]) == first for s in stats)


def connect_edges(nodes, edges, settle):
    for i, j in edges:
        nodes[i].post("/connect", {"addrs": nodes[j].addrs})
    time.sleep(settle)  # let the gossipsub topic mesh form


def drive_to_fixpoint(mode, nodes, edges, all_docids, max_rounds, settle):
    """Repeat anti-entropy rounds over the edges until every node reports an
    identical blockstore (full convergence) or max_rounds is hit. Returns
    (rounds, wall_ms, converged)."""
    start = time.time()
    rounds = 0
    while rounds < max_rounds:
        rounds += 1
        if mode == "ranges":
            # One reconcile per edge converges both endpoints (bidirectional push).
            for i, j in edges:
                nodes[i].post("/reconcile", {"peerID": nodes[j].peer_id, "collection": COLLECTION})
        else:
            # Default broadcast: every node pulls the global docID list from its
            # neighbours. The list must be supplied — the default path cannot
            # discover which docIDs exist; that is the asymmetry reconciliation removes.
            for nd in nodes:
                nd.post("/sync", {"collection": COLLECTION, "docIDs": all_docids})
            time.sleep(settle)  # SyncDocuments is async; let merges land
        if all_equal(blockstats(nodes)):
            return rounds, (time.time() - start) * 1000, True
    return rounds, (time.time() - start) * 1000, False


def add_schema(nodes):
    for nd in nodes:
        nd.post("/schema", {"sdl": SCHEMA_SDL})


def seed_scenario(scenario, nodes, edges, docs, settle, max_rounds):
    """Seed initial data per the scenario and (for tiny-diff) establish a shared
    base via an untimed convergence. Returns the global list of docIDs."""
    add_schema(nodes)
    n = len(nodes)

    if scenario == "cold-start":
        # One seed node holds everything; the rest start empty.
        res = nodes[0].post("/seed", {"collection": COLLECTION, "prefix": "d", "count": docs, "updates": 0})
        return res["docIDs"]

    if scenario == "multi-writer":
        # Each node seeds a disjoint shard; convergence is the union.
        per = max(1, docs // n)
        all_ids = []
        for nd in nodes:
            res = nd.post("/seed", {"collection": COLLECTION, "prefix": f"n{nd.index}-", "count": per, "updates": 0})
            all_ids += res["docIDs"]
        return all_ids

    if scenario == "tiny-diff":
        # Shared base of `docs` docs (seeded on node 0), converged to everyone
        # untimed, then exactly one doc diverged on node 0.
        res = nodes[0].post("/seed", {"collection": COLLECTION, "prefix": "d", "count": docs, "updates": 0})
        all_ids = res["docIDs"]
        connect_edges(nodes, edges, settle)
        # Untimed base convergence (use ranges if available, else default — but the
        # node set is one mode at a time, so reuse that mode's path generically via
        # reconcile when peer ids resolve; default falls back to /sync).
        _untimed_base(nodes, edges, all_ids, max_rounds, settle)
        nodes[0].post("/update", {"collection": COLLECTION, "docID": all_ids[0], "value": 1})
        return all_ids

    raise ValueError(f"unknown scenario {scenario!r}")


def _untimed_base(nodes, edges, all_docids, max_rounds, settle):
    """Bring all nodes to the same base, mode-agnostically: try reconcile per edge,
    fall back to default sync. Stops when blockstores equalise."""
    for _ in range(max_rounds):
        for i, j in edges:
            try:
                nodes[i].post("/reconcile", {"peerID": nodes[j].peer_id, "collection": COLLECTION})
            except urllib.error.HTTPError:
                nodes[i].post("/sync", {"collection": COLLECTION, "docIDs": all_docids})
                nodes[j].post("/sync", {"collection": COLLECTION, "docIDs": all_docids})
        time.sleep(settle)
        if all_equal(blockstats(nodes)):
            return
    raise RuntimeError("tiny-diff base did not converge during setup")


# --------------------------------------------------------------------------
# Run a single (topology, scenario, mode)
# --------------------------------------------------------------------------

def run_mode(mode, n, edges, scenario, docs, binary, devices, settle, max_rounds):
    reconciliation = (mode == "ranges")
    kind, spec = devices
    if kind == "local":
        nodes = provision_local(n, reconciliation, binary)
    elif kind == "node":
        if len(spec) < n:
            raise RuntimeError(f"topology needs {n} nodes but only {len(spec)} node endpoints given")
        nodes = provision_node(spec[:n], reconciliation)
    else:
        if len(spec) < n:
            raise RuntimeError(f"topology needs {n} nodes but only {len(spec)} ssh hosts given")
        nodes = provision_ssh(spec[:n], reconciliation, binary)
    try:
        for nd in nodes:
            nd.wait_ready()
        all_docids = seed_scenario(scenario, nodes, edges, docs, settle, max_rounds)
        if scenario != "tiny-diff":
            connect_edges(nodes, edges, settle)
        for nd in nodes:
            nd.post("/counters/reset", {})
        rounds, wall_ms, converged = drive_to_fixpoint(mode, nodes, edges, all_docids, max_rounds, settle)
        rows = []
        for nd in nodes:
            c = nd.get("/counters")
            bs = nd.get("/blockstats")
            rows.append({
                "nodeID": nd.index,
                "ctrlBytesSent": c["totalBytesSent"], "ctrlBytesRecv": c["totalBytesRecv"],
                "ctrlMsgs": c["totalMsgsSent"] + c["totalMsgsRecv"],
                "blocks": bs["blocks"], "blockBytes": bs["blockBytes"],
                "wallMs": round(wall_ms, 1), "rounds": rounds, "converged": converged,
            })
        return rows, converged
    finally:
        teardown(nodes)


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def parse_devices(s):
    if s.startswith("local:"):
        return ("local", int(s.split(":", 1)[1]))
    if s.startswith("ssh:"):
        return ("ssh", s.split(":", 1)[1].split(","))
    if s.startswith("node:"):
        # node:host:port,host:port — connect to already-running bench-nodes.
        specs = []
        for hp in s.split(":", 1)[1].split(","):
            h, p = hp.rsplit(":", 1)
            specs.append((h, int(p)))
        return ("node", specs)
    raise ValueError("--devices must be local:N, ssh:host,..., or node:host:port,...")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--devices", required=True, help="local:N or ssh:hostA,hostB,...")
    ap.add_argument("--topology", default="pair", help="pair|line|ring|star|mesh|tree:<fanout>")
    ap.add_argument("--topology-file", help="path to a {nodes,edges,seeds} JSON graph")
    ap.add_argument("--scenario", default="cold-start", choices=["cold-start", "tiny-diff", "multi-writer"])
    ap.add_argument("--docs", type=int, default=30, help="documents to seed (total)")
    ap.add_argument("--modes", default="ranges,default", help="comma list of: ranges,default")
    ap.add_argument("--nodes", type=int, default=3, help="node count for parametrised topologies")
    ap.add_argument("--settle", type=float, default=1.5, help="post-connect / per-round settle seconds")
    ap.add_argument("--max-rounds", type=int, default=20, help="fixpoint round cap")
    ap.add_argument("--binary", help="benchnode binary path (built natively if omitted)")
    ap.add_argument("--out", help="CSV output path (default results/<label>.csv)")
    args = ap.parse_args()

    if args.topology_file:
        n, edges, _ = topology_from_file(args.topology_file)
        topo_name = os.path.basename(args.topology_file)
    else:
        n, edges = topology(args.topology, args.nodes)
        topo_name = args.topology

    devices = parse_devices(args.devices)
    # node: mode connects to already-running nodes, so no binary is needed.
    binary = args.binary or (None if devices[0] == "node" else build_benchnode())
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]

    label = f"{topo_name}-n{n}-{args.scenario}-docs{args.docs}"
    out = args.out or os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", f"{label}.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)

    all_rows = []
    summary = {}
    for mode in modes:
        print(f"\n=== {label} mode={mode} ===")
        rows, converged = run_mode(mode, n, edges, args.scenario, args.docs,
                                   binary, devices, args.settle, args.max_rounds)
        net_ctrl = sum(r["ctrlBytesSent"] + r["ctrlBytesRecv"] for r in rows)
        net_msgs = sum(r["ctrlMsgs"] for r in rows)
        wall = rows[0]["wallMs"] if rows else 0
        rounds = rows[0]["rounds"] if rows else 0
        summary[mode] = (net_ctrl, net_msgs, wall, rounds, converged)
        print(f"  converged={converged} rounds={rounds} wallMs={wall} "
              f"netCtrlBytes={net_ctrl} netCtrlMsgs={net_msgs}")
        for r in rows:
            all_rows.append({"topology": topo_name, "N": n, "scenario": args.scenario,
                             "mode": mode, **r})

    _write_csv(out, all_rows)
    print(f"\nwrote {out}")
    _print_summary(label, summary)


def _write_csv(path, rows):
    cols = ["topology", "N", "scenario", "mode", "nodeID", "ctrlBytesSent",
            "ctrlBytesRecv", "ctrlMsgs", "blocks", "blockBytes", "wallMs", "rounds", "converged"]
    with open(path, "w") as f:
        f.write(",".join(cols) + "\n")
        for r in rows:
            f.write(",".join(str(r[c]) for c in cols) + "\n")


def _print_summary(label, summary):
    print(f"\n--- summary: {label} ---")
    print(f"{'mode':<10} {'netCtrlBytes':>14} {'netCtrlMsgs':>12} {'wallMs':>10} {'rounds':>7} {'converged':>10}")
    for mode, (ctrl, msgs, wall, rounds, conv) in summary.items():
        print(f"{mode:<10} {ctrl:>14} {msgs:>12} {wall:>10} {rounds:>7} {str(conv):>10}")
    if "ranges" in summary and "default" in summary:
        r, d = summary["ranges"][0], summary["default"][0]
        if r > 0:
            print(f"\nranges control bytes vs default: {d/r:.2f}x "
                  f"({'ranges lower' if r < d else 'default lower'})")


if __name__ == "__main__":
    main()
