#!/usr/bin/env python3
# Copyright 2025 Democratized Data Foundation
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""Render the reconciliation comparison CSV into dependency-free SVG charts.

Reads data/comparison.csv (produced by `DEFRA_RECONCILE_REPORT=1 go test`) and
writes one SVG per chart into plots/, plus an index.html that embeds them with a
short narrative. Pure standard library so it runs anywhere without matplotlib.

Usage: python3 plot.py
"""

import csv
import math
import os

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data", "comparison.csv")
MEASURED = os.path.join(HERE, "data", "measured_m1.csv")
PLOTS = os.path.join(HERE, "plots")

BASELINE_COLOR = "#d1495b"  # default full-set exchange
NG_COLOR = "#2e86ab"        # negentropy
ACCENT = "#3c8c5a"          # reduction / savings
GRID = "#e6e6e6"
AXIS = "#444444"
TEXT = "#222222"

W, H = 880, 480
# PAD_T leaves headroom below the title/subtitle so top-of-plot value labels
# (drawn a few px above the highest point/bar) don't collide with the subtitle text.
PAD_L, PAD_R, PAD_T, PAD_B = 78, 250, 78, 64


def load():
    with open(DATA, newline="") as f:
        return list(csv.DictReader(f))


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fmt_bytes(v):
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f}MB"
    if v >= 1000:
        return f"{v/1000:.0f}KB"
    return f"{int(v)}B"


def fmt_int(v):
    v = int(round(v))
    if v >= 1000:
        return f"{v//1000}k" if v % 1000 == 0 else f"{v/1000:.1f}k"
    return str(v)


class Axis:
    """Maps a data range to a pixel range, linearly or on a log10 scale."""

    def __init__(self, lo, hi, p0, p1, log, int_ticks=False):
        self.log = log
        self.int_ticks = int_ticks
        if log:
            self.lo, self.hi = math.log10(lo), math.log10(hi)
        else:
            self.lo, self.hi = lo, hi
        if self.hi == self.lo:
            self.hi = self.lo + 1
        self.p0, self.p1 = p0, p1

    def px(self, v):
        t = (math.log10(v) - self.lo) if self.log else (v - self.lo)
        return self.p0 + (self.p1 - self.p0) * t / (self.hi - self.lo)

    def ticks(self):
        if self.log:
            out = []
            e0, e1 = int(math.floor(self.lo)), int(math.ceil(self.hi))
            for e in range(e0, e1 + 1):
                val = 10 ** e
                if 10 ** self.lo - 1e-9 <= val <= 10 ** self.hi + 1e-9:
                    out.append(val)
            return out
        if self.int_ticks:
            return list(range(int(math.ceil(self.lo)), int(math.floor(self.hi)) + 1))
        # linear: 5 evenly spaced ticks
        return [self.lo + (self.hi - self.lo) * i / 5 for i in range(6)]


def line_chart(filename, title, subtitle, xlabel, ylabel, series,
               xlog, ylog, xfmt, yfmt, y_int=False):
    xs = [x for s in series for x, _ in s["points"]]
    ys = [y for s in series for _, y in s["points"]]
    xa = Axis(min(xs), max(xs), PAD_L, W - PAD_R, xlog)
    lo_y = min(ys)
    if ylog:
        lo_y = max(lo_y, 1)
    ya = Axis(lo_y, max(ys), H - PAD_B, PAD_T, ylog, int_ticks=y_int)

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}" font-family="-apple-system,Segoe UI,Roboto,sans-serif">',
        f'<rect width="{W}" height="{H}" fill="white"/>',
        f'<text x="{PAD_L}" y="26" font-size="17" font-weight="700" fill="{TEXT}">{esc(title)}</text>',
        f'<text x="{PAD_L}" y="44" font-size="12" fill="#666">{esc(subtitle)}</text>',
    ]

    # gridlines + tick labels
    for tv in ya.ticks():
        py = ya.px(tv)
        svg.append(f'<line x1="{PAD_L}" y1="{py:.1f}" x2="{W-PAD_R}" y2="{py:.1f}" stroke="{GRID}"/>')
        svg.append(f'<text x="{PAD_L-8}" y="{py+4:.1f}" font-size="11" fill="{AXIS}" text-anchor="end">{yfmt(tv)}</text>')
    for tv in xa.ticks():
        px = xa.px(tv)
        svg.append(f'<line x1="{px:.1f}" y1="{PAD_T}" x2="{px:.1f}" y2="{H-PAD_B}" stroke="{GRID}"/>')
        svg.append(f'<text x="{px:.1f}" y="{H-PAD_B+18:.1f}" font-size="11" fill="{AXIS}" text-anchor="middle">{xfmt(tv)}</text>')

    # axes
    svg.append(f'<line x1="{PAD_L}" y1="{PAD_T}" x2="{PAD_L}" y2="{H-PAD_B}" stroke="{AXIS}"/>')
    svg.append(f'<line x1="{PAD_L}" y1="{H-PAD_B}" x2="{W-PAD_R}" y2="{H-PAD_B}" stroke="{AXIS}"/>')

    # axis labels
    svg.append(f'<text x="{(PAD_L+W-PAD_R)/2:.0f}" y="{H-18}" font-size="12" fill="{TEXT}" text-anchor="middle">{esc(xlabel)}</text>')
    cy = (PAD_T + H - PAD_B) / 2
    svg.append(f'<text x="20" y="{cy:.0f}" font-size="12" fill="{TEXT}" text-anchor="middle" transform="rotate(-90 20 {cy:.0f})">{esc(ylabel)}</text>')

    # series
    for s in series:
        pts = " ".join(f"{xa.px(x):.1f},{ya.px(y):.1f}" for x, y in s["points"])
        dash = ' stroke-dasharray="6 4"' if s.get("dashed") else ""
        svg.append(f'<polyline points="{pts}" fill="none" stroke="{s["color"]}" stroke-width="2.5"{dash}/>')
        for x, y in s["points"]:
            svg.append(f'<circle cx="{xa.px(x):.1f}" cy="{ya.px(y):.1f}" r="3.4" fill="{s["color"]}"/>')

    # legend (right gutter)
    lx, ly = W - PAD_R + 16, PAD_T + 6
    for i, s in enumerate(series):
        yy = ly + i * 22
        svg.append(f'<line x1="{lx}" y1="{yy}" x2="{lx+22}" y2="{yy}" stroke="{s["color"]}" stroke-width="3"/>')
        svg.append(f'<text x="{lx+28}" y="{yy+4}" font-size="11.5" fill="{TEXT}">{esc(s["label"])}</text>')

    # annotations
    for a in series:
        for note in a.get("notes", []):
            x, y, txt = note
            svg.append(f'<text x="{xa.px(x):.1f}" y="{ya.px(y)-9:.1f}" font-size="11" font-weight="600" '
                       f'fill="{a["color"]}" text-anchor="middle">{esc(txt)}</text>')

    svg.append("</svg>")
    out = os.path.join(PLOTS, filename)
    with open(out, "w") as f:
        f.write("\n".join(svg))
    return filename


def bar_chart(filename, title, subtitle, ylabel, bars, ylog, yfmt):
    """bars: list of {label, value, color}. A simple vertical bar chart."""
    vals = [bvalue for _, bvalue, _ in bars]
    lo = 1 if ylog else 0
    ya = Axis(max(lo, min(vals + [lo])), max(vals), H - PAD_B, PAD_T, ylog)

    n = len(bars)
    plot_w = (W - 220) - PAD_L
    slot = plot_w / n
    bw = slot * 0.55

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}" font-family="-apple-system,Segoe UI,Roboto,sans-serif">',
        f'<rect width="{W}" height="{H}" fill="white"/>',
        f'<text x="{PAD_L}" y="26" font-size="17" font-weight="700" fill="{TEXT}">{esc(title)}</text>',
        f'<text x="{PAD_L}" y="44" font-size="12" fill="#666">{esc(subtitle)}</text>',
    ]
    for tv in ya.ticks():
        py = ya.px(tv)
        svg.append(f'<line x1="{PAD_L}" y1="{py:.1f}" x2="{W-220}" y2="{py:.1f}" stroke="{GRID}"/>')
        svg.append(f'<text x="{PAD_L-8}" y="{py+4:.1f}" font-size="11" fill="{AXIS}" text-anchor="end">{yfmt(tv)}</text>')
    svg.append(f'<line x1="{PAD_L}" y1="{PAD_T}" x2="{PAD_L}" y2="{H-PAD_B}" stroke="{AXIS}"/>')
    svg.append(f'<line x1="{PAD_L}" y1="{H-PAD_B}" x2="{W-220}" y2="{H-PAD_B}" stroke="{AXIS}"/>')
    cy = (PAD_T + H - PAD_B) / 2
    svg.append(f'<text x="20" y="{cy:.0f}" font-size="12" fill="{TEXT}" text-anchor="middle" transform="rotate(-90 20 {cy:.0f})">{esc(ylabel)}</text>')

    base_y = ya.px(max(lo, 1) if ylog else 0)
    for i, (label, value, color) in enumerate(bars):
        cx = PAD_L + slot * (i + 0.5)
        top = ya.px(value)
        svg.append(f'<rect x="{cx-bw/2:.1f}" y="{top:.1f}" width="{bw:.1f}" height="{base_y-top:.1f}" fill="{color}"/>')
        svg.append(f'<text x="{cx:.1f}" y="{top-7:.1f}" font-size="12" font-weight="600" fill="{color}" text-anchor="middle">{yfmt(value)}</text>')
        for j, line in enumerate(label.split("\n")):
            svg.append(f'<text x="{cx:.1f}" y="{H-PAD_B+18+j*14:.1f}" font-size="11" fill="{TEXT}" text-anchor="middle">{esc(line)}</text>')

    svg.append("</svg>")
    out = os.path.join(PLOTS, filename)
    with open(out, "w") as f:
        f.write("\n".join(svg))
    return filename


def load_measured():
    if not os.path.exists(MEASURED):
        return None
    with open(MEASURED, newline="") as f:
        return list(csv.DictReader(f))


def main():
    os.makedirs(PLOTS, exist_ok=True)
    rows = load()

    def pick(sweep, approach, xkey, ykey):
        pts = [(float(r[xkey]), float(r[ykey])) for r in rows
               if r["sweep"] == sweep and r["approach"] == approach]
        return sorted(pts)

    written = []

    # 1. Headline: control bytes vs n at a fixed 2-item difference (log-log).
    bl = pick("bytes_vs_n", "baseline", "n", "control_bytes")
    ng = pick("bytes_vs_n", "negentropy", "n", "control_bytes")
    written.append(line_chart(
        "01_bytes_vs_n.svg",
        "Control traffic to find a 2-item difference",
        "Symmetric difference fixed at d=2; only the set size n grows. Log-log axes.",
        "set size  n  (items, log scale)",
        "control bytes (log scale)",
        [
            {"label": "default (full-set exchange)", "color": BASELINE_COLOR, "points": bl,
             "notes": [(bl[-1][0], bl[-1][1], fmt_bytes(bl[-1][1]))]},
            {"label": "negentropy", "color": NG_COLOR, "points": ng,
             "notes": [(ng[-1][0], ng[-1][1], fmt_bytes(ng[-1][1]))]},
        ],
        xlog=True, ylog=True, xfmt=fmt_int, yfmt=fmt_bytes))

    # 2. Reduction factor vs n (semilog-x).
    red = []
    for (xn, yb), (_, yg) in zip(bl, ng):
        red.append((xn, yb / yg))
    written.append(line_chart(
        "02_reduction_vs_n.svg",
        "Negentropy control-byte reduction factor",
        "How many times less control traffic than full-set exchange, at d=2.",
        "set size  n  (items, log scale)",
        "reduction  (baseline / negentropy)",
        [
            {"label": "reduction factor", "color": ACCENT, "points": red,
             "notes": [(red[-1][0], red[-1][1], f"{red[-1][1]:.0f}x"),
                       (red[len(red)//2][0], red[len(red)//2][1], f"{red[len(red)//2][1]:.0f}x")]},
        ],
        xlog=True, ylog=False, xfmt=fmt_int, yfmt=lambda v: f"{v:.0f}x"))

    # 3. Control bytes vs difference size d at fixed n=10000 (log-log) — crossover.
    bld = pick("bytes_vs_d", "baseline", "d", "control_bytes")
    ngd = pick("bytes_vs_d", "negentropy", "d", "control_bytes")
    written.append(line_chart(
        "03_bytes_vs_d.svg",
        "Control traffic vs difference size  (set fixed at n=10k)",
        "Negentropy scales with the difference d; full exchange is flat. They cross near d~5-10% of n.",
        "symmetric difference  d  (items, log scale)",
        "control bytes (log scale)",
        [
            {"label": "default (full-set exchange)", "color": BASELINE_COLOR, "points": bld},
            {"label": "negentropy", "color": NG_COLOR, "points": ngd},
        ],
        xlog=True, ylog=True, xfmt=fmt_int, yfmt=fmt_bytes))

    # 4. Round-trips vs n (semilog-x).
    blr = pick("bytes_vs_n", "baseline", "n", "round_trips")
    ngr = pick("bytes_vs_n", "negentropy", "n", "round_trips")
    written.append(line_chart(
        "04_rounds_vs_n.svg",
        "Round-trips vs set size  (the trade-off)",
        "Negentropy spends a few more logarithmic rounds to buy the byte savings above.",
        "set size  n  (items, log scale)",
        "round-trips",
        [
            {"label": "default sync", "color": BASELINE_COLOR, "points": blr, "dashed": True},
            {"label": "negentropy", "color": NG_COLOR, "points": ngr},
        ],
        xlog=True, ylog=False, xfmt=fmt_int, yfmt=lambda v: f"{v:.0f}", y_int=True))

    # 5/6. Measured Phase-3 (M1) real-node control cost: default doc-sync vs
    # per-document reconciliation, same post-partition divergence.
    measured = load_measured()
    if measured:
        def m(scenario, approach, key):
            for r in measured:
                if r["scenario"] == scenario and r["approach"] == approach:
                    return float(r[key])
            return 0.0

        sc = "manyhead_docs10_base5_k3"
        written.append(bar_chart(
            "05_measured_control_bytes.svg",
            "Measured control bytes — 10-doc post-partition (M1, real nodes)",
            "Same payload transferred either way; reconciliation's overhead dominates at per-document scale. Log scale.",
            "control bytes (log scale)",
            [
                ("default doc-sync\n(1 broadcast)", m(sc, "default_sync", "ctrl_bytes"), BASELINE_COLOR),
                ("reconcile\n(10 sessions)", m(sc, "reconcile", "ctrl_bytes"), NG_COLOR),
                ("reconcile\n1 doc", m("singledoc_base5_k3", "reconcile", "ctrl_bytes"), NG_COLOR),
            ],
            ylog=True, yfmt=fmt_bytes))

        written.append(bar_chart(
            "06_measured_round_trips.svg",
            "Measured round-trips — 10-doc post-partition (M1, real nodes)",
            "M1 runs one reconciliation session per document; per-collection batching (M2) collapses this.",
            "control messages (round-trip proxy)",
            [
                ("default doc-sync", m(sc, "default_sync", "ctrl_msgs"), BASELINE_COLOR),
                ("reconcile\n(10 sessions)", m(sc, "reconcile", "ctrl_msgs"), NG_COLOR),
                ("reconcile\n1 doc", m("singledoc_base5_k3", "reconcile", "ctrl_msgs"), NG_COLOR),
            ],
            ylog=False, yfmt=lambda v: f"{v:.0f}"))

    # 7/8. M2 collection-scope collapse: default doc-sync vs M1 per-document vs M2
    # per-collection, all converging the same 10-doc set on real nodes.
    m2_path = os.path.join(HERE, "data", "measured_m2.csv")
    if os.path.exists(m2_path):
        with open(m2_path, newline="") as f:
            m2rows = list(csv.DictReader(f))

        def m2(approach, key):
            for r in m2rows:
                if r["scenario"] == "coldstart_docs10" and r["approach"] == approach:
                    return float(r[key])
            return 0.0

        written.append(bar_chart(
            "07_m2_round_trips.svg",
            "Round-trips to converge 10 docs (real nodes)",
            "One collection reconciliation session converges the whole set in round-trips "
            "comparable to a single broadcast.",
            "control messages (round-trip proxy)",
            [
                ("default\ndoc-sync", m2("default_sync", "ctrl_msgs"), BASELINE_COLOR),
                ("reconcile\n(1 collection session)", m2("reconcile_m2_collection", "ctrl_msgs"), NG_COLOR),
            ],
            ylog=False, yfmt=lambda v: f"{v:.0f}"))

        written.append(bar_chart(
            "08_m2_control_bytes.svg",
            "Control bytes to converge 10 docs (real nodes)",
            "Collection-scope reconciliation lands near a single broadcast; same payload. Log scale.",
            "control bytes (log scale)",
            [
                ("default\ndoc-sync", m2("default_sync", "ctrl_bytes"), BASELINE_COLOR),
                ("reconcile\n(collection)", m2("reconcile_m2_collection", "ctrl_bytes"), NG_COLOR),
            ],
            ylog=True, yfmt=fmt_bytes))

    # 9. M5 tiny-diff: the asymptotic bandwidth win on real nodes. One diverged
    # doc in a growing collection — default control scales with the collection
    # size (it must list every docID), reconciliation tracks the single-block diff.
    m5_path = os.path.join(HERE, "data", "measured_m5.csv")
    if os.path.exists(m5_path):
        with open(m5_path, newline="") as f:
            m5rows = list(csv.DictReader(f))

        def m5series(approach):
            pts = [(float(r["docs"]), float(r["ctrl_bytes"])) for r in m5rows
                   if r["approach"] == approach]
            return sorted(pts)

        dflt = m5series("default_sync")
        recon = m5series("reconcile_m2_collection")
        written.append(line_chart(
            "09_tinydiff_bytes_vs_docs.svg",
            "Control bytes to sync ONE changed doc vs collection size (real nodes)",
            "Default must enumerate every docID (control grows with the collection); "
            "reconciliation tracks the single-block diff and stays ~flat.",
            "collection size  (documents)",
            "control bytes",
            [
                {"label": "default doc-sync", "color": BASELINE_COLOR, "points": dflt,
                 "notes": [(d[0], d[1], fmt_bytes(d[1])) for d in dflt]},
                {"label": "reconcile (M2)", "color": NG_COLOR, "points": recon,
                 "notes": [(d[0], d[1], fmt_bytes(d[1])) for d in recon]},
            ],
            xlog=False, ylog=False, xfmt=lambda v: f"{v:.0f}", yfmt=fmt_bytes))

    # 10. M5 index-write cost: the always-on maintenance-hook overhead, measured
    # locally (no peer) with the flag off vs on.
    iw_path = os.path.join(HERE, "data", "measured_indexwrite.csv")
    if os.path.exists(iw_path):
        with open(iw_path, newline="") as f:
            iwrows = {r["flag"]: r for r in csv.DictReader(f)}
        off_ms = float(iwrows["off"]["ns_op"]) / 1e6
        on_ms = float(iwrows["on"]["ns_op"]) / 1e6
        pct = (on_ms / off_ms - 1) * 100 if off_ms else 0
        written.append(bar_chart(
            "10_indexwrite_cost.svg",
            "Local write cost with reconciliation off vs on (real node)",
            f"Seeding 50 docs x 5 updates; the extra per-commit ordered-index write adds ~{pct:.0f}%.",
            "write time per op (ms, lower is better)",
            [
                ("reconciliation off", off_ms, BASELINE_COLOR),
                ("reconciliation on", on_ms, NG_COLOR),
            ],
            ylog=False, yfmt=lambda v: f"{v:.0f}ms"))

    # 11. Cross-container validation, real Docker containers on one host bridge: the
    # tiny-diff win as the NETWORK grows. N benchnode containers in a star (node0 is
    # the diverging source), converging one changed doc in a 50-doc collection. Total
    # network control bytes (summed over all nodes): reconciliation scales ~linearly
    # with the star's edges while default broadcast grows super-linearly (every node
    # re-lists all docIDs), so the reduction factor COMPOUNDS with node count. Log-y.
    # (tests/bench/crossdevice/docker/)
    xd_path = os.path.join(HERE, "data", "measured_crossdevice.csv")
    if os.path.exists(xd_path):
        with open(xd_path, newline="") as f:
            xdrows = list(csv.DictReader(f))

        def xdseries(approach):
            pts = [(float(r["nodes"]), float(r["net_ctrl_bytes"])) for r in xdrows
                   if r["approach"] == approach]
            return sorted(pts)

        dflt = xdseries("default_sync")
        recon = xdseries("reconcile")
        # reduction annotation at the largest node count
        red = dflt[-1][1] / recon[-1][1] if recon and dflt else 0
        written.append(line_chart(
            "11_crossdevice_validation.svg",
            "Cross-container validation — Docker containers on one host bridge",
            "N benchnode containers (star; node0 diverges one doc in a 50-doc collection). "
            "Total network control bytes to converge, vs node count. Log-y.",
            "nodes  (containers)",
            "network control bytes (all nodes, log scale)",
            [
                {"label": "default doc-sync", "color": BASELINE_COLOR, "points": dflt,
                 "notes": [(d[0], d[1], fmt_bytes(d[1])) for d in dflt]},
                {"label": "reconcile (ranges)", "color": NG_COLOR, "points": recon,
                 "notes": [(recon[-1][0], recon[-1][1], f"{fmt_bytes(recon[-1][1])}  ({red:.0f}x fewer)")]
                          if recon else []},
            ],
            xlog=False, ylog=True, xfmt=lambda v: f"{v:.0f}", yfmt=fmt_bytes))

    # 12. The other axis (real nodes): control bytes vs DIFFERENCE size at a fixed
    # collection (500 docs). Reconciliation is O(diff · log n) — it rises with the
    # number of changed docs — while default broadcast is O(n), flat in the diff
    # (it lists every docID regardless). Reconciliation wins while the diff is small
    # and crosses ABOVE default once the diff is a large fraction of the collection.
    ds_path = os.path.join(HERE, "data", "measured_diffsweep.csv")
    if os.path.exists(ds_path):
        with open(ds_path, newline="") as f:
            dsrows = list(csv.DictReader(f))

        def dsseries(approach):
            pts = [(float(r["diff"]), float(r["ctrl_bytes"])) for r in dsrows
                   if r["approach"] == approach]
            return sorted(pts)

        dsr = dsseries("reconcile_m2_collection")
        dsd = dsseries("default_sync")
        written.append(line_chart(
            "12_diffsweep_bytes_vs_diff.svg",
            "Control bytes vs difference size  (collection fixed at 500 docs, real nodes)",
            "At fixed n, O(diff·log n) is linear in diff (log n constant); default is O(n), flat. "
            "Reconciliation crosses ABOVE default at ~diff 395 (~79% of the collection).",
            "changed documents  (difference size, log scale)",
            "control bytes",
            [
                {"label": "default doc-sync (O(n))", "color": BASELINE_COLOR, "points": dsd, "dashed": True,
                 "notes": [(dsd[-1][0], dsd[-1][1], fmt_bytes(dsd[-1][1]))] if dsd else []},
                {"label": "reconcile (O(diff·log n))", "color": NG_COLOR, "points": dsr,
                 "notes": [(dsr[0][0], dsr[0][1], fmt_bytes(dsr[0][1])),
                           (dsr[-1][0], dsr[-1][1], fmt_bytes(dsr[-1][1]))] if dsr else []},
            ],
            xlog=True, ylog=False, xfmt=lambda v: f"{v:.0f}", yfmt=fmt_bytes))

    # 13/14. Depth-invariance (real nodes): a two-sided fork over 10 docs, with the
    # divergent branches 3 vs 50 commits deep. Both sync and reconciliation discover
    # divergence head-first, so the control cost is unchanged by depth (chart 13)
    # while the payload — blocks fetched + merged — scales with it (chart 14). This is
    # the long-differing-branch result: reconciliation pays nothing extra to discover
    # a deep divergence; depth is a pure payload/merge cost.
    di_path = os.path.join(HERE, "data", "measured_depthinvariance.csv")
    if os.path.exists(di_path):
        with open(di_path, newline="") as f:
            dirows = list(csv.DictReader(f))

        def di(scenario, approach, depth, key):
            for r in dirows:
                if (r["scenario"] == scenario and r["approach"] == approach
                        and int(r["depth"]) == depth):
                    return float(r[key])
            return 0.0

        written.append(bar_chart(
            "13_depth_invariance_control.svg",
            "Control cost is invariant to branch depth (10-doc fork, real nodes)",
            "Branches 16x deeper (k=3 -> k=50) leave control bytes unchanged; only the "
            "payload scales (chart 14). Log scale.",
            "control bytes (log scale)",
            [
                ("default sync\nk=3", di("manyhead_docs10", "default_sync", 3, "ctrl_bytes"), BASELINE_COLOR),
                ("default sync\nk=50", di("manyhead_docs10", "default_sync", 50, "ctrl_bytes"), BASELINE_COLOR),
                ("reconcile\nk=3", di("manyhead_docs10", "reconcile", 3, "ctrl_bytes"), NG_COLOR),
                ("reconcile\nk=50", di("manyhead_docs10", "reconcile", 50, "ctrl_bytes"), NG_COLOR),
            ],
            ylog=True, yfmt=fmt_bytes))

        recon_blocks = sorted(
            (int(r["depth"]), float(r["blocks"])) for r in dirows
            if r["scenario"] == "manyhead_docs10" and r["approach"] == "reconcile")
        written.append(line_chart(
            "14_depth_payload.svg",
            "Payload scales with branch depth (same 10-doc fork, real nodes)",
            "The blocks reconciliation fetches + merges grow with depth — the cost it "
            "does NOT avoid (contrast the flat control bytes in chart 13).",
            "branch depth  (commits per branch)",
            "blocks transferred",
            [
                {"label": "reconcile (blocks fetched)", "color": NG_COLOR, "points": recon_blocks,
                 "notes": [(d, v, fmt_int(v)) for d, v in recon_blocks]},
            ],
            xlog=False, ylog=False, xfmt=lambda v: f"{v:.0f}", yfmt=fmt_int))

    # 15. The measured mirror of the modelled chart 3: the difference is brand-NEW
    # documents (not updates to existing ones), so the default's docID list grows with the
    # diff too. Both lines rise — default ~O(base+new), reconcile ~O(new·log n) — unlike
    # chart 12's update-churn case where the default stays flat. This is the apples-to-apples
    # measured version of the model's difference-size sweep.
    nd_path = os.path.join(HERE, "data", "measured_newdocs.csv")
    if os.path.exists(nd_path):
        with open(nd_path, newline="") as f:
            ndrows = list(csv.DictReader(f))

        def ndseries(approach):
            return sorted((float(r["new"]), float(r["ctrl_bytes"])) for r in ndrows
                          if r["approach"] == approach)

        ndd = ndseries("default_sync")
        ndr = ndseries("reconcile")
        written.append(line_chart(
            "15_newdocs_bytes_vs_added.svg",
            "Control bytes vs difference size — brand-NEW docs (500 base, real nodes)",
            "Measured mirror of chart 3: the diff is new documents, so the default's docID "
            "list grows too. Both rise — default O(base+new), reconcile O(new·log n).",
            "new documents added  (difference size, log scale)",
            "control bytes",
            [
                {"label": "default doc-sync (O(base+new))", "color": BASELINE_COLOR, "points": ndd,
                 "dashed": True,
                 "notes": [(ndd[0][0], ndd[0][1], fmt_bytes(ndd[0][1])),
                           (ndd[-1][0], ndd[-1][1], fmt_bytes(ndd[-1][1]))] if ndd else []},
                {"label": "reconcile (O(new·log n))", "color": NG_COLOR, "points": ndr,
                 "notes": [(ndr[0][0], ndr[0][1], fmt_bytes(ndr[0][1])),
                           (ndr[-1][0], ndr[-1][1], fmt_bytes(ndr[-1][1]))] if ndr else []},
            ],
            xlog=True, ylog=False, xfmt=lambda v: f"{v:.0f}", yfmt=fmt_bytes))

    # index.html — grouped, each group with a blurb and each chart with a 2-line caption.
    groups = [
        ("A · Modelled asymptotic sweep",
         "Synthetic sweep of the real Phase-2 negentropy engine against an idealized "
         "full-identifier baseline — the asymptotic shape at sizes too large to run "
         "end-to-end. Directional, not wall-clock; the baseline is deliberately generous, "
         "so these <em>understate</em> the real win.",
         [
             ("01_bytes_vs_n.svg",
              "<b>x</b> = set size n (diff fixed at 2); <b>y</b> = control bytes, log-log. "
              "Default climbs with the set, negentropy with its logarithm — 188× fewer at n=100k."),
             ("02_reduction_vs_n.svg",
              "<b>x</b> = set size n; <b>y</b> = default ÷ negentropy. Chart 1 as a ratio: "
              "the saving compounds with set size — the larger the shared set, the bigger the win."),
             ("03_bytes_vs_d.svg",
              "<b>x</b> = difference size d = <em>added</em> items (base ~10k); <b>y</b> = control bytes, log-log. "
              "Default is ~flat until d nears n then rises (added items enlarge the set); negentropy rises "
              "throughout, crossing ~10%. Validated by the measured chart 15 (same added-items setup)."),
             ("04_rounds_vs_n.svg",
              "<b>x</b> = set size n; <b>y</b> = round-trips. Negentropy spends 2–4 logarithmic "
              "rounds where default takes 1 — the latency price paid for the byte savings."),
         ]),
        ("B · Collection convergence at broadcast cost (real nodes)",
         "Reconciliation converges a whole small collection in one session, at round-trips "
         "and control bytes comparable to a single default broadcast — same payload.",
         [
             ("07_m2_round_trips.svg",
              "Bars = round-trips to converge 10 docs. One collection session (4) collapses "
              "M1's 42 back toward default's 3."),
             ("08_m2_control_bytes.svg",
              "Bars = control bytes, log-y. Collection-scope reconciliation (2.2KB) lands near "
              "broadcast (1.5KB), erasing M1's 44× penalty — same payload throughout."),
         ]),
        ("C · The payoff — both axes of O(diff·log n) vs O(n) (real nodes)",
         "The core value, measured, across both variables of the complexity: grow the "
         "collection (chart 9) or grow the difference (charts 12 &amp; 15). Note what "
         "counts as a \"difference\": <b>updates to existing docs</b> keep the docID set "
         "fixed, so default stays flat (chart 12); <b>brand-new docs</b> enlarge it, so "
         "default grows too (chart 15, the measured mirror of chart 3). Y is coordination "
         "cost; payload is identical either way.",
         [
             ("09_tinydiff_bytes_vs_docs.svg",
              "<b>x</b> = collection size 50→1000 (one doc changed); <b>y</b> = control bytes. "
              "Default is O(n) (2.1→42KB); reconciliation ~logarithmic (2.7→6.4KB) — 6.5× fewer at 1000 docs."),
             ("12_diffsweep_bytes_vs_diff.svg",
              "<b>x</b> = <em>updated</em> docs 1→500 (collection fixed at 500); <b>y</b> = control bytes. "
              "Difference = new versions of existing docs, so the docID set stays 500 → default flat at 21KB; "
              "reconciliation linear (5.3→23.2KB), crossing only at ~79% changed."),
             ("15_newdocs_bytes_vs_added.svg",
              "<b>x</b> = <em>brand-new</em> docs added 1→500 (500 base); <b>y</b> = control bytes. "
              "Difference = new docIDs, so BOTH grow — default O(base+new), reconcile O(new·log n). "
              "The measured analogue of the modelled chart 3."),
         ]),
        ("D · Maintenance cost (real node)",
         "What it costs when the flag is on: a small, always-on bookkeeping write on every "
         "local commit. Paid whether or not you ever reconcile.",
         [
             ("10_indexwrite_cost.svg",
              "Bars = local write latency, flag off vs on. The ordered-index write adds ~4.6% "
              "(76.4→80.0ms) — bounded, and only when enabled."),
         ]),
        ("E · Cross-container network (real Docker containers)",
         "Off the single-process bench and onto real Docker containers on one host bridge, "
         "scaling the network 2→10 nodes (star topology, one diverged doc in a 50-doc collection).",
         [
             ("11_crossdevice_validation.svg",
              "<b>x</b> = node count 2→10; <b>y</b> = total network control bytes, log-y. "
              "Default is super-linear (18KB→1.28MB); reconciliation linear (10→90KB) — the "
              "reduction compounds 1.8×→14×. Every run converged with identical document state."),
         ]),
        ("F · Long-branch depth-invariance (real nodes)",
         "The long-differing-branch case — does a deep divergence cost more to <em>discover</em>? "
         "A two-sided fork with branches 3 vs 50 commits deep.",
         [
             ("13_depth_invariance_control.svg",
              "<b>x</b> = branch depth (3 vs 50); <b>y</b> = control bytes, log-y. Flat across a "
              "16× depth increase for both — a deep branch is still one differing head."),
             ("14_depth_payload.svg",
              "<b>x</b> = branch depth; <b>y</b> = blocks fetched. Payload scales with depth "
              "(60→1000) — depth is a transfer/merge cost, not a discovery cost."),
         ]),
    ]

    written_set = set(written)
    sections = []
    for gtitle, gdesc, charts in groups:
        cards = [
            f'<figure><img src="plots/{fn}" alt="{fn}"/><figcaption>{cap}</figcaption></figure>'
            for fn, cap in charts if fn in written_set
        ]
        if cards:
            sections.append(
                f'<section><h2>{esc(gtitle)}</h2><p class="gdesc">{gdesc}</p>\n'
                + "\n".join(cards) + "\n</section>")
    body = "\n".join(sections)

    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Range-based set reconciliation vs default sync — evidence</title>
<style>
 body{{font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:900px;margin:40px auto;padding:0 16px;color:#222;line-height:1.5}}
 h1{{font-size:24px}} h2{{font-size:18px;margin-top:44px;border-bottom:2px solid {ACCENT};padding-bottom:5px}}
 figure{{margin:34px 0;text-align:center}} img{{max-width:100%;border:1px solid #eee;border-radius:8px}}
 figcaption{{font-size:13px;color:#555;margin:14px auto 0;max-width:820px;text-align:left;line-height:1.45}}
 code{{background:#f4f4f4;padding:1px 5px;border-radius:4px}}
 .take{{background:#f6f9f7;border-left:4px solid {ACCENT};padding:12px 16px;border-radius:6px}}
 .key{{background:#f7f8fa;border:1px solid #e6e6e6;border-radius:6px;padding:10px 16px;font-size:14px}}
 .gdesc{{color:#444;font-size:14px}}
</style></head><body>
<h1>Range-based set reconciliation vs default sync</h1>
<p class="take"><b>The claim:</b> to find a small difference inside a large shared set,
default sync must communicate the whole set (<code>O(n)</code>), while reconciliation
narrows in on just the difference (<code>O(diff&middot;log&nbsp;n)</code>). It wins in
that regime, costs more outside it, and is <b>default-off</b>.</p>
<p class="key"><b>How to read every chart.</b> (1) The y-axis is <b>control /
coordination bytes</b> — the cost of <em>discovering</em> what differs, <b>not</b> the
data transferred (payload is identical either way). (2) <b>Group&nbsp;A is modelled</b>
(a directional sweep); <b>Groups&nbsp;B–F are measured on real nodes</b>. Where a model
and a measurement disagree, the measurement wins. See
<code>tests/bench/reconcile/README.md</code> for methodology.</p>
{body}
</body></html>"""
    with open(os.path.join(HERE, "index.html"), "w") as f:
        f.write(html)

    print("wrote:", ", ".join(written), "and index.html")


if __name__ == "__main__":
    main()
