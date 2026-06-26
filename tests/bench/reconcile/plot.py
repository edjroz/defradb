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

W, H = 880, 470
PAD_L, PAD_R, PAD_T, PAD_B = 78, 250, 56, 64


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
            "Per-collection reconciliation (M2) runs ONE session, collapsing M1's per-document sessions.",
            "control messages (round-trip proxy)",
            [
                ("default\ndoc-sync", m2("default_sync", "ctrl_msgs"), BASELINE_COLOR),
                ("M1 per-doc\n(10 sessions)", m2("reconcile_m1_perdoc", "ctrl_msgs"), "#e8a13a"),
                ("M2 per-collection\n(1 session)", m2("reconcile_m2_collection", "ctrl_msgs"), NG_COLOR),
            ],
            ylog=False, yfmt=lambda v: f"{v:.0f}"))

        written.append(bar_chart(
            "08_m2_control_bytes.svg",
            "Control bytes to converge 10 docs (real nodes)",
            "M2 collapses M1's per-document overhead to ~broadcast levels; same payload. Log scale.",
            "control bytes (log scale)",
            [
                ("default\ndoc-sync", m2("default_sync", "ctrl_bytes"), BASELINE_COLOR),
                ("M1 per-doc", m2("reconcile_m1_perdoc", "ctrl_bytes"), "#e8a13a"),
                ("M2 per-collection", m2("reconcile_m2_collection", "ctrl_bytes"), NG_COLOR),
            ],
            ylog=True, yfmt=fmt_bytes))

    # index.html
    cards = "\n".join(
        f'<figure><img src="plots/{fn}" alt="{fn}"/></figure>' for fn in written)
    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Negentropy vs default sync — control-traffic comparison</title>
<style>
 body{{font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:900px;margin:40px auto;padding:0 16px;color:#222;line-height:1.5}}
 h1{{font-size:24px}} figure{{margin:24px 0;text-align:center}} img{{max-width:100%;border:1px solid #eee;border-radius:8px}}
 code{{background:#f4f4f4;padding:1px 5px;border-radius:4px}}
 .take{{background:#f6f9f7;border-left:4px solid {ACCENT};padding:12px 16px;border-radius:6px}}
</style></head><body>
<h1>Range-based set reconciliation vs default sync</h1>
<p class="take"><b>The case in one line:</b> to discover a tiny difference inside a large
set, the default sync must communicate the whole set (O(n) control bytes), while
negentropy narrows in on just the difference (O(d&middot;log&nbsp;n)) — a
<b>188&times; control-traffic reduction at n=100k</b> for a 2-item difference, at the
cost of a few extra logarithmic round-trips.</p>
<p>Negentropy numbers are measured directly from the Phase-2 engine
(<code>internal/db/p2p/negentropy</code>); the default-sync baseline is modelled as a
naive full-identifier exchange at the same per-item accounting — a generous lower
bound on what the real DAG-walk costs. See <code>tests/bench/reconcile/README.md</code> for methodology.</p>
<p><b>Charts 1&ndash;4</b> are the modelled synthetic sweep (the asymptotic case).
<b>Charts 5&ndash;6</b> are <b>measured on real nodes</b> for Phase&nbsp;3/M1: at
per-document scale reconciliation costs <i>more</i> control traffic than broadcast
doc-sync (one session per document), with identical payload. The asymptotic win
needs M2's per-collection batching &mdash; see <code>tests/bench/reconcile/README.md</code>.</p>
{cards}
</body></html>"""
    with open(os.path.join(HERE, "index.html"), "w") as f:
        f.write(html)

    print("wrote:", ", ".join(written), "and index.html")


if __name__ == "__main__":
    main()
