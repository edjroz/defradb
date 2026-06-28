#!/usr/bin/env python3
# Copyright 2026 Democratized Data Foundation
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""Render the dockerised cross-device sweep CSV into dependency-free SVG charts.

Reads the combined sweep CSV (one row per node per mode per size) and writes two
SVGs + an index.html into ../results/plots-sweep/:
  1. net control bytes vs #docs  (ranges vs default)
  2. reduction factor (default/ranges) vs #docs

Pure standard library — no matplotlib. Usage: python3 plot_sweep.py <combined.csv>
"""

import csv
import glob
import os
import re
import sys
from collections import defaultdict

NG_COLOR = "#2e86ab"        # ranges (reconciliation)
BASELINE_COLOR = "#d1495b"  # default broadcast
ACCENT = "#3c8c5a"          # reduction
GRID = "#e6e6e6"
AXIS = "#444444"
TEXT = "#222222"

W, H = 820, 480
PAD_L, PAD_R, PAD_T, PAD_B = 84, 150, 54, 64


FNAME = re.compile(r"docker-pair-tiny-diff-docs(\d+)-(ranges|default)\.csv$")


def load(results_dir):
    """Aggregate net control bytes per (mode, docs) by reading every per-size CSV
    in results_dir (the document count lives in the filename, not a column).
    Net = sum of sent+recv over both nodes."""
    agg = defaultdict(lambda: {"ctrl": 0, "converged": True, "state": True})
    for path in sorted(glob.glob(os.path.join(results_dir, "docker-pair-tiny-diff-docs*-*.csv"))):
        match = FNAME.search(os.path.basename(path))
        if not match:
            continue
        docs, mode = int(match.group(1)), match.group(2)
        for r in csv.DictReader(open(path)):
            key = (mode, docs)
            agg[key]["ctrl"] += int(r["ctrlBytesSent"]) + int(r["ctrlBytesRecv"])
            agg[key]["converged"] &= (r["converged"] == "True")
            agg[key]["state"] &= (r["stateMatch"] == "True")
    return agg


def nice_ticks(lo, hi, n=5):
    if hi <= lo:
        hi = lo + 1
    span = hi - lo
    raw = span / n
    mag = 10 ** (len(str(int(raw))) - 1) if raw >= 1 else 1
    step = max(1, round(raw / mag) * mag)
    t, ticks = 0, []
    while t <= hi + step:
        ticks.append(t)
        t += step
    return ticks


def line_chart(path, title, sizes, series, yfmt, ylabel):
    xmax = max(sizes)
    ymax = max(max(v for v in s["data"]) for s in series) or 1
    yticks = nice_ticks(0, ymax)
    ymax = max(yticks)

    def px(x): return PAD_L + (x / xmax) * (W - PAD_L - PAD_R)
    def py(y): return H - PAD_B - (y / ymax) * (H - PAD_T - PAD_B)

    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
           f'font-family="-apple-system,Segoe UI,Roboto,sans-serif">',
           f'<rect width="{W}" height="{H}" fill="white"/>',
           f'<text x="{PAD_L}" y="28" font-size="16" font-weight="700" fill="{TEXT}">{title}</text>']
    # y grid + labels
    for tv in yticks:
        yy = py(tv)
        svg.append(f'<line x1="{PAD_L}" y1="{yy:.1f}" x2="{W-PAD_R}" y2="{yy:.1f}" stroke="{GRID}"/>')
        svg.append(f'<text x="{PAD_L-8}" y="{yy+4:.1f}" font-size="11" fill="{AXIS}" text-anchor="end">{yfmt(tv)}</text>')
    # x labels
    for xv in sizes:
        xx = px(xv)
        svg.append(f'<line x1="{xx:.1f}" y1="{H-PAD_B}" x2="{xx:.1f}" y2="{H-PAD_B+5}" stroke="{AXIS}"/>')
        svg.append(f'<text x="{xx:.1f}" y="{H-PAD_B+20}" font-size="11" fill="{AXIS}" text-anchor="middle">{xv}</text>')
    svg.append(f'<text x="{(PAD_L+W-PAD_R)/2:.0f}" y="{H-18}" font-size="12" fill="{TEXT}" text-anchor="middle">documents (shared base, 1 diverged)</text>')
    svg.append(f'<text x="18" y="{(PAD_T+H-PAD_B)/2:.0f}" font-size="12" fill="{TEXT}" text-anchor="middle" transform="rotate(-90 18 {(PAD_T+H-PAD_B)/2:.0f})">{ylabel}</text>')
    # series
    ly = PAD_T + 6
    for s in series:
        pts = " ".join(f"{px(x):.1f},{py(y):.1f}" for x, y in zip(sizes, s["data"]))
        svg.append(f'<polyline points="{pts}" fill="none" stroke="{s["color"]}" stroke-width="2.5"/>')
        for x, y in zip(sizes, s["data"]):
            svg.append(f'<circle cx="{px(x):.1f}" cy="{py(y):.1f}" r="3.5" fill="{s["color"]}"/>')
        svg.append(f'<rect x="{W-PAD_R+14}" y="{ly}" width="12" height="12" fill="{s["color"]}"/>')
        svg.append(f'<text x="{W-PAD_R+30}" y="{ly+11}" font-size="12" fill="{TEXT}">{s["label"]}</text>')
        ly += 22
    svg.append('</svg>')
    open(path, "w").write("\n".join(svg))


def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")
    outdir = os.path.join(results_dir, "plots-sweep")
    os.makedirs(outdir, exist_ok=True)

    agg = load(results_dir)
    # only sizes measured in BOTH modes can be compared
    sizes = sorted({d for (_, d) in agg if ("ranges", d) in agg and ("default", d) in agg})
    if not sizes:
        sys.exit("no comparable docker-pair-tiny-diff-docs*-{ranges,default}.csv pairs found")
    ranges = [agg[("ranges", d)]["ctrl"] for d in sizes]
    default = [agg[("default", d)]["ctrl"] for d in sizes]

    line_chart(
        os.path.join(outdir, "01_ctrlbytes_vs_docs.svg"),
        "Control bytes to discover a 1-doc diff (dockerised pair)",
        sizes,
        [{"label": "ranges (RBSR)", "color": NG_COLOR, "data": ranges},
         {"label": "default (broadcast)", "color": BASELINE_COLOR, "data": default}],
        yfmt=lambda v: f"{v/1000:.0f}k" if v >= 1000 else f"{v:.0f}",
        ylabel="net control bytes (both nodes)",
    )

    reduction = [d / r if r else 0 for r, d in zip(ranges, default)]
    line_chart(
        os.path.join(outdir, "02_reduction_vs_docs.svg"),
        "Reduction factor: default ÷ ranges control bytes",
        sizes,
        [{"label": "× fewer (ranges)", "color": ACCENT, "data": reduction}],
        yfmt=lambda v: f"{v:.1f}x",
        ylabel="default ÷ ranges",
    )

    # index.html + a printed table
    rows = "".join(
        f"<tr><td>{d}</td><td>{agg[('ranges',d)]['ctrl']}</td>"
        f"<td>{agg[('default',d)]['ctrl']}</td><td>{(agg[('default',d)]['ctrl']/agg[('ranges',d)]['ctrl']):.2f}x</td>"
        f"<td>{agg[('ranges',d)]['converged'] and agg[('default',d)]['converged']}</td>"
        f"<td>{agg[('ranges',d)]['state'] and agg[('default',d)]['state']}</td></tr>"
        for d in sizes)
    html = f"""<!doctype html><meta charset=utf-8><title>Docker sweep — set reconciliation</title>
<body style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:900px;margin:2rem auto;color:#222">
<h1>Dockerised cross-device sweep — tiny-diff</h1>
<p>Two benchnode containers on one host, large shared base with one diverged document.
Net control bytes = sum of both nodes' sent+received over the measured window.</p>
<img src="01_ctrlbytes_vs_docs.svg" width="820"><img src="02_reduction_vs_docs.svg" width="820">
<table border=1 cellpadding=6 style="border-collapse:collapse;margin-top:1rem">
<tr><th>docs</th><th>ranges bytes</th><th>default bytes</th><th>reduction</th><th>converged</th><th>stateMatch</th></tr>
{rows}</table></body>"""
    open(os.path.join(outdir, "index.html"), "w").write(html)

    print(f"{'docs':>6} {'ranges':>10} {'default':>10} {'reduction':>10} {'converged':>10} {'stateMatch':>11}")
    for d in sizes:
        r, x = agg[("ranges", d)], agg[("default", d)]
        print(f"{d:>6} {r['ctrl']:>10} {x['ctrl']:>10} {x['ctrl']/r['ctrl']:>9.2f}x "
              f"{str(r['converged'] and x['converged']):>10} {str(r['state'] and x['state']):>11}")
    print(f"\nwrote {outdir}/ (2 SVGs + index.html)")


if __name__ == "__main__":
    main()
