#!/usr/bin/env sh
# Sweep the dockerised pair over several document counts for both modes, so the
# control-byte-vs-size curve can be plotted. tiny-diff scenario (large shared
# base, one diverged doc) — the steady-state regime reconciliation targets.
#
#   ./sweep.sh                 # default sizes: 50 100 500 1000
#   ./sweep.sh "100 500 1000"  # custom sizes
#
# Writes one CSV per (size,mode) into ../results/ and a combined
# ../results/docker-sweep-tiny-diff.csv, then renders plots via plot_sweep.py.
set -eu

SIZES="${1:-50 100 500 1000}"
HERE="$(cd "$(dirname "$0")" && pwd)"
CROSSDEV="$(dirname "$HERE")"
REPO_ROOT="$(cd "$CROSSDEV/../../.." && pwd)"
COMPOSE="docker compose -f $HERE/docker-compose.yml"
RESULTS="$CROSSDEV/results"
COMBINED="$RESULTS/docker-sweep-tiny-diff.csv"

echo ">> ensuring image is built"
docker build -q -f "$HERE/Dockerfile" -t benchnode:local "$REPO_ROOT" >/dev/null

: > "$COMBINED.tmp"
for docs in $SIZES; do
  # Give bigger transfers more settle + round budget so convergence is reached.
  if [ "$docs" -ge 1000 ]; then settle=3.5; elif [ "$docs" -ge 500 ]; then settle=2.5; else settle=1.5; fi
  for mode in ranges default; do
    case "$mode" in ranges) RECON="--reconciliation" ;; *) RECON="" ;; esac
    export RECON
    out="$RESULTS/docker-pair-tiny-diff-docs$docs-$mode.csv"
    echo ">> docs=$docs mode=$mode settle=$settle"
    $COMPOSE down -v >/dev/null 2>&1 || true
    $COMPOSE up -d --wait >/dev/null
    python3 "$CROSSDEV/run.py" \
        --devices node:127.0.0.1:17701,127.0.0.1:17702 \
        --topology pair --scenario tiny-diff --docs "$docs" --modes "$mode" \
        --settle "$settle" --max-rounds 40 --out "$out"
    $COMPOSE down -v >/dev/null 2>&1 || true
    # append to the combined CSV with a leading self-describing `docs` column.
    if [ ! -s "$COMBINED.tmp" ]; then
      printf 'docs,%s\n' "$(head -1 "$out")" > "$COMBINED.tmp"
    fi
    tail -n +2 "$out" | sed "s/^/$docs,/" >> "$COMBINED.tmp"
  done
done
mv "$COMBINED.tmp" "$COMBINED"
echo ">> combined -> $COMBINED"

echo ">> rendering plots"
python3 "$HERE/plot_sweep.py" "$RESULTS"
echo ">> done"
