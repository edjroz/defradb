#!/usr/bin/env sh
# Run the dockerised tiny-diff benchmark for an N-node stack on one host, both
# modes (ranges + default), and write per-mode + combined CSVs. Generalises
# compose-run.sh (pair-only) to arbitrary N via gen-compose.py, so the 2/5/10-node
# node-count sweep runs as real containers on a single bridge network.
#
#   ./multinode-run.sh N [docs] [topology]
#   ./multinode-run.sh 2            # pair, docs50, star
#   ./multinode-run.sh 5 50 star
#   ./multinode-run.sh 10 50 star
#
# tiny-diff seeds a shared base on node0 and diverges one doc on node0; `star`
# makes node0 the hub so every other node reconciles directly with the source.
set -eu

N="${1:?usage: multinode-run.sh N [docs] [topology]}"
DOCS="${2:-50}"
TOPO="${3:-star}"

HERE="$(cd "$(dirname "$0")" && pwd)"
CROSSDEV="$(dirname "$HERE")"
REPO_ROOT="$(cd "$CROSSDEV/../../.." && pwd)"
RESULTS="$CROSSDEV/results"
COMPOSE_FILE="$HERE/docker-compose.$N.yml"

echo ">> generating $COMPOSE_FILE"
python3 "$HERE/gen-compose.py" "$N" > "$COMPOSE_FILE"
COMPOSE="docker compose -f $COMPOSE_FILE"

# --devices node:127.0.0.1:17701,...,1770N (one control endpoint per node)
DEVICES="node:"
i=0
while [ "$i" -lt "$N" ]; do
  port=$((17701 + i))
  if [ "$i" -eq 0 ]; then DEVICES="${DEVICES}127.0.0.1:${port}"
  else DEVICES="${DEVICES},127.0.0.1:${port}"; fi
  i=$((i + 1))
done

echo ">> ensuring image built (context: $REPO_ROOT)"
docker build -q -f "$HERE/Dockerfile" -t benchnode:local "$REPO_ROOT" >/dev/null

# Give bigger sets / more nodes more settle + round budget to converge.
if [ "$DOCS" -ge 500 ] || [ "$N" -ge 10 ]; then settle=2.5; else settle=1.5; fi

OUT="$RESULTS/docker-$TOPO-n$N-tiny-diff-docs$DOCS.csv"
: > "$OUT.tmp"
for mode in ranges default; do
  case "$mode" in ranges) RECON="--reconciliation" ;; *) RECON="" ;; esac
  export RECON
  echo ">> N=$N topo=$TOPO docs=$DOCS mode=$mode settle=$settle"
  $COMPOSE down -v >/dev/null 2>&1 || true
  $COMPOSE up -d --wait >/dev/null
  modeout="$RESULTS/docker-$TOPO-n$N-tiny-diff-docs$DOCS-$mode.csv"
  python3 "$CROSSDEV/run.py" \
    --devices "$DEVICES" \
    --topology "$TOPO" --nodes "$N" --scenario tiny-diff --docs "$DOCS" \
    --modes "$mode" --settle "$settle" --max-rounds 40 --out "$modeout"
  $COMPOSE down -v >/dev/null 2>&1 || true
  if [ ! -s "$OUT.tmp" ]; then head -1 "$modeout" > "$OUT.tmp"; fi
  tail -n +2 "$modeout" >> "$OUT.tmp"
done
mv "$OUT.tmp" "$OUT"
echo ">> combined -> $OUT"
