#!/usr/bin/env sh
# Drive one benchmark pass through the dockerised benchnode pair.
#
#   ./compose-run.sh <ranges|default> [scenario] [docs]
#
# Recreates the stack fresh (clearing the badger volumes) with the matching
# --reconciliation flag, runs run.py in node: mode over the published control
# ports, then tears the stack down. Run the two modes as separate invocations:
#   ./compose-run.sh ranges  tiny-diff 50
#   ./compose-run.sh default tiny-diff 50
set -eu

MODE="${1:-ranges}"
SCENARIO="${2:-tiny-diff}"
DOCS="${3:-50}"

HERE="$(cd "$(dirname "$0")" && pwd)"           # .../tests/bench/crossdevice/docker
CROSSDEV="$(dirname "$HERE")"                    # .../tests/bench/crossdevice
REPO_ROOT="$(cd "$CROSSDEV/../../.." && pwd)"    # repo root (build context)
COMPOSE="docker compose -f $HERE/docker-compose.yml"

case "$MODE" in
  ranges)  RECON="--reconciliation" ;;
  default) RECON="" ;;
  *) echo "mode must be 'ranges' or 'default', got '$MODE'" >&2; exit 2 ;;
esac
export RECON

echo ">> building benchnode:local (context: $REPO_ROOT)"
docker build -f "$HERE/Dockerfile" -t benchnode:local "$REPO_ROOT"

echo ">> recreating stack fresh for mode=$MODE (RECON='$RECON')"
$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d --wait

echo ">> orchestrating run.py (mode=$MODE scenario=$SCENARIO docs=$DOCS)"
OUT="$CROSSDEV/results/docker-pair-$SCENARIO-docs$DOCS-$MODE.csv"
python3 "$CROSSDEV/run.py" \
    --devices node:127.0.0.1:17701,127.0.0.1:17702 \
    --topology pair --scenario "$SCENARIO" --docs "$DOCS" --modes "$MODE" \
    --out "$OUT" || { rc=$?; echo ">> run failed (rc=$rc); leaving stack up for inspection"; exit "$rc"; }

echo ">> wrote $OUT; tearing down"
$COMPOSE down -v
