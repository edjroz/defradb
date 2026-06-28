#!/usr/bin/env sh
# Sample per-node block growth + container CPU/mem to a CSV while a run is in
# flight, so you can observe how both nodes grow over a session. Run it on the
# HOST in a second terminal, alongside compose-run.sh.
#
#   ./monitor.sh [out.csv] [interval_seconds]
#
# Stop with Ctrl-C. Columns: ts,node,blocks,blockBytes,cpuPct,memUsage
set -eu

OUT="${1:-growth.csv}"
INT="${2:-2}"
PROJECT="benchnode-crossdevice"

echo "ts,node,blocks,blockBytes,cpuPct,memUsage" > "$OUT"
echo ">> sampling every ${INT}s -> $OUT (Ctrl-C to stop)"

while true; do
  TS=$(date +%s)
  # "service host:port" pairs — control ports published by docker-compose.yml.
  for entry in "node0:127.0.0.1:17701" "node1:127.0.0.1:17702"; do
    NAME="${entry%%:*}"
    ADDR="${entry#*:}"
    BS=$(curl -s -m 5 "http://$ADDR/blockstats" 2>/dev/null || echo '{}')
    PARSED=$(printf '%s' "$BS" | python3 -c \
      "import sys,json;d=json.load(sys.stdin);print(d.get('blocks',''),d.get('blockBytes',''))" \
      2>/dev/null || echo " ")
    BLOCKS="${PARSED%% *}"; BYTES="${PARSED##* }"
    STATS=$(docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}}' \
      "${PROJECT}-${NAME}-1" 2>/dev/null || echo ',')
    echo "$TS,$NAME,$BLOCKS,$BYTES,$STATS" >> "$OUT"
  done
  sleep "$INT"
done
