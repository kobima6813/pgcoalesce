#!/bin/bash
# proxies.sh <binary> <logtag> [extra -set args...]  : (re)start bench2ms/bench5ms/secure/correct2ms/volpol(secure@56613)
BIN=$1; TAG=$2; shift 2; cd /workspace/coalesce/proxy3; L=/workspace/pgcoalesce-fixed/logs
pkill -f -- "-config ../conf3/" ; sleep 1.5
for c in bench2ms bench5ms secure correct2ms; do nohup $BIN -config ../conf3/$c.ini "$@" > $L/${TAG}_$c.log 2>&1 & done
nohup $BIN -config ../conf3/secure.ini -set listen_addr=127.0.0.1:56613 -set metrics_addr=127.0.0.1:56713 "$@" > $L/${TAG}_volpol.log 2>&1 &
sleep 2; pgrep -af -- "-config ../conf3/" | cut -c1-200
