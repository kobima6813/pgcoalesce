#!/bin/bash
# ab.sh rate reps -> alternating old(56600) / fixed(56620) prepared uniform 4s
cd /workspace/coalesce; ulimit -n 65535; R=/workspace/pgcoalesce-fixed/runs
PO=$(pgrep -f "v3orig -config ../conf3/bench2ms.ini"); PN=$(pgrep -f "listen_addr=127.0.0.1:56620")
for i in $(seq 1 $2); do A="old 56600 56700 $PO"; B="fixed 56620 56720 $PN"; [ -n "$REV" ] && { T=$A; A=$B; B=$T; }
for x in "$A" "$B"; do set -- $x $1; n=$1; port=$2; mp=$3; pid=$4; rate=$5
 s0=$(curl -s http://127.0.0.1:$mp/stats)
 ./loadgen2/loadgen2 -target "postgres://postgres@127.0.0.1:$port/postgres?sslmode=disable" -pids $pid -mode prepared -conns 500 -rate $rate -dur 4s -dist uniform -label "ab_$n" >> $R/ab_$rate.jsonl 2>>$R/ab_err.log
 s1=$(curl -s http://127.0.0.1:$mp/stats)
 python3 -c "import json,sys;a=json.loads(sys.argv[1]);b=json.loads(sys.argv[2]);print(sys.argv[3], {k:b[k]-a[k] for k in b if isinstance(b[k],(int,float)) and k in a and b[k]!=a[k] and k not in ('uptime_s',)})" "$s0" "$s1" $n >> $R/ab_${rate}_stats.txt
 set -- $rate
done; done
