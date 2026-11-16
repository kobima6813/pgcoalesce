#!/bin/bash
ulimit -n 65535; cd /workspace/coalesce
LG=./loadgen2/loadgen2; U=postgres://postgres@127.0.0.1; OUT=/workspace/pgc-issue12/bench.jsonl
B4=$(cat pgb_conf/pgb4.pid)
PB=$(pgrep -f "listen_addr=127.0.0.1:56650"); PA=$(pgrep -f "listen_addr=127.0.0.1:56651"); PAF=$(pgrep -f "listen_addr=127.0.0.1:56652"); PBF=$(pgrep -f "listen_addr=127.0.0.1:56653")
run(){ $LG -target "$U:$2/postgres?sslmode=disable" -pids "$3" -mode $4 -conns 500 -rate $5 -dur $7 -dist $6 -label "$8:$1" >> $OUT 2>>/workspace/pgc-issue12/bench_err.log; }
# warm
for p in "56650 $PB" "56651 $PA" "56652 $PAF" "56653 $PBF"; do set -- $p; run warm $1 $2 prepared 2000 uniform 2s warm; run warm $1 $2 exec 2000 uniform 1s warm; done
for rep in 1 2 3; do for d in uniform zipf; do for m in prepared exec; do
  run pgb4 56433 $B4 $m 5000 $d 2s main_$m
  run before 56650 $PB $m 5000 $d 2s main_$m
  run after 56651 $PA $m 5000 $d 2s main_$m
done; done; done
for rep in 1 2 3; do
  run pgb4 56433 $B4 prepared 100 uniform 6s low100
  run before 56650 $PB prepared 100 uniform 6s low100
  run before_fixed 56653 $PBF prepared 100 uniform 6s low100
  run after 56651 $PA prepared 100 uniform 6s low100
  run after_fixed 56652 $PAF prepared 100 uniform 6s low100
done
echo DONE
