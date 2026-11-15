#!/bin/bash
ulimit -n 65535; cd /workspace/coalesce
LG=./loadgen2/loadgen2; U=postgres://postgres@127.0.0.1
PA=$(pgrep -f "conf3/bench2ms.ini"); PF=$(pgrep -f "conf3/bench5ms.ini"); PV1=$(pgrep -f "proxy/proxy -listen 127.0.0.1:56503")
B20=$(cat pgb_conf/pgb.pid); B4=$(cat pgb_conf/pgb4.pid)
OUT=${OUT:-results_v3.jsonl}
run(){ $LG -target "$U:$2/postgres?sslmode=disable" -pids "$3" -mode $4 -conns 500 -rate $5 -dur $7 -dist $6 -label "$8:$1" >> $OUT 2>>err_v3.log; }
if [ "$PHASE" != "overload" ]; then
for rep in 1 2 3; do for d in uniform zipf; do for m in prepared exec; do
  run pgb20 56432 $B20 $m 5000 $d 2s main_$m
  run pgb4 56433 $B4 $m 5000 $d 2s main_$m
  run adapt2ms 56600 $PA $m 5000 $d 2s main_$m
  run adapt5ms 56601 $PF $m 5000 $d 2s main_$m
done; done; done
for rep in 1 2 3; do for rate in 250 1000; do
  run pgb4 56433 $B4 prepared $rate uniform 4s low
  run adapt2ms 56600 $PA prepared $rate uniform 4s low
  run adapt5ms 56601 $PF prepared $rate uniform 4s low
done; done
for rate in 2500 10000 20000; do for d in uniform zipf; do
  run pgb4 56433 $B4 prepared $rate $d 2s sweep
  run adapt2ms 56600 $PA prepared $rate $d 2s sweep
  run adapt5ms 56601 $PF prepared $rate $d 2s sweep
done; done
fi
if [ "$PHASE" != "main" ]; then
# overload under host CPU contention: 7 busy-loop burners on 8 cores
for i in 1 2 3 4 5 6 7; do (timeout 400 sh -c 'while :; do :; done' &) ; done
sleep 2
for rep in 1 2; do for rate in 20000 40000; do
  run pgb4 56433 $B4 prepared $rate uniform 3s overload
  run adapt2ms 56600 $PA prepared $rate uniform 3s overload
  run v1_5ms 56503 $PV1 simple $rate uniform 3s overload
done; done
pkill -f "while :; do :; done"
fi
echo DONE
