#!/bin/bash
ulimit -n 65535; cd /workspace/coalesce
LG=./loadgen2/loadgen2; U=postgres://postgres@127.0.0.1
PA=$(pgrep -f "pgcoalesce -listen 127.0.0.1:56600"); PF=$(pgrep -f "pgcoalesce -listen 127.0.0.1:56601")
B20=$(cat pgb_conf/pgb.pid); B4=$(cat pgb_conf/pgb4.pid)
run(){ # name port pid mode rate dist dur label
  $LG -target "$U:$2/postgres?sslmode=disable" -pids "$3" -mode $4 -conns 500 -rate $5 -dur $7 -dist $6 -label "$8:$1" >> results_v2.jsonl 2>>err_v2.log; }
OUT=results_v2.jsonl
for rep in 1 2 3; do for d in uniform zipf; do
  for m in prepared exec; do
    run pgb20 56432 $B20 $m 5000 $d 2s main_$m
    run pgb4  56433 $B4  $m 5000 $d 2s main_$m
    run adaptive 56600 $PA $m 5000 $d 2s main_$m
    run fixed2ms 56601 $PF $m 5000 $d 2s main_$m
  done
done; done
for rep in 1 2 3; do for rate in 250 1000; do
  run pgb20 56432 $B20 prepared $rate uniform 4s low
  run pgb4  56433 $B4  prepared $rate uniform 4s low
  run adaptive 56600 $PA prepared $rate uniform 4s low
  run fixed2ms 56601 $PF prepared $rate uniform 4s low
done; done
for rate in 2500 10000 20000; do for d in uniform zipf; do
  run pgb20 56432 $B20 prepared $rate $d 2s sweep
  run pgb4  56433 $B4  prepared $rate $d 2s sweep
  run adaptive 56600 $PA prepared $rate $d 2s sweep
done; done
echo DONE
