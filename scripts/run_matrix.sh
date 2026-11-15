#!/bin/bash
ulimit -n 65535
cd /workspace/coalesce
LG=./loadgen/loadgen
U=postgres://postgres@127.0.0.1
declare -A T=( [pgb_simple]="56432 simple" [pgb_prepared]="56432 prepared" [proxy_pass]="56501 simple" [coal_1ms]="56502 simple" [coal_2ms]="56500 simple" [coal_5ms]="56503 simple" [coal_10ms]="56504 simple" )
ORDER="pgb_simple pgb_prepared proxy_pass coal_1ms coal_2ms coal_5ms coal_10ms"
# main test: 5000 req in 1s, 500 conns, uniform & zipf, 3 reps
for rep in 1 2 3; do for d in uniform zipf; do for k in $ORDER; do set -- ${T[$k]}
  $LG -target "$U:$1/postgres?sslmode=disable" -mode $2 -conns 500 -rate 5000 -dur 1s -dist $d -label "main:$k:rep$rep" >> results_main.jsonl 2>>err.log
done; done; done
# sweep: rates, zipf+uniform, 2s duration
for d in uniform zipf; do for rate in 250 1000 2500 5000 10000 20000; do for k in pgb_simple pgb_prepared coal_1ms coal_5ms; do set -- ${T[$k]}
  $LG -target "$U:$1/postgres?sslmode=disable" -mode $2 -conns 500 -rate $rate -dur 2s -dist $d -label "sweep:$k" >> results_sweep.jsonl 2>>err.log
done; done; done
echo DONE
