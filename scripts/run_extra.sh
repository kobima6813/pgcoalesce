#!/bin/bash
ulimit -n 65535; cd /workspace/coalesce; U=postgres://postgres@127.0.0.1
for rep in 1 2 3; do for d in uniform zipf; do
 ./loadgen/loadgen -target "$U:56433/postgres?sslmode=disable" -mode simple -conns 500 -rate 5000 -dur 1s -dist $d -label "main:pgb4_simple:rep$rep" >> results_extra.jsonl
 ./loadgen/loadgen -target "$U:56432/postgres?sslmode=disable" -mode simple -conns 500 -rate 5000 -dur 1s -dist $d -label "main:pgb_simple:rep$rep" >> results_extra.jsonl
 ./loadgen/loadgen -target "$U:56500/postgres?sslmode=disable" -mode simple -conns 500 -rate 5000 -dur 1s -dist $d -label "main:coal_2ms:rep$rep" >> results_extra.jsonl
done; done
for r in 10000 20000; do for p in "56433 pgb4_simple" "56503 coal_5ms" "56502 coal_1ms"; do set -- $p
 ./loadgen/loadgen -target "$U:$1/postgres?sslmode=disable" -mode simple -conns 500 -rate $r -dur 2s -dist uniform -label "sweep:$2" >> results_extra.jsonl
done; done
echo DONE
