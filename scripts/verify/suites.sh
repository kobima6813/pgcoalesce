#!/bin/bash
# suites.sh <tag> <seed> <dur>
T=$1; S=$2; D=$3; R=/workspace/pgcoalesce-fixed/runs; PY=/workspace/coalesce/venv/bin/python; W="$PY $R/wrap.py"
cd /workspace/coalesce/tests
$W http://127.0.0.1:56710 iso_$T -- $PY iso_test.py $D $S 56610 http://127.0.0.1:56710/stats > $R/${T}_iso.out 2>&1
$W http://127.0.0.1:56705 diff_$T -- $PY $R/diff_correct.py $S $D > $R/${T}_diff.out 2>&1
$W http://127.0.0.1:56710 txiso_$T -- /workspace/coalesce/proxy3/txiso.fixed -dur ${D}s -seed $S -proxy 56610 -direct 55433 > $R/${T}_txiso.out 2>&1
(cd ../nodet; $W http://127.0.0.1:56700 differr_$T -- node differr.js $S $D > $R/${T}_differr.out 2>&1)
$W http://127.0.0.1:56713 volpol_$T -- $PY volpol_test.py > $R/${T}_volpol.out 2>&1
echo SUITES_DONE $T
