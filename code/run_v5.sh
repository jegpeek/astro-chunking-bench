#!/bin/bash
# v5 main run: grid (aws_bench_v5.py) then AI-style concurrent load (aws_ai_load.py), sequential on one instance.
W=$PWD; cp ./aws_ai_load.py ./aws_bench_v5.py ./joint2.py ~/bench/; cd ~/bench; L=$W/run_v5.log
PY=~/miniforge3/envs/roman/bin/python; B=s3://jegp-chunkbench-20261125
rm -f aws_results_v5.parquet aws_results_ai.parquet
run(){ echo "== $(date +%T) $*" >> $L; "$@" 2>&1 | grep -v -i warn | grep -E "done|DONE|q/s|Error|Traceback|line [0-9]" >> $L; }
run $PY aws_bench_v5.py jegp-chunkbench-20261125 10 8
cp aws_results_v5.parquet $W/
for N in 50 200; do
  AI_LABEL=tiled_tm_1p run $PY aws_ai_load.py tiled $N 300 1 16 $B/stores/tm_N$N 14 hats
  AI_LABEL=tiled_tm_8p run $PY aws_ai_load.py tiled $N 300 8 2 $B/stores/tm_N$N 14 hats
done
AI_LABEL=blob_o14_8p     run $PY aws_ai_load.py tiled 200 300 8 2 $B/stores_v5/blob_o14 14 blob
AI_LABEL=blob_o14_lz4_8p run $PY aws_ai_load.py tiled 200 300 8 2 $B/stores_v5/blob_o14_lz4 14 blob
AI_LABEL=tm_o13_8p       run $PY aws_ai_load.py tiled 200 300 8 2 $B/stores_v5/tm_o13 13 hats
AI_LABEL=native_best_8p  run $PY aws_ai_load.py native_best 50 100 8 2
AI_LABEL=native_best_8p  run $PY aws_ai_load.py native_best 200 50 8 2
AI_LABEL=roman_official_8p run $PY aws_ai_load.py roman_official 50 100 8 2
cp aws_results_ai.parquet $W/
set +u; source ~/lsst/loadLSST.sh >/dev/null 2>&1; setup lsst_distrib >/dev/null 2>&1
AI_LABEL=butler_8p run python aws_ai_load.py butler 50 100 8 2
AI_LABEL=butler_8p run python aws_ai_load.py butler 200 50 8 2
cp aws_results_ai.parquet $W/; echo "== $(date +%T) ALL DONE" >> $L
