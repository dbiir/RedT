while true
do
    if [[ -z "$(ps -aux | grep perf | grep report)" ]]
    then
        cd /data1/deneva/FlameGraph
        perf script -i /data1/deneva/perf.data | ./stackcollapse-perf.pl > out.perf-folded
        ./flamegraph.pl out.perf-folded > /data1/deneva/perf.svg
        break
    fi
done
