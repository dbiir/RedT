while true
do
    if [[ -z "$(ps -aux | grep perf | grep report)" ]]
    then
        cd /home/centos/FlameGraph
        perf script -i /home/centos/perf.data | ./stackcollapse-perf.pl > out.perf-folded
        ./flamegraph.pl out.perf-folded > /home/centos/perf.svg
        break
    fi
done
