set -x
DELAY=${1}
RANGE=${2:-0}
for i in $(seq 10 21)
do
    # if [[ $i -ne 19 ]] 
    # then
    if [[ $RANGE -eq 0 ]]
    then 
        ssh 192.168.10.$i "sudo tc qdisc change dev ib0 parent 1:5 handle 50: netem delay ${DELAY}ms"
    else
        ssh 192.168.10.$i "sudo tc qdisc change dev ib0 parent 1:5 handle 50: netem delay ${DELAY}ms ${RANGE}ms distribution normal"
    fi
    # ssh 192.168.10.$i "sudo tc qdisc change dev ib0 parent 1:5 handle 50: netem delay ${DELAY}ms"
    # fi
done
