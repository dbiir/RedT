set -x
DELAY=${1}
CENTER_COUNT=${2}
for i in $(seq 10 17)
do
    ssh 192.168.10.$i "sudo tc qdisc del root dev ib0 2>/dev/null"
    ssh 192.168.10.$i "sudo tc qdisc add dev ib0 root handle 1: prio bands 5"
    ssh 192.168.10.$i "sudo tc qdisc add dev ib0 parent 1:5 handle 50: netem delay ${DELAY}ms"
    for j in $(seq 10 17)
    do
        let diff=i-j
        if [[ $diff -lt 0 ]]  
        then
            let diff=0-diff
        fi
        if [[ $diff -ne ${CENTER_COUNT} ]] && [[ $diff -ne 0 ]] 
        then
            ssh 192.168.10.$i "sudo tc filter add dev ib0 protocol ip parent 1:0 prio 4 u32 match ip dst 192.168.10.$j flowid 1:5"
        fi
    done
done
