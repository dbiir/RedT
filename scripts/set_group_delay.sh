set -x
DELAY=${1:-0}
CENTER_COUNT=${2:-4}
for i in $(seq 10 21)
do
    # if [[ $i -ne 19 ]] 
    # then
        ssh 192.168.10.$i "sudo tc qdisc del root dev ib0 2>/dev/null"
        ssh 192.168.10.$i "sudo tc qdisc add dev ib0 root handle 1: prio bands 5"
        ssh 192.168.10.$i "sudo tc qdisc add dev ib0 parent 1:5 handle 50: netem delay ${DELAY}ms"
        for j in $(seq 10 21)
        do
            # if [[ $j -ne 19 ]] 
            # then
                let diff=i-j
                if [[ $diff -lt 0 ]]  
                then
                    let diff=0-diff
                fi
                let diff2=diff%CENTER_COUNT
                if [[ $diff2 -ne 0 ]] && [[ $diff -ne 0 ]] 
                then
                    ssh 192.168.10.$i "sudo tc filter add dev ib0 protocol ip parent 1:0 prio 4 u32 match ip dst 192.168.10.$j flowid 1:5"
                fi
            # fi
        done
    # fi
done

# ssh 192.168.10.18 "sudo tc qdisc del root dev ib0 2>/dev/null"
# ssh 192.168.10.18 "sudo tc qdisc add dev ib0 root handle 1: prio bands 5"
# ssh 192.168.10.18 "sudo tc qdisc add dev ib0 parent 1:5 handle 50: netem delay ${DELAY}ms"
# ssh 192.168.10.18 "sudo tc filter add dev ib0 protocol ip parent 1:0 prio 4 u32 match ip dst 192.168.10.10 flowid 1:5"
# for j in $(seq 12 17)
# do
#     ssh 192.168.10.18 "sudo tc filter add dev ib0 protocol ip parent 1:0 prio 4 u32 match ip dst 192.168.10.$j flowid 1:5"
# done
