set -x
DELAY=${1:-0}
CENTER_COUNT=${2:-4}
IP_TITLE=192.168.1
DEVICE=lo
for i in $(seq 1 4)
do
    ssh $IP_TITLE.$i "sudo tc qdisc del root dev $DEVICE 2>/dev/null"
    ssh $IP_TITLE.$i "sudo tc qdisc add dev $DEVICE root handle 1: prio bands 5"
    ssh $IP_TITLE.$i "sudo tc qdisc add dev $DEVICE parent 1:5 handle 50: netem delay ${DELAY}ms"
    for j in $(seq 1 4)
    do
        ssh $IP_TITLE.$i "sudo tc filter add dev $DEVICE protocol ip parent 1:0 prio 4 u32 match ip dst $IP_TITLE.$j flowid 1:5"
    done
done

# ssh 192.168.10.18 "sudo tc qdisc del root dev ib0 2>/dev/null"
# ssh 192.168.10.18 "sudo tc qdisc add dev ib0 root handle 1: prio bands 5"
# ssh 192.168.10.18 "sudo tc qdisc add dev ib0 parent 1:5 handle 50: netem delay ${DELAY}ms"
# ssh 192.168.10.18 "sudo tc filter add dev ib0 protocol ip parent 1:0 prio 4 u32 match ip dst 192.168.10.10 flowid 1:5"
# for j in $(seq 12 17)
# do
#     ssh 192.168.10.18 "sudo tc filter add dev ib0 protocol ip parent 1:0 prio 4 u32 match ip dst 192.168.10.$j flowid 1:5"
# done
