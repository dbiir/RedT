set -x
DELAY=${1}
RANGE=${2:-0}
IP_TITLE=192.168.1
DEVICE=ib0.806c
password=zb7022406
for i in $(seq 1 4)
do
    # if [[ $i -ne 19 ]] 
    # then
    if [[ $RANGE -eq 0 ]]
    then 
        ssh $IP_TITLE.$i "sudo tc qdisc change dev $DEVICE parent 1:5 handle 50: netem delay ${DELAY}ms"
    else
        ssh $IP_TITLE.$i "sudo tc qdisc change dev $DEVICE parent 1:5 handle 50: netem delay ${DELAY}ms ${RANGE}ms distribution normal"
    fi
    # ssh 192.168.10.$i "sudo tc qdisc change dev ib0 parent 1:5 handle 50: netem delay ${DELAY}ms"
    # fi
done
