set -x
IP_TITLE=192.168.1
DEVICE=ib0.806c
password=zb7022406
for i in $(seq 1 4)
do
    ssh $IP_TITLE.$i "sudo tc qdisc del root dev $DEVICE 2>/dev/null"
done
