set -x
IP_TITLE=192.168.1
DEVICE=ib0.806c
DEVICE2=lo
password=zb7022406
for i in $(seq 1 4)
do
    # if [[ $i -ne 19 ]] 
    # then
    ssh $IP_TITLE.$i "sudo tc qdisc del dev $DEVICE root"
    ssh $IP_TITLE.$i "sudo tc qdisc del dev $DEVICE2 root"
    # fi
done
