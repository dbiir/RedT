set -x
for i in $(seq 10 17)
do
    ssh 192.168.10.$i "sudo tc qdisc del dev ib0 root"
done
