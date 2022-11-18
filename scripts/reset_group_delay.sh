set -x
DELAY=${1}
for i in $(seq 10 17)
do
    ssh 192.168.10.$i "sudo tc qdisc change dev ib0 parent 1:5 handle 50: netem delay ${DELAY}ms"
done
