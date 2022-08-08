set -x
for i in $(seq 144 147)
do
    ssh 10.77.110.$i "sudo tc qdisc del root dev em1 2>/dev/null"
    ssh 10.77.110.$i "sudo tc qdisc add dev em1 root netem delay ${1}ms"
done
