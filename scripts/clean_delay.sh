set -x
for i in $(seq 144 147)
do
    ssh 10.77.110.$i "sudo tc qdisc del root dev em1 2>/dev/null"
done
