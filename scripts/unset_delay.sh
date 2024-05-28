set -x
for i in {144..147}; do
    if [ $i -eq 147 ] || [ $i -eq 148 ]; then
        INTERFACE="eno1"
    else
        INTERFACE="em1"
    fi
    ssh 10.77.110.$i "sudo tc qdisc del root dev $INTERFACE 2>/dev/null"
done