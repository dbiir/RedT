set -x
declare -A array

# 144->145
array[0,0]=11
# 144->146
array[0,1]=15
# 144->147
array[0,2]=97
# 144->148
array[0,3]=0

# 145->146
array[1,0]=16
# 145->147
array[1,1]=102
# 145->148
array[1,2]=0

# 146->147
array[2,0]=112
# 146->148
array[2,1]=0

# 147->148
array[3,0]=0


for i in {144..147}; do
    if [ $i -eq 147 ] || [ $i -eq 148 ]; then
        INTERFACE="eno1"
    else
        INTERFACE="em1"
    fi
    ssh 10.77.110.$i "sudo tc qdisc del root dev $INTERFACE 2>/dev/null"
    ssh 10.77.110.$i "sudo tc qdisc add dev $INTERFACE root handle 1:0 htb"

    n=$((147 - i + 1))
    j=0
    while [ $j -lt $n ]; do
        if [ ${array[$((i-144)),$j]} -ne 0 ]; then
            ssh 10.77.110.$i "sudo tc filter add dev $INTERFACE parent 1:0 prior 2 protocol ip u32 match ip dst 10.77.110.$((i + j + 1)) classid 1:$((j + 1))"
            ssh 10.77.110.$i "sudo tc class add dev $INTERFACE parent 1:0 classid 1:$((j + 1)) htb rate 10Gbit"
            ssh 10.77.110.$i "sudo tc qdisc add dev $INTERFACE parent 1:$((j + 1)) handle $((j + 2)):0 netem delay ${array[$((i-144)),$j]}ms"
        fi
        j=$((j + 1))
    done
done