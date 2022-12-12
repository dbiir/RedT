set -x
for i in $(seq 10 21)
do
    # if [[ $i -ne 19 ]] 
    # then
    ssh 192.168.10.$i "sudo tc qdisc del dev ib0 root"
    # fi
done
