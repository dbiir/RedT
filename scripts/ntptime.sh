set -x
for i in $(seq 10 17)
do
    ssh 192.168.10.$i "sudo ntpdate 192.168.10.12" 2>/dev/null 1>/dev/null
done

