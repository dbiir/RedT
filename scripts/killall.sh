set -x
ps -aux | grep runcl | awk '{print $2}' | xargs kill -9 2>/dev/null 1>/dev/null
ps -aux | grep rundb | awk '{print $2}' | xargs kill -9 2>/dev/null 1>/dev/null

for i in $(seq 10 17)
do
    ssh 192.168.10.$i "ps -aux | grep rundb | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
    ssh 192.168.10.$i "ps -aux | grep runcl | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
    ssh 192.168.10.$i "rm -rf /data/core/*" 2>/dev/null 1>/dev/null
    ssh 192.168.10.$i "rm -rf /home/core/*" 2>/dev/null 1>/dev/null
    ssh 192.168.10.$i "rm -rf /home/ibtest/core*" 2>/dev/null 1>/dev/null
done
