#!/bin/bash
LOCAL_UNAME=centos
USERNAME=centos
HOSTS="$1"
NODE_CNT="$3"
count=0

for HOSTNAME in ${HOSTS}; do
    #SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 10m 10m gdb -batch -ex \"run\" -ex \"bt\" --args ./rundb -nid${count} >> results.out 2>&1 | grep -v ^\"No stack.\"$"
    if [ $count -ge $NODE_CNT ]; then
        SCRIPT="source /etc/profile; env SCHEMA_PATH=\"$2\" timeout -k 15m 15m /root/runcl -nid${count} > /root/clresults.out 2>&1"
        echo "${HOSTNAME}: runcl ${count}"
    else
        SCRIPT="source /etc/profile; env SCHEMA_PATH=\"$2\" timeout -k 15m 15m /root/rundb -nid${count} > /root/dbresults.out 2>&1"
        echo "${HOSTNAME}: rundb ${count}"
    fi
    ssh 10.77.110.${HOSTNAME} "${SCRIPT}" &
    count=`expr $count + 1`
done

sleep 90
scp wkdbperf.sh 10.77.110.146:/root/
ssh 10.77.110.146 "bash /root/wkdbperf.sh $4"

while [ $count -gt 0 ]
do
    wait $pids
    count=`expr $count - 1`
done
