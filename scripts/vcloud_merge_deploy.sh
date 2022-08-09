#!/bin/bash
LOCAL_UNAME=deneva
USERNAME=deneva
HOSTS="$1"
NODE_CNT="$3"
count=0

for HOSTNAME in ${HOSTS}; do
    #SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 10m 10m gdb -batch -ex \"run\" -ex \"bt\" --args ./rundb -nid${count} >> results.out 2>&1 | grep -v ^\"No stack.\"$"
    if [ $count -ge $NODE_CNT ]; then
        SCRIPT="source /etc/profile; env SCHEMA_PATH=\"$2\" timeout -k 15m 15m /data1/deneva/runcl -nid${count} > /data1/deneva/clresults${count}.out 2>&1"
        echo "${HOSTNAME}: runcl ${count}"
    else
        SCRIPT="source /etc/profile; env SCHEMA_PATH=\"$2\" timeout -k 15m 15m /data1/deneva/rundb -nid${count} > /data1/deneva/dbresults${count}.out 2>&1"
        echo "${HOSTNAME}: rundb ${count}"
    fi
    ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${HOSTNAME} "${SCRIPT}" &
    count=`expr $count + 1`
done

sleep 90
scp wkdbperf.sh deneva@9.39.242.136:/data1/deneva/
ssh deneva@9.39.242.136 "bash /data1/deneva/wkdbperf.sh $4"

while [ $count -gt 0 ]
do
    wait $pids
    count=`expr $count - 1`
done

