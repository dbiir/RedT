#!/bin/bash
LOCAL_UNAME=centos
USERNAME=centos
HOSTS="$1"
NODE_CNT="$3"
count=0

for HOSTNAME in ${HOSTS}; do
    #SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 10m 10m gdb -batch -ex \"run\" -ex \"bt\" --args ./rundb -nid${count} >> results.out 2>&1 | grep -v ^\"No stack.\"$"
    if [ $count -ge $NODE_CNT ]; then
        SCRIPT="source /etc/profile;env SCHEMA_PATH=\"$2\" timeout -k 8m 8m /home/centos/runcl -nid${count} > /home/centos/results.out 2>&1"
        echo "${HOSTNAME}: runcl ${count}"
    else
        SCRIPT="source /etc/profile;env SCHEMA_PATH=\"$2\" timeout -k 8m 8m /home/centos/rundb -nid${count} > /home/centos/results.out 2>&1"
        echo "${HOSTNAME}: rundb ${count}"
    fi
    ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} 10.77.70.${HOSTNAME} "${SCRIPT}" &
    count=`expr $count + 1`
done

while [ $count -gt 0 ]
do
    wait $pids
    count=`expr $count - 1`
done
