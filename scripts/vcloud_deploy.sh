#!/bin/bash

LOCAL_UNAME="$5"
USERNAME="$5"
HOSTS="$1"
PATHE="$2"
NODE_CNT="$3"
count=0
mc=0
for HOSTNAME in ${HOSTS}; do
    scp cpu_monitor.sh ${USERNAME}@${HOSTNAME}:${PATHE}
    ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${USERNAME}@${HOSTNAME} "rm -rf /tmp/${USERNAME}_* ${USERNAME} ${count}" &
done
for HOSTNAME in ${HOSTS}; do
    #SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 10m 10m gdb -batch -ex \"run\" -ex \"bt\" --args ./rundb -nid${count} >> results.out 2>&1 | grep -v ^\"No stack.\"$"
    if [ $count -ge $NODE_CNT ]; then
        SCRIPT="source /etc/profile;env SCHEMA_PATH=\"$2\" timeout -k 15m 15m ${PATHE}runcl -nid${count} > ${PATHE}clresults${count}.out 2>&1"
        echo "${HOSTNAME}: runcl ${count}"
    else
        SCRIPT="source /etc/profile;env SCHEMA_PATH=\"$2\" timeout -k 15m 15m ${PATHE}rundb -nid${count} > ${PATHE}dbresults${count}.out 2>&1"
        echo "${HOSTNAME}: rundb ${count}"
        ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${USERNAME}@${HOSTNAME} "source /etc/profile;bash ${PATHE}cpu_monitor.sh ${USERNAME} ${count}" &
        let mc=mc+1
    fi
    ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${USERNAME}@${HOSTNAME} "${SCRIPT}" &
    count=`expr $count + 1`
done

sleep 90
OLD_IFS="$IFS"
IFS=" "
HOSTLIST=($HOSTS)
IFS="$OLD_IFS"
scp wkdbperf.sh ${USERNAME}@${HOSTLIST[0]}:${PATHE}
ssh ${USERNAME}@${HOSTLIST[0]} "bash ${PATHE}/wkdbperf.sh $4"

let count=count+mc
while [ $count -gt 0 ]
do
    wait $pids
    count=`expr $count - 1`
done
