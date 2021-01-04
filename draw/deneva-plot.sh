#!/bin/bash
set -x

while [[ $# -gt 0 ]]
do
    case $1 in
        -a)
            TEST_TYPE=$2
            shift
            shift
            ;;
        -c)
            CC=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        -c2)
            CC2=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        -t)
            RESULT_PATH=../results/$2
            shift
            shift
            ;;
        *)
            shift
            ;;
    esac
done

TpmC="plot "
Rollback="plot "
Distributed="plot "
l=${#CC[@]}
let l--
for i in $(seq 0 ${l})
do
    TpmC=$TpmC"\"${RESULT_PATH}/tmp-"${CC[$i]}"\" using 1:2 title \"${CC[$i]}\" w lp lw 2 ps 1 pt $i dt 1"
    Rollback=$Rollback"\"${RESULT_PATH}/tmp-"${CC[$i]}"\" using 1:(\$3*100) title \"${CC[$i]}\" w lp lw 2 ps 1 pt $i dt 1"
    Distributed=$Distributed"\"${RESULT_PATH}/tmp-"${CC[$i]}"\" using 1:(\$4*100) title \"${CC[$i]}\" w lp lw 2 ps 1 pt $i dt 1"
    if [ $i != ${l} ]
    then
        TpmC=$TpmC","
        Rollback=$Rollback","
        Distributed=$Distributed","
    fi
    # echo ${CC[$i]}
    # echo ${RESULT_PATH[$i]}
done
#echo $TpmC
#echo $Rollback
#echo $Distributed
if [[ ! -z "${CC2[@]}" ]]
then
l=${#CC2[@]}
let l--
for i in $(seq 0 ${l})
do
    let "I=$i+${#CC[@]}"
    TpmC=$TpmC",""\"${RESULT_PATH}/tmp-"${CC2[$i]}".back\" using 1:2 title \"${CC2[$i]}\" w lp lw 2 ps 1 pt $I dt 1"
    Rollback=$Rollback",""\"${RESULT_PATH}/tmp-"${CC2[$i]}".back\" using 1:(\$3*100) title \"${CC2[$i]}\" w lp lw 2 ps 1 pt $I dt 1"
    Distributed=$Distributed",""\"${RESULT_PATH}/tmp-"${CC2[$i]}".back\" using 1:(\$4*100) title \"${CC2[$i]}\" w lp lw 2 ps 1 pt $I dt 1"
done
fi

cp deneva-draw.plt ${RESULT_PATH}/draw-multi.plt
sed -i 's?OUTPUT?'${RESULT_PATH}'?g' ${RESULT_PATH}/draw-multi.plt
sed -i "17c $TpmC" ${RESULT_PATH}/draw-multi.plt
sed -i "22c $Rollback" ${RESULT_PATH}/draw-multi.plt
sed -i "29c $Distributed" ${RESULT_PATH}/draw-multi.plt

if [[ $TEST_TYPE == "ycsb_skew" ]]
then
sed -i "6c set xlabel \"Skew Factor (Theta)\"" ${RESULT_PATH}/draw-multi.plt
elif [[ $TEST_TYPE == "ycsb_writes" ]]
then
sed -i "6c set xlabel \"% of Update Transactions\"" ${RESULT_PATH}/draw-multi.plt
elif [[ $TEST_TYPE == "ycsb_scaling" ]]
then
sed -i "6c set xlabel \"Server Count (Log Scale)\"" ${RESULT_PATH}/draw-multi.plt
elif [[ $TEST_TYPE == "tpcc_scaling2" ]]
then
sed -i "6c set xlabel \"Server Count (Log Scale)\"" ${RESULT_PATH}/draw-multi.plt
elif [[ $TEST_TYPE == "ycsb_stress" ]]
then
sed -i "6c set xlabel \"Max Trans In Flight\"" ${RESULT_PATH}/draw-multi.plt
elif [[ $TEST_TYPE == "tpcc_stress" ]]
then
sed -i "6c set xlabel \"Max Trans In Flight\"" ${RESULT_PATH}/draw-multi.plt
elif [[ $TEST_TYPE == "tpcc_cstress" ]]
then
sed -i "6c set xlabel \"Max Trans In Flight\"" ${RESULT_PATH}/draw-multi.plt
fi


../gnuplot-5.2.8/bin/gnuplot ${RESULT_PATH}/draw-multi.plt

cat ${RESULT_PATH}/tmp-MVCC
cat ${RESULT_PATH}/tmp-WOOKONG
