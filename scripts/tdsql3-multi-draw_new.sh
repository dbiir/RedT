#!/bin/bash
set -x
index=0
for i in $@
do
    index=`expr $index + 1`
    isPath=`expr $index % 2`

    if [[ $isPath == 1 ]]
    then
        j=`expr $index / 2`
    else
        j=`expr $index / 2 - 1`
    fi

    if [ $index == 1 ]
    then
        CONFIG_FILE=$i
        source "$(cd "$(dirname "$0")"; pwd -P)/$CONFIG_FILE"
    elif [ $index == 2 ]
    then
        TEST_TYPE=$i
    elif [ $isPath == 1 ]
    then
        RESULT_PATH[$j]="$BASE_PATH/result/$i"
        SHORT_PATH[$j]=$i
    else
        LEVEL[$j]=$i
    fi
done


if [[ $TEST_TYPE == "tpcc" ]]
then
    TpmC="plot "
    Rollback="plot "
    Distributed="plot "
    for i in $(seq 1 $j)
    do
        TpmC=$TpmC"\"${RESULT_PATH[$i]}/$TEST_TYPE/result_set.txt\" using 1:2 title \"${LEVEL[$i]}\" w lp lw 2 ps 2 pt $i dt 1"
        Rollback=$Rollback"\"${RESULT_PATH[$i]}/$TEST_TYPE/analyse.txt\" using 1:2 title \"${LEVEL[$i]}\" w lp lw 2 ps 2 pt $i dt 1"
        Distributed=$Distributed"\"${RESULT_PATH[$i]}/$TEST_TYPE/analyse.txt\" using 1:3 title \"${LEVEL[$i]}\" w lp lw 2 ps 2 pt $i dt 1"
        if [ $i != $j ]
        then
            TpmC=$TpmC","
            Rollback=$Rollback","
            Distributed=$Distributed","
        fi
        echo ${LEVEL[$i]}
        echo ${RESULT_PATH[$i]}
    done
    echo $TpmC
    echo $Rollback
    echo $Distributed

    cp draw-multi-template-tpcc-ran.plt ${RESULT_PATH[1]}/draw-multi.plt
    sed -i 's?OUTPUT?'${RESULT_PATH[1]}'?g' ${RESULT_PATH[1]}/draw-multi.plt
    sed -i "17c $TpmC" ${RESULT_PATH[1]}/draw-multi.plt
    sed -i "22c $Rollback" ${RESULT_PATH[1]}/draw-multi.plt
    sed -i "29c $Distributed" ${RESULT_PATH[1]}/draw-multi.plt
elif [[ $TEST_TYPE == "ycsb" ]]
then
    TpmC="plot "
    Rollback="plot "
    Distributed="plot "
    for i in $(seq 1 $j)
    do
        TpmC=$TpmC"\"${RESULT_PATH[$i]}/${TEST_TYPE}${YCSB_WORKLOAD_TYPE}/result_set.txt\" using 1:2 title \"${LEVEL[$i]}\" w lp lw 2 ps 2 pt $i dt 1"
        Rollback=$Rollback"\"${RESULT_PATH[$i]}/$TEST_TYPE${YCSB_WORKLOAD_TYPE}/analyse.txt\" using 1:2 title \"${LEVEL[$i]}\" w lp lw 2 ps 2 pt $i dt 1"
        Distributed=$Distributed"\"${RESULT_PATH[$i]}/$TEST_TYPE${YCSB_WORKLOAD_TYPE}/analyse.txt\" using 1:3 title \"${LEVEL[$i]}\" w lp lw 2 ps 2 pt $i dt 1"
        if [ $i != $j ]
        then
            TpmC=$TpmC","
            Rollback=$Rollback","
            Distributed=$Distributed","
        fi
        echo ${LEVEL[$i]}
        echo ${RESULT_PATH[$i]}
    done
    echo $TpmC
    echo $Rollback
    echo $Distributed

    cp draw-multi-template-tpcc-ran.plt ${RESULT_PATH[1]}/draw-multi.plt
    sed -i 's?OUTPUT?'${RESULT_PATH[1]}'?g' ${RESULT_PATH[1]}/draw-multi.plt
    sed -i "17c $TpmC" ${RESULT_PATH[1]}/draw-multi.plt
    sed -i "22c $Rollback" ${RESULT_PATH[1]}/draw-multi.plt
    sed -i "29c $Distributed" ${RESULT_PATH[1]}/draw-multi.plt


../gnuplot-5.2.8/gnuplot/bin/gnuplot ${RESULT_PATH[1]}/draw-multi.plt


cd ${RESULT_PATH[1]}

main=index.html
cat /dev/null > $main
echo '<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><title>DBT-Report</title></head>' > $main
echo '<body><h1>Deneva 测试</h1>' >> $main

echo '<h2>测试情况概述</h2>' >> $main
RAW_TIME=(${SHORT_PATH[1]//'/'/ })
RAW_TIME2=${RAW_TIME[2]}
echo '测试时间:'${SHORT_PATH[1]}'<br />' >> $main
if [ "$TEST_TYPE" == "tpcc" ]
then
    echo '测试工具:'$TEST_TYPE'<br />' >> $main
elif [ "$TEST_TYPE" == "ycsb" ]
then
    echo '测试工具:'$TEST_TYPE"-"$YCSB_WORKLOAD_TYPE'<br />' >> $main
else
    echo '测试工具:sysbench-'$TEST_TYPE'<br />' >> $main
fi
if [ "$TEST_TYPE" == "tpcc" ]
then
    echo '数据量warehouse:'$WAREHOUSE'<br />' >> $main
elif [ "$TEST_TYPE" == "ycsb" ]
then
    echo '数据量YCSB_RECORD_COUNT:'$YCSB_RECORD_COUNT'<br />' >> $main
else
    echo '表数量:'$TABLE_COUNT'<br />' >> $main
    echo '表大小:'$TABLE_SIZE'<br />' >> $main
fi

echo '<h2>测试结果对比</h2>' >> $main

files=$(ls ${RESULT_PATH[1]} | grep .svg)
for i in $files; do
    echo '<img width="33%" src="./'$i'">' >> $main
done

echo '<h2>单轮测试报告</h2>' >> $main

echo '<ul>' >> $main

for i in $(seq 1 $j)
do
    perfs=$(ls ${RESULT_PATH[$i]} | grep $TEST_TYPE)
    echo '<li><a href="'/result/${SHORT_PATH[$i]}/$perfs'" target="_Blank">'${LEVEL[$i]}'</a></li>' >> $main
done

#for i in $files; do
#  if [ $i != 'index.html' ] && [ $i != 'getindex.sh' ] && [ $i != 'main.html' ]; then
#    echo '<li><a href="'$i'">'$i'</a></li>' >> $main
#  fi
#done
echo '</ul></body></html>' >> $main
echo '============index.html generate finished ============'
echo 'Open the report in Web Browser @ http://'$HOST':8081/result/'${SHORT_PATH[1]}

