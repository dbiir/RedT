set -x

PHASE=5
Latency="trans_total_run_time
          trans_process_time trans_process_time_percent
          trans_2pc_time trans_2pc_time_percent
          trans_prepare_time trans_prepare_time_percent
          trans_validate_time trans_validate_time_percent
          trans_finish_time trans_finish_time_percent
          trans_commit_time trans_commit_time_percent
          trans_abort_time trans_abort_time_percent
          lat_cc_block_time lat_cc_block_time_percent
          txn_index_time txn_index_time_percent
          txn_manager_time txn_manager_time_percent
          lat_l_loc_cc_time lat_l_loc_cc_time_percent
          trans_init_time trans_init_time_percent"
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
        -C)
            CT=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        --ft)
            FT=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        --tt)
            TT=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        -n)
            NUMBEROFNODE=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        -p)
            PHASE=$2
            shift
            shift
            ;;
        -s)
            SKEW=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        -t)
            RESULT_PATH=../results/$2
            shift
            shift
            ;;
        --wr)
            WR=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        -l)
            LOAD=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        *)
            shift
            ;;
    esac
done

addContent() {
    echo $1 >> ${RESULT_PATH}/index.html
}

addHeading() {
    addContent "<h$1>$2</h$1>"
}

addParagraph() {
    addContent "<p>$1</p>"
}

addTableTitle() {
    addContent '<table border=1>'
}

addTableTuple() {
    addContent '<tr>'
    for arg in "$@"
    do
        addContent "<td>${arg}</td>"
    done
    addContent "</tr>"
}

addTableTail() {
    addContent '</table>'
}

addLabel() {
    addContent "<label>$1</label>"
}

initHTMLFile() {
    rm -rf ${RESULT_PATH}/index.html
    addContent '<!DOCTYPE html>'
    addContent '<html lang="zh-CN">'
    addContent '<head><meta charset="UTF-8"><title>Report</title><style type="text/css">
            td{
                text-align: center;
            }
        </style></head>'
    addContent '<body>'
    if [[ ${TEST_TYPE} == "tpcc_scaling" ]]
    then
        addHeading 1 'Deneva TPCC性能测试报告'
        addParagraph '本次测试是TPC-C测试'
    elif [[ ${TEST_TYPE} == "ycsb_scaling" ]]
    then
        addHeading 1 'Deneva YCSB性能测试报告'
        addParagraph '本次测试是YCSB测试'
    else
        addHeading 1 'WooKongDB 性能测试报告'
        addParagraph ""
    fi
}

EndHtmlFile() {
    addContent "</body></html>"
}

ArgsType() {
    if [[ "${TEST_TYPE}" == 'ycsb_skew' ]]
    then
        args=("${SKEW[@]}")
    elif [[ "${TEST_TYPE}" == 'ycsb_scaling' ]]
    then
        args=("${NUMBEROFNODE[@]}")
    elif [[ "${TEST_TYPE}" == 'ycsb_writes' ]]
    then
        args=("${WR[@]}")
    elif [[ "${TEST_TYPE}" == 'tpcc_scaling' ]]
    then
        args=("${NUMBEROFNODE[@]}")
    elif [[ "${TEST_TYPE}" == 'ycsb_stress' ]]
    then
        args=("${LOAD[@]}")
    elif [[ "${TEST_TYPE}" == 'tpcc_stress' ]]
    then
        args=("${LOAD[@]}")
    elif [[ "${TEST_TYPE}" == 'tpcc_stress_ctx' ]]
    then
        args=("${LOAD[@]}")
    fi   
}

FileName() {
    if [[ "${TEST_TYPE}" == 'ycsb_skew' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep ${cc} | grep _SKEW-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'ycsb_scaling' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep [0-9]_${cc}_ | grep _N-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'ycsb_writes' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep ${cc} | grep _WR-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'tpcc_scaling' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep [0-9]_${cc}_ | grep _N-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'ycsb_stress' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep ${cc}_TIF-${arg}_ | grep _SKEW-${SKEW[0]}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'tpcc_stress' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep ${cc}_TIF-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'tpcc_stress_ctx' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep [0-9]_${cc}_ | grep _CT-${CT}_TIF-${arg}_ | grep ^${i}_)
    fi
}

TmpFileNum() {
    if [[ "${TEST_TYPE}" == 'ycsb_scaling' ]]
    then
        TMPN=${arg}
    elif [[ "${TEST_TYPE}" == 'tpcc_scaling' ]]
    then
        TMPN=${arg}
    else
        TMPN=${NUMBEROFNODE[0]}
    fi
}

initHTMLFile
addHeading 2 "测试结果"
# 通用的结果解析部分
LATFILE=lat
LTFILE=lt
rm -rf ${LATFILE} ${LTFILE}
touch ${LATFILE} ${LTFILE}
addTableTitle
addContent '<tr>'
addContent "<td>AlgoName\\NodeCount</td>"
echo "根据测试，确定第一个循环体类型"
ArgsType
#根据测试，确定第一个循环体类型
for arg in ${args[@]}
do
    addContent "<td colspan=\"3\">${arg}</td>"
done
addContent '</tr>'
for cc in ${CC[@]}
do
    addContent '<tr>'
    LS=''
    echo -n ${cc}" " >> ${LATFILE}
    addContent "<td>${cc}</td>"
    TMPFILE=tmp-${cc}
    rm -rf ${TMPFILE}
    touch ${TMPFILE}
    IDLEFILE=idle-${cc}
    rm -rf ${IDLEFILE}
    touch ${IDLEFILE}
    CCLATFILE=lat-${cc}
    rm -rf ${CCLATFILE}
    touch ${CCLATFILE}
    DIS_FILE=dis-${cc}
    rm -rf ${DIS_FILE}
    touch ${DIS_FILE}
    touch ${DIS_FILE}
    echo "根据测试，确定第2个循环体类型"
    #根据测试，确定第2个循环体类型
    ArgsType
    #根据测试，确定第2个循环体类型
    for arg in ${args[@]}
    do
        echo -n ${arg}" " >> ${TMPFILE}
        echo -n ${arg}" " >> ${CCLATFILE}
        echo -n ${arg}" " >> ${IDLEFILE}
        echo -n ${arg}" " >> ${DIS_FILE}
        AS=''
        echo "根据测试，确定TMPN"
        #根据测试，确定TMPN
        TmpFileNum
        #根据测试，确定TMPN
        let TMPN--
        for i in $(seq 0 $TMPN)
        do
            echo "根据测试，确定文件名"
            #根据测试，确定文件名
            FileName
            #根据测试，确定文件名            
            AS=${AS}$(readlink -f ${RESULT_PATH}/$f)" "
            LS=${LS}$(readlink -f ${RESULT_PATH}/$f)" "
        done
        tmpresult=$(python parse_results.py $AS)
        echo ${tmpresult} >> ${TMPFILE}
        dis_tmpresult=$(python pl/parse_latency_dis.py $AS)
        echo ${dis_tmpresult} >> ${DIS_FILE}
        python parse_latency.py $AS >> ${CCLATFILE}
        python parse_cpu_idle.py $AS >> ${IDLEFILE}
        tput=$(echo ${tmpresult} | awk '{print $1}')
        ar=$(echo ${tmpresult} | awk '{print $2}')
        dr=$(echo ${tmpresult} | awk '{print $3}')
        addContent "<td>${tput}</td>"
        addContent "<td>${ar}</td>"
        addContent "<td>${dr}</td>"
    done
    python parse_trans_latency.py $LS >> ${LTFILE}
    mv ${DIS_FILE} ${RESULT_PATH}/
    mv ${TMPFILE} ${RESULT_PATH}/
    mv ${CCLATFILE} ${RESULT_PATH}/
    mv ${IDLEFILE} ${RESULT_PATH}/
    cp ${LTFILE} ${RESULT_PATH}/
    addContent "</tr>"
done
addTableTail
addContent "<img src=\"./1tpmc.svg\" />"
echo >> ${LATFILE}
echo "abort_time txn_manager_time txn_validate_time txn_cleanup_time txn_total_process_time" >> ${LATFILE}
awk -F' ' '{for(i=1;i<=NF;i=i+1){a[NR,i]=$i}}END{for(j=1;j<=NF;j++){str=a[1,j];for(i=2;i<=NR;i++){str=str " " a[i,j]}print str}}' ${LTFILE} >> ${LATFILE}
/data1/deneva/anaconda3/bin/python3 getLATENCY.py ${LATFILE} ${PHASE}
mv 1.png ${RESULT_PATH}/
addHeading 2 "时间使用占比分析图"
addContent "<img src=\"./1.png\" />"
addHeading 2 "资源使用分析"
indexlen=${#FT[@]}
let indexlen--
indexofcc=0
indexofnn=0
# addTableTitle
# addTableTuple "实验参数" "rundbCPU使用率" "rundb内存使用率" "runclCPU使用率" "runcl内存使用率" "性能监控"
# for index in $(seq 0 ${indexlen})
# do
#     ft=${FT[${index}]}
#     ft0=$(echo $ft | cut -b -10)
#     tt=${TT[${index}]}
#     tt0=$(echo $tt | cut -b -10)
#     let pt0=tt0-ft0
#     a=($(curl -g "http://9.39.242.189:8080/api/v1/query?query=avg_over_time(KPI_PROCESS_cpu{pname=\"rundb\"}[${pt0}s])&time=${tt0}" | tr "\"" "\n"))
#     rundbcpu=${a[-2]}
#     a=($(curl -g "http://9.39.242.189:8080/api/v1/query?query=avg_over_time(KPI_PROCESS_mem{pname=\"rundb\"}[${pt0}s])&time=${tt0}" | tr "\"" "\n"))
#     rundbmem=${a[-2]}
#     a=($(curl -g "http://9.39.242.189:8080/api/v1/query?query=avg_over_time(KPI_PROCESS_cpu{pname=\"runcl\"}[${pt0}s])&time=${tt0}" | tr "\"" "\n"))
#     runclcpu=${a[-2]}
#     a=($(curl -g "http://9.39.242.189:8080/api/v1/query?query=avg_over_time(KPI_PROCESS_mem{pname=\"runcl\"}[${pt0}s])&time=${tt0}" | tr "\"" "\n"))
#     runclmem=${a[-2]}
#     addTableTuple ${CC[${indexofcc}]}"\\"${NUMBEROFNODE[${indexofnn}]} $rundbcpu $rundbmem $runclcpu $runclmem "<a href=\"http://9.39.242.189:8081/d/e2HCbA5Gk/denevamonitor?orgId=1&from=${ft}&to=${tt}\">性能监控</a>"
#     let indexofcc++
#     if [[ "$indexofcc" == "${#CC[@]}" ]]
#     then
#         indexofcc=0
#         let indexofnn++
#     fi
# done
# addTableTail

for cc in ${CC[@]}
do
    addHeading 3 "算法${cc} Break Down数据"
    addTableTitle
    addContent '<tr>'
    addContent "<td>NodeCount\\Break Down</td>"
    for latency in ${Latency[@]}
    do
        addContent "<td>${latency}</td>"
    done
    addContent '</tr>'
    #根据测试，确定第2个循环体类型
    ArgsType
    
    for arg in ${args[@]}
    do
        addContent '<tr>'
        addContent "<td>${arg}</td>"
        CCLATFILE=lat-${cc}
        rm -rf ${CCLATFILE}
        touch ${CCLATFILE}

        echo -n ${arg}" " >> ${TMPFILE}
        echo -n ${arg}" " >> ${CCLATFILE}
        AS=''

        TmpFileNum

        let TMPN--
        for i in $(seq 0 $TMPN)
        do
            #根据测试，确定第2个循环体类型
            FileName
            #根据测试，确定第2个循环体类型  
            AS=${AS}$(readlink -f ${RESULT_PATH}/$f)" "
            LS=${LS}$(readlink -f ${RESULT_PATH}/$f)" "
        done
        tmpresult=$(python parse_latency.py $AS)
        OLD_IFS="$IFS"
        IFS=" "
        tmpr=($tmpresult)
        IFS="$OLD_IFS"
        count=${#tmpr[@]}
        count=`expr $count - 1`
        for i in $(seq 0 $count)
        do
        addContent "<td>${tmpr[$i]}</td>"
        done
        if [[ "${cc}" == 'MVCC' ]]
        then
        alg_tmpresult=$(python pl/parse_latency_mvcc.py $AS)
        ./draw_latency.sh ${cc} "$tmpresult" "$alg_tmpresult"
        elif [[ "${cc}" == 'DLI_OCC' ]] || [[ "${cc}" == 'DLI_DTA3' ]] || [[ "${cc}" == 'DLI_DTA' ]] || [[ "${cc}" == 'DLI_DTA2' ]]
        then
        alg_tmpresult=$(python pl/parse_latency_dli.py $AS)
        ./draw_latency.sh ${cc} "$tmpresult" "$alg_tmpresult"
        else
        ./draw_latency.sh ${cc} "$tmpresult"
        fi
        dot -Tjpg draw_latency_tmp.dot -o draw_latency_${cc}_${arg}.jpg
        mv draw_latency_${cc}_${arg}.jpg ${RESULT_PATH}/
        addHeading 3 "算法${cc} nn${arg} Break Down数据"
        addContent "<img src=\"./draw_latency_${cc}_${arg}.jpg\" />"
    done
    addContent "</tr>"
    addTableTail
done
addHeading 2 "rundb Perf 图"
for f in $(ls ${RESULT_PATH}/perf)
do
    addParagraph "$f"
    addContent "<img src=\"./perf/$f\" />"
done
EndHtmlFile