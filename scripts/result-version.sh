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
        -T)
            THREAD=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        -P)
            PART=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        -CO)
            COROUTINE=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        -p)
            PHASE=$2
            shift
            shift
            ;;
        -D)
            DCS=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        -s)
            SKEW=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        -dc)
            CROSSDCPERC=($(echo $2 | tr ',' ' '))
            shift
            shift
            ;;
        -nd)
            NETWORKDELAY=($(echo $2 | tr ',' ' '))
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
        --VA)
            VA=($(echo $2 | tr ',' ' '))
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
    elif [[ "${TEST_TYPE}" == 'ycsb_cross_dc' ]]
    then
        args=("${CROSSDCPERC[@]}")
    elif [[ "${TEST_TYPE}" == 'ycsb_network_delay' ]]
    then
        args=("${NETWORKDELAY[@]}")
    elif [[ "${TEST_TYPE}" == 'ycsb_scaling' || "${TEST_TYPE}" == 'ycsb_scaling_tcp' || "${TEST_TYPE}" == 'ycsb_scaling_two_sided' || "${TEST_TYPE}" == 'ycsb_scaling_one_sided' || "${TEST_TYPE}" == 'ycsb_scaling_coroutine' || "${TEST_TYPE}" == 'ycsb_scaling_dbpa' || "${TEST_TYPE}" == 'ycsb_scaling_all' ]]
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
    elif [[ "${TEST_TYPE}" == 'ycsb_thread' ]]
    then
        args=("${THREAD[@]}")
    elif [[ "${TEST_TYPE}" == 'tpcc_thread' ]]
    then
        args=("${THREAD[@]}")
    elif [[ "${TEST_TYPE}" == 'ycsb_partitions' ]]
    then
        args=("${PART[@]}")
    elif [[ "${TEST_TYPE}" == 'ycsb_dcs' ]]
    then
        args=("${DCS[@]}")
    elif [[ "${TEST_TYPE}" == 'ycsb_sk_partitions' ]]
    then
        args=("${PART[@]}")
    elif [[ "${TEST_TYPE}" == 'ycsb_coroutine' ]]
    then
        args=("${COROUTINE[@]}")
    elif [[ "${TEST_TYPE}" == 'ycsb_version_array' ]]
    then
        args=("${VA[@]}")
    fi   
}

ArgsType1() {
    if [[ "${TEST_TYPE}" == 'ycsb_version_array' ]]
    then
        args1=("${SKEW[@]}")
    else 
        args1=("${CC[@]}")
    fi   
}

FileName() {
    if [[ "${TEST_TYPE}" == 'ycsb_skew' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep ${cc} | grep _SKEW-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'ycsb_cross_dc' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep ${cc} | grep _CROSS_DC_TXN_PERC-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'ycsb_network_delay' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep ${cc} | grep _NDLY-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'ycsb_scaling' || "${TEST_TYPE}" == 'ycsb_scaling_tcp' || "${TEST_TYPE}" == 'ycsb_scaling_two_sided' || "${TEST_TYPE}" == 'ycsb_scaling_one_sided' || "${TEST_TYPE}" == 'ycsb_scaling_coroutine' || "${TEST_TYPE}" == 'ycsb_scaling_dbpa' || "${TEST_TYPE}" == 'ycsb_scaling_all' ]]
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
    elif [[ "${TEST_TYPE}" == 'ycsb_thread' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep [0-9]_${cc}_ | grep _T-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'tpcc_thread' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep [0-9]_${cc}_ | grep _T-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'ycsb_partitions' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep [0-9]_${cc}_ | grep _PPT-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'ycsb_dcs' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep [0-9]_${cc}_ | grep _DC_PER_TXN-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'ycsb_sk_partitions' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep [0-9]_${cc}_ | grep _PPT-${arg}_ | grep ^${i}_ | grep _SKEW-${SKEW})
    elif [[ "${TEST_TYPE}" == 'ycsb_coroutine' ]]
    then
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep [0-9]_${cc}_ | grep _CO-${arg}_ | grep ^${i}_)
    elif [[ "${TEST_TYPE}" == 'ycsb_version_array' ]]
    then 
        f=$(ls ${RESULT_PATH} | grep -v .cfg | grep [0-9]_${cc}_ | grep _VA-${arg}_ | grep _SKEW-${arg1}_ | grep ^${i}_)
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
# echo "根据测试，确定第一个循环体类型"
ArgsType1
#根据测试，确定第一个循环体类型
for arg in ${args[@]}
do
    addContent "<td colspan=\"3\">${arg}</td>"
done
addContent '</tr>'
num=0
# for cc in ${CC[@]}
# do
    for arg1 in ${args1[@]}
    do
        addContent '<tr>'
        LS=''
        echo -n ${arg1}" " >> ${LATFILE}
        addContent "<td>${arg1}</td>"
        TMPFILE=tmp-${arg1}
        rm -rf ${TMPFILE}
        touch ${TMPFILE}
        IDLEFILE=idle-${arg1}
        rm -rf ${IDLEFILE}
        touch ${IDLEFILE}
        CCLATFILE=lat-${arg1}
        rm -rf ${CCLATFILE}
        touch ${CCLATFILE}
        DIS_FILE=dis-${arg1}
        rm -rf ${DIS_FILE}
        touch ${DIS_FILE}
        touch ${DIS_FILE}
        CPUFILE=cpu-${arg1}
        rm -rf ${CPUFILE}
        touch ${CPUFILE}
        # echo "根据测试，确定第2个循环体类型"
        #根据测试，确定第2个循环体类型
        ArgsType
        #根据测试，确定第2个循环体类型
        
        for arg in ${args[@]}
        do
            echo -n ${arg}" " >> ${TMPFILE}
            echo -n ${arg}" " >> ${CCLATFILE}
            echo -n ${arg}" " >> ${IDLEFILE}
            echo -n ${arg}" " >> ${DIS_FILE}
            echo -n ${arg}" " >> ${CPUFILE}
            AS=''
            # echo "根据测试，确定TMPN"
            #根据测试，确定TMPN
            TmpFileNum
            #根据测试，确定TMPN
            let TMPN--
            for i in $(seq 0 $TMPN)
            do
                # echo "根据测试，确定文件名"
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
            echo $(cat ${RESULT_PATH}/cpu_usage_${num}/root_*_avg| awk '{sum+=$1}END{print "",sum}') >> ${CPUFILE}
            
        done
        let num++
        python parse_trans_latency.py $LS >> ${LTFILE}
        mv ${DIS_FILE} ${RESULT_PATH}/
        mv ${TMPFILE} ${RESULT_PATH}/
        mv ${CPUFILE} ${RESULT_PATH}/
        mv ${CCLATFILE} ${RESULT_PATH}/
        mv ${IDLEFILE} ${RESULT_PATH}/
        cp ${LTFILE} ${RESULT_PATH}/
        addContent "</tr>"
    done
# done
