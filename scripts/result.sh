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
        -n)
            NUMBEROFNODE=($(echo $2 | tr ',' ' '))
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
        *)
            shift
            ;;
    esac
done

if [[ "${TEST_TYPE}" == 'ycsb_skew' ]]
then
    LATFILE=lat
    LTFILE=lt
    rm -rf ${LATFILE} ${LTFILE}
    touch ${LATFILE} ${LTFILE}
    for cc in ${CC[@]}
    do
        LS=''
        if [[ "${cc}" == "TIMESTAMP" ]]
        then
            echo -n "T/O " >> ${LATFILE}
        else
            echo -n ${cc}" " >> ${LATFILE}
        fi
        TMPFILE=tmp-${cc}
        rm -rf ${TMPFILE}
        touch ${TMPFILE}
        for skew in ${SKEW[@]}
        do
            echo -n ${skew}" " >> ${TMPFILE}
            AS=''

            TMPN=${NUMBEROFNODE[0]}
            let TMPN--
            for i in $(seq 0 $TMPN)
            do
                f=$(ls ${RESULT_PATH} | grep -v .cfg | grep _${cc}_ | grep _SKEW-${skew}_ | grep ^${i}_)
                AS=${AS}$(readlink -f ${RESULT_PATH}/$f)" "
                LS=${LS}$(readlink -f ${RESULT_PATH}/$f)" "
            done
            python parse_results.py ${AS} >> ${TMPFILE}
        done
        python parse_latency.py ${LS} >> ${LTFILE}
        mv ${TMPFILE} ${RESULT_PATH}/
    done
    echo >> ${LATFILE}
    echo "abort manager validate cleanup process" >> ${LATFILE}
    awk -F' ' '{for(i=1;i<=NF;i=i+1){a[NR,i]=$i}}END{for(j=1;j<=NF;j++){str=a[1,j];for(i=2;i<=NR;i++){str=str " " a[i,j]}print str}}' ${LTFILE} >> ${LATFILE}
    /data/Anaconda3/bin/python getLATENCY.py ${LATFILE} ${PHASE}
    mv 1.pdf ${RESULT_PATH}/latency.pdf
    mv ${LATFILE} ${RESULT_PATH}/
elif [[ "${TEST_TYPE}" == 'ycsb_scaling' ]]
then
    LATFILE=lat
    LTFILE=lt
    rm -rf ${LATFILE} ${LTFILE}
    touch ${LATFILE} ${LTFILE}
    for cc in ${CC[@]}
    do
        LS=''
        if [[ "${cc}" == "TIMESTAMP" ]]
        then
            echo -n "T/O " >> ${LATFILE}
        else
            echo -n ${cc}" " >> ${LATFILE}
        fi
        TMPFILE=tmp-${cc}
        rm -rf ${TMPFILE}
        touch ${TMPFILE}
        for nn in ${NUMBEROFNODE[@]}
        do
            echo -n ${nn}" " >> ${TMPFILE}
            AS=''

            TMPN=${nn}
            let TMPN--
            for i in $(seq 0 $TMPN)
            do
                f=$(ls ${RESULT_PATH} | grep -v .cfg | grep _${cc}_ | grep _N-${nn}_ | grep ^${i}_)
                AS=${AS}$(readlink -f ${RESULT_PATH}/$f)" "
                LS=${LS}$(readlink -f ${RESULT_PATH}/$f)" "
            done
            python parse_results.py ${AS} >> ${TMPFILE}
        done
        python parse_latency.py ${LS} >> ${LTFILE}
        mv ${TMPFILE} ${RESULT_PATH}/
    done
    echo >> ${LATFILE}
    echo "abort manager validate cleanup process" >> ${LATFILE}
    awk -F' ' '{for(i=1;i<=NF;i=i+1){a[NR,i]=$i}}END{for(j=1;j<=NF;j++){str=a[1,j];for(i=2;i<=NR;i++){str=str " " a[i,j]}print str}}' ${LTFILE} >> ${LATFILE}
    /data/Anaconda3/bin/python getLATENCY.py ${LATFILE} ${PHASE}
    mv 1.pdf ${RESULT_PATH}/latency.pdf
    mv ${LATFILE} ${RESULT_PATH}/
elif [[ "${TEST_TYPE}" == 'ycsb_writes' ]]
then
    LATFILE=lat
    LTFILE=lt
    rm -rf ${LATFILE} ${LTFILE}
    touch ${LATFILE} ${LTFILE}
    for cc in ${CC[@]}
    do
        LS=''
        if [[ "${cc}" == "TIMESTAMP" ]]
        then
            echo -n "T/O " >> ${LATFILE}
        else
            echo -n ${cc}" " >> ${LATFILE}
        fi
        TMPFILE=tmp-${cc}
        rm -rf ${TMPFILE}
        touch ${TMPFILE}
        for wr in ${WR[@]}
        do
            echo -n ${wr}" " >> ${TMPFILE}
            AS=''

            TMPN=${NUMBEROFNODE[0]}
            let TMPN--
            for i in $(seq 0 $TMPN)
            do
                f=$(ls ${RESULT_PATH} | grep -v .cfg | grep _${cc}_ | grep _WR-${wr}_ | grep ^${i}_)
                AS=${AS}$(readlink -f ${RESULT_PATH}/$f)" "
                LS=${LS}$(readlink -f ${RESULT_PATH}/$f)" "
            done
            python parse_results.py ${AS} >> ${TMPFILE}
        done
        python parse_latency.py ${LS} >> ${LTFILE}
        mv ${TMPFILE} ${RESULT_PATH}/
    done
    echo >> ${LATFILE}
    echo "abort manager validate cleanup process" >> ${LATFILE}
    awk -F' ' '{for(i=1;i<=NF;i=i+1){a[NR,i]=$i}}END{for(j=1;j<=NF;j++){str=a[1,j];for(i=2;i<=NR;i++){str=str " " a[i,j]}print str}}' ${LTFILE} >> ${LATFILE}
    /data/Anaconda3/bin/python getLATENCY.py ${LATFILE} ${PHASE}
    mv 1.pdf ${RESULT_PATH}/latency.pdf
    mv ${LATFILE} ${RESULT_PATH}/
elif [[ "${TEST_TYPE}" == 'tpcc_scaling2' ]]
then
    LATFILE=lat
    LTFILE=lt
    rm -rf ${LATFILE} ${LTFILE}
    touch ${LATFILE} ${LTFILE}
    for cc in ${CC[@]}
    do
        LS=''
        if [[ "${cc}" == "TIMESTAMP" ]]
        then
            echo -n "T/O " >> ${LATFILE}
        else
            echo -n ${cc}" " >> ${LATFILE}
        fi
        TMPFILE=tmp-${cc}
        rm -rf ${TMPFILE}
        touch ${TMPFILE}
        for nn in ${NUMBEROFNODE[@]}
        do
            echo -n ${nn}" " >> ${TMPFILE}
            AS=''

            TMPN=${nn}
            let TMPN--
            for i in $(seq 0 $TMPN)
            do
                f=$(ls ${RESULT_PATH} | grep -v .cfg | grep _${cc}_ | grep _N-${nn}_ | grep ^${i}_)
                AS=${AS}$(readlink -f ${RESULT_PATH}/$f)" "
                LS=${LS}$(readlink -f ${RESULT_PATH}/$f)" "
            done
            python parse_results.py ${AS} >> ${TMPFILE}
        done
        python parse_latency.py ${LS} >> ${LTFILE}
        mv ${TMPFILE} ${RESULT_PATH}/
    done
    echo >> ${LATFILE}
    echo "abort manager validate cleanup process" >> ${LATFILE}
    awk -F' ' '{for(i=1;i<=NF;i=i+1){a[NR,i]=$i}}END{for(j=1;j<=NF;j++){str=a[1,j];for(i=2;i<=NR;i++){str=str " " a[i,j]}print str}}' ${LTFILE} >> ${LATFILE}
    /data/Anaconda3/bin/python getLATENCY.py ${LATFILE} ${PHASE}
    mv 1.pdf ${RESULT_PATH}/latency.pdf
    mv ${LATFILE} ${RESULT_PATH}/
fi

