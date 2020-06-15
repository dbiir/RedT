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
    for cc in ${CC[@]}
    do
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
                f=$(ls ${RESULT_PATH} | grep -v .cfg | grep ${cc} | grep SKEW-${skew} | grep ^${i}_)
                AS=${AS}$(readlink -f ${RESULT_PATH}/$f)" "
            done

            python parse_results.py $AS >> ${TMPFILE}
        done
        mv ${TMPFILE} ${RESULT_PATH}/
    done
elif [[ "${TEST_TYPE}" == 'ycsb_scaling' ]]
then
    for cc in ${CC[@]}
    do
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
                f=$(ls ${RESULT_PATH} | grep -v .cfg | grep ${cc} | grep N-${nn} | grep ^${i}_)
                AS=${AS}$(readlink -f ${RESULT_PATH}/$f)" "
            done

            python parse_results.py $AS >> ${TMPFILE}
        done
        mv ${TMPFILE} ${RESULT_PATH}/
    done
elif [[ "${TEST_TYPE}" == 'ycsb_writes' ]]
then
    for cc in ${CC[@]}
    do
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
                f=$(ls ${RESULT_PATH} | grep -v .cfg | grep ${cc} | grep WR-${wr} | grep ^${i}_)
                AS=${AS}$(readlink -f ${RESULT_PATH}/$f)" "
            done

            python parse_results.py $AS >> ${TMPFILE}
        done
        mv ${TMPFILE} ${RESULT_PATH}/
    done
elif [[ "${TEST_TYPE}" == 'tpcc_scaling2' ]]
then
    for cc in ${CC[@]}
    do
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
                f=$(ls ${RESULT_PATH} | grep -v .cfg | grep ${cc} | grep N-${nn} | grep ^${i}_)
                AS=${AS}$(readlink -f ${RESULT_PATH}/$f)" "
            done

            python parse_results.py $AS >> ${TMPFILE}
        done
        mv ${TMPFILE} ${RESULT_PATH}/
    done
fi

