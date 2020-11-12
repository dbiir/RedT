set -x
ROOT_PATH1=/root/deneva-code
ROOT_PATH2=/root/mogg-deneva

while [[ $# -gt 0 ]]
do
    case $1 in
        -a)
            TEST_TYPE=$2
            shift
            shift
            ;;
        -p1)
            PATH1=$2
            shift
            shift
            ;;
        -p2)
            PATH2=$2
            shift
            shift
            ;;
        *)
            shift
            ;;
    esac
done

RESULT_PATH1=${ROOT_PATH1}/results/${PATH1}
RESULT_PATH2=${ROOT_PATH2}/results/${PATH2}
cd ${RESULT_PATH1}
ALGO1=($(ls -l tmp-* | grep -v WOOKONG | awk '{print $(NF-0)}' | cut -b 5-))
cd ${RESULT_PATH2}
ALGO2=($(ls -l tmp-* | grep -v WOOKONG | awk '{print $(NF-0)}' | cut -b 5-))

cd ${RESULT_PATH2}
for f in $(ls -l tmp-* | awk '{print $(NF-0)}')
do
    cp $f ${RESULT_PATH1}/$f.back
done

separator=","
r1="$( printf "${separator}%s" "${ALGO1[@]}" )"
r1="${r1:${#separator}}"
r2="$( printf "${separator}%s" "${ALGO2[@]}" )"
r2="${r2:${#separator}}"
cd ${ROOT_PATH1}/draw
./deneva-plot.sh -a ${TEST_TYPE} -t ${PATH1} -c ${r1} -c2 ${r2}
