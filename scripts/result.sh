set -x

while [[ $# -gt 0 ]]
do
    case $1 in
        -c)
            CC=($(echo $2 | tr ',' ' '))
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
        *)
            shift
            ;;
    esac
done

cd $RESULT_PATH

