while true
do
    if [[ -z "$(ps -aux | grep run_exper)" ]]
    then
        ./testall.sh
        break
    fi
done
