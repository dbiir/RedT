sleep 60
filename=/tmp/${1}_${2}
echo > $filename
for i in $(seq 1 30)
do
    ps -aux | grep rundb | grep ${1} | grep -v grep | grep -v ssh | grep -v timeout | awk '{print $2}' | xargs top -bn1 -p | tail -n 1 | awk '{print $9}' >> $filename
    sleep 1
done
tail -n 20 $filename | awk '{sum+=$1} END {print sum/NR}' > ${filename}_avg