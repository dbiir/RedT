sleep 10
filename=/tmp/${1}_${2}
echo > $filename
for i in $(seq 1 90)
do
    ps -aux | grep rundb | grep ${1} | grep -v grep | grep -v ssh | grep -v timeout | awk '{print $2}' |head -1| xargs top -bn1 -p | tail -n 1 | awk '{print $9}' >> $filename
    sleep 1
done
# tail -n 20  | awk '{sum+=$1} END {print sum/NR}' > ${filename}_avg
cat $filename |awk '{print $1}'|sort -rn|head -1> ${filename}_avg