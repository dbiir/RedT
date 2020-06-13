ps -aux | grep runcl | awk '{print $2}' | xargs kill -9 2>/dev/null 1>/dev/null
ps -aux | grep rundb | awk '{print $2}' | xargs kill -9 2>/dev/null 1>/dev/null
for i in $(seq 203 231)
do
    ssh 10.77.70.$i "ps -aux | grep rundb | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
    ssh 10.77.70.$i "ps -aux | grep runcl | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
done
