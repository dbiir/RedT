set -x
ps -aux | grep runcl | awk '{print $2}' | xargs kill -9 2>/dev/null 1>/dev/null
ps -aux | grep rundb | awk '{print $2}' | xargs kill -9 2>/dev/null 1>/dev/null

for i in $(seq 144 147)
do
    ssh 10.77.110.$i "ps -aux | grep rundb | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
    ssh 10.77.110.$i "ps -aux | grep runcl | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
    ssh 10.77.110.$i "rm -rf /data/core/*" 2>/dev/null 1>/dev/null
    ssh 10.77.110.$i "rm -rf /home/core/*" 2>/dev/null 1>/dev/null
done

    ssh 10.77.110.148 "ps -aux | grep rundb | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
    ssh 10.77.110.148 "ps -aux | grep runcl | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
    ssh 10.77.110.148 "rm -rf /data/core/*" 2>/dev/null 1>/dev/null
    ssh 10.77.110.148 "rm -rf /home/core/*" 2>/dev/null 1>/dev/null
# for i in $(seq 202 253)
# do
#     ssh 10.77.70.$i "ps -aux | grep rundb | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
#     ssh 10.77.70.$i "ps -aux | grep runcl | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
# done

#     ssh 10.77.70.111 "ps -aux | grep rundb | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
#     ssh 10.77.70.111 "ps -aux | grep runcl | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null

# for i in $(seq 113 117)
# do
#     ssh 10.77.70.$i "ps -aux | grep rundb | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
#     ssh 10.77.70.$i "ps -aux | grep runcl | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
# done

# for i in $(seq 143 148)
# do
#     ssh 10.77.70.$i "ps -aux | grep rundb | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
#     ssh 10.77.70.$i "ps -aux | grep runcl | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
# done