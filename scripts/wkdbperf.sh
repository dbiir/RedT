screen -dmS test bash -c "perf record -g -F 99 -o /data1/deneva/perf.data -p \$(ps -aux | grep deneva | grep rundb | grep -v timeout | grep -v grep |awk '{print \$2}' | head -n 1) -- sleep $1"
