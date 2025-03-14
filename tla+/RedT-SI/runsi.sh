#!/bin/bash

LOG_DIR="test_results"
mkdir -p $LOG_DIR

# 测试配置数组
CONFIGS=(
    "KEY = {k1} CLIENT = {c1} REPLICA_COUNT = 1 QUORUM_SIZE = 1 MAX_TXN_PER_CLIENT = 2 MAX_OPS_PER_TXN = 1"
    "KEY = {k1, k2} CLIENT = {c1, c2} REPLICA_COUNT = 3 QUORUM_SIZE = 2  MAX_TXN_PER_CLIENT = 1 MAX_OPS_PER_TXN = 2"
    "KEY = {k1, k2} CLIENT = {c1, c2} REPLICA_COUNT = 5 QUORUM_SIZE = 4  MAX_TXN_PER_CLIENT = 1 MAX_OPS_PER_TXN = 2"
)

# 添加错误处理函数
handle_error() {
    echo "错误: $1" >&2
    exit 1
}

# 遍历每个配置并运行测试
test_number=1
for config in "${CONFIGS[@]}"; do
    echo "Testing configuration ${test_number}: $config"
    
    # 生成时间戳
    timestamp=$(date +"%Y%m%d_%H%M%S")
    
    # 创建带编号的配置文件
    config_file="$LOG_DIR/test_${test_number}_${timestamp}.cfg"
    cat > "$config_file" << EOL
CONSTANTS
    $config

SPECIFICATION Spec

INVARIANTS
    TypeInvariant
    SnapshotIsolation
    SnapshotReadInvariant
    AbortedConsistency
    CommittedConsistency
    WriteConsistency

EOL
    
    # 运行测试并记录结果，添加错误处理
    echo "=== Test #${test_number} Configuration: $config ===" > "$LOG_DIR/test_${test_number}_${timestamp}.log"
    echo "Start Time: $(date)" >> "$LOG_DIR/test_${test_number}_${timestamp}.log"
    echo "Configuration File: $config_file" >> "$LOG_DIR/test_${test_number}_${timestamp}.log"
    echo "----------------------------------------" >> "$LOG_DIR/test_${test_number}_${timestamp}.log"
    
    # 优化JVM参数
    java -XX:+UseParallelGC -Xmx8192m -cp tla2tools.jar tlc2.TLC RedTSI.tla -config "$config_file" -workers auto 2>&1 | tee -a "$LOG_DIR/test_${test_number}_${timestamp}.log"
    # java -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Xmx8g -XX:+HeapDumpOnOutOfMemoryError \
    #      -cp tla2tools.jar tlc2.TLC -workers auto \
    #      -dump dot,colorize,actionlabels -coverage 3 \
    #      -config "$config_file" RedTSI.tla 2>&1 | tee -a "$LOG_DIR/test_${test_number}_${timestamp}.log" || \
    # handle_error "TLC执行失败，配置: $config"
    
    echo "----------------------------------------" >> "$LOG_DIR/test_${test_number}_${timestamp}.log"
    echo "End Time: $(date)" >> "$LOG_DIR/test_${test_number}_${timestamp}.log"
    
    echo "Test #${test_number} completed. Results saved in test_${test_number}_${timestamp}.log"
    echo "Configuration saved in ${config_file}"
    echo "----------------------------------------"
    
    ((test_number++))
done

# 修改总结报告生成部分
echo "Generating summary report..."
cat > "$LOG_DIR/summary.md" << EOL
# TLA+ 测试结果总结报告

生成时间: $(date)

## 测试配置和结果

EOL

for log in "$LOG_DIR"/test_*_*.log; do
    test_num=$(echo "$log" | grep -o "test_[0-9]\+_" | cut -d'_' -f2)
    # 在总结报告中添加更多信息
    echo "### 测试 #${test_num}" >> "$LOG_DIR/summary.md"
    echo "\`\`\`" >> "$LOG_DIR/summary.md"
    grep -A 1 "Test #" "$log" >> "$LOG_DIR/summary.md"
    grep "Configuration File:" "$log" >> "$LOG_DIR/summary.md"
    grep "Model checking completed" "$log" >> "$LOG_DIR/summary.md"
    # 添加状态空间信息
    grep "distinct states found" "$log" >> "$LOG_DIR/summary.md"
    # 添加执行时间信息
    grep "Time" "$log" >> "$LOG_DIR/summary.md"
    echo "\`\`\`" >> "$LOG_DIR/summary.md"
    echo "" >> "$LOG_DIR/summary.md"
done

echo "Testing completed. Summary report generated at $LOG_DIR/summary.md"