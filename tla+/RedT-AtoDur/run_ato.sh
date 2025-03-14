#!/bin/bash

LOG_DIR="test_results"
mkdir -p $LOG_DIR

# 测试配置数组
CONFIGS=(
    "N=2 M=3 C=1 FAILEDPOINT1=FALSE FAILEDPOINT2=FALSE"
    # "N=1 M=5 C=1 FAILEDPOINT1=TRUE FAILEDPOINT2=FALSE"
    # "N=1 M=5 C=1 FAILEDPOINT1=FALSE FAILEDPOINT2=TRUE"
)

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
    FailedC = 1
    FailedN = 1
    defaultInitValue = defaultInitValue

SPECIFICATION Spec

INVARIANTS
    Atomicity
    ConsistentFinishedTransactions
    NoConflictingTransactionStatus
EOL
    
    # 运行测试并记录结果
    echo "=== Test #${test_number} Configuration: $config ===" > "$LOG_DIR/test_${test_number}_${timestamp}.log"
    echo "Start Time: $(date)" >> "$LOG_DIR/test_${test_number}_${timestamp}.log"
    echo "Configuration File: $config_file" >> "$LOG_DIR/test_${test_number}_${timestamp}.log"
    echo "----------------------------------------" >> "$LOG_DIR/test_${test_number}_${timestamp}.log"
    
    java -XX:+UseParallelGC -Xmx8192m -cp tla2tools.jar tlc2.TLC redtatom.tla -config "$config_file" -workers auto 2>&1 | tee -a "$LOG_DIR/test_${test_number}_${timestamp}.log"
    
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
    echo "### 测试 #${test_num}" >> "$LOG_DIR/summary.md"
    echo "\`\`\`" >> "$LOG_DIR/summary.md"
    grep -A 1 "Test #" "$log" >> "$LOG_DIR/summary.md"
    grep "Configuration File:" "$log" >> "$LOG_DIR/summary.md"
    grep "Model checking completed" "$log" >> "$LOG_DIR/summary.md"
    echo "\`\`\`" >> "$LOG_DIR/summary.md"
    echo "" >> "$LOG_DIR/summary.md"
done

echo "Testing completed. Summary report generated at $LOG_DIR/summary.md"