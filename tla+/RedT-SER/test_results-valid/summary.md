# TLA+ 测试结果总结报告

生成时间: 2025年 3月 7日 星期五 09时22分56秒 CST

## 测试配置和结果

### 测试 #1
```
=== Test #1 Configuration: KEY = {k1} CLIENT = {c1} REPLICA_COUNT = 1 QUORUM_SIZE = 1 MAX_TXN_PER_CLIENT = 2 MAX_OPS_PER_TXN = 1 ===
Start Time: 2025年 3月 7日 星期五 09时22分29秒 CST
Configuration File: test_results/test_1_20250307_092229.cfg
Model checking completed. No error has been found.
461 states generated, 234 distinct states found, 0 states left on queue.
Start Time: 2025年 3月 7日 星期五 09时22分29秒 CST
End Time: 2025年 3月 7日 星期五 09时22分34秒 CST
```

### 测试 #2
```
=== Test #2 Configuration: KEY = {k1, k2} CLIENT = {c1, c2} REPLICA_COUNT = 3 QUORUM_SIZE = 2  MAX_TXN_PER_CLIENT = 1 MAX_OPS_PER_TXN = 2 ===
Start Time: 2025年 3月 7日 星期五 09时22分34秒 CST
Configuration File: test_results/test_2_20250307_092234.cfg
Model checking completed. No error has been found.
Progress(13) at 2025-03-07 09:22:38: 186,808 states generated (186,808 s/min), 73,106 distinct states found (73,106 ds/min), 36,961 states left on queue.
3030147 states generated, 762255 distinct states found, 0 states left on queue.
Start Time: 2025年 3月 7日 星期五 09时22分34秒 CST
End Time: 2025年 3月 7日 星期五 09时22分56秒 CST
```

### 测试 #3
=== Test #3 Configuration: KEY = {k1, k2, k3} CLIENT = {c1, c2, c3} REPLICA_COUNT = 1 QUORUM_SIZE = 1  MAX_TXN_PER_CLIENT = 1 MAX_OPS_PER_TXN = 2 ===
Starting... (2025-03-13 17:07:21)
Computing initial states...
Finished computing initial states: 1 distinct state generated at 2025-03-13 17:07:21.
Progress(12) at 2025-03-13 17:07:24: 2,052,209 states generated (2,052,209 s/min), 561,696 distinct states found (561,696 ds/min), 344,176 states left on queue.
Model checking completed. No error has been found.
  Estimates of the probability that TLC did not check all reachable states
  because two distinct states had the same fingerprint:
  calculated (optimistic):  val = 1.6E-6
  based on the actual fingerprints:  val = 7.2E-7
12790219 states generated, 3039850 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 13.
The average outdegree of the complete state graph is 1 (minimum is 0, the maximum 27 and the 95th percentile is 5).
Finished in 13s at (2025-03-13 17:07:34)