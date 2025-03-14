-------------------------------- MODULE RedTSI--------------------------------
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS KEY,    \* 所有可能的键的集合
          CLIENT  \* 所有可能的客户端的集合
CONSTANTS REPLICA_COUNT,     \* 每个键的副本数量
          QUORUM_SIZE,       \* 仲裁数量 (通常是 3/4 * REPLICA_COUNT)
          MAX_TXN_PER_CLIENT, \* 每个客户端最大事务数
          MAX_OPS_PER_TXN

ASSUME /\ REPLICA_COUNT > 0
       /\ QUORUM_SIZE <= REPLICA_COUNT
       /\ QUORUM_SIZE > REPLICA_COUNT \div 2

VARIABLES next_ts,           \* 全局递增时间戳
          client_state,      \* 客户端状态
          client_ts,         \* 客户端的开始和提交时间戳
          client_read_set,   \* 客户端的读集
          client_write_set,  \* 客户端的写集
          key_replica_data,  \* 每个副本的数据
          key_replica_lock,  \* 每个副本的锁状态
          key_replica_ts,     \* 每个副本的时间戳
          key_replica_last_read_ts,  \* 每个键的最后读取时间戳
          key_replica_si,            \* 每个键的快照隔离状态
          client_txn_count,   \* 记录每个客户端执行的事务数
          client_op_count     \* 记录每个事务的操作数

vars == <<next_ts, client_state, client_ts, client_read_set, 
          client_write_set, 
          key_replica_data, key_replica_lock, key_replica_ts, key_replica_last_read_ts, key_replica_si, client_txn_count, client_op_count
          >>

\* 客户端状态
States == {"init", "active", "failed", "committed", "aborted"}

\* 初始化
Init ==
  /\ next_ts = 0
  /\ client_state = [c \in CLIENT |-> "init"]
  /\ client_ts = [c \in CLIENT |-> [start_ts |-> 0, commit_ts |-> 0]]
  /\ client_read_set = [c \in CLIENT |-> {}]
  /\ client_write_set = [c \in CLIENT |-> {}]
  /\ key_replica_data = [k \in KEY |-> 
       [r \in 1..REPLICA_COUNT |-> <<[ts |-> 0, val |-> "initial"]>>]]
  /\ key_replica_lock = [k \in KEY |-> [r \in 1..REPLICA_COUNT |-> {}]]
  /\ key_replica_ts = [k \in KEY |-> [r \in 1..REPLICA_COUNT |-> 0]]
  /\ key_replica_si = [k \in KEY |-> [r \in 1..REPLICA_COUNT |-> TRUE]]
  /\ key_replica_last_read_ts = [k \in KEY |-> [r \in 1..REPLICA_COUNT |-> 0]]
  /\ client_txn_count = [c \in CLIENT |-> 0]  \* 初始化事务计数器
  /\ client_op_count = [c \in CLIENT |-> 0]

HasQuorum(k, pred(_)) ==
  LET replicas == {r \in 1..REPLICA_COUNT : pred(r)}
  IN Cardinality(replicas) >= QUORUM_SIZE

checkSnapshotIsolation(k, commit_ts) ==
  /\ \A r \in 1..REPLICA_COUNT :
       IF key_replica_last_read_ts[k][r] >= commit_ts
       THEN key_replica_si' = [key_replica_si EXCEPT ![k][r] = FALSE]
       ELSE UNCHANGED key_replica_si

\* 开始新事务
Start(c) ==
  /\ client_state[c] = "init"
  /\ next_ts' = next_ts + 1
  /\ client_state' = [client_state EXCEPT ![c] = "active"]
  /\ client_ts' = [client_ts EXCEPT ![c].start_ts = next_ts']
  /\ client_op_count' = [client_op_count EXCEPT ![c] = 0]  \* 重置操作计数
  /\ UNCHANGED <<client_read_set, client_write_set, 
                key_replica_data, key_replica_ts, key_replica_lock,
                key_replica_last_read_ts, key_replica_si, client_txn_count>>


\* 读取数据时检查读写冲突
Read(c, k) ==
  /\ client_state[c] = "active"
  /\ client_op_count[c] < MAX_OPS_PER_TXN
  /\ k \notin client_read_set[c]  \* 确保这个键没有被读过
  /\ \/ /\ HasQuorum(k, LAMBDA r : key_replica_lock[k][r] = {})  \* 确保足够多的副本无锁
        /\ \E data \in DOMAIN key_replica_data[k][1] :  \* 使用第一个副本的数据作为基准
            /\ key_replica_data[k][1][data].ts <= client_ts[c].start_ts
            /\ ~\E newer \in DOMAIN key_replica_data[k][1] :
                /\ key_replica_data[k][1][newer].ts <= client_ts[c].start_ts
                /\ key_replica_data[k][1][newer].ts > key_replica_data[k][1][data].ts
            /\ HasQuorum(k, LAMBDA r :  \* 确保足够多的副本有相同版本
                    \E d \in DOMAIN key_replica_data[k][r] :
                    key_replica_data[k][r][d].ts = key_replica_data[k][1][data].ts)
            /\ client_read_set' = [client_read_set EXCEPT ![c] = @ \union {k}]
            /\ key_replica_last_read_ts' = [key_replica_last_read_ts EXCEPT ![k] = 
                [r \in 1..REPLICA_COUNT |-> 
                    IF r <= QUORUM_SIZE THEN client_ts[c].start_ts ELSE @[r]]]
            /\ UNCHANGED <<next_ts, client_state, client_ts, client_write_set, 
                    key_replica_data, key_replica_ts, key_replica_lock, key_replica_si, client_txn_count>>
     \/ /\ client_state' = [client_state EXCEPT ![c] = "failed"]
        /\ UNCHANGED <<next_ts, client_ts, client_read_set, client_write_set, 
                    key_replica_data, key_replica_lock, key_replica_ts, key_replica_last_read_ts, key_replica_si, client_txn_count>>
  /\ client_op_count' = [client_op_count EXCEPT ![c] = @ + 1]
  

\* 写入数据时检查写写冲突
Write(c, k) ==
  /\ client_state[c] = "active"
  /\ client_op_count[c] < MAX_OPS_PER_TXN
  /\ k \notin client_write_set[c]  \* 确保这个键没有被写过
  /\ \/ /\ HasQuorum(k, LAMBDA r : key_replica_lock[k][r] = {})  \* 确保足够多的副本无锁
        /\ \A r \in 1..REPLICA_COUNT :  \* 添加写写冲突检查：确保没有比当前事务开始时间戳更新的写入
             \/ r > QUORUM_SIZE
             \/ key_replica_ts[k][r] <= client_ts[c].start_ts
        /\ key_replica_lock' = [key_replica_lock EXCEPT ![k] = 
             [r \in 1..REPLICA_COUNT |-> 
               IF r <= QUORUM_SIZE THEN {c} ELSE key_replica_lock[k][r]]]
        /\ client_write_set' = [client_write_set EXCEPT ![c] = @ \union {k}]
        /\ UNCHANGED <<next_ts, client_state, client_ts, client_read_set, 
                      key_replica_data, key_replica_ts, key_replica_last_read_ts, 
                      key_replica_si, client_txn_count>>
     \/ /\ client_state' = [client_state EXCEPT ![c] = "failed"]
        /\ UNCHANGED <<next_ts, client_ts, client_read_set, client_write_set, 
                      key_replica_data, key_replica_lock, key_replica_ts, 
                      key_replica_last_read_ts, key_replica_si, client_txn_count>>
  /\ client_op_count' = [client_op_count EXCEPT ![c] = @ + 1]
  

\* 提交事务
Commit(c) ==
  /\ client_state[c] = "active"
  /\ \A k \in client_write_set[c] :
       HasQuorum(k, LAMBDA r : c \in key_replica_lock[k][r])  \* 确保写锁仲裁
  /\ next_ts' = next_ts + 1
  /\ client_state' = [client_state EXCEPT ![c] = "committed"]
  /\ client_ts' = [client_ts EXCEPT ![c].commit_ts = next_ts']
  /\ IF client_write_set[c] = {}  \* 处理写集为空的情况
     THEN /\ UNCHANGED key_replica_data
          /\ UNCHANGED key_replica_ts
          /\ UNCHANGED key_replica_lock
          /\ UNCHANGED key_replica_last_read_ts
          /\ UNCHANGED key_replica_si
     ELSE /\ \A k \in client_write_set[c] :
            /\ checkSnapshotIsolation(k, next_ts')
            /\ key_replica_data' = [key_replica_data EXCEPT ![k] = 
                 [r \in 1..REPLICA_COUNT |->
                   IF r <= QUORUM_SIZE 
                   THEN Append(@[r], [ts |-> next_ts', val |-> "written"])
                   ELSE @[r]]]
            /\ key_replica_ts' = [key_replica_ts EXCEPT ![k] = 
                 [r \in 1..REPLICA_COUNT |->
                   IF r <= QUORUM_SIZE THEN next_ts' ELSE @[r]]]
            /\ key_replica_lock' = [key_replica_lock EXCEPT ![k] = 
                 [r \in 1..REPLICA_COUNT |-> {}]]
            /\ UNCHANGED key_replica_last_read_ts  \* 明确指定不变
  /\ UNCHANGED <<client_read_set, client_write_set, client_txn_count, client_op_count>>

\* 中止事务
Abort(c) ==
  /\ \/ client_state[c] = "active"
     \/ client_state[c] = "failed" 
  /\ client_state' = [client_state EXCEPT ![c] = "aborted"]
  /\ key_replica_lock' = [k \in KEY |->  \* 释放所有副本上的锁
       [r \in 1..REPLICA_COUNT |->
         IF c \in key_replica_lock[k][r] THEN {} ELSE key_replica_lock[k][r]]]
  /\ UNCHANGED <<next_ts, client_ts, client_read_set, client_write_set, 
                 key_replica_data, key_replica_ts, key_replica_last_read_ts, key_replica_si, client_txn_count, client_op_count>>


\* 重置事务状态
Reset(c) ==
  /\ \/ client_state[c] = "committed"
     \/ client_state[c] = "aborted"  \* 允许从 aborted 状态重置
  /\ client_txn_count[c] < MAX_TXN_PER_CLIENT  \* 检查是否达到事务限制
  /\ client_state' = [client_state EXCEPT ![c] = "init"]
  /\ client_ts' = [client_ts EXCEPT ![c] = [start_ts |-> 0, commit_ts |-> 0]]
  /\ client_op_count' = [client_op_count EXCEPT ![c] = 0]
  /\ client_read_set' = [client_read_set EXCEPT ![c] = {}]
  /\ client_write_set' = [client_write_set EXCEPT ![c] = {}]
  /\ client_txn_count' = [client_txn_count EXCEPT ![c] = @ + 1]  \* 增加事务计数
  /\ UNCHANGED <<next_ts, key_replica_data, key_replica_lock, key_replica_ts,
                key_replica_last_read_ts, key_replica_si>>

Termination ==
  \A c \in CLIENT : client_txn_count[c] >= MAX_TXN_PER_CLIENT
  /\ UNCHANGED vars
\* 修改 Next
Next ==
    \E c \in CLIENT :
        \/ Start(c)
        \/ \E k \in KEY : Read(c, k)
        \/ \E k \in KEY : Write(c, k)
        \/ Commit(c)
        \/ Abort(c)
        \/ Reset(c)  \* 添加重置操作
    \/ Termination

\* 完整的规范
Spec == Init /\ [][Next]_vars

\* 类型不变量
TypeInvariant ==
  /\ next_ts \in Nat
  /\ client_state \in [CLIENT -> States]
  /\ client_ts \in [CLIENT -> [start_ts : Nat, commit_ts : Nat]]
  /\ client_read_set \in [CLIENT -> SUBSET KEY]
  /\ client_write_set \in [CLIENT -> SUBSET KEY]

\* 写一致性不变量
WriteConsistency ==
  \A k \in KEY :
    \A r \in 1..REPLICA_COUNT :
      \A n \in 1..(Len(key_replica_data[k][r]) - 1) :
        /\ key_replica_data[k][r][n].ts < key_replica_data[k][r][n+1].ts

\* 提交一致性不变量
CommittedConsistency ==
  \A c \in CLIENT :
    client_state[c] = "committed" =>
      \A k \in client_write_set[c] :
        /\ HasQuorum(k, LAMBDA r : 
             \E n \in DOMAIN key_replica_data[k][r] :
               /\ key_replica_data[k][r][n].ts = client_ts[c].commit_ts
               /\ key_replica_lock[k][r] # c)
        /\ HasQuorum(k, LAMBDA r : 
             \E n \in DOMAIN key_replica_data[k][r] :
               /\ key_replica_data[k][r][n].ts = client_ts[c].commit_ts)

\* 回滚一致性不变量
AbortedConsistency ==
  \A c \in CLIENT :
    (/\ client_state[c] = "aborted"
     /\ client_ts[c].commit_ts # 0
    ) =>
      \A k \in client_write_set[c] :
        \A r \in 1..REPLICA_COUNT :
          ~\E n \in DOMAIN key_replica_data[k][r] :
            key_replica_data[k][r][n].ts = client_ts[c].commit_ts

SnapshotIsolation ==
  \A c1, c2 \in CLIENT :
    (/\ client_state[c1] = "committed"
     /\ client_state[c2] = "committed"
     /\ c1 # c2
    ) =>
    \/ client_write_set[c1] \intersect client_write_set[c2] = {}
    \/ client_ts[c1].commit_ts < client_ts[c2].start_ts
    \/ client_ts[c2].commit_ts < client_ts[c1].start_ts
    
SnapshotReadInvariant ==
  \A k \in KEY :
    \A r \in 1..REPLICA_COUNT :
      key_replica_si[k][r] = TRUE


\* 修改安全性定理
THEOREM Safety == Spec => [](TypeInvariant /\ SnapshotIsolation /\ SnapshotReadInvariant 
                            /\ WriteConsistency /\ CommittedConsistency /\ AbortedConsistency)


================================================================================