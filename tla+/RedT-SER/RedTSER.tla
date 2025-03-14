-------------------------------- MODULE RedTSER--------------------------------
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

VARIABLES next_ts,           
          client_state,      
          client_ts,         
          client_read_set,   
          client_write_set,  
          key_replica_data,  
          key_replica_lock,  \* 修改为包含读写锁信息 {[client |-> c, mode |-> "read"|"write"]}
          key_replica_ts,    
          client_txn_count,  
          client_op_count    

vars == <<next_ts, client_state, client_ts, client_read_set, 
          client_write_set, key_replica_data, key_replica_lock, 
          key_replica_ts, client_txn_count, client_op_count>>

\* 客户端状态
States == {"init", "active", "failed", "committed", "aborted"}

\* 定义已提交事务集合
CommittedTxns == {c \in CLIENT : client_state[c] = "committed"}

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
  /\ client_txn_count = [c \in CLIENT |-> 0]
  /\ client_op_count = [c \in CLIENT |-> 0]

HasQuorum(k, pred(_)) ==
  LET replicas == {r \in 1..REPLICA_COUNT : pred(r)}
  IN Cardinality(replicas) >= QUORUM_SIZE

\* 检查是否可以获取读锁
CanAcquireReadLock(k, r, c) ==
  \A lock \in key_replica_lock[k][r] :
    \/ lock.client = c  \* 自己持有的锁
    \/ lock.mode = "read"  \* 其他客户端的读锁

\* 检查是否可以获取写锁
CanAcquireWriteLock(k, r, c) ==
  \A lock \in key_replica_lock[k][r] :
    lock.client = c

\* 定义事务依赖关系
Depends(t1, t2) ==
    /\ t1 # t2
    /\ client_state[t1] = "committed"
    /\ client_state[t2] = "committed"
    /\ \/ \E k1 \in KEY :  \* 写写冲突
          /\ k1 \in client_write_set[t1]
          /\ k1 \in client_write_set[t2]
    \/ \E k2 \in KEY :  \* 读写冲突
          /\ k2 \in client_read_set[t1]
          /\ k2 \in client_write_set[t2]
    \/ \E k3 \in KEY :  \* 写读冲突
          /\ k3 \in client_write_set[t1]
          /\ k3 \in client_read_set[t2]

RECURSIVE IsAcyclic(_, _, _, _)
IsAcyclic(G, visited, current, path) ==
    \/ path = {}
    \/ \A next \in {t \in CommittedTxns : <<current, t>> \in G} :
         /\ next \notin visited
         /\ IsAcyclic(G, visited \union {next}, next, path \ {current})

\* 开始新事务
Start(c) ==
  /\ client_state[c] = "init"
  /\ next_ts' = next_ts + 1
  /\ client_state' = [client_state EXCEPT ![c] = "active"]
  /\ client_ts' = [client_ts EXCEPT ![c].start_ts = next_ts']
  /\ client_op_count' = [client_op_count EXCEPT ![c] = 0]  \* 重置操作计数
  /\ UNCHANGED <<client_read_set, client_write_set, 
                key_replica_data, key_replica_ts, key_replica_lock, client_txn_count>>

\* 读取数据时检查读写冲突
Read(c, k) ==
  /\ client_state[c] = "active"
  /\ client_op_count[c] < MAX_OPS_PER_TXN
  /\ k \notin client_read_set[c]
  /\ \/ /\ HasQuorum(k, LAMBDA r : CanAcquireReadLock(k, r, c))
        /\ \E data \in DOMAIN key_replica_data[k][1] :
             /\ key_replica_data[k][1][data].ts <= client_ts[c].start_ts
             /\ ~\E newer \in DOMAIN key_replica_data[k][1] :
                  /\ key_replica_data[k][1][newer].ts <= client_ts[c].start_ts
                  /\ key_replica_data[k][1][newer].ts > key_replica_data[k][1][data].ts
             /\ HasQuorum(k, LAMBDA r :
                  \E d \in DOMAIN key_replica_data[k][r] :
                     key_replica_data[k][r][d].ts = key_replica_data[k][1][data].ts)
             /\ client_read_set' = [client_read_set EXCEPT ![c] = @ \union {k}]
             /\ key_replica_lock' = [key_replica_lock EXCEPT ![k] = 
                  [r \in 1..REPLICA_COUNT |-> 
                     IF r <= QUORUM_SIZE 
                     THEN @[r] \union {[client |-> c, mode |-> "read"]}
                     ELSE @[r]]]
             /\ UNCHANGED <<next_ts, client_state, client_ts, client_write_set, 
                           key_replica_data, key_replica_ts, client_txn_count>>
     \/ /\ client_state' = [client_state EXCEPT ![c] = "failed"]
        /\ UNCHANGED <<next_ts, client_ts, client_read_set, client_write_set, 
                      key_replica_data, key_replica_lock, key_replica_ts, client_txn_count>>
  /\ client_op_count' = [client_op_count EXCEPT ![c] = @ + 1]
  

\* 写入数据时检查写写冲突
Write(c, k) ==
  /\ client_state[c] = "active"
  /\ client_op_count[c] < MAX_OPS_PER_TXN
  /\ k \notin client_write_set[c]
  /\ \/ /\ HasQuorum(k, LAMBDA r : CanAcquireWriteLock(k, r, c))
        /\ key_replica_lock' = [key_replica_lock EXCEPT ![k] = 
             [r \in 1..REPLICA_COUNT |-> 
               IF r <= QUORUM_SIZE 
               THEN {[client |-> c, mode |-> "write"]}
               ELSE @[r]]]
        /\ client_write_set' = [client_write_set EXCEPT ![c] = @ \union {k}]
        /\ UNCHANGED <<next_ts, client_state, client_ts, client_read_set, 
                      key_replica_data, key_replica_ts, client_txn_count>>
     \/ /\ client_state' = [client_state EXCEPT ![c] = "failed"]
        /\ UNCHANGED <<next_ts, client_ts, client_read_set, client_write_set, 
                      key_replica_data, key_replica_lock, key_replica_ts, client_txn_count>>
  /\ client_op_count' = [client_op_count EXCEPT ![c] = @ + 1]
  

\* 提交事务
Commit(c) ==
  /\ client_state[c] = "active"
  /\ \A k \in client_write_set[c] :  \* 检查写锁仲裁
       HasQuorum(k, LAMBDA r : 
         \E lock \in key_replica_lock[k][r] : 
           /\ lock.client = c 
           /\ lock.mode = "write")
  /\ \A k \in client_read_set[c] :   \* 检查读锁仲裁
       HasQuorum(k, LAMBDA r : 
         \E lock \in key_replica_lock[k][r] : 
           /\ lock.client = c 
           /\ lock.mode = "read")
  /\ next_ts' = next_ts + 1
  /\ client_state' = [client_state EXCEPT ![c] = "committed"]
  /\ client_ts' = [client_ts EXCEPT ![c].commit_ts = next_ts']
  /\ IF client_write_set[c] = {}  \* 处理写集为空的情况
     THEN /\ UNCHANGED key_replica_data
          /\ UNCHANGED key_replica_ts
          /\ UNCHANGED key_replica_lock
     ELSE /\ \A k \in client_write_set[c] :
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
                 key_replica_data, key_replica_ts, client_txn_count, client_op_count>>


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
  /\ UNCHANGED <<next_ts, key_replica_data, key_replica_lock, key_replica_ts>>

\* Termination ==
\*   \A c \in CLIENT : client_txn_count[c] >= MAX_TXN_PER_CLIENT
\*   /\ UNCHANGED vars

Termination ==
  \A c \in CLIENT : 
    \/ client_state[c] = "committed"
    \/ client_state[c] = "aborted"
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
\*   /\ key_replica_lock \in [KEY -> [1..REPLICA_COUNT -> SUBSET [client : CLIENT, mode : {"read", "write"}]]]

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

\* 可串行化不变量
Serializability ==
    LET 
        \* 构建事务依赖图的边集
        Edges == {t \in CommittedTxns \X CommittedTxns : 
                    /\ t[1] # t[2]
                    /\ Depends(t[1], t[2])
                    /\ client_ts[t[1]].commit_ts < client_ts[t[2]].commit_ts}
    IN 
    \* 对于任意起点，验证不存在环路
    \A start \in CommittedTxns :
        IsAcyclic(Edges, {start}, start, CommittedTxns)



\* 修改安全性定理
THEOREM Safety == Spec => [](TypeInvariant /\ WriteConsistency /\ 
                            CommittedConsistency /\ AbortedConsistency /\ Serializability)


================================================================================