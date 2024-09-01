------------------------------- MODULE redtsi -------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS N \* number of data items
CONSTANTS M \* number of replicas for each data item
CONSTANTS T \* number of transactions
CONSTANTS C \* number of coordinators
CONSTANTS FCount \* The number of failures at the same node.
CONSTANTS FailedC \* The identifier number of the failed coordinator node
CONSTANTS FailedN \* The identifier number of the failed replica node


(* --algorithm RedT 
  variables
    Replicas = [item \in 1..N |-> [rep \in 1..M |-> {[value |-> 0, timestamp |-> 0]}]],
    Transactions = [tid \in 1..T |->
                      LET readSet == {item \in 1..N : (item + tid) % 2 = 0} IN
                      LET writeSet == {item \in 1..N : item % 2 = 0} IN
                      [read |-> readSet, write |-> writeSet, StartTS |-> 0, 
                       coordinator_id |-> CHOOSE c \in (1+(N * M))..(C+(N * M)) : (tid % C) = (c % C) ]],
    Messages = {},
    CoordinatorMessages = {},
    \* locks
    WriteLocks = [item \in 1..N |-> [rep \in 1..M |-> FALSE]],
    \* the logs on each replica node.
    Logs = [rep \in 1..(N * M) |-> {}],
    \* the status of transactions, maintained in each node
    TransactionStatusCoor = [c \in (1+(N * M))..(C+(N * M)) |-> [tid \in 1..T |-> [status |-> "NotStarted", CommitTS |-> 0]]],
    TransactionStatus = [rep \in 1..(N * M) |-> [tid \in 1..T |-> [status |-> "NotStarted", CommitTS |-> 0]]],
    \* The current execution transactions on each coordinator
    CurrentTransaction = [c \in (1+(N * M))..(C+(N * M)) |-> 0], 
    \* The execution result of first round in RedT on each replica
    SuccessCounts = [c \in (1+(N * M))..(C+(N * M)) |-> [item \in 1..N |-> 0]]; 
    FailureCounts = [c \in (1+(N * M))..(C+(N * M)) |-> [item \in 1..N |-> 0]]; 
    ReadResults = [c \in (1+(N * M))..(C+(N * M))  |-> [tid \in 1..T |-> [item \in 1..N |-> {}]]];
    ReadConsistency = [c \in (1+(N * M))..(C+(N * M))  |-> TRUE];
    \* The execution result of second round in RedT on each replica
    DoneCounts = [c \in (1+(N * M))..(C+(N * M)) |-> [item \in 1..N |-> 0]]; 
    \* Node status of coordinator node and replica node
    NodeStatus = [nid \in (1+(N * M))..(C+(N * M)) \cup (1..(N * M)) |-> "Active"];  \* 协调器和副本的状态
    FailedCounts = [nid \in 1..(C+(N*M)) |-> 0];  \* 每个节点故障的次数
    \* The failed count for each node 
    CommitCounts = [c \in (1+(N * M))..(C+(N * M)) |-> 0]; 
    CommitTS = [c \in (1+(N * M))..(C+(N * M)) |-> 0]; 
    AbortCounts = [c \in (1+(N * M))..(C+(N * M)) |-> 0];
    \* The count for recover failed transactions
    SyncCounts = [item \in 1..(N * M) |-> 0]; 
    CurrentTime = 1;
    \* The variable indicating whether the system has stopped.
    SystemTerminated = FALSE;
    RecieveACKCount = [c \in (1+(N * M))..(C+(N * M)) |-> 0];
    
  \* Check if the write lock is available.
  define 
    IsWriteLockAvailable(item, replica) == WriteLocks[item][replica] = FALSE
    Max(S) == CHOOSE x \in S : \A y \in S : x >= y
    ConsistentFinishedTransactions == 
        \A tid \in 1..T :
          IF TransactionStatusCoor[Transactions[tid].coordinator_id][tid].status = "Finished" 
            THEN 
            LET dataItems == Transactions[tid].read \cup Transactions[tid].write IN
            \A item \in dataItems :
              LET committedReplicas == {r \in 1..M : TransactionStatus[((item-1)*M) + r][tid].status = "Committed"} IN
              LET abortedReplicas == {r \in 1..M : TransactionStatus[((item-1)*M) + r][tid].status = "Aborted"} IN
              (Cardinality(committedReplicas) > (M \div 2) /\ Cardinality(abortedReplicas) = 0) \/
              (Cardinality(abortedReplicas) > (M \div 2) /\ Cardinality(committedReplicas) = 0)
          ELSE TRUE
    NoConflictingTransactionStatus ==
    \A tid \in 1..T :
      LET allStatuses == {TransactionStatusCoor[c][tid].status: c \in (1+(N * M))..(C+(N * M))} \cup 
                         {TransactionStatus[r][tid].status: r \in 1..(N * M)} IN
      ~("Committed" \in allStatuses /\ "Aborted" \in allStatuses)
  end define;      
              
  \* The coordinator analyzes and dispatches transactions.
  procedure SendTransactions(cid)  
  variables t = 0, i1 = 0, j1 = 0, Test = {}, targetReplica = 0, s_msg = {};
  begin
  ProcSendTransaction:
    if CurrentTransaction[cid] = 0 /\
       \E tid \in 1..T: Transactions[tid].coordinator_id = cid /\ TransactionStatusCoor[cid][tid].status = "NotStarted" then
      t := CHOOSE tid \in 1..T: Transactions[tid].coordinator_id = cid /\ TransactionStatusCoor[cid][tid].status = "NotStarted";
      if t /= 0 then
        CurrentTransaction[cid] := t;
        TransactionStatusCoor[cid][t].status := "Pending";
        Transactions[t].StartTS := CurrentTime;  \* transaction get start timestamp
        ProcSendTransactionsOuterLoop:
        while i1 < N do
          i1 := i1 + 1;
          if i1 \in Transactions[t].read \cup Transactions[t].write then
            j1 := 0;
            ProcSendTransactionsInnerLoop:
            while j1 < M do
              j1 := j1 + 1;
              targetReplica := ((i1 - 1) * M) + j1;
              s_msg := [item |-> i1,
                        replica |-> j1,
                        transaction |-> CurrentTransaction[cid],
                        type |-> CHOOSE msgType \in {"Read", "Write"}:
                                IF i1 \in Transactions[t].write THEN msgType = "Write"
                                ELSE msgType = "Read",
                        coordinator_id |-> cid,
                        sendTime |-> CurrentTime,
                        targetReplica |-> targetReplica];
              Messages := Messages \cup {s_msg}; 
            end while;
          end if;
        end while;
      end if;
    end if;
    ProcSendTransactionB:
    \* print Messages;
    return;
  end procedure;

  \* Calculate the count of successful and failed read and write operations.
  procedure CalculateACKCounts(t, msg, cid)      
  begin
    ProcCalculateCounts:
    if msg.status = "ReadSuccess" then
      SuccessCounts[cid][msg.item] := SuccessCounts[cid][msg.item] + 1;
      ReadResults[cid][t][msg.item] := ReadResults[cid][t][msg.item] \cup 
                                        {[replica |-> msg.replica, value |-> msg.value, timestamp |-> msg.wts]};
      if ReadConsistency[cid] /\ Cardinality(ReadResults[cid][t][msg.item]) > 1 then
        ReadConsistency[cid] := \A r1, r2 \in ReadResults[cid][t][msg.item]: 
                                      (r1.value = r2.value) /\ (r1.timestamp = r2.timestamp);
      end if;
    elsif msg.status = "ReadFailed" then
      FailureCounts[cid][msg.item] := FailureCounts[cid][msg.item] + 1;
    elsif msg.status = "WriteSuccess" then
      SuccessCounts[cid][msg.item] := SuccessCounts[cid][msg.item] + 1;
    elsif msg.status = "WriteFailed" then
      FailureCounts[cid][msg.item] := FailureCounts[cid][msg.item] + 1;
    end if;
    \* ProcCalculateCountsB:
    if msg.type = "Report" then
      if msg.status = "Committed" then
        CommitCounts[cid] := CommitCounts[cid] + 1;
        CommitTS[cid] := msg.CommitTS;
      elsif msg.status = "Aborted" then
        AbortCounts[cid] := AbortCounts[cid] + 1;
      end if;
    end if;  
    ProcCalculateCountsC:
    \* print "Coordinator " \o ToString(cid) \o " gather transaction " \o ToString(t) \o " status " \o ToString(SuccessCounts[cid]) \o " " \o ToString(FailureCounts[cid]);
    return;
  end procedure;

  procedure CoordinatorDecide(cid, quorom, minority) 
  variables t = 0, i7 = 0, j7 = 0, commitTimestamp = 0, targetReplica = 0, decision = "Pending", allSuccessful = TRUE, s_msg = {};
  begin
    ProcCoorDec:
    if CurrentTransaction[cid] = 0 then
      return;
    end if;
    ProcCoorDecA:
    t := CurrentTransaction[cid];
    if CommitCounts[cid] > 0 then 
      decision := "Commit";
    elsif AbortCounts[cid] > 0 \/ ReadConsistency[cid] = FALSE then
      decision := "Abort";
    elsif \E i5 \in Transactions[t].read \cup Transactions[t].write : FailureCounts[cid][i5] > minority then
      decision := "Abort";
    elsif \A i5 \in Transactions[t].read \cup Transactions[t].write : SuccessCounts[cid][i5] > quorom then
      decision := "Commit";
    end if;
    if decision = "Commit" then 
      commitTimestamp := CurrentTime;
      CurrentTime := CurrentTime + 1; 
      TransactionStatusCoor[cid][t] := [status |-> "Committed", CommitTS |-> commitTimestamp];
      if NodeStatus[cid] = "Recover" then 
        NodeStatus[cid] := "Active";
      end if;
      \* print "Coordinator " \o ToString(cid) \o " changing transaction" \o ToString(t) \o "status to Committed";
      ProcHandleProcAckSendCommitOuterLoop:
      while i7 < N do
        i7 := i7 + 1;
        if i7 \in Transactions[t].read \cup Transactions[t].write then
          j7 := 0;
          ProcHandleProcAckSendCommitInnerLoop:
          while j7 < M do
            j7 := j7 + 1;
            targetReplica := ((i7 - 1) * M) + j7;
            s_msg := [item |-> i7, replica |-> j7, transaction |-> t, type |-> "Commit", timestamp |-> commitTimestamp, coordinator_id |-> Transactions[t].coordinator_id, sendTime |-> CurrentTime, targetReplica |-> targetReplica];
            Messages := Messages \cup {s_msg}; 
          end while;
        end if;
      end while;
    elsif decision = "Abort" then
      TransactionStatusCoor[cid][t].status := "Aborted";
      if NodeStatus[cid] = "Recover" then 
        NodeStatus[cid] := "Active";
      end if;
      \* print "Coordinator " \o ToString(cid) \o " changing transaction" \o ToString(t) \o "status to Aborted";
      ProcHandleProcAckSendAbortOuterLoop:
      while i7 < N do
        i7 := i7 + 1;
        if i7 \in Transactions[t].read \cup Transactions[t].write then
          j7 := 0;
          ProcHandleProcAckSendAbortInnerLoop:
          while j7 < M do
            j7 := j7 + 1;
            targetReplica := ((i7 - 1) * M) + j7;
            s_msg := [item |-> i7, replica |-> j7, transaction |-> t, type |-> "Abort", timestamp |-> commitTimestamp, coordinator_id |-> Transactions[t].coordinator_id, sendTime |-> CurrentTime, targetReplica |-> targetReplica];
            Messages := Messages \cup {s_msg}; 
          end while;
        end if;
      end while;
    end if;
    IfSuccess:
    \* print Messages;
    \* print "Txn " \o ToString(t) \o " needs to " \o decision;
    return;
  end procedure;

  procedure CoordinatorFinDecide(cid, quorom) 
  variables t = 0, targetReplica = 0;
  begin
    ProcCoorFinDecide:
    if CurrentTransaction[cid] = 0 then
      return;
    end if;
    ProcCoorFinDecideB:
    t := CurrentTransaction[cid];
    if \A i6 \in Transactions[t].read \cup Transactions[t].write : DoneCounts[cid][i6] > quorom then
      \* CoordinatorMessages := {amsg \in CoordinatorMessages : amsg.transaction /= t /\ amsg.coordinator_id /= cid};
      TransactionStatusCoor[cid][t].status := "Finished";
      \* print "Coordinator " \o ToString(cid) \o " changing transaction " \o ToString(t) \o " status to Finished";
      CurrentTransaction[cid] := 0;
      SuccessCounts[cid] := [item \in 1..N |-> 0]; 
      FailureCounts[cid] := [item \in 1..N |-> 0]; 
      ReadResults[cid] := [tid \in 1..T |-> [item \in 1..N |-> {}]];
      ReadConsistency[cid] := TRUE;
      DoneCounts[cid] := [item \in 1..N |-> 0]; 
      CommitCounts[cid] := 0; 
      CommitTS[cid] := 0; 
      AbortCounts[cid] := 0;
    end if;
    ProcCoorFinDecideC:
    return;
  end procedure;

  procedure CoordinatorHandleACKs(cid) 
  variables msg = {}, type = "", recoveryTid = 0;
  begin
    ProcCoorHandleACK:
    if \E m \in CoordinatorMessages : m.coordinator_id = cid /\ m.type /= "Report" then
      msg := CHOOSE m \in CoordinatorMessages : m.coordinator_id = cid /\ m.type /= "Report";
      CoordinatorMessages := CoordinatorMessages \ {msg};
      RecieveACKCount[cid] := RecieveACKCount[cid] + 1;
      \* print "recieve coor msg " \o ToString(RecieveACKCount[cid]) \o " " \o ToString(msg);
      if CurrentTransaction[cid] = msg.transaction /\ msg.type = "Fin" then
        DoneCounts[cid][msg.item] := DoneCounts[cid][msg.item] + 1;
        \* print DoneCounts;
        call CoordinatorFinDecide(cid, (3*M) \div 4);
      elsif CurrentTransaction[cid] = msg.transaction /\ msg.type = "Process-ack" then 
        call CalculateACKCounts(CurrentTransaction[cid], msg, cid); 
        ProcCoorRecACKB:
        call CoordinatorDecide(cid, (3*M) \div 4, M \div 4);
      end if;
    end if;  
    ProcCoorHandleACKB:
    return;
  end procedure;

  procedure CoordinatorHandleReports(cid) 
  variables msg = {}, type = "", recoveryTid = 0;
  begin
    ProcCoorHandleReport:
    if \E m \in CoordinatorMessages : m.coordinator_id = cid /\ m.type = "Report" then
      msg := CHOOSE m \in CoordinatorMessages : m.coordinator_id = cid /\ m.type = "Report";
      CoordinatorMessages := CoordinatorMessages \ {msg};
      call CalculateACKCounts(CurrentTransaction[cid], msg, cid); 
      ProcCoorHandleReportB:
      call CoordinatorDecide(cid, M \div 2, M \div 2);
    end if;  
    ProcCoorHandleReportC:
    return;
  end procedure;

  \* Handle Read operation for replica node
  procedure HandleRead(msg, id)
  variables i = 0, j = 0, t = 0, readTimestamp = 0, readData = {}, res_msg = {};
  begin
    ProcHandleRead:
    i := msg.item;
    j := msg.replica;
    t := msg.transaction;
    readTimestamp := Transactions[t].StartTS; 

    \* read the latest version with timestamp smaller than T's start timestamp  
    readData := CHOOSE v \in {v \in Replicas[i][j]: v.timestamp <= readTimestamp /\
                                   \A u \in Replicas[i][j] : (u.timestamp <= readTimestamp) => (u.timestamp <= v.timestamp)}: TRUE;
    if TransactionStatus[id][t].status = "NotStarted" /\ IsWriteLockAvailable(i, j) then
      \* print "Replica" \o ToString(i) \o ToString(j) \o "changing transaction" \o ToString(t) \o "status from NotStarted to ReadSuccess";
      TransactionStatus[id][t].status := "ReadSuccess";
      res_msg := [item |-> i, replica |-> j, transaction |-> t, type |-> "Process-ack", status |-> "ReadSuccess", value |-> readData.value, wts |-> readData.timestamp, sendTime |-> CurrentTime, coordinator_id |-> msg.coordinator_id];
      CoordinatorMessages := CoordinatorMessages \cup {res_msg}; 
    elsif TransactionStatus[id][t].status = "NotStarted" then
    \*   print "Replica" \o ToString(i) \o ToString(j) \o "changing transaction" \o ToString(t) \o "status from NotStarted to ReadFailed";
      TransactionStatus[id][t].status := "ReadFailed";
      res_msg := [item |-> i, replica |-> j, transaction |-> t, type |-> "Process-ack", status |-> "ReadFailed", sendTime |-> CurrentTime, coordinator_id |-> msg.coordinator_id];
      CoordinatorMessages := CoordinatorMessages \cup {res_msg}; 
    end if;
    ProcHandleReadB:
    \* print ToString(TransactionStatus);
    return;
  end procedure;

  \* Handle Write operation for replica node
  procedure HandleWrite(msg, id)
  variables i = 0, j = 0, t = 0, res_msg = {};
  begin
    ProcHandleWrite:
    i := msg.item;
    j := msg.replica;
    t := msg.transaction;
    if TransactionStatus[id][t].status = "NotStarted" /\ IsWriteLockAvailable(i, j) then
    \*   print "Replica" \o ToString(i) \o ToString(j) \o "changing transaction" \o ToString(t) \o "status from NotStarted to WriteSuccess";
      WriteLocks[i][j] := TRUE;
      Logs[id] := Logs[id] \cup {[transaction |-> t, item |-> i, replica |-> j, type |-> "write-pending", timestamp |-> 0, coordinator_id |-> msg.coordinator_id]};
      TransactionStatus[id][t].status := "WriteSuccess";
      res_msg := [item |-> i, replica |-> j, transaction |-> t, type |-> "Process-ack", status |-> "WriteSuccess",  sendTime |-> CurrentTime, coordinator_id |-> msg.coordinator_id];
      CoordinatorMessages := CoordinatorMessages \cup {res_msg}; 
    elsif TransactionStatus[id][t].status = "NotStarted" then
    \*   print "Replica" \o ToString(i) \o ToString(j) \o "changing transaction" \o ToString(t) \o "status from NotStarted to WriteFailed";
      TransactionStatus[id][t].status := "WriteFailed";
      res_msg := [item |-> i, replica |-> j, transaction |-> t, type |-> "Process-ack", status |-> "WriteFailed",  sendTime |-> CurrentTime, coordinator_id |-> msg.coordinator_id];
      CoordinatorMessages := CoordinatorMessages \cup {res_msg}; 
    end if;
     ProcHandleWriteB:
    return;
  end procedure;

  \* Handle Inquire Result operation for replica node
  procedure HandleInquire(msg, id)
  variables i = 0, j = 0, t = 0, res_msg = {}, readValue = 0, readWTS = 0;
  begin
    ProcHandleInquire:
    i := msg.item;
    j := msg.replica;
    t := msg.transaction;
    readTimestamp := Transactions[t].StartTS;   \* 使用事务开始时间戳
    if TransactionStatus[id][t].status = "ReadSuccess" then
      readData := CHOOSE v \in {v \in Replicas[i][j]: v.timestamp <= readTimestamp /\
                                   \A u \in Replicas[i][j] : (u.timestamp <= readTimestamp) => (u.timestamp <= v.timestamp)}: TRUE;
      readValue := readData.value;
      readWTS := readData.timestamp;
    else
      readValue := 0;
      readWTS := 0;
    end if;
    res_msg := [item |-> i, replica |-> j, transaction |-> t, type |-> "Report", status |-> TransactionStatus[id][t].status, value |-> readValue, wts |-> readWTS, sendTime |-> CurrentTime, CommitTS |->TransactionStatus[id][t].CommitTS,  coordinator_id |-> msg.coordinator_id];
    CoordinatorMessages := CoordinatorMessages \cup {res_msg}; 
    ProcHandleWriteB:
    return;
  end procedure;

  \* Handle Commit operation for replica node
  procedure HandleCommit(msg, id)
  variables i = 0, j = 0, t = 0, idx = 0, res_msg = {};
  begin
    ProcHandleCommit:
    i := msg.item;
    j := msg.replica;
    t := msg.transaction;
    \* Timestamps[i][j] := msg.timestamp;
    if \E log \in Logs[id] : log.transaction = t /\ log.item = i then
      Logs[id] := Logs[id] \cup {[transaction |-> t, item |-> i, replica |-> j, type |-> "write-commit", timestamp |-> msg.timestamp, coordinator_id |-> msg.coordinator_id]};
    end if;

    if WriteLocks[i][j] then
      WriteLocks[i][j] := FALSE;
      Replicas[i][j] := Replicas[i][j] \cup {[value |-> msg.value, timestamp |-> msg.timestamp]}; 
    end if;
    TransactionStatus[id][t] := [status |-> "Committed", CommitTS |-> msg.timestamp];
    \* print "Replica" \o ToString(i) \o ToString(j) \o "changing transaction" \o ToString(t) \o "status to Committed";
    res_msg := [item |-> i, replica |-> j, transaction |-> t, type |-> "Fin", sendTime |-> CurrentTime, coordinator_id |-> msg.coordinator_id];
    CoordinatorMessages := CoordinatorMessages \cup {res_msg}; 

    ProcHandleCommitB:
    return;
  end procedure;

  \* Handle Abort operation for replica node
  procedure HandleAbort(msg, id)
  variables i = 0, j = 0, t = 0, res_msg = {};
  begin
    ProcHandleAbort:
    i := msg.item;
    j := msg.replica;
    t := msg.transaction;
    Logs[id] := {log \in Logs[id] : log.transaction /= t};
    if WriteLocks[i][j] then
      WriteLocks[i][j] := FALSE;
    end if;
    TransactionStatus[id][t].status := "Aborted";
    \* print "Replica" \o ToString(i) \o ToString(j) \o "changing transaction" \o ToString(t) \o "status to Aborted";
    res_msg := [item |-> i, replica |-> j, transaction |-> t, type |-> "Fin", sendTime |-> CurrentTime, coordinator_id |-> msg.coordinator_id];
    CoordinatorMessages := CoordinatorMessages \cup {res_msg}; 
    ProcHandleAbortB:
    return;
  end procedure;

  procedure HandleSync(msg, id)
  variables i, j;
  begin
    ProcHandleSync:
    i := msg.item;  
    j := msg.replica;  

    \* 发送当前副本的日志和数据项
    s_msg := [type |-> "SyncAck", 
                      item |-> msg.item, 
                      replica |-> msg.replica, 
                      log |-> Logs[id],   
                      value |-> Replicas[i][j], 
                      sendTime |-> CurrentTime, 
                      targetReplica |-> msg.targetReplica];
    Messages := Messages \cup {s_msg};
    ProcHandleSyncB:
    return;
  end procedure;

  procedure HandleSyncAck(id)
  variables msg = {}, i13 = 0, j13 = 0; 
  begin
    ProcHandleSyncAck:
    if \E m \in Messages : m.targetReplica = id /\ m.type = "SyncAck" then
      msg := CHOOSE m \in Messages : m.targetReplica = id /\ m.type = "SyncAck";
      Messages := Messages \ {msg};
      SyncCounts[id] := SyncCounts[id] + 1;
      Logs[id] := Logs[id] \cup msg.Log;
      i13 := (id - 1) \div M + 1; 
      j13 := ((id - 1) % M) + 1;
      Replicas[i][j] := Replicas[i][j] \cup msg.value;
    \*   if Timestamps[i13][j13] < msg.wts then 
    \*     Timestamps[i13][j13] := msg.wts;
    \*     Replicas[i13][j13] := msg.value;
    \*   end if;
      if SyncCounts[id] >= Cardinality({i14 \in ((i13 - 1) * M + 1)..(i13 * M) : NodeStatus[i14] = "Active"}) then
        NodeStatus[id] := "Active";
      end if;
    end if;
    ProcHandleSyncAckB:
    return;
  end procedure;

  \* Replica node handle request from the coordinators
  procedure ReplicaReceiveRequest(id) 
  variables msg = {}, type = "";
  begin
    ProcRepRecReq:
    if \E m \in Messages : m.targetReplica = id /\ m.type /= "SyncAck" then
      msg := CHOOSE m \in Messages : m.targetReplica = id /\ m.type /= "SyncAck";
      Messages := Messages \ {msg};
      ProcRepRecReqB:
      if msg.type = "Read" then 
        call HandleRead(msg, id)
      elsif msg.type = "Write" then
        call HandleWrite(msg, id)
      elsif msg.type = "Commit" then 
        call HandleCommit(msg, id)
      elsif msg.type = "Abort" then
        call HandleAbort(msg, id)
      elsif msg.type = "Inquire" then
        call HandleInquire(msg, id)
      elsif msg.type = "Sync" then
        call HandleSync(msg, id)
      end if;
    end if;
    ProcRepRecReqC:
    return;
  end procedure;

  procedure NodeFail(id) 
  variables msg = {}, type = "";
  begin
    ProcSetNodeFail:
    if FailedN /= 0 /\ id = FailedN /\ FailedCounts[id] < FCount then 
      NodeStatus[id] := "Failed";
      FailedCounts[id] := FailedCounts[id] + 1;
      TransactionStatus[id] := [tid \in 1..T |-> [status |-> "NotStarted", CommitTS |-> 0]];
      with i12 \in 1..N do
        WriteLocks[i12][id] := FALSE;
      end with;
      Logs[id] := {};
      \* print ToString(id) \o "Failed"
    elsif FailedC /= 0 /\ id = FailedC + (N * M) /\ FailedCounts[id] < FCount then
      NodeStatus[id] := "Failed";
      FailedCounts[id] := FailedCounts[id] + 1;
      CurrentTransaction[id] := 0;
      SuccessCounts[id] := [item \in 1..N |-> 0]; 
      FailureCounts[id] := [item \in 1..N |-> 0]; 
      ReadResults[id] := [tid \in 1..T |-> [item \in 1..N |-> {}]];
      ReadConsistency[id] := TRUE;
      DoneCounts[id] := [item \in 1..N |-> 0]; 
      CommitCounts[id] := 0; 
      CommitTS[id] := 0; 
      AbortCounts[id] := 0;
      TransactionStatusCoor[id] := 
        [ tid \in 1..T 
        |-> IF TransactionStatusCoor[id][tid].status /= "Finished" /\ TransactionStatusCoor[id][tid].status /= "NotStarted"
            THEN [TransactionStatusCoor[id][tid] EXCEPT !.status = "Recover"]
            ELSE TransactionStatusCoor[id][tid] ];
    end if;
    ProcSetNodeFailB:
    return;
  end procedure;

  procedure CoordinatorRecover(id) 
  variables s_msg = {}, i8 = 0, j8 = 0;
  begin
    ProcCoordinatorRecover:
    NodeStatus[id] := "Recover";
    if \E tid \in 1..T : Transactions[tid].coordinator_id = id /\ TransactionStatusCoor[id][tid].status = "Recover" then
      \* Start to recover the failed transaction
      t := CHOOSE tid \in 1..T: Transactions[tid].coordinator_id = id /\ TransactionStatusCoor[id][tid].status = "Recover";
      CurrentTransaction[id] := t;
      ProcCoordinatorRecoverOuterLoop:
      while i8 < N do
        i8 := i8 + 1;
        if i8 \in Transactions[t].read \cup Transactions[t].write then
          j8 := 0;
          ProcCoordinatorRecoverInnerLoop:
          while j8 < M do
            j8 := j8 + 1;
            targetReplica := ((i8 - 1) * M) + j8;
            s_msg := [item |-> i8, replica |-> j8, transaction |-> t, type |-> "Inquire", timestamp |-> 0, coordinator_id |-> Transactions[t].coordinator_id, sendTime |-> CurrentTime, targetReplica |-> targetReplica];
            Messages := Messages \cup {s_msg}; 
          end while;
        end if;
      end while;
    end if;
    ProcCoordinatorRecoverB:
    return;
  end procedure;

  procedure ReplicaRecover(id) 
  variables s_msg = {}, dataItemId = 0;
  begin
    ProcReplicaRecover:
    NodeStatus[id] := "Recover";
    dataItemId := (id - 1) \div M + 1; 
    with j11 \in 1..M do
      targetReplica := ((dataItemId - 1) * M) + j11; 
      if targetReplica /= id then
        s_msg := [type |-> "Sync", 
                          item |-> dataItemId,
                          replica |-> j11,
                          sendTime |-> CurrentTime,
                          targetReplica |-> targetReplica];
        Messages := Messages \cup {s_msg}; 
      end if;
    end with;
    ProcReplicaRecoverB:
    return;
  end procedure;

  process Coordinator  \in (1+(N * M))..(C+(N * M))
  variable id = self; 
  begin
    CoordinatorMain:
    while SystemTerminated = FALSE do
      if NodeStatus[id] = "Active" then
        call SendTransactions(id);
        CoordinatorMainB:
        call CoordinatorHandleACKs(id); 
        CoordinatorMainC:
        call NodeFail(id);
        CoordinatorMainD:
        if \A tid1 \in 1..T : TransactionStatusCoor[Transactions[tid1].coordinator_id][tid1].status = "Finished" then
          SystemTerminated := TRUE;
        end if;
      elsif NodeStatus[id] = "Recover" then
        call CoordinatorHandleReports(id); 
      else 
        call CoordinatorRecover(id);
      end if;
    end while;
  end process;


  process Replica \in 1..(N * M)
  variable id = self; 
  begin
    ReplicaMain:
    while SystemTerminated = FALSE do
      if NodeStatus[id] = "Active" then
        call ReplicaReceiveRequest(id); 
        ReplicaMainB:
        call NodeFail(id);
        ReplicaMainC:
        if \A tid1 \in 1..T : TransactionStatusCoor[Transactions[tid1].coordinator_id][tid1].status = "Finished" then
          SystemTerminated := TRUE;
        end if;
      elsif NodeStatus[id] = "Recover" then
        call HandleSyncAck(id); 
      else
        call ReplicaRecover(id);
      end if;
    end while;
  end process;
end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "62c50b61" /\ chksum(tla) = "31458f49")
\* Label ProcHandleWriteB of procedure HandleWrite at line 340 col 5 changed to ProcHandleWriteB_
\* Process variable id of process Coordinator at line 575 col 12 changed to id_
\* Process variable id of process Replica at line 599 col 12 changed to id_R
\* Procedure variable t of procedure SendTransactions at line 84 col 13 changed to t_
\* Procedure variable targetReplica of procedure SendTransactions at line 84 col 47 changed to targetReplica_
\* Procedure variable s_msg of procedure SendTransactions at line 84 col 66 changed to s_msg_
\* Procedure variable t of procedure CoordinatorDecide at line 157 col 13 changed to t_C
\* Procedure variable targetReplica of procedure CoordinatorDecide at line 157 col 57 changed to targetReplica_C
\* Procedure variable s_msg of procedure CoordinatorDecide at line 157 col 120 changed to s_msg_C
\* Procedure variable t of procedure CoordinatorFinDecide at line 225 col 13 changed to t_Co
\* Procedure variable msg of procedure CoordinatorHandleACKs at line 252 col 13 changed to msg_
\* Procedure variable type of procedure CoordinatorHandleACKs at line 252 col 23 changed to type_
\* Procedure variable recoveryTid of procedure CoordinatorHandleACKs at line 252 col 34 changed to recoveryTid_
\* Procedure variable msg of procedure CoordinatorHandleReports at line 275 col 13 changed to msg_C
\* Procedure variable type of procedure CoordinatorHandleReports at line 275 col 23 changed to type_C
\* Procedure variable i of procedure HandleRead at line 291 col 13 changed to i_
\* Procedure variable j of procedure HandleRead at line 291 col 20 changed to j_
\* Procedure variable t of procedure HandleRead at line 291 col 27 changed to t_H
\* Procedure variable res_msg of procedure HandleRead at line 291 col 68 changed to res_msg_
\* Procedure variable i of procedure HandleWrite at line 320 col 13 changed to i_H
\* Procedure variable j of procedure HandleWrite at line 320 col 20 changed to j_H
\* Procedure variable t of procedure HandleWrite at line 320 col 27 changed to t_Ha
\* Procedure variable res_msg of procedure HandleWrite at line 320 col 34 changed to res_msg_H
\* Procedure variable i of procedure HandleInquire at line 345 col 13 changed to i_Ha
\* Procedure variable j of procedure HandleInquire at line 345 col 20 changed to j_Ha
\* Procedure variable t of procedure HandleInquire at line 345 col 27 changed to t_Han
\* Procedure variable res_msg of procedure HandleInquire at line 345 col 34 changed to res_msg_Ha
\* Procedure variable i of procedure HandleCommit at line 369 col 13 changed to i_Han
\* Procedure variable j of procedure HandleCommit at line 369 col 20 changed to j_Han
\* Procedure variable t of procedure HandleCommit at line 369 col 27 changed to t_Hand
\* Procedure variable res_msg of procedure HandleCommit at line 369 col 43 changed to res_msg_Han
\* Procedure variable i of procedure HandleAbort at line 395 col 13 changed to i_Hand
\* Procedure variable j of procedure HandleAbort at line 395 col 20 changed to j_Hand
\* Procedure variable t of procedure HandleAbort at line 395 col 27 changed to t_Handl
\* Procedure variable msg of procedure HandleSyncAck at line 435 col 13 changed to msg_H
\* Procedure variable msg of procedure ReplicaReceiveRequest at line 461 col 13 changed to msg_R
\* Procedure variable type of procedure ReplicaReceiveRequest at line 461 col 23 changed to type_R
\* Procedure variable msg of procedure NodeFail at line 488 col 13 changed to msg_N
\* Procedure variable s_msg of procedure CoordinatorRecover at line 526 col 13 changed to s_msg_Co
\* Parameter cid of procedure SendTransactions at line 83 col 30 changed to cid_
\* Parameter msg of procedure CalculateACKCounts at line 124 col 35 changed to msg_Ca
\* Parameter cid of procedure CalculateACKCounts at line 124 col 40 changed to cid_C
\* Parameter cid of procedure CoordinatorDecide at line 156 col 31 changed to cid_Co
\* Parameter quorom of procedure CoordinatorDecide at line 156 col 36 changed to quorom_
\* Parameter cid of procedure CoordinatorFinDecide at line 224 col 34 changed to cid_Coo
\* Parameter cid of procedure CoordinatorHandleACKs at line 251 col 35 changed to cid_Coor
\* Parameter msg of procedure HandleRead at line 290 col 24 changed to msg_Ha
\* Parameter id of procedure HandleRead at line 290 col 29 changed to id_H
\* Parameter msg of procedure HandleWrite at line 319 col 25 changed to msg_Han
\* Parameter id of procedure HandleWrite at line 319 col 30 changed to id_Ha
\* Parameter msg of procedure HandleInquire at line 344 col 27 changed to msg_Hand
\* Parameter id of procedure HandleInquire at line 344 col 32 changed to id_Han
\* Parameter msg of procedure HandleCommit at line 368 col 26 changed to msg_Handl
\* Parameter id of procedure HandleCommit at line 368 col 31 changed to id_Hand
\* Parameter msg of procedure HandleAbort at line 394 col 25 changed to msg_Handle
\* Parameter id of procedure HandleAbort at line 394 col 30 changed to id_Handl
\* Parameter id of procedure HandleSync at line 413 col 29 changed to id_Handle
\* Parameter id of procedure HandleSyncAck at line 434 col 27 changed to id_HandleS
\* Parameter id of procedure ReplicaReceiveRequest at line 460 col 35 changed to id_Re
\* Parameter id of procedure NodeFail at line 487 col 22 changed to id_N
\* Parameter id of procedure CoordinatorRecover at line 525 col 32 changed to id_C
CONSTANT defaultInitValue
VARIABLES Replicas, Transactions, Messages, CoordinatorMessages, WriteLocks, 
          Logs, TransactionStatusCoor, TransactionStatus, CurrentTransaction, 
          SuccessCounts, FailureCounts, ReadResults, ReadConsistency, 
          DoneCounts, NodeStatus, FailedCounts, CommitCounts, CommitTS, 
          AbortCounts, SyncCounts, CurrentTime, SystemTerminated, 
          RecieveACKCount, pc, stack

(* define statement *)
IsWriteLockAvailable(item, replica) == WriteLocks[item][replica] = FALSE
Max(S) == CHOOSE x \in S : \A y \in S : x >= y
ConsistentFinishedTransactions ==
    \A tid \in 1..T :
      IF TransactionStatusCoor[Transactions[tid].coordinator_id][tid].status = "Finished"
        THEN
        LET dataItems == Transactions[tid].read \cup Transactions[tid].write IN
        \A item \in dataItems :
          LET committedReplicas == {r \in 1..M : TransactionStatus[((item-1)*M) + r][tid].status = "Committed"} IN
          LET abortedReplicas == {r \in 1..M : TransactionStatus[((item-1)*M) + r][tid].status = "Aborted"} IN
          (Cardinality(committedReplicas) > (M \div 2) /\ Cardinality(abortedReplicas) = 0) \/
          (Cardinality(abortedReplicas) > (M \div 2) /\ Cardinality(committedReplicas) = 0)
      ELSE TRUE
NoConflictingTransactionStatus ==
\A tid \in 1..T :
  LET allStatuses == {TransactionStatusCoor[c][tid].status: c \in (1+(N * M))..(C+(N * M))} \cup
                     {TransactionStatus[r][tid].status: r \in 1..(N * M)} IN
  ~("Committed" \in allStatuses /\ "Aborted" \in allStatuses)

VARIABLES cid_, t_, i1, j1, Test, targetReplica_, s_msg_, t, msg_Ca, cid_C, 
          cid_Co, quorom_, minority, t_C, i7, j7, commitTimestamp, 
          targetReplica_C, decision, allSuccessful, s_msg_C, cid_Coo, quorom, 
          t_Co, targetReplica, cid_Coor, msg_, type_, recoveryTid_, cid, 
          msg_C, type_C, recoveryTid, msg_Ha, id_H, i_, j_, t_H, 
          readTimestamp, readData, res_msg_, msg_Han, id_Ha, i_H, j_H, t_Ha, 
          res_msg_H, msg_Hand, id_Han, i_Ha, j_Ha, t_Han, res_msg_Ha, 
          readValue, readWTS, msg_Handl, id_Hand, i_Han, j_Han, t_Hand, idx, 
          res_msg_Han, msg_Handle, id_Handl, i_Hand, j_Hand, t_Handl, res_msg, 
          msg, id_Handle, i, j, id_HandleS, msg_H, i13, j13, id_Re, msg_R, 
          type_R, id_N, msg_N, type, id_C, s_msg_Co, i8, j8, id, s_msg, 
          dataItemId, id_, id_R

vars == << Replicas, Transactions, Messages, CoordinatorMessages, WriteLocks, 
           Logs, TransactionStatusCoor, TransactionStatus, CurrentTransaction, 
           SuccessCounts, FailureCounts, ReadResults, ReadConsistency, 
           DoneCounts, NodeStatus, FailedCounts, CommitCounts, CommitTS, 
           AbortCounts, SyncCounts, CurrentTime, SystemTerminated, 
           RecieveACKCount, pc, stack, cid_, t_, i1, j1, Test, targetReplica_, 
           s_msg_, t, msg_Ca, cid_C, cid_Co, quorom_, minority, t_C, i7, j7, 
           commitTimestamp, targetReplica_C, decision, allSuccessful, s_msg_C, 
           cid_Coo, quorom, t_Co, targetReplica, cid_Coor, msg_, type_, 
           recoveryTid_, cid, msg_C, type_C, recoveryTid, msg_Ha, id_H, i_, 
           j_, t_H, readTimestamp, readData, res_msg_, msg_Han, id_Ha, i_H, 
           j_H, t_Ha, res_msg_H, msg_Hand, id_Han, i_Ha, j_Ha, t_Han, 
           res_msg_Ha, readValue, readWTS, msg_Handl, id_Hand, i_Han, j_Han, 
           t_Hand, idx, res_msg_Han, msg_Handle, id_Handl, i_Hand, j_Hand, 
           t_Handl, res_msg, msg, id_Handle, i, j, id_HandleS, msg_H, i13, 
           j13, id_Re, msg_R, type_R, id_N, msg_N, type, id_C, s_msg_Co, i8, 
           j8, id, s_msg, dataItemId, id_, id_R >>

ProcSet == ((1+(N * M))..(C+(N * M))) \cup (1..(N * M))

Init == (* Global variables *)
        /\ Replicas = [item \in 1..N |-> [rep \in 1..M |-> {[value |-> 0, timestamp |-> 0]}]]
        /\ Transactions = [tid \in 1..T |->
                             LET readSet == {item \in 1..N : (item + tid) % 2 = 0} IN
                             LET writeSet == {item \in 1..N : item % 2 = 0} IN
                             [read |-> readSet, write |-> writeSet, StartTS |-> 0,
                              coordinator_id |-> CHOOSE c \in (1+(N * M))..(C+(N * M)) : (tid % C) = (c % C) ]]
        /\ Messages = {}
        /\ CoordinatorMessages = {}
        /\ WriteLocks = [item \in 1..N |-> [rep \in 1..M |-> FALSE]]
        /\ Logs = [rep \in 1..(N * M) |-> {}]
        /\ TransactionStatusCoor = [c \in (1+(N * M))..(C+(N * M)) |-> [tid \in 1..T |-> [status |-> "NotStarted", CommitTS |-> 0]]]
        /\ TransactionStatus = [rep \in 1..(N * M) |-> [tid \in 1..T |-> [status |-> "NotStarted", CommitTS |-> 0]]]
        /\ CurrentTransaction = [c \in (1+(N * M))..(C+(N * M)) |-> 0]
        /\ SuccessCounts = [c \in (1+(N * M))..(C+(N * M)) |-> [item \in 1..N |-> 0]]
        /\ FailureCounts = [c \in (1+(N * M))..(C+(N * M)) |-> [item \in 1..N |-> 0]]
        /\ ReadResults = [c \in (1+(N * M))..(C+(N * M))  |-> [tid \in 1..T |-> [item \in 1..N |-> {}]]]
        /\ ReadConsistency = [c \in (1+(N * M))..(C+(N * M))  |-> TRUE]
        /\ DoneCounts = [c \in (1+(N * M))..(C+(N * M)) |-> [item \in 1..N |-> 0]]
        /\ NodeStatus = [nid \in (1+(N * M))..(C+(N * M)) \cup (1..(N * M)) |-> "Active"]
        /\ FailedCounts = [nid \in 1..(C+(N*M)) |-> 0]
        /\ CommitCounts = [c \in (1+(N * M))..(C+(N * M)) |-> 0]
        /\ CommitTS = [c \in (1+(N * M))..(C+(N * M)) |-> 0]
        /\ AbortCounts = [c \in (1+(N * M))..(C+(N * M)) |-> 0]
        /\ SyncCounts = [item \in 1..(N * M) |-> 0]
        /\ CurrentTime = 1
        /\ SystemTerminated = FALSE
        /\ RecieveACKCount = [c \in (1+(N * M))..(C+(N * M)) |-> 0]
        (* Procedure SendTransactions *)
        /\ cid_ = [ self \in ProcSet |-> defaultInitValue]
        /\ t_ = [ self \in ProcSet |-> 0]
        /\ i1 = [ self \in ProcSet |-> 0]
        /\ j1 = [ self \in ProcSet |-> 0]
        /\ Test = [ self \in ProcSet |-> {}]
        /\ targetReplica_ = [ self \in ProcSet |-> 0]
        /\ s_msg_ = [ self \in ProcSet |-> {}]
        (* Procedure CalculateACKCounts *)
        /\ t = [ self \in ProcSet |-> defaultInitValue]
        /\ msg_Ca = [ self \in ProcSet |-> defaultInitValue]
        /\ cid_C = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure CoordinatorDecide *)
        /\ cid_Co = [ self \in ProcSet |-> defaultInitValue]
        /\ quorom_ = [ self \in ProcSet |-> defaultInitValue]
        /\ minority = [ self \in ProcSet |-> defaultInitValue]
        /\ t_C = [ self \in ProcSet |-> 0]
        /\ i7 = [ self \in ProcSet |-> 0]
        /\ j7 = [ self \in ProcSet |-> 0]
        /\ commitTimestamp = [ self \in ProcSet |-> 0]
        /\ targetReplica_C = [ self \in ProcSet |-> 0]
        /\ decision = [ self \in ProcSet |-> "Pending"]
        /\ allSuccessful = [ self \in ProcSet |-> TRUE]
        /\ s_msg_C = [ self \in ProcSet |-> {}]
        (* Procedure CoordinatorFinDecide *)
        /\ cid_Coo = [ self \in ProcSet |-> defaultInitValue]
        /\ quorom = [ self \in ProcSet |-> defaultInitValue]
        /\ t_Co = [ self \in ProcSet |-> 0]
        /\ targetReplica = [ self \in ProcSet |-> 0]
        (* Procedure CoordinatorHandleACKs *)
        /\ cid_Coor = [ self \in ProcSet |-> defaultInitValue]
        /\ msg_ = [ self \in ProcSet |-> {}]
        /\ type_ = [ self \in ProcSet |-> ""]
        /\ recoveryTid_ = [ self \in ProcSet |-> 0]
        (* Procedure CoordinatorHandleReports *)
        /\ cid = [ self \in ProcSet |-> defaultInitValue]
        /\ msg_C = [ self \in ProcSet |-> {}]
        /\ type_C = [ self \in ProcSet |-> ""]
        /\ recoveryTid = [ self \in ProcSet |-> 0]
        (* Procedure HandleRead *)
        /\ msg_Ha = [ self \in ProcSet |-> defaultInitValue]
        /\ id_H = [ self \in ProcSet |-> defaultInitValue]
        /\ i_ = [ self \in ProcSet |-> 0]
        /\ j_ = [ self \in ProcSet |-> 0]
        /\ t_H = [ self \in ProcSet |-> 0]
        /\ readTimestamp = [ self \in ProcSet |-> 0]
        /\ readData = [ self \in ProcSet |-> {}]
        /\ res_msg_ = [ self \in ProcSet |-> {}]
        (* Procedure HandleWrite *)
        /\ msg_Han = [ self \in ProcSet |-> defaultInitValue]
        /\ id_Ha = [ self \in ProcSet |-> defaultInitValue]
        /\ i_H = [ self \in ProcSet |-> 0]
        /\ j_H = [ self \in ProcSet |-> 0]
        /\ t_Ha = [ self \in ProcSet |-> 0]
        /\ res_msg_H = [ self \in ProcSet |-> {}]
        (* Procedure HandleInquire *)
        /\ msg_Hand = [ self \in ProcSet |-> defaultInitValue]
        /\ id_Han = [ self \in ProcSet |-> defaultInitValue]
        /\ i_Ha = [ self \in ProcSet |-> 0]
        /\ j_Ha = [ self \in ProcSet |-> 0]
        /\ t_Han = [ self \in ProcSet |-> 0]
        /\ res_msg_Ha = [ self \in ProcSet |-> {}]
        /\ readValue = [ self \in ProcSet |-> 0]
        /\ readWTS = [ self \in ProcSet |-> 0]
        (* Procedure HandleCommit *)
        /\ msg_Handl = [ self \in ProcSet |-> defaultInitValue]
        /\ id_Hand = [ self \in ProcSet |-> defaultInitValue]
        /\ i_Han = [ self \in ProcSet |-> 0]
        /\ j_Han = [ self \in ProcSet |-> 0]
        /\ t_Hand = [ self \in ProcSet |-> 0]
        /\ idx = [ self \in ProcSet |-> 0]
        /\ res_msg_Han = [ self \in ProcSet |-> {}]
        (* Procedure HandleAbort *)
        /\ msg_Handle = [ self \in ProcSet |-> defaultInitValue]
        /\ id_Handl = [ self \in ProcSet |-> defaultInitValue]
        /\ i_Hand = [ self \in ProcSet |-> 0]
        /\ j_Hand = [ self \in ProcSet |-> 0]
        /\ t_Handl = [ self \in ProcSet |-> 0]
        /\ res_msg = [ self \in ProcSet |-> {}]
        (* Procedure HandleSync *)
        /\ msg = [ self \in ProcSet |-> defaultInitValue]
        /\ id_Handle = [ self \in ProcSet |-> defaultInitValue]
        /\ i = [ self \in ProcSet |-> defaultInitValue]
        /\ j = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure HandleSyncAck *)
        /\ id_HandleS = [ self \in ProcSet |-> defaultInitValue]
        /\ msg_H = [ self \in ProcSet |-> {}]
        /\ i13 = [ self \in ProcSet |-> 0]
        /\ j13 = [ self \in ProcSet |-> 0]
        (* Procedure ReplicaReceiveRequest *)
        /\ id_Re = [ self \in ProcSet |-> defaultInitValue]
        /\ msg_R = [ self \in ProcSet |-> {}]
        /\ type_R = [ self \in ProcSet |-> ""]
        (* Procedure NodeFail *)
        /\ id_N = [ self \in ProcSet |-> defaultInitValue]
        /\ msg_N = [ self \in ProcSet |-> {}]
        /\ type = [ self \in ProcSet |-> ""]
        (* Procedure CoordinatorRecover *)
        /\ id_C = [ self \in ProcSet |-> defaultInitValue]
        /\ s_msg_Co = [ self \in ProcSet |-> {}]
        /\ i8 = [ self \in ProcSet |-> 0]
        /\ j8 = [ self \in ProcSet |-> 0]
        (* Procedure ReplicaRecover *)
        /\ id = [ self \in ProcSet |-> defaultInitValue]
        /\ s_msg = [ self \in ProcSet |-> {}]
        /\ dataItemId = [ self \in ProcSet |-> 0]
        (* Process Coordinator *)
        /\ id_ = [self \in (1+(N * M))..(C+(N * M)) |-> self]
        (* Process Replica *)
        /\ id_R = [self \in 1..(N * M) |-> self]
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self \in (1+(N * M))..(C+(N * M)) -> "CoordinatorMain"
                                        [] self \in 1..(N * M) -> "ReplicaMain"]

ProcSendTransaction(self) == /\ pc[self] = "ProcSendTransaction"
                             /\ IF CurrentTransaction[cid_[self]] = 0 /\
                                   \E tid \in 1..T: Transactions[tid].coordinator_id = cid_[self] /\ TransactionStatusCoor[cid_[self]][tid].status = "NotStarted"
                                   THEN /\ t_' = [t_ EXCEPT ![self] = CHOOSE tid \in 1..T: Transactions[tid].coordinator_id = cid_[self] /\ TransactionStatusCoor[cid_[self]][tid].status = "NotStarted"]
                                        /\ IF t_'[self] /= 0
                                              THEN /\ CurrentTransaction' = [CurrentTransaction EXCEPT ![cid_[self]] = t_'[self]]
                                                   /\ TransactionStatusCoor' = [TransactionStatusCoor EXCEPT ![cid_[self]][t_'[self]].status = "Pending"]
                                                   /\ Transactions' = [Transactions EXCEPT ![t_'[self]].StartTS = CurrentTime]
                                                   /\ pc' = [pc EXCEPT ![self] = "ProcSendTransactionsOuterLoop"]
                                              ELSE /\ pc' = [pc EXCEPT ![self] = "ProcSendTransactionB"]
                                                   /\ UNCHANGED << Transactions, 
                                                                   TransactionStatusCoor, 
                                                                   CurrentTransaction >>
                                   ELSE /\ pc' = [pc EXCEPT ![self] = "ProcSendTransactionB"]
                                        /\ UNCHANGED << Transactions, 
                                                        TransactionStatusCoor, 
                                                        CurrentTransaction, t_ >>
                             /\ UNCHANGED << Replicas, Messages, 
                                             CoordinatorMessages, WriteLocks, 
                                             Logs, TransactionStatus, 
                                             SuccessCounts, FailureCounts, 
                                             ReadResults, ReadConsistency, 
                                             DoneCounts, NodeStatus, 
                                             FailedCounts, CommitCounts, 
                                             CommitTS, AbortCounts, SyncCounts, 
                                             CurrentTime, SystemTerminated, 
                                             RecieveACKCount, stack, cid_, i1, 
                                             j1, Test, targetReplica_, s_msg_, 
                                             t, msg_Ca, cid_C, cid_Co, quorom_, 
                                             minority, t_C, i7, j7, 
                                             commitTimestamp, targetReplica_C, 
                                             decision, allSuccessful, s_msg_C, 
                                             cid_Coo, quorom, t_Co, 
                                             targetReplica, cid_Coor, msg_, 
                                             type_, recoveryTid_, cid, msg_C, 
                                             type_C, recoveryTid, msg_Ha, id_H, 
                                             i_, j_, t_H, readTimestamp, 
                                             readData, res_msg_, msg_Han, 
                                             id_Ha, i_H, j_H, t_Ha, res_msg_H, 
                                             msg_Hand, id_Han, i_Ha, j_Ha, 
                                             t_Han, res_msg_Ha, readValue, 
                                             readWTS, msg_Handl, id_Hand, 
                                             i_Han, j_Han, t_Hand, idx, 
                                             res_msg_Han, msg_Handle, id_Handl, 
                                             i_Hand, j_Hand, t_Handl, res_msg, 
                                             msg, id_Handle, i, j, id_HandleS, 
                                             msg_H, i13, j13, id_Re, msg_R, 
                                             type_R, id_N, msg_N, type, id_C, 
                                             s_msg_Co, i8, j8, id, s_msg, 
                                             dataItemId, id_, id_R >>

ProcSendTransactionsOuterLoop(self) == /\ pc[self] = "ProcSendTransactionsOuterLoop"
                                       /\ IF i1[self] < N
                                             THEN /\ i1' = [i1 EXCEPT ![self] = i1[self] + 1]
                                                  /\ IF i1'[self] \in Transactions[t_[self]].read \cup Transactions[t_[self]].write
                                                        THEN /\ j1' = [j1 EXCEPT ![self] = 0]
                                                             /\ pc' = [pc EXCEPT ![self] = "ProcSendTransactionsInnerLoop"]
                                                        ELSE /\ pc' = [pc EXCEPT ![self] = "ProcSendTransactionsOuterLoop"]
                                                             /\ j1' = j1
                                             ELSE /\ pc' = [pc EXCEPT ![self] = "ProcSendTransactionB"]
                                                  /\ UNCHANGED << i1, j1 >>
                                       /\ UNCHANGED << Replicas, Transactions, 
                                                       Messages, 
                                                       CoordinatorMessages, 
                                                       WriteLocks, Logs, 
                                                       TransactionStatusCoor, 
                                                       TransactionStatus, 
                                                       CurrentTransaction, 
                                                       SuccessCounts, 
                                                       FailureCounts, 
                                                       ReadResults, 
                                                       ReadConsistency, 
                                                       DoneCounts, NodeStatus, 
                                                       FailedCounts, 
                                                       CommitCounts, CommitTS, 
                                                       AbortCounts, SyncCounts, 
                                                       CurrentTime, 
                                                       SystemTerminated, 
                                                       RecieveACKCount, stack, 
                                                       cid_, t_, Test, 
                                                       targetReplica_, s_msg_, 
                                                       t, msg_Ca, cid_C, 
                                                       cid_Co, quorom_, 
                                                       minority, t_C, i7, j7, 
                                                       commitTimestamp, 
                                                       targetReplica_C, 
                                                       decision, allSuccessful, 
                                                       s_msg_C, cid_Coo, 
                                                       quorom, t_Co, 
                                                       targetReplica, cid_Coor, 
                                                       msg_, type_, 
                                                       recoveryTid_, cid, 
                                                       msg_C, type_C, 
                                                       recoveryTid, msg_Ha, 
                                                       id_H, i_, j_, t_H, 
                                                       readTimestamp, readData, 
                                                       res_msg_, msg_Han, 
                                                       id_Ha, i_H, j_H, t_Ha, 
                                                       res_msg_H, msg_Hand, 
                                                       id_Han, i_Ha, j_Ha, 
                                                       t_Han, res_msg_Ha, 
                                                       readValue, readWTS, 
                                                       msg_Handl, id_Hand, 
                                                       i_Han, j_Han, t_Hand, 
                                                       idx, res_msg_Han, 
                                                       msg_Handle, id_Handl, 
                                                       i_Hand, j_Hand, t_Handl, 
                                                       res_msg, msg, id_Handle, 
                                                       i, j, id_HandleS, msg_H, 
                                                       i13, j13, id_Re, msg_R, 
                                                       type_R, id_N, msg_N, 
                                                       type, id_C, s_msg_Co, 
                                                       i8, j8, id, s_msg, 
                                                       dataItemId, id_, id_R >>

ProcSendTransactionsInnerLoop(self) == /\ pc[self] = "ProcSendTransactionsInnerLoop"
                                       /\ IF j1[self] < M
                                             THEN /\ j1' = [j1 EXCEPT ![self] = j1[self] + 1]
                                                  /\ targetReplica_' = [targetReplica_ EXCEPT ![self] = ((i1[self] - 1) * M) + j1'[self]]
                                                  /\ s_msg_' = [s_msg_ EXCEPT ![self] = [item |-> i1[self],
                                                                                         replica |-> j1'[self],
                                                                                         transaction |-> CurrentTransaction[cid_[self]],
                                                                                         type |-> CHOOSE msgType \in {"Read", "Write"}:
                                                                                                 IF i1[self] \in Transactions[t_[self]].write THEN msgType = "Write"
                                                                                                 ELSE msgType = "Read",
                                                                                         coordinator_id |-> cid_[self],
                                                                                         sendTime |-> CurrentTime,
                                                                                         targetReplica |-> targetReplica_'[self]]]
                                                  /\ Messages' = (Messages \cup {s_msg_'[self]})
                                                  /\ pc' = [pc EXCEPT ![self] = "ProcSendTransactionsInnerLoop"]
                                             ELSE /\ pc' = [pc EXCEPT ![self] = "ProcSendTransactionsOuterLoop"]
                                                  /\ UNCHANGED << Messages, j1, 
                                                                  targetReplica_, 
                                                                  s_msg_ >>
                                       /\ UNCHANGED << Replicas, Transactions, 
                                                       CoordinatorMessages, 
                                                       WriteLocks, Logs, 
                                                       TransactionStatusCoor, 
                                                       TransactionStatus, 
                                                       CurrentTransaction, 
                                                       SuccessCounts, 
                                                       FailureCounts, 
                                                       ReadResults, 
                                                       ReadConsistency, 
                                                       DoneCounts, NodeStatus, 
                                                       FailedCounts, 
                                                       CommitCounts, CommitTS, 
                                                       AbortCounts, SyncCounts, 
                                                       CurrentTime, 
                                                       SystemTerminated, 
                                                       RecieveACKCount, stack, 
                                                       cid_, t_, i1, Test, t, 
                                                       msg_Ca, cid_C, cid_Co, 
                                                       quorom_, minority, t_C, 
                                                       i7, j7, commitTimestamp, 
                                                       targetReplica_C, 
                                                       decision, allSuccessful, 
                                                       s_msg_C, cid_Coo, 
                                                       quorom, t_Co, 
                                                       targetReplica, cid_Coor, 
                                                       msg_, type_, 
                                                       recoveryTid_, cid, 
                                                       msg_C, type_C, 
                                                       recoveryTid, msg_Ha, 
                                                       id_H, i_, j_, t_H, 
                                                       readTimestamp, readData, 
                                                       res_msg_, msg_Han, 
                                                       id_Ha, i_H, j_H, t_Ha, 
                                                       res_msg_H, msg_Hand, 
                                                       id_Han, i_Ha, j_Ha, 
                                                       t_Han, res_msg_Ha, 
                                                       readValue, readWTS, 
                                                       msg_Handl, id_Hand, 
                                                       i_Han, j_Han, t_Hand, 
                                                       idx, res_msg_Han, 
                                                       msg_Handle, id_Handl, 
                                                       i_Hand, j_Hand, t_Handl, 
                                                       res_msg, msg, id_Handle, 
                                                       i, j, id_HandleS, msg_H, 
                                                       i13, j13, id_Re, msg_R, 
                                                       type_R, id_N, msg_N, 
                                                       type, id_C, s_msg_Co, 
                                                       i8, j8, id, s_msg, 
                                                       dataItemId, id_, id_R >>

ProcSendTransactionB(self) == /\ pc[self] = "ProcSendTransactionB"
                              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                              /\ t_' = [t_ EXCEPT ![self] = Head(stack[self]).t_]
                              /\ i1' = [i1 EXCEPT ![self] = Head(stack[self]).i1]
                              /\ j1' = [j1 EXCEPT ![self] = Head(stack[self]).j1]
                              /\ Test' = [Test EXCEPT ![self] = Head(stack[self]).Test]
                              /\ targetReplica_' = [targetReplica_ EXCEPT ![self] = Head(stack[self]).targetReplica_]
                              /\ s_msg_' = [s_msg_ EXCEPT ![self] = Head(stack[self]).s_msg_]
                              /\ cid_' = [cid_ EXCEPT ![self] = Head(stack[self]).cid_]
                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                              /\ UNCHANGED << Replicas, Transactions, Messages, 
                                              CoordinatorMessages, WriteLocks, 
                                              Logs, TransactionStatusCoor, 
                                              TransactionStatus, 
                                              CurrentTransaction, 
                                              SuccessCounts, FailureCounts, 
                                              ReadResults, ReadConsistency, 
                                              DoneCounts, NodeStatus, 
                                              FailedCounts, CommitCounts, 
                                              CommitTS, AbortCounts, 
                                              SyncCounts, CurrentTime, 
                                              SystemTerminated, 
                                              RecieveACKCount, t, msg_Ca, 
                                              cid_C, cid_Co, quorom_, minority, 
                                              t_C, i7, j7, commitTimestamp, 
                                              targetReplica_C, decision, 
                                              allSuccessful, s_msg_C, cid_Coo, 
                                              quorom, t_Co, targetReplica, 
                                              cid_Coor, msg_, type_, 
                                              recoveryTid_, cid, msg_C, type_C, 
                                              recoveryTid, msg_Ha, id_H, i_, 
                                              j_, t_H, readTimestamp, readData, 
                                              res_msg_, msg_Han, id_Ha, i_H, 
                                              j_H, t_Ha, res_msg_H, msg_Hand, 
                                              id_Han, i_Ha, j_Ha, t_Han, 
                                              res_msg_Ha, readValue, readWTS, 
                                              msg_Handl, id_Hand, i_Han, j_Han, 
                                              t_Hand, idx, res_msg_Han, 
                                              msg_Handle, id_Handl, i_Hand, 
                                              j_Hand, t_Handl, res_msg, msg, 
                                              id_Handle, i, j, id_HandleS, 
                                              msg_H, i13, j13, id_Re, msg_R, 
                                              type_R, id_N, msg_N, type, id_C, 
                                              s_msg_Co, i8, j8, id, s_msg, 
                                              dataItemId, id_, id_R >>

SendTransactions(self) == ProcSendTransaction(self)
                             \/ ProcSendTransactionsOuterLoop(self)
                             \/ ProcSendTransactionsInnerLoop(self)
                             \/ ProcSendTransactionB(self)

ProcCalculateCounts(self) == /\ pc[self] = "ProcCalculateCounts"
                             /\ IF msg_Ca[self].status = "ReadSuccess"
                                   THEN /\ SuccessCounts' = [SuccessCounts EXCEPT ![cid_C[self]][msg_Ca[self].item] = SuccessCounts[cid_C[self]][msg_Ca[self].item] + 1]
                                        /\ ReadResults' = [ReadResults EXCEPT ![cid_C[self]][t[self]][msg_Ca[self].item] = ReadResults[cid_C[self]][t[self]][msg_Ca[self].item] \cup
                                                                                                                            {[replica |-> msg_Ca[self].replica, value |-> msg_Ca[self].value, timestamp |-> msg_Ca[self].wts]}]
                                        /\ IF ReadConsistency[cid_C[self]] /\ Cardinality(ReadResults'[cid_C[self]][t[self]][msg_Ca[self].item]) > 1
                                              THEN /\ ReadConsistency' = [ReadConsistency EXCEPT ![cid_C[self]] = \A r1, r2 \in ReadResults'[cid_C[self]][t[self]][msg_Ca[self].item]:
                                                                                                                        (r1.value = r2.value) /\ (r1.timestamp = r2.timestamp)]
                                              ELSE /\ TRUE
                                                   /\ UNCHANGED ReadConsistency
                                        /\ UNCHANGED FailureCounts
                                   ELSE /\ IF msg_Ca[self].status = "ReadFailed"
                                              THEN /\ FailureCounts' = [FailureCounts EXCEPT ![cid_C[self]][msg_Ca[self].item] = FailureCounts[cid_C[self]][msg_Ca[self].item] + 1]
                                                   /\ UNCHANGED SuccessCounts
                                              ELSE /\ IF msg_Ca[self].status = "WriteSuccess"
                                                         THEN /\ SuccessCounts' = [SuccessCounts EXCEPT ![cid_C[self]][msg_Ca[self].item] = SuccessCounts[cid_C[self]][msg_Ca[self].item] + 1]
                                                              /\ UNCHANGED FailureCounts
                                                         ELSE /\ IF msg_Ca[self].status = "WriteFailed"
                                                                    THEN /\ FailureCounts' = [FailureCounts EXCEPT ![cid_C[self]][msg_Ca[self].item] = FailureCounts[cid_C[self]][msg_Ca[self].item] + 1]
                                                                    ELSE /\ TRUE
                                                                         /\ UNCHANGED FailureCounts
                                                              /\ UNCHANGED SuccessCounts
                                        /\ UNCHANGED << ReadResults, 
                                                        ReadConsistency >>
                             /\ IF msg_Ca[self].type = "Report"
                                   THEN /\ IF msg_Ca[self].status = "Committed"
                                              THEN /\ CommitCounts' = [CommitCounts EXCEPT ![cid_C[self]] = CommitCounts[cid_C[self]] + 1]
                                                   /\ CommitTS' = [CommitTS EXCEPT ![cid_C[self]] = msg_Ca[self].CommitTS]
                                                   /\ UNCHANGED AbortCounts
                                              ELSE /\ IF msg_Ca[self].status = "Aborted"
                                                         THEN /\ AbortCounts' = [AbortCounts EXCEPT ![cid_C[self]] = AbortCounts[cid_C[self]] + 1]
                                                         ELSE /\ TRUE
                                                              /\ UNCHANGED AbortCounts
                                                   /\ UNCHANGED << CommitCounts, 
                                                                   CommitTS >>
                                   ELSE /\ TRUE
                                        /\ UNCHANGED << CommitCounts, CommitTS, 
                                                        AbortCounts >>
                             /\ pc' = [pc EXCEPT ![self] = "ProcCalculateCountsC"]
                             /\ UNCHANGED << Replicas, Transactions, Messages, 
                                             CoordinatorMessages, WriteLocks, 
                                             Logs, TransactionStatusCoor, 
                                             TransactionStatus, 
                                             CurrentTransaction, DoneCounts, 
                                             NodeStatus, FailedCounts, 
                                             SyncCounts, CurrentTime, 
                                             SystemTerminated, RecieveACKCount, 
                                             stack, cid_, t_, i1, j1, Test, 
                                             targetReplica_, s_msg_, t, msg_Ca, 
                                             cid_C, cid_Co, quorom_, minority, 
                                             t_C, i7, j7, commitTimestamp, 
                                             targetReplica_C, decision, 
                                             allSuccessful, s_msg_C, cid_Coo, 
                                             quorom, t_Co, targetReplica, 
                                             cid_Coor, msg_, type_, 
                                             recoveryTid_, cid, msg_C, type_C, 
                                             recoveryTid, msg_Ha, id_H, i_, j_, 
                                             t_H, readTimestamp, readData, 
                                             res_msg_, msg_Han, id_Ha, i_H, 
                                             j_H, t_Ha, res_msg_H, msg_Hand, 
                                             id_Han, i_Ha, j_Ha, t_Han, 
                                             res_msg_Ha, readValue, readWTS, 
                                             msg_Handl, id_Hand, i_Han, j_Han, 
                                             t_Hand, idx, res_msg_Han, 
                                             msg_Handle, id_Handl, i_Hand, 
                                             j_Hand, t_Handl, res_msg, msg, 
                                             id_Handle, i, j, id_HandleS, 
                                             msg_H, i13, j13, id_Re, msg_R, 
                                             type_R, id_N, msg_N, type, id_C, 
                                             s_msg_Co, i8, j8, id, s_msg, 
                                             dataItemId, id_, id_R >>

ProcCalculateCountsC(self) == /\ pc[self] = "ProcCalculateCountsC"
                              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                              /\ t' = [t EXCEPT ![self] = Head(stack[self]).t]
                              /\ msg_Ca' = [msg_Ca EXCEPT ![self] = Head(stack[self]).msg_Ca]
                              /\ cid_C' = [cid_C EXCEPT ![self] = Head(stack[self]).cid_C]
                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                              /\ UNCHANGED << Replicas, Transactions, Messages, 
                                              CoordinatorMessages, WriteLocks, 
                                              Logs, TransactionStatusCoor, 
                                              TransactionStatus, 
                                              CurrentTransaction, 
                                              SuccessCounts, FailureCounts, 
                                              ReadResults, ReadConsistency, 
                                              DoneCounts, NodeStatus, 
                                              FailedCounts, CommitCounts, 
                                              CommitTS, AbortCounts, 
                                              SyncCounts, CurrentTime, 
                                              SystemTerminated, 
                                              RecieveACKCount, cid_, t_, i1, 
                                              j1, Test, targetReplica_, s_msg_, 
                                              cid_Co, quorom_, minority, t_C, 
                                              i7, j7, commitTimestamp, 
                                              targetReplica_C, decision, 
                                              allSuccessful, s_msg_C, cid_Coo, 
                                              quorom, t_Co, targetReplica, 
                                              cid_Coor, msg_, type_, 
                                              recoveryTid_, cid, msg_C, type_C, 
                                              recoveryTid, msg_Ha, id_H, i_, 
                                              j_, t_H, readTimestamp, readData, 
                                              res_msg_, msg_Han, id_Ha, i_H, 
                                              j_H, t_Ha, res_msg_H, msg_Hand, 
                                              id_Han, i_Ha, j_Ha, t_Han, 
                                              res_msg_Ha, readValue, readWTS, 
                                              msg_Handl, id_Hand, i_Han, j_Han, 
                                              t_Hand, idx, res_msg_Han, 
                                              msg_Handle, id_Handl, i_Hand, 
                                              j_Hand, t_Handl, res_msg, msg, 
                                              id_Handle, i, j, id_HandleS, 
                                              msg_H, i13, j13, id_Re, msg_R, 
                                              type_R, id_N, msg_N, type, id_C, 
                                              s_msg_Co, i8, j8, id, s_msg, 
                                              dataItemId, id_, id_R >>

CalculateACKCounts(self) == ProcCalculateCounts(self)
                               \/ ProcCalculateCountsC(self)

ProcCoorDec(self) == /\ pc[self] = "ProcCoorDec"
                     /\ IF CurrentTransaction[cid_Co[self]] = 0
                           THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                /\ t_C' = [t_C EXCEPT ![self] = Head(stack[self]).t_C]
                                /\ i7' = [i7 EXCEPT ![self] = Head(stack[self]).i7]
                                /\ j7' = [j7 EXCEPT ![self] = Head(stack[self]).j7]
                                /\ commitTimestamp' = [commitTimestamp EXCEPT ![self] = Head(stack[self]).commitTimestamp]
                                /\ targetReplica_C' = [targetReplica_C EXCEPT ![self] = Head(stack[self]).targetReplica_C]
                                /\ decision' = [decision EXCEPT ![self] = Head(stack[self]).decision]
                                /\ allSuccessful' = [allSuccessful EXCEPT ![self] = Head(stack[self]).allSuccessful]
                                /\ s_msg_C' = [s_msg_C EXCEPT ![self] = Head(stack[self]).s_msg_C]
                                /\ cid_Co' = [cid_Co EXCEPT ![self] = Head(stack[self]).cid_Co]
                                /\ quorom_' = [quorom_ EXCEPT ![self] = Head(stack[self]).quorom_]
                                /\ minority' = [minority EXCEPT ![self] = Head(stack[self]).minority]
                                /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "ProcCoorDecA"]
                                /\ UNCHANGED << stack, cid_Co, quorom_, 
                                                minority, t_C, i7, j7, 
                                                commitTimestamp, 
                                                targetReplica_C, decision, 
                                                allSuccessful, s_msg_C >>
                     /\ UNCHANGED << Replicas, Transactions, Messages, 
                                     CoordinatorMessages, WriteLocks, Logs, 
                                     TransactionStatusCoor, TransactionStatus, 
                                     CurrentTransaction, SuccessCounts, 
                                     FailureCounts, ReadResults, 
                                     ReadConsistency, DoneCounts, NodeStatus, 
                                     FailedCounts, CommitCounts, CommitTS, 
                                     AbortCounts, SyncCounts, CurrentTime, 
                                     SystemTerminated, RecieveACKCount, cid_, 
                                     t_, i1, j1, Test, targetReplica_, s_msg_, 
                                     t, msg_Ca, cid_C, cid_Coo, quorom, t_Co, 
                                     targetReplica, cid_Coor, msg_, type_, 
                                     recoveryTid_, cid, msg_C, type_C, 
                                     recoveryTid, msg_Ha, id_H, i_, j_, t_H, 
                                     readTimestamp, readData, res_msg_, 
                                     msg_Han, id_Ha, i_H, j_H, t_Ha, res_msg_H, 
                                     msg_Hand, id_Han, i_Ha, j_Ha, t_Han, 
                                     res_msg_Ha, readValue, readWTS, msg_Handl, 
                                     id_Hand, i_Han, j_Han, t_Hand, idx, 
                                     res_msg_Han, msg_Handle, id_Handl, i_Hand, 
                                     j_Hand, t_Handl, res_msg, msg, id_Handle, 
                                     i, j, id_HandleS, msg_H, i13, j13, id_Re, 
                                     msg_R, type_R, id_N, msg_N, type, id_C, 
                                     s_msg_Co, i8, j8, id, s_msg, dataItemId, 
                                     id_, id_R >>

ProcCoorDecA(self) == /\ pc[self] = "ProcCoorDecA"
                      /\ t_C' = [t_C EXCEPT ![self] = CurrentTransaction[cid_Co[self]]]
                      /\ IF CommitCounts[cid_Co[self]] > 0
                            THEN /\ decision' = [decision EXCEPT ![self] = "Commit"]
                            ELSE /\ IF AbortCounts[cid_Co[self]] > 0 \/ ReadConsistency[cid_Co[self]] = FALSE
                                       THEN /\ decision' = [decision EXCEPT ![self] = "Abort"]
                                       ELSE /\ IF \E i5 \in Transactions[t_C'[self]].read \cup Transactions[t_C'[self]].write : FailureCounts[cid_Co[self]][i5] > minority[self]
                                                  THEN /\ decision' = [decision EXCEPT ![self] = "Abort"]
                                                  ELSE /\ IF \A i5 \in Transactions[t_C'[self]].read \cup Transactions[t_C'[self]].write : SuccessCounts[cid_Co[self]][i5] > quorom_[self]
                                                             THEN /\ decision' = [decision EXCEPT ![self] = "Commit"]
                                                             ELSE /\ TRUE
                                                                  /\ UNCHANGED decision
                      /\ IF decision'[self] = "Commit"
                            THEN /\ commitTimestamp' = [commitTimestamp EXCEPT ![self] = CurrentTime]
                                 /\ CurrentTime' = CurrentTime + 1
                                 /\ TransactionStatusCoor' = [TransactionStatusCoor EXCEPT ![cid_Co[self]][t_C'[self]] = [status |-> "Committed", CommitTS |-> commitTimestamp'[self]]]
                                 /\ IF NodeStatus[cid_Co[self]] = "Recover"
                                       THEN /\ NodeStatus' = [NodeStatus EXCEPT ![cid_Co[self]] = "Active"]
                                       ELSE /\ TRUE
                                            /\ UNCHANGED NodeStatus
                                 /\ pc' = [pc EXCEPT ![self] = "ProcHandleProcAckSendCommitOuterLoop"]
                            ELSE /\ IF decision'[self] = "Abort"
                                       THEN /\ TransactionStatusCoor' = [TransactionStatusCoor EXCEPT ![cid_Co[self]][t_C'[self]].status = "Aborted"]
                                            /\ IF NodeStatus[cid_Co[self]] = "Recover"
                                                  THEN /\ NodeStatus' = [NodeStatus EXCEPT ![cid_Co[self]] = "Active"]
                                                  ELSE /\ TRUE
                                                       /\ UNCHANGED NodeStatus
                                            /\ pc' = [pc EXCEPT ![self] = "ProcHandleProcAckSendAbortOuterLoop"]
                                       ELSE /\ pc' = [pc EXCEPT ![self] = "IfSuccess"]
                                            /\ UNCHANGED << TransactionStatusCoor, 
                                                            NodeStatus >>
                                 /\ UNCHANGED << CurrentTime, commitTimestamp >>
                      /\ UNCHANGED << Replicas, Transactions, Messages, 
                                      CoordinatorMessages, WriteLocks, Logs, 
                                      TransactionStatus, CurrentTransaction, 
                                      SuccessCounts, FailureCounts, 
                                      ReadResults, ReadConsistency, DoneCounts, 
                                      FailedCounts, CommitCounts, CommitTS, 
                                      AbortCounts, SyncCounts, 
                                      SystemTerminated, RecieveACKCount, stack, 
                                      cid_, t_, i1, j1, Test, targetReplica_, 
                                      s_msg_, t, msg_Ca, cid_C, cid_Co, 
                                      quorom_, minority, i7, j7, 
                                      targetReplica_C, allSuccessful, s_msg_C, 
                                      cid_Coo, quorom, t_Co, targetReplica, 
                                      cid_Coor, msg_, type_, recoveryTid_, cid, 
                                      msg_C, type_C, recoveryTid, msg_Ha, id_H, 
                                      i_, j_, t_H, readTimestamp, readData, 
                                      res_msg_, msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                      res_msg_H, msg_Hand, id_Han, i_Ha, j_Ha, 
                                      t_Han, res_msg_Ha, readValue, readWTS, 
                                      msg_Handl, id_Hand, i_Han, j_Han, t_Hand, 
                                      idx, res_msg_Han, msg_Handle, id_Handl, 
                                      i_Hand, j_Hand, t_Handl, res_msg, msg, 
                                      id_Handle, i, j, id_HandleS, msg_H, i13, 
                                      j13, id_Re, msg_R, type_R, id_N, msg_N, 
                                      type, id_C, s_msg_Co, i8, j8, id, s_msg, 
                                      dataItemId, id_, id_R >>

ProcHandleProcAckSendCommitOuterLoop(self) == /\ pc[self] = "ProcHandleProcAckSendCommitOuterLoop"
                                              /\ IF i7[self] < N
                                                    THEN /\ i7' = [i7 EXCEPT ![self] = i7[self] + 1]
                                                         /\ IF i7'[self] \in Transactions[t_C[self]].read \cup Transactions[t_C[self]].write
                                                               THEN /\ j7' = [j7 EXCEPT ![self] = 0]
                                                                    /\ pc' = [pc EXCEPT ![self] = "ProcHandleProcAckSendCommitInnerLoop"]
                                                               ELSE /\ pc' = [pc EXCEPT ![self] = "ProcHandleProcAckSendCommitOuterLoop"]
                                                                    /\ j7' = j7
                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "IfSuccess"]
                                                         /\ UNCHANGED << i7, 
                                                                         j7 >>
                                              /\ UNCHANGED << Replicas, 
                                                              Transactions, 
                                                              Messages, 
                                                              CoordinatorMessages, 
                                                              WriteLocks, Logs, 
                                                              TransactionStatusCoor, 
                                                              TransactionStatus, 
                                                              CurrentTransaction, 
                                                              SuccessCounts, 
                                                              FailureCounts, 
                                                              ReadResults, 
                                                              ReadConsistency, 
                                                              DoneCounts, 
                                                              NodeStatus, 
                                                              FailedCounts, 
                                                              CommitCounts, 
                                                              CommitTS, 
                                                              AbortCounts, 
                                                              SyncCounts, 
                                                              CurrentTime, 
                                                              SystemTerminated, 
                                                              RecieveACKCount, 
                                                              stack, cid_, t_, 
                                                              i1, j1, Test, 
                                                              targetReplica_, 
                                                              s_msg_, t, 
                                                              msg_Ca, cid_C, 
                                                              cid_Co, quorom_, 
                                                              minority, t_C, 
                                                              commitTimestamp, 
                                                              targetReplica_C, 
                                                              decision, 
                                                              allSuccessful, 
                                                              s_msg_C, cid_Coo, 
                                                              quorom, t_Co, 
                                                              targetReplica, 
                                                              cid_Coor, msg_, 
                                                              type_, 
                                                              recoveryTid_, 
                                                              cid, msg_C, 
                                                              type_C, 
                                                              recoveryTid, 
                                                              msg_Ha, id_H, i_, 
                                                              j_, t_H, 
                                                              readTimestamp, 
                                                              readData, 
                                                              res_msg_, 
                                                              msg_Han, id_Ha, 
                                                              i_H, j_H, t_Ha, 
                                                              res_msg_H, 
                                                              msg_Hand, id_Han, 
                                                              i_Ha, j_Ha, 
                                                              t_Han, 
                                                              res_msg_Ha, 
                                                              readValue, 
                                                              readWTS, 
                                                              msg_Handl, 
                                                              id_Hand, i_Han, 
                                                              j_Han, t_Hand, 
                                                              idx, res_msg_Han, 
                                                              msg_Handle, 
                                                              id_Handl, i_Hand, 
                                                              j_Hand, t_Handl, 
                                                              res_msg, msg, 
                                                              id_Handle, i, j, 
                                                              id_HandleS, 
                                                              msg_H, i13, j13, 
                                                              id_Re, msg_R, 
                                                              type_R, id_N, 
                                                              msg_N, type, 
                                                              id_C, s_msg_Co, 
                                                              i8, j8, id, 
                                                              s_msg, 
                                                              dataItemId, id_, 
                                                              id_R >>

ProcHandleProcAckSendCommitInnerLoop(self) == /\ pc[self] = "ProcHandleProcAckSendCommitInnerLoop"
                                              /\ IF j7[self] < M
                                                    THEN /\ j7' = [j7 EXCEPT ![self] = j7[self] + 1]
                                                         /\ targetReplica_C' = [targetReplica_C EXCEPT ![self] = ((i7[self] - 1) * M) + j7'[self]]
                                                         /\ s_msg_C' = [s_msg_C EXCEPT ![self] = [item |-> i7[self], replica |-> j7'[self], transaction |-> t_C[self], type |-> "Commit", timestamp |-> commitTimestamp[self], coordinator_id |-> Transactions[t_C[self]].coordinator_id, sendTime |-> CurrentTime, targetReplica |-> targetReplica_C'[self]]]
                                                         /\ Messages' = (Messages \cup {s_msg_C'[self]})
                                                         /\ pc' = [pc EXCEPT ![self] = "ProcHandleProcAckSendCommitInnerLoop"]
                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "ProcHandleProcAckSendCommitOuterLoop"]
                                                         /\ UNCHANGED << Messages, 
                                                                         j7, 
                                                                         targetReplica_C, 
                                                                         s_msg_C >>
                                              /\ UNCHANGED << Replicas, 
                                                              Transactions, 
                                                              CoordinatorMessages, 
                                                              WriteLocks, Logs, 
                                                              TransactionStatusCoor, 
                                                              TransactionStatus, 
                                                              CurrentTransaction, 
                                                              SuccessCounts, 
                                                              FailureCounts, 
                                                              ReadResults, 
                                                              ReadConsistency, 
                                                              DoneCounts, 
                                                              NodeStatus, 
                                                              FailedCounts, 
                                                              CommitCounts, 
                                                              CommitTS, 
                                                              AbortCounts, 
                                                              SyncCounts, 
                                                              CurrentTime, 
                                                              SystemTerminated, 
                                                              RecieveACKCount, 
                                                              stack, cid_, t_, 
                                                              i1, j1, Test, 
                                                              targetReplica_, 
                                                              s_msg_, t, 
                                                              msg_Ca, cid_C, 
                                                              cid_Co, quorom_, 
                                                              minority, t_C, 
                                                              i7, 
                                                              commitTimestamp, 
                                                              decision, 
                                                              allSuccessful, 
                                                              cid_Coo, quorom, 
                                                              t_Co, 
                                                              targetReplica, 
                                                              cid_Coor, msg_, 
                                                              type_, 
                                                              recoveryTid_, 
                                                              cid, msg_C, 
                                                              type_C, 
                                                              recoveryTid, 
                                                              msg_Ha, id_H, i_, 
                                                              j_, t_H, 
                                                              readTimestamp, 
                                                              readData, 
                                                              res_msg_, 
                                                              msg_Han, id_Ha, 
                                                              i_H, j_H, t_Ha, 
                                                              res_msg_H, 
                                                              msg_Hand, id_Han, 
                                                              i_Ha, j_Ha, 
                                                              t_Han, 
                                                              res_msg_Ha, 
                                                              readValue, 
                                                              readWTS, 
                                                              msg_Handl, 
                                                              id_Hand, i_Han, 
                                                              j_Han, t_Hand, 
                                                              idx, res_msg_Han, 
                                                              msg_Handle, 
                                                              id_Handl, i_Hand, 
                                                              j_Hand, t_Handl, 
                                                              res_msg, msg, 
                                                              id_Handle, i, j, 
                                                              id_HandleS, 
                                                              msg_H, i13, j13, 
                                                              id_Re, msg_R, 
                                                              type_R, id_N, 
                                                              msg_N, type, 
                                                              id_C, s_msg_Co, 
                                                              i8, j8, id, 
                                                              s_msg, 
                                                              dataItemId, id_, 
                                                              id_R >>

ProcHandleProcAckSendAbortOuterLoop(self) == /\ pc[self] = "ProcHandleProcAckSendAbortOuterLoop"
                                             /\ IF i7[self] < N
                                                   THEN /\ i7' = [i7 EXCEPT ![self] = i7[self] + 1]
                                                        /\ IF i7'[self] \in Transactions[t_C[self]].read \cup Transactions[t_C[self]].write
                                                              THEN /\ j7' = [j7 EXCEPT ![self] = 0]
                                                                   /\ pc' = [pc EXCEPT ![self] = "ProcHandleProcAckSendAbortInnerLoop"]
                                                              ELSE /\ pc' = [pc EXCEPT ![self] = "ProcHandleProcAckSendAbortOuterLoop"]
                                                                   /\ j7' = j7
                                                   ELSE /\ pc' = [pc EXCEPT ![self] = "IfSuccess"]
                                                        /\ UNCHANGED << i7, j7 >>
                                             /\ UNCHANGED << Replicas, 
                                                             Transactions, 
                                                             Messages, 
                                                             CoordinatorMessages, 
                                                             WriteLocks, Logs, 
                                                             TransactionStatusCoor, 
                                                             TransactionStatus, 
                                                             CurrentTransaction, 
                                                             SuccessCounts, 
                                                             FailureCounts, 
                                                             ReadResults, 
                                                             ReadConsistency, 
                                                             DoneCounts, 
                                                             NodeStatus, 
                                                             FailedCounts, 
                                                             CommitCounts, 
                                                             CommitTS, 
                                                             AbortCounts, 
                                                             SyncCounts, 
                                                             CurrentTime, 
                                                             SystemTerminated, 
                                                             RecieveACKCount, 
                                                             stack, cid_, t_, 
                                                             i1, j1, Test, 
                                                             targetReplica_, 
                                                             s_msg_, t, msg_Ca, 
                                                             cid_C, cid_Co, 
                                                             quorom_, minority, 
                                                             t_C, 
                                                             commitTimestamp, 
                                                             targetReplica_C, 
                                                             decision, 
                                                             allSuccessful, 
                                                             s_msg_C, cid_Coo, 
                                                             quorom, t_Co, 
                                                             targetReplica, 
                                                             cid_Coor, msg_, 
                                                             type_, 
                                                             recoveryTid_, cid, 
                                                             msg_C, type_C, 
                                                             recoveryTid, 
                                                             msg_Ha, id_H, i_, 
                                                             j_, t_H, 
                                                             readTimestamp, 
                                                             readData, 
                                                             res_msg_, msg_Han, 
                                                             id_Ha, i_H, j_H, 
                                                             t_Ha, res_msg_H, 
                                                             msg_Hand, id_Han, 
                                                             i_Ha, j_Ha, t_Han, 
                                                             res_msg_Ha, 
                                                             readValue, 
                                                             readWTS, 
                                                             msg_Handl, 
                                                             id_Hand, i_Han, 
                                                             j_Han, t_Hand, 
                                                             idx, res_msg_Han, 
                                                             msg_Handle, 
                                                             id_Handl, i_Hand, 
                                                             j_Hand, t_Handl, 
                                                             res_msg, msg, 
                                                             id_Handle, i, j, 
                                                             id_HandleS, msg_H, 
                                                             i13, j13, id_Re, 
                                                             msg_R, type_R, 
                                                             id_N, msg_N, type, 
                                                             id_C, s_msg_Co, 
                                                             i8, j8, id, s_msg, 
                                                             dataItemId, id_, 
                                                             id_R >>

ProcHandleProcAckSendAbortInnerLoop(self) == /\ pc[self] = "ProcHandleProcAckSendAbortInnerLoop"
                                             /\ IF j7[self] < M
                                                   THEN /\ j7' = [j7 EXCEPT ![self] = j7[self] + 1]
                                                        /\ targetReplica_C' = [targetReplica_C EXCEPT ![self] = ((i7[self] - 1) * M) + j7'[self]]
                                                        /\ s_msg_C' = [s_msg_C EXCEPT ![self] = [item |-> i7[self], replica |-> j7'[self], transaction |-> t_C[self], type |-> "Abort", timestamp |-> commitTimestamp[self], coordinator_id |-> Transactions[t_C[self]].coordinator_id, sendTime |-> CurrentTime, targetReplica |-> targetReplica_C'[self]]]
                                                        /\ Messages' = (Messages \cup {s_msg_C'[self]})
                                                        /\ pc' = [pc EXCEPT ![self] = "ProcHandleProcAckSendAbortInnerLoop"]
                                                   ELSE /\ pc' = [pc EXCEPT ![self] = "ProcHandleProcAckSendAbortOuterLoop"]
                                                        /\ UNCHANGED << Messages, 
                                                                        j7, 
                                                                        targetReplica_C, 
                                                                        s_msg_C >>
                                             /\ UNCHANGED << Replicas, 
                                                             Transactions, 
                                                             CoordinatorMessages, 
                                                             WriteLocks, Logs, 
                                                             TransactionStatusCoor, 
                                                             TransactionStatus, 
                                                             CurrentTransaction, 
                                                             SuccessCounts, 
                                                             FailureCounts, 
                                                             ReadResults, 
                                                             ReadConsistency, 
                                                             DoneCounts, 
                                                             NodeStatus, 
                                                             FailedCounts, 
                                                             CommitCounts, 
                                                             CommitTS, 
                                                             AbortCounts, 
                                                             SyncCounts, 
                                                             CurrentTime, 
                                                             SystemTerminated, 
                                                             RecieveACKCount, 
                                                             stack, cid_, t_, 
                                                             i1, j1, Test, 
                                                             targetReplica_, 
                                                             s_msg_, t, msg_Ca, 
                                                             cid_C, cid_Co, 
                                                             quorom_, minority, 
                                                             t_C, i7, 
                                                             commitTimestamp, 
                                                             decision, 
                                                             allSuccessful, 
                                                             cid_Coo, quorom, 
                                                             t_Co, 
                                                             targetReplica, 
                                                             cid_Coor, msg_, 
                                                             type_, 
                                                             recoveryTid_, cid, 
                                                             msg_C, type_C, 
                                                             recoveryTid, 
                                                             msg_Ha, id_H, i_, 
                                                             j_, t_H, 
                                                             readTimestamp, 
                                                             readData, 
                                                             res_msg_, msg_Han, 
                                                             id_Ha, i_H, j_H, 
                                                             t_Ha, res_msg_H, 
                                                             msg_Hand, id_Han, 
                                                             i_Ha, j_Ha, t_Han, 
                                                             res_msg_Ha, 
                                                             readValue, 
                                                             readWTS, 
                                                             msg_Handl, 
                                                             id_Hand, i_Han, 
                                                             j_Han, t_Hand, 
                                                             idx, res_msg_Han, 
                                                             msg_Handle, 
                                                             id_Handl, i_Hand, 
                                                             j_Hand, t_Handl, 
                                                             res_msg, msg, 
                                                             id_Handle, i, j, 
                                                             id_HandleS, msg_H, 
                                                             i13, j13, id_Re, 
                                                             msg_R, type_R, 
                                                             id_N, msg_N, type, 
                                                             id_C, s_msg_Co, 
                                                             i8, j8, id, s_msg, 
                                                             dataItemId, id_, 
                                                             id_R >>

IfSuccess(self) == /\ pc[self] = "IfSuccess"
                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                   /\ t_C' = [t_C EXCEPT ![self] = Head(stack[self]).t_C]
                   /\ i7' = [i7 EXCEPT ![self] = Head(stack[self]).i7]
                   /\ j7' = [j7 EXCEPT ![self] = Head(stack[self]).j7]
                   /\ commitTimestamp' = [commitTimestamp EXCEPT ![self] = Head(stack[self]).commitTimestamp]
                   /\ targetReplica_C' = [targetReplica_C EXCEPT ![self] = Head(stack[self]).targetReplica_C]
                   /\ decision' = [decision EXCEPT ![self] = Head(stack[self]).decision]
                   /\ allSuccessful' = [allSuccessful EXCEPT ![self] = Head(stack[self]).allSuccessful]
                   /\ s_msg_C' = [s_msg_C EXCEPT ![self] = Head(stack[self]).s_msg_C]
                   /\ cid_Co' = [cid_Co EXCEPT ![self] = Head(stack[self]).cid_Co]
                   /\ quorom_' = [quorom_ EXCEPT ![self] = Head(stack[self]).quorom_]
                   /\ minority' = [minority EXCEPT ![self] = Head(stack[self]).minority]
                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   /\ UNCHANGED << Replicas, Transactions, Messages, 
                                   CoordinatorMessages, WriteLocks, Logs, 
                                   TransactionStatusCoor, TransactionStatus, 
                                   CurrentTransaction, SuccessCounts, 
                                   FailureCounts, ReadResults, ReadConsistency, 
                                   DoneCounts, NodeStatus, FailedCounts, 
                                   CommitCounts, CommitTS, AbortCounts, 
                                   SyncCounts, CurrentTime, SystemTerminated, 
                                   RecieveACKCount, cid_, t_, i1, j1, Test, 
                                   targetReplica_, s_msg_, t, msg_Ca, cid_C, 
                                   cid_Coo, quorom, t_Co, targetReplica, 
                                   cid_Coor, msg_, type_, recoveryTid_, cid, 
                                   msg_C, type_C, recoveryTid, msg_Ha, id_H, 
                                   i_, j_, t_H, readTimestamp, readData, 
                                   res_msg_, msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                   res_msg_H, msg_Hand, id_Han, i_Ha, j_Ha, 
                                   t_Han, res_msg_Ha, readValue, readWTS, 
                                   msg_Handl, id_Hand, i_Han, j_Han, t_Hand, 
                                   idx, res_msg_Han, msg_Handle, id_Handl, 
                                   i_Hand, j_Hand, t_Handl, res_msg, msg, 
                                   id_Handle, i, j, id_HandleS, msg_H, i13, 
                                   j13, id_Re, msg_R, type_R, id_N, msg_N, 
                                   type, id_C, s_msg_Co, i8, j8, id, s_msg, 
                                   dataItemId, id_, id_R >>

CoordinatorDecide(self) == ProcCoorDec(self) \/ ProcCoorDecA(self)
                              \/ ProcHandleProcAckSendCommitOuterLoop(self)
                              \/ ProcHandleProcAckSendCommitInnerLoop(self)
                              \/ ProcHandleProcAckSendAbortOuterLoop(self)
                              \/ ProcHandleProcAckSendAbortInnerLoop(self)
                              \/ IfSuccess(self)

ProcCoorFinDecide(self) == /\ pc[self] = "ProcCoorFinDecide"
                           /\ IF CurrentTransaction[cid_Coo[self]] = 0
                                 THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                      /\ t_Co' = [t_Co EXCEPT ![self] = Head(stack[self]).t_Co]
                                      /\ targetReplica' = [targetReplica EXCEPT ![self] = Head(stack[self]).targetReplica]
                                      /\ cid_Coo' = [cid_Coo EXCEPT ![self] = Head(stack[self]).cid_Coo]
                                      /\ quorom' = [quorom EXCEPT ![self] = Head(stack[self]).quorom]
                                      /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                 ELSE /\ pc' = [pc EXCEPT ![self] = "ProcCoorFinDecideB"]
                                      /\ UNCHANGED << stack, cid_Coo, quorom, 
                                                      t_Co, targetReplica >>
                           /\ UNCHANGED << Replicas, Transactions, Messages, 
                                           CoordinatorMessages, WriteLocks, 
                                           Logs, TransactionStatusCoor, 
                                           TransactionStatus, 
                                           CurrentTransaction, SuccessCounts, 
                                           FailureCounts, ReadResults, 
                                           ReadConsistency, DoneCounts, 
                                           NodeStatus, FailedCounts, 
                                           CommitCounts, CommitTS, AbortCounts, 
                                           SyncCounts, CurrentTime, 
                                           SystemTerminated, RecieveACKCount, 
                                           cid_, t_, i1, j1, Test, 
                                           targetReplica_, s_msg_, t, msg_Ca, 
                                           cid_C, cid_Co, quorom_, minority, 
                                           t_C, i7, j7, commitTimestamp, 
                                           targetReplica_C, decision, 
                                           allSuccessful, s_msg_C, cid_Coor, 
                                           msg_, type_, recoveryTid_, cid, 
                                           msg_C, type_C, recoveryTid, msg_Ha, 
                                           id_H, i_, j_, t_H, readTimestamp, 
                                           readData, res_msg_, msg_Han, id_Ha, 
                                           i_H, j_H, t_Ha, res_msg_H, msg_Hand, 
                                           id_Han, i_Ha, j_Ha, t_Han, 
                                           res_msg_Ha, readValue, readWTS, 
                                           msg_Handl, id_Hand, i_Han, j_Han, 
                                           t_Hand, idx, res_msg_Han, 
                                           msg_Handle, id_Handl, i_Hand, 
                                           j_Hand, t_Handl, res_msg, msg, 
                                           id_Handle, i, j, id_HandleS, msg_H, 
                                           i13, j13, id_Re, msg_R, type_R, 
                                           id_N, msg_N, type, id_C, s_msg_Co, 
                                           i8, j8, id, s_msg, dataItemId, id_, 
                                           id_R >>

ProcCoorFinDecideB(self) == /\ pc[self] = "ProcCoorFinDecideB"
                            /\ t_Co' = [t_Co EXCEPT ![self] = CurrentTransaction[cid_Coo[self]]]
                            /\ IF \A i6 \in Transactions[t_Co'[self]].read \cup Transactions[t_Co'[self]].write : DoneCounts[cid_Coo[self]][i6] > quorom[self]
                                  THEN /\ TransactionStatusCoor' = [TransactionStatusCoor EXCEPT ![cid_Coo[self]][t_Co'[self]].status = "Finished"]
                                       /\ CurrentTransaction' = [CurrentTransaction EXCEPT ![cid_Coo[self]] = 0]
                                       /\ SuccessCounts' = [SuccessCounts EXCEPT ![cid_Coo[self]] = [item \in 1..N |-> 0]]
                                       /\ FailureCounts' = [FailureCounts EXCEPT ![cid_Coo[self]] = [item \in 1..N |-> 0]]
                                       /\ ReadResults' = [ReadResults EXCEPT ![cid_Coo[self]] = [tid \in 1..T |-> [item \in 1..N |-> {}]]]
                                       /\ ReadConsistency' = [ReadConsistency EXCEPT ![cid_Coo[self]] = TRUE]
                                       /\ DoneCounts' = [DoneCounts EXCEPT ![cid_Coo[self]] = [item \in 1..N |-> 0]]
                                       /\ CommitCounts' = [CommitCounts EXCEPT ![cid_Coo[self]] = 0]
                                       /\ CommitTS' = [CommitTS EXCEPT ![cid_Coo[self]] = 0]
                                       /\ AbortCounts' = [AbortCounts EXCEPT ![cid_Coo[self]] = 0]
                                  ELSE /\ TRUE
                                       /\ UNCHANGED << TransactionStatusCoor, 
                                                       CurrentTransaction, 
                                                       SuccessCounts, 
                                                       FailureCounts, 
                                                       ReadResults, 
                                                       ReadConsistency, 
                                                       DoneCounts, 
                                                       CommitCounts, CommitTS, 
                                                       AbortCounts >>
                            /\ pc' = [pc EXCEPT ![self] = "ProcCoorFinDecideC"]
                            /\ UNCHANGED << Replicas, Transactions, Messages, 
                                            CoordinatorMessages, WriteLocks, 
                                            Logs, TransactionStatus, 
                                            NodeStatus, FailedCounts, 
                                            SyncCounts, CurrentTime, 
                                            SystemTerminated, RecieveACKCount, 
                                            stack, cid_, t_, i1, j1, Test, 
                                            targetReplica_, s_msg_, t, msg_Ca, 
                                            cid_C, cid_Co, quorom_, minority, 
                                            t_C, i7, j7, commitTimestamp, 
                                            targetReplica_C, decision, 
                                            allSuccessful, s_msg_C, cid_Coo, 
                                            quorom, targetReplica, cid_Coor, 
                                            msg_, type_, recoveryTid_, cid, 
                                            msg_C, type_C, recoveryTid, msg_Ha, 
                                            id_H, i_, j_, t_H, readTimestamp, 
                                            readData, res_msg_, msg_Han, id_Ha, 
                                            i_H, j_H, t_Ha, res_msg_H, 
                                            msg_Hand, id_Han, i_Ha, j_Ha, 
                                            t_Han, res_msg_Ha, readValue, 
                                            readWTS, msg_Handl, id_Hand, i_Han, 
                                            j_Han, t_Hand, idx, res_msg_Han, 
                                            msg_Handle, id_Handl, i_Hand, 
                                            j_Hand, t_Handl, res_msg, msg, 
                                            id_Handle, i, j, id_HandleS, msg_H, 
                                            i13, j13, id_Re, msg_R, type_R, 
                                            id_N, msg_N, type, id_C, s_msg_Co, 
                                            i8, j8, id, s_msg, dataItemId, id_, 
                                            id_R >>

ProcCoorFinDecideC(self) == /\ pc[self] = "ProcCoorFinDecideC"
                            /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                            /\ t_Co' = [t_Co EXCEPT ![self] = Head(stack[self]).t_Co]
                            /\ targetReplica' = [targetReplica EXCEPT ![self] = Head(stack[self]).targetReplica]
                            /\ cid_Coo' = [cid_Coo EXCEPT ![self] = Head(stack[self]).cid_Coo]
                            /\ quorom' = [quorom EXCEPT ![self] = Head(stack[self]).quorom]
                            /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                            /\ UNCHANGED << Replicas, Transactions, Messages, 
                                            CoordinatorMessages, WriteLocks, 
                                            Logs, TransactionStatusCoor, 
                                            TransactionStatus, 
                                            CurrentTransaction, SuccessCounts, 
                                            FailureCounts, ReadResults, 
                                            ReadConsistency, DoneCounts, 
                                            NodeStatus, FailedCounts, 
                                            CommitCounts, CommitTS, 
                                            AbortCounts, SyncCounts, 
                                            CurrentTime, SystemTerminated, 
                                            RecieveACKCount, cid_, t_, i1, j1, 
                                            Test, targetReplica_, s_msg_, t, 
                                            msg_Ca, cid_C, cid_Co, quorom_, 
                                            minority, t_C, i7, j7, 
                                            commitTimestamp, targetReplica_C, 
                                            decision, allSuccessful, s_msg_C, 
                                            cid_Coor, msg_, type_, 
                                            recoveryTid_, cid, msg_C, type_C, 
                                            recoveryTid, msg_Ha, id_H, i_, j_, 
                                            t_H, readTimestamp, readData, 
                                            res_msg_, msg_Han, id_Ha, i_H, j_H, 
                                            t_Ha, res_msg_H, msg_Hand, id_Han, 
                                            i_Ha, j_Ha, t_Han, res_msg_Ha, 
                                            readValue, readWTS, msg_Handl, 
                                            id_Hand, i_Han, j_Han, t_Hand, idx, 
                                            res_msg_Han, msg_Handle, id_Handl, 
                                            i_Hand, j_Hand, t_Handl, res_msg, 
                                            msg, id_Handle, i, j, id_HandleS, 
                                            msg_H, i13, j13, id_Re, msg_R, 
                                            type_R, id_N, msg_N, type, id_C, 
                                            s_msg_Co, i8, j8, id, s_msg, 
                                            dataItemId, id_, id_R >>

CoordinatorFinDecide(self) == ProcCoorFinDecide(self)
                                 \/ ProcCoorFinDecideB(self)
                                 \/ ProcCoorFinDecideC(self)

ProcCoorHandleACK(self) == /\ pc[self] = "ProcCoorHandleACK"
                           /\ IF \E m \in CoordinatorMessages : m.coordinator_id = cid_Coor[self] /\ m.type /= "Report"
                                 THEN /\ msg_' = [msg_ EXCEPT ![self] = CHOOSE m \in CoordinatorMessages : m.coordinator_id = cid_Coor[self] /\ m.type /= "Report"]
                                      /\ CoordinatorMessages' = CoordinatorMessages \ {msg_'[self]}
                                      /\ RecieveACKCount' = [RecieveACKCount EXCEPT ![cid_Coor[self]] = RecieveACKCount[cid_Coor[self]] + 1]
                                      /\ IF CurrentTransaction[cid_Coor[self]] = msg_'[self].transaction /\ msg_'[self].type = "Fin"
                                            THEN /\ DoneCounts' = [DoneCounts EXCEPT ![cid_Coor[self]][msg_'[self].item] = DoneCounts[cid_Coor[self]][msg_'[self].item] + 1]
                                                 /\ /\ cid_Coo' = [cid_Coo EXCEPT ![self] = cid_Coor[self]]
                                                    /\ quorom' = [quorom EXCEPT ![self] = (3*M) \div 4]
                                                    /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "CoordinatorFinDecide",
                                                                                             pc        |->  "ProcCoorHandleACKB",
                                                                                             t_Co      |->  t_Co[self],
                                                                                             targetReplica |->  targetReplica[self],
                                                                                             cid_Coo   |->  cid_Coo[self],
                                                                                             quorom    |->  quorom[self] ] >>
                                                                                         \o stack[self]]
                                                 /\ t_Co' = [t_Co EXCEPT ![self] = 0]
                                                 /\ targetReplica' = [targetReplica EXCEPT ![self] = 0]
                                                 /\ pc' = [pc EXCEPT ![self] = "ProcCoorFinDecide"]
                                                 /\ UNCHANGED << t, msg_Ca, 
                                                                 cid_C >>
                                            ELSE /\ IF CurrentTransaction[cid_Coor[self]] = msg_'[self].transaction /\ msg_'[self].type = "Process-ack"
                                                       THEN /\ /\ cid_C' = [cid_C EXCEPT ![self] = cid_Coor[self]]
                                                               /\ msg_Ca' = [msg_Ca EXCEPT ![self] = msg_'[self]]
                                                               /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "CalculateACKCounts",
                                                                                                        pc        |->  "ProcCoorRecACKB",
                                                                                                        t         |->  t[self],
                                                                                                        msg_Ca    |->  msg_Ca[self],
                                                                                                        cid_C     |->  cid_C[self] ] >>
                                                                                                    \o stack[self]]
                                                               /\ t' = [t EXCEPT ![self] = CurrentTransaction[cid_Coor[self]]]
                                                            /\ pc' = [pc EXCEPT ![self] = "ProcCalculateCounts"]
                                                       ELSE /\ pc' = [pc EXCEPT ![self] = "ProcCoorHandleACKB"]
                                                            /\ UNCHANGED << stack, 
                                                                            t, 
                                                                            msg_Ca, 
                                                                            cid_C >>
                                                 /\ UNCHANGED << DoneCounts, 
                                                                 cid_Coo, 
                                                                 quorom, t_Co, 
                                                                 targetReplica >>
                                 ELSE /\ pc' = [pc EXCEPT ![self] = "ProcCoorHandleACKB"]
                                      /\ UNCHANGED << CoordinatorMessages, 
                                                      DoneCounts, 
                                                      RecieveACKCount, stack, 
                                                      t, msg_Ca, cid_C, 
                                                      cid_Coo, quorom, t_Co, 
                                                      targetReplica, msg_ >>
                           /\ UNCHANGED << Replicas, Transactions, Messages, 
                                           WriteLocks, Logs, 
                                           TransactionStatusCoor, 
                                           TransactionStatus, 
                                           CurrentTransaction, SuccessCounts, 
                                           FailureCounts, ReadResults, 
                                           ReadConsistency, NodeStatus, 
                                           FailedCounts, CommitCounts, 
                                           CommitTS, AbortCounts, SyncCounts, 
                                           CurrentTime, SystemTerminated, cid_, 
                                           t_, i1, j1, Test, targetReplica_, 
                                           s_msg_, cid_Co, quorom_, minority, 
                                           t_C, i7, j7, commitTimestamp, 
                                           targetReplica_C, decision, 
                                           allSuccessful, s_msg_C, cid_Coor, 
                                           type_, recoveryTid_, cid, msg_C, 
                                           type_C, recoveryTid, msg_Ha, id_H, 
                                           i_, j_, t_H, readTimestamp, 
                                           readData, res_msg_, msg_Han, id_Ha, 
                                           i_H, j_H, t_Ha, res_msg_H, msg_Hand, 
                                           id_Han, i_Ha, j_Ha, t_Han, 
                                           res_msg_Ha, readValue, readWTS, 
                                           msg_Handl, id_Hand, i_Han, j_Han, 
                                           t_Hand, idx, res_msg_Han, 
                                           msg_Handle, id_Handl, i_Hand, 
                                           j_Hand, t_Handl, res_msg, msg, 
                                           id_Handle, i, j, id_HandleS, msg_H, 
                                           i13, j13, id_Re, msg_R, type_R, 
                                           id_N, msg_N, type, id_C, s_msg_Co, 
                                           i8, j8, id, s_msg, dataItemId, id_, 
                                           id_R >>

ProcCoorRecACKB(self) == /\ pc[self] = "ProcCoorRecACKB"
                         /\ /\ cid_Co' = [cid_Co EXCEPT ![self] = cid_Coor[self]]
                            /\ minority' = [minority EXCEPT ![self] = M \div 4]
                            /\ quorom_' = [quorom_ EXCEPT ![self] = (3*M) \div 4]
                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "CoordinatorDecide",
                                                                     pc        |->  "ProcCoorHandleACKB",
                                                                     t_C       |->  t_C[self],
                                                                     i7        |->  i7[self],
                                                                     j7        |->  j7[self],
                                                                     commitTimestamp |->  commitTimestamp[self],
                                                                     targetReplica_C |->  targetReplica_C[self],
                                                                     decision  |->  decision[self],
                                                                     allSuccessful |->  allSuccessful[self],
                                                                     s_msg_C   |->  s_msg_C[self],
                                                                     cid_Co    |->  cid_Co[self],
                                                                     quorom_   |->  quorom_[self],
                                                                     minority  |->  minority[self] ] >>
                                                                 \o stack[self]]
                         /\ t_C' = [t_C EXCEPT ![self] = 0]
                         /\ i7' = [i7 EXCEPT ![self] = 0]
                         /\ j7' = [j7 EXCEPT ![self] = 0]
                         /\ commitTimestamp' = [commitTimestamp EXCEPT ![self] = 0]
                         /\ targetReplica_C' = [targetReplica_C EXCEPT ![self] = 0]
                         /\ decision' = [decision EXCEPT ![self] = "Pending"]
                         /\ allSuccessful' = [allSuccessful EXCEPT ![self] = TRUE]
                         /\ s_msg_C' = [s_msg_C EXCEPT ![self] = {}]
                         /\ pc' = [pc EXCEPT ![self] = "ProcCoorDec"]
                         /\ UNCHANGED << Replicas, Transactions, Messages, 
                                         CoordinatorMessages, WriteLocks, Logs, 
                                         TransactionStatusCoor, 
                                         TransactionStatus, CurrentTransaction, 
                                         SuccessCounts, FailureCounts, 
                                         ReadResults, ReadConsistency, 
                                         DoneCounts, NodeStatus, FailedCounts, 
                                         CommitCounts, CommitTS, AbortCounts, 
                                         SyncCounts, CurrentTime, 
                                         SystemTerminated, RecieveACKCount, 
                                         cid_, t_, i1, j1, Test, 
                                         targetReplica_, s_msg_, t, msg_Ca, 
                                         cid_C, cid_Coo, quorom, t_Co, 
                                         targetReplica, cid_Coor, msg_, type_, 
                                         recoveryTid_, cid, msg_C, type_C, 
                                         recoveryTid, msg_Ha, id_H, i_, j_, 
                                         t_H, readTimestamp, readData, 
                                         res_msg_, msg_Han, id_Ha, i_H, j_H, 
                                         t_Ha, res_msg_H, msg_Hand, id_Han, 
                                         i_Ha, j_Ha, t_Han, res_msg_Ha, 
                                         readValue, readWTS, msg_Handl, 
                                         id_Hand, i_Han, j_Han, t_Hand, idx, 
                                         res_msg_Han, msg_Handle, id_Handl, 
                                         i_Hand, j_Hand, t_Handl, res_msg, msg, 
                                         id_Handle, i, j, id_HandleS, msg_H, 
                                         i13, j13, id_Re, msg_R, type_R, id_N, 
                                         msg_N, type, id_C, s_msg_Co, i8, j8, 
                                         id, s_msg, dataItemId, id_, id_R >>

ProcCoorHandleACKB(self) == /\ pc[self] = "ProcCoorHandleACKB"
                            /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                            /\ msg_' = [msg_ EXCEPT ![self] = Head(stack[self]).msg_]
                            /\ type_' = [type_ EXCEPT ![self] = Head(stack[self]).type_]
                            /\ recoveryTid_' = [recoveryTid_ EXCEPT ![self] = Head(stack[self]).recoveryTid_]
                            /\ cid_Coor' = [cid_Coor EXCEPT ![self] = Head(stack[self]).cid_Coor]
                            /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                            /\ UNCHANGED << Replicas, Transactions, Messages, 
                                            CoordinatorMessages, WriteLocks, 
                                            Logs, TransactionStatusCoor, 
                                            TransactionStatus, 
                                            CurrentTransaction, SuccessCounts, 
                                            FailureCounts, ReadResults, 
                                            ReadConsistency, DoneCounts, 
                                            NodeStatus, FailedCounts, 
                                            CommitCounts, CommitTS, 
                                            AbortCounts, SyncCounts, 
                                            CurrentTime, SystemTerminated, 
                                            RecieveACKCount, cid_, t_, i1, j1, 
                                            Test, targetReplica_, s_msg_, t, 
                                            msg_Ca, cid_C, cid_Co, quorom_, 
                                            minority, t_C, i7, j7, 
                                            commitTimestamp, targetReplica_C, 
                                            decision, allSuccessful, s_msg_C, 
                                            cid_Coo, quorom, t_Co, 
                                            targetReplica, cid, msg_C, type_C, 
                                            recoveryTid, msg_Ha, id_H, i_, j_, 
                                            t_H, readTimestamp, readData, 
                                            res_msg_, msg_Han, id_Ha, i_H, j_H, 
                                            t_Ha, res_msg_H, msg_Hand, id_Han, 
                                            i_Ha, j_Ha, t_Han, res_msg_Ha, 
                                            readValue, readWTS, msg_Handl, 
                                            id_Hand, i_Han, j_Han, t_Hand, idx, 
                                            res_msg_Han, msg_Handle, id_Handl, 
                                            i_Hand, j_Hand, t_Handl, res_msg, 
                                            msg, id_Handle, i, j, id_HandleS, 
                                            msg_H, i13, j13, id_Re, msg_R, 
                                            type_R, id_N, msg_N, type, id_C, 
                                            s_msg_Co, i8, j8, id, s_msg, 
                                            dataItemId, id_, id_R >>

CoordinatorHandleACKs(self) == ProcCoorHandleACK(self)
                                  \/ ProcCoorRecACKB(self)
                                  \/ ProcCoorHandleACKB(self)

ProcCoorHandleReport(self) == /\ pc[self] = "ProcCoorHandleReport"
                              /\ IF \E m \in CoordinatorMessages : m.coordinator_id = cid[self] /\ m.type = "Report"
                                    THEN /\ msg_C' = [msg_C EXCEPT ![self] = CHOOSE m \in CoordinatorMessages : m.coordinator_id = cid[self] /\ m.type = "Report"]
                                         /\ CoordinatorMessages' = CoordinatorMessages \ {msg_C'[self]}
                                         /\ /\ cid_C' = [cid_C EXCEPT ![self] = cid[self]]
                                            /\ msg_Ca' = [msg_Ca EXCEPT ![self] = msg_C'[self]]
                                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "CalculateACKCounts",
                                                                                     pc        |->  "ProcCoorHandleReportB",
                                                                                     t         |->  t[self],
                                                                                     msg_Ca    |->  msg_Ca[self],
                                                                                     cid_C     |->  cid_C[self] ] >>
                                                                                 \o stack[self]]
                                            /\ t' = [t EXCEPT ![self] = CurrentTransaction[cid[self]]]
                                         /\ pc' = [pc EXCEPT ![self] = "ProcCalculateCounts"]
                                    ELSE /\ pc' = [pc EXCEPT ![self] = "ProcCoorHandleReportC"]
                                         /\ UNCHANGED << CoordinatorMessages, 
                                                         stack, t, msg_Ca, 
                                                         cid_C, msg_C >>
                              /\ UNCHANGED << Replicas, Transactions, Messages, 
                                              WriteLocks, Logs, 
                                              TransactionStatusCoor, 
                                              TransactionStatus, 
                                              CurrentTransaction, 
                                              SuccessCounts, FailureCounts, 
                                              ReadResults, ReadConsistency, 
                                              DoneCounts, NodeStatus, 
                                              FailedCounts, CommitCounts, 
                                              CommitTS, AbortCounts, 
                                              SyncCounts, CurrentTime, 
                                              SystemTerminated, 
                                              RecieveACKCount, cid_, t_, i1, 
                                              j1, Test, targetReplica_, s_msg_, 
                                              cid_Co, quorom_, minority, t_C, 
                                              i7, j7, commitTimestamp, 
                                              targetReplica_C, decision, 
                                              allSuccessful, s_msg_C, cid_Coo, 
                                              quorom, t_Co, targetReplica, 
                                              cid_Coor, msg_, type_, 
                                              recoveryTid_, cid, type_C, 
                                              recoveryTid, msg_Ha, id_H, i_, 
                                              j_, t_H, readTimestamp, readData, 
                                              res_msg_, msg_Han, id_Ha, i_H, 
                                              j_H, t_Ha, res_msg_H, msg_Hand, 
                                              id_Han, i_Ha, j_Ha, t_Han, 
                                              res_msg_Ha, readValue, readWTS, 
                                              msg_Handl, id_Hand, i_Han, j_Han, 
                                              t_Hand, idx, res_msg_Han, 
                                              msg_Handle, id_Handl, i_Hand, 
                                              j_Hand, t_Handl, res_msg, msg, 
                                              id_Handle, i, j, id_HandleS, 
                                              msg_H, i13, j13, id_Re, msg_R, 
                                              type_R, id_N, msg_N, type, id_C, 
                                              s_msg_Co, i8, j8, id, s_msg, 
                                              dataItemId, id_, id_R >>

ProcCoorHandleReportB(self) == /\ pc[self] = "ProcCoorHandleReportB"
                               /\ /\ cid_Co' = [cid_Co EXCEPT ![self] = cid[self]]
                                  /\ minority' = [minority EXCEPT ![self] = M \div 2]
                                  /\ quorom_' = [quorom_ EXCEPT ![self] = M \div 2]
                                  /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "CoordinatorDecide",
                                                                           pc        |->  "ProcCoorHandleReportC",
                                                                           t_C       |->  t_C[self],
                                                                           i7        |->  i7[self],
                                                                           j7        |->  j7[self],
                                                                           commitTimestamp |->  commitTimestamp[self],
                                                                           targetReplica_C |->  targetReplica_C[self],
                                                                           decision  |->  decision[self],
                                                                           allSuccessful |->  allSuccessful[self],
                                                                           s_msg_C   |->  s_msg_C[self],
                                                                           cid_Co    |->  cid_Co[self],
                                                                           quorom_   |->  quorom_[self],
                                                                           minority  |->  minority[self] ] >>
                                                                       \o stack[self]]
                               /\ t_C' = [t_C EXCEPT ![self] = 0]
                               /\ i7' = [i7 EXCEPT ![self] = 0]
                               /\ j7' = [j7 EXCEPT ![self] = 0]
                               /\ commitTimestamp' = [commitTimestamp EXCEPT ![self] = 0]
                               /\ targetReplica_C' = [targetReplica_C EXCEPT ![self] = 0]
                               /\ decision' = [decision EXCEPT ![self] = "Pending"]
                               /\ allSuccessful' = [allSuccessful EXCEPT ![self] = TRUE]
                               /\ s_msg_C' = [s_msg_C EXCEPT ![self] = {}]
                               /\ pc' = [pc EXCEPT ![self] = "ProcCoorDec"]
                               /\ UNCHANGED << Replicas, Transactions, 
                                               Messages, CoordinatorMessages, 
                                               WriteLocks, Logs, 
                                               TransactionStatusCoor, 
                                               TransactionStatus, 
                                               CurrentTransaction, 
                                               SuccessCounts, FailureCounts, 
                                               ReadResults, ReadConsistency, 
                                               DoneCounts, NodeStatus, 
                                               FailedCounts, CommitCounts, 
                                               CommitTS, AbortCounts, 
                                               SyncCounts, CurrentTime, 
                                               SystemTerminated, 
                                               RecieveACKCount, cid_, t_, i1, 
                                               j1, Test, targetReplica_, 
                                               s_msg_, t, msg_Ca, cid_C, 
                                               cid_Coo, quorom, t_Co, 
                                               targetReplica, cid_Coor, msg_, 
                                               type_, recoveryTid_, cid, msg_C, 
                                               type_C, recoveryTid, msg_Ha, 
                                               id_H, i_, j_, t_H, 
                                               readTimestamp, readData, 
                                               res_msg_, msg_Han, id_Ha, i_H, 
                                               j_H, t_Ha, res_msg_H, msg_Hand, 
                                               id_Han, i_Ha, j_Ha, t_Han, 
                                               res_msg_Ha, readValue, readWTS, 
                                               msg_Handl, id_Hand, i_Han, 
                                               j_Han, t_Hand, idx, res_msg_Han, 
                                               msg_Handle, id_Handl, i_Hand, 
                                               j_Hand, t_Handl, res_msg, msg, 
                                               id_Handle, i, j, id_HandleS, 
                                               msg_H, i13, j13, id_Re, msg_R, 
                                               type_R, id_N, msg_N, type, id_C, 
                                               s_msg_Co, i8, j8, id, s_msg, 
                                               dataItemId, id_, id_R >>

ProcCoorHandleReportC(self) == /\ pc[self] = "ProcCoorHandleReportC"
                               /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                               /\ msg_C' = [msg_C EXCEPT ![self] = Head(stack[self]).msg_C]
                               /\ type_C' = [type_C EXCEPT ![self] = Head(stack[self]).type_C]
                               /\ recoveryTid' = [recoveryTid EXCEPT ![self] = Head(stack[self]).recoveryTid]
                               /\ cid' = [cid EXCEPT ![self] = Head(stack[self]).cid]
                               /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                               /\ UNCHANGED << Replicas, Transactions, 
                                               Messages, CoordinatorMessages, 
                                               WriteLocks, Logs, 
                                               TransactionStatusCoor, 
                                               TransactionStatus, 
                                               CurrentTransaction, 
                                               SuccessCounts, FailureCounts, 
                                               ReadResults, ReadConsistency, 
                                               DoneCounts, NodeStatus, 
                                               FailedCounts, CommitCounts, 
                                               CommitTS, AbortCounts, 
                                               SyncCounts, CurrentTime, 
                                               SystemTerminated, 
                                               RecieveACKCount, cid_, t_, i1, 
                                               j1, Test, targetReplica_, 
                                               s_msg_, t, msg_Ca, cid_C, 
                                               cid_Co, quorom_, minority, t_C, 
                                               i7, j7, commitTimestamp, 
                                               targetReplica_C, decision, 
                                               allSuccessful, s_msg_C, cid_Coo, 
                                               quorom, t_Co, targetReplica, 
                                               cid_Coor, msg_, type_, 
                                               recoveryTid_, msg_Ha, id_H, i_, 
                                               j_, t_H, readTimestamp, 
                                               readData, res_msg_, msg_Han, 
                                               id_Ha, i_H, j_H, t_Ha, 
                                               res_msg_H, msg_Hand, id_Han, 
                                               i_Ha, j_Ha, t_Han, res_msg_Ha, 
                                               readValue, readWTS, msg_Handl, 
                                               id_Hand, i_Han, j_Han, t_Hand, 
                                               idx, res_msg_Han, msg_Handle, 
                                               id_Handl, i_Hand, j_Hand, 
                                               t_Handl, res_msg, msg, 
                                               id_Handle, i, j, id_HandleS, 
                                               msg_H, i13, j13, id_Re, msg_R, 
                                               type_R, id_N, msg_N, type, id_C, 
                                               s_msg_Co, i8, j8, id, s_msg, 
                                               dataItemId, id_, id_R >>

CoordinatorHandleReports(self) == ProcCoorHandleReport(self)
                                     \/ ProcCoorHandleReportB(self)
                                     \/ ProcCoorHandleReportC(self)

ProcHandleRead(self) == /\ pc[self] = "ProcHandleRead"
                        /\ i_' = [i_ EXCEPT ![self] = msg_Ha[self].item]
                        /\ j_' = [j_ EXCEPT ![self] = msg_Ha[self].replica]
                        /\ t_H' = [t_H EXCEPT ![self] = msg_Ha[self].transaction]
                        /\ readTimestamp' = [readTimestamp EXCEPT ![self] = Transactions[t_H'[self]].StartTS]
                        /\ readData' = [readData EXCEPT ![self] = CHOOSE v \in {v \in Replicas[i_'[self]][j_'[self]]: v.timestamp <= readTimestamp'[self] /\
                                                                                     \A u \in Replicas[i_'[self]][j_'[self]] : (u.timestamp <= readTimestamp'[self]) => (u.timestamp <= v.timestamp)}: TRUE]
                        /\ IF TransactionStatus[id_H[self]][t_H'[self]].status = "NotStarted" /\ IsWriteLockAvailable(i_'[self], j_'[self])
                              THEN /\ TransactionStatus' = [TransactionStatus EXCEPT ![id_H[self]][t_H'[self]].status = "ReadSuccess"]
                                   /\ res_msg_' = [res_msg_ EXCEPT ![self] = [item |-> i_'[self], replica |-> j_'[self], transaction |-> t_H'[self], type |-> "Process-ack", status |-> "ReadSuccess", value |-> readData'[self].value, wts |-> readData'[self].timestamp, sendTime |-> CurrentTime, coordinator_id |-> msg_Ha[self].coordinator_id]]
                                   /\ CoordinatorMessages' = (CoordinatorMessages \cup {res_msg_'[self]})
                              ELSE /\ IF TransactionStatus[id_H[self]][t_H'[self]].status = "NotStarted"
                                         THEN /\ TransactionStatus' = [TransactionStatus EXCEPT ![id_H[self]][t_H'[self]].status = "ReadFailed"]
                                              /\ res_msg_' = [res_msg_ EXCEPT ![self] = [item |-> i_'[self], replica |-> j_'[self], transaction |-> t_H'[self], type |-> "Process-ack", status |-> "ReadFailed", sendTime |-> CurrentTime, coordinator_id |-> msg_Ha[self].coordinator_id]]
                                              /\ CoordinatorMessages' = (CoordinatorMessages \cup {res_msg_'[self]})
                                         ELSE /\ TRUE
                                              /\ UNCHANGED << CoordinatorMessages, 
                                                              TransactionStatus, 
                                                              res_msg_ >>
                        /\ pc' = [pc EXCEPT ![self] = "ProcHandleReadB"]
                        /\ UNCHANGED << Replicas, Transactions, Messages, 
                                        WriteLocks, Logs, 
                                        TransactionStatusCoor, 
                                        CurrentTransaction, SuccessCounts, 
                                        FailureCounts, ReadResults, 
                                        ReadConsistency, DoneCounts, 
                                        NodeStatus, FailedCounts, CommitCounts, 
                                        CommitTS, AbortCounts, SyncCounts, 
                                        CurrentTime, SystemTerminated, 
                                        RecieveACKCount, stack, cid_, t_, i1, 
                                        j1, Test, targetReplica_, s_msg_, t, 
                                        msg_Ca, cid_C, cid_Co, quorom_, 
                                        minority, t_C, i7, j7, commitTimestamp, 
                                        targetReplica_C, decision, 
                                        allSuccessful, s_msg_C, cid_Coo, 
                                        quorom, t_Co, targetReplica, cid_Coor, 
                                        msg_, type_, recoveryTid_, cid, msg_C, 
                                        type_C, recoveryTid, msg_Ha, id_H, 
                                        msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                        res_msg_H, msg_Hand, id_Han, i_Ha, 
                                        j_Ha, t_Han, res_msg_Ha, readValue, 
                                        readWTS, msg_Handl, id_Hand, i_Han, 
                                        j_Han, t_Hand, idx, res_msg_Han, 
                                        msg_Handle, id_Handl, i_Hand, j_Hand, 
                                        t_Handl, res_msg, msg, id_Handle, i, j, 
                                        id_HandleS, msg_H, i13, j13, id_Re, 
                                        msg_R, type_R, id_N, msg_N, type, id_C, 
                                        s_msg_Co, i8, j8, id, s_msg, 
                                        dataItemId, id_, id_R >>

ProcHandleReadB(self) == /\ pc[self] = "ProcHandleReadB"
                         /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                         /\ i_' = [i_ EXCEPT ![self] = Head(stack[self]).i_]
                         /\ j_' = [j_ EXCEPT ![self] = Head(stack[self]).j_]
                         /\ t_H' = [t_H EXCEPT ![self] = Head(stack[self]).t_H]
                         /\ readTimestamp' = [readTimestamp EXCEPT ![self] = Head(stack[self]).readTimestamp]
                         /\ readData' = [readData EXCEPT ![self] = Head(stack[self]).readData]
                         /\ res_msg_' = [res_msg_ EXCEPT ![self] = Head(stack[self]).res_msg_]
                         /\ msg_Ha' = [msg_Ha EXCEPT ![self] = Head(stack[self]).msg_Ha]
                         /\ id_H' = [id_H EXCEPT ![self] = Head(stack[self]).id_H]
                         /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                         /\ UNCHANGED << Replicas, Transactions, Messages, 
                                         CoordinatorMessages, WriteLocks, Logs, 
                                         TransactionStatusCoor, 
                                         TransactionStatus, CurrentTransaction, 
                                         SuccessCounts, FailureCounts, 
                                         ReadResults, ReadConsistency, 
                                         DoneCounts, NodeStatus, FailedCounts, 
                                         CommitCounts, CommitTS, AbortCounts, 
                                         SyncCounts, CurrentTime, 
                                         SystemTerminated, RecieveACKCount, 
                                         cid_, t_, i1, j1, Test, 
                                         targetReplica_, s_msg_, t, msg_Ca, 
                                         cid_C, cid_Co, quorom_, minority, t_C, 
                                         i7, j7, commitTimestamp, 
                                         targetReplica_C, decision, 
                                         allSuccessful, s_msg_C, cid_Coo, 
                                         quorom, t_Co, targetReplica, cid_Coor, 
                                         msg_, type_, recoveryTid_, cid, msg_C, 
                                         type_C, recoveryTid, msg_Han, id_Ha, 
                                         i_H, j_H, t_Ha, res_msg_H, msg_Hand, 
                                         id_Han, i_Ha, j_Ha, t_Han, res_msg_Ha, 
                                         readValue, readWTS, msg_Handl, 
                                         id_Hand, i_Han, j_Han, t_Hand, idx, 
                                         res_msg_Han, msg_Handle, id_Handl, 
                                         i_Hand, j_Hand, t_Handl, res_msg, msg, 
                                         id_Handle, i, j, id_HandleS, msg_H, 
                                         i13, j13, id_Re, msg_R, type_R, id_N, 
                                         msg_N, type, id_C, s_msg_Co, i8, j8, 
                                         id, s_msg, dataItemId, id_, id_R >>

HandleRead(self) == ProcHandleRead(self) \/ ProcHandleReadB(self)

ProcHandleWrite(self) == /\ pc[self] = "ProcHandleWrite"
                         /\ i_H' = [i_H EXCEPT ![self] = msg_Han[self].item]
                         /\ j_H' = [j_H EXCEPT ![self] = msg_Han[self].replica]
                         /\ t_Ha' = [t_Ha EXCEPT ![self] = msg_Han[self].transaction]
                         /\ IF TransactionStatus[id_Ha[self]][t_Ha'[self]].status = "NotStarted" /\ IsWriteLockAvailable(i_H'[self], j_H'[self])
                               THEN /\ WriteLocks' = [WriteLocks EXCEPT ![i_H'[self]][j_H'[self]] = TRUE]
                                    /\ Logs' = [Logs EXCEPT ![id_Ha[self]] = Logs[id_Ha[self]] \cup {[transaction |-> t_Ha'[self], item |-> i_H'[self], replica |-> j_H'[self], type |-> "write-pending", timestamp |-> 0, coordinator_id |-> msg_Han[self].coordinator_id]}]
                                    /\ TransactionStatus' = [TransactionStatus EXCEPT ![id_Ha[self]][t_Ha'[self]].status = "WriteSuccess"]
                                    /\ res_msg_H' = [res_msg_H EXCEPT ![self] = [item |-> i_H'[self], replica |-> j_H'[self], transaction |-> t_Ha'[self], type |-> "Process-ack", status |-> "WriteSuccess",  sendTime |-> CurrentTime, coordinator_id |-> msg_Han[self].coordinator_id]]
                                    /\ CoordinatorMessages' = (CoordinatorMessages \cup {res_msg_H'[self]})
                               ELSE /\ IF TransactionStatus[id_Ha[self]][t_Ha'[self]].status = "NotStarted"
                                          THEN /\ TransactionStatus' = [TransactionStatus EXCEPT ![id_Ha[self]][t_Ha'[self]].status = "WriteFailed"]
                                               /\ res_msg_H' = [res_msg_H EXCEPT ![self] = [item |-> i_H'[self], replica |-> j_H'[self], transaction |-> t_Ha'[self], type |-> "Process-ack", status |-> "WriteFailed",  sendTime |-> CurrentTime, coordinator_id |-> msg_Han[self].coordinator_id]]
                                               /\ CoordinatorMessages' = (CoordinatorMessages \cup {res_msg_H'[self]})
                                          ELSE /\ TRUE
                                               /\ UNCHANGED << CoordinatorMessages, 
                                                               TransactionStatus, 
                                                               res_msg_H >>
                                    /\ UNCHANGED << WriteLocks, Logs >>
                         /\ pc' = [pc EXCEPT ![self] = "ProcHandleWriteB_"]
                         /\ UNCHANGED << Replicas, Transactions, Messages, 
                                         TransactionStatusCoor, 
                                         CurrentTransaction, SuccessCounts, 
                                         FailureCounts, ReadResults, 
                                         ReadConsistency, DoneCounts, 
                                         NodeStatus, FailedCounts, 
                                         CommitCounts, CommitTS, AbortCounts, 
                                         SyncCounts, CurrentTime, 
                                         SystemTerminated, RecieveACKCount, 
                                         stack, cid_, t_, i1, j1, Test, 
                                         targetReplica_, s_msg_, t, msg_Ca, 
                                         cid_C, cid_Co, quorom_, minority, t_C, 
                                         i7, j7, commitTimestamp, 
                                         targetReplica_C, decision, 
                                         allSuccessful, s_msg_C, cid_Coo, 
                                         quorom, t_Co, targetReplica, cid_Coor, 
                                         msg_, type_, recoveryTid_, cid, msg_C, 
                                         type_C, recoveryTid, msg_Ha, id_H, i_, 
                                         j_, t_H, readTimestamp, readData, 
                                         res_msg_, msg_Han, id_Ha, msg_Hand, 
                                         id_Han, i_Ha, j_Ha, t_Han, res_msg_Ha, 
                                         readValue, readWTS, msg_Handl, 
                                         id_Hand, i_Han, j_Han, t_Hand, idx, 
                                         res_msg_Han, msg_Handle, id_Handl, 
                                         i_Hand, j_Hand, t_Handl, res_msg, msg, 
                                         id_Handle, i, j, id_HandleS, msg_H, 
                                         i13, j13, id_Re, msg_R, type_R, id_N, 
                                         msg_N, type, id_C, s_msg_Co, i8, j8, 
                                         id, s_msg, dataItemId, id_, id_R >>

ProcHandleWriteB_(self) == /\ pc[self] = "ProcHandleWriteB_"
                           /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                           /\ i_H' = [i_H EXCEPT ![self] = Head(stack[self]).i_H]
                           /\ j_H' = [j_H EXCEPT ![self] = Head(stack[self]).j_H]
                           /\ t_Ha' = [t_Ha EXCEPT ![self] = Head(stack[self]).t_Ha]
                           /\ res_msg_H' = [res_msg_H EXCEPT ![self] = Head(stack[self]).res_msg_H]
                           /\ msg_Han' = [msg_Han EXCEPT ![self] = Head(stack[self]).msg_Han]
                           /\ id_Ha' = [id_Ha EXCEPT ![self] = Head(stack[self]).id_Ha]
                           /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                           /\ UNCHANGED << Replicas, Transactions, Messages, 
                                           CoordinatorMessages, WriteLocks, 
                                           Logs, TransactionStatusCoor, 
                                           TransactionStatus, 
                                           CurrentTransaction, SuccessCounts, 
                                           FailureCounts, ReadResults, 
                                           ReadConsistency, DoneCounts, 
                                           NodeStatus, FailedCounts, 
                                           CommitCounts, CommitTS, AbortCounts, 
                                           SyncCounts, CurrentTime, 
                                           SystemTerminated, RecieveACKCount, 
                                           cid_, t_, i1, j1, Test, 
                                           targetReplica_, s_msg_, t, msg_Ca, 
                                           cid_C, cid_Co, quorom_, minority, 
                                           t_C, i7, j7, commitTimestamp, 
                                           targetReplica_C, decision, 
                                           allSuccessful, s_msg_C, cid_Coo, 
                                           quorom, t_Co, targetReplica, 
                                           cid_Coor, msg_, type_, recoveryTid_, 
                                           cid, msg_C, type_C, recoveryTid, 
                                           msg_Ha, id_H, i_, j_, t_H, 
                                           readTimestamp, readData, res_msg_, 
                                           msg_Hand, id_Han, i_Ha, j_Ha, t_Han, 
                                           res_msg_Ha, readValue, readWTS, 
                                           msg_Handl, id_Hand, i_Han, j_Han, 
                                           t_Hand, idx, res_msg_Han, 
                                           msg_Handle, id_Handl, i_Hand, 
                                           j_Hand, t_Handl, res_msg, msg, 
                                           id_Handle, i, j, id_HandleS, msg_H, 
                                           i13, j13, id_Re, msg_R, type_R, 
                                           id_N, msg_N, type, id_C, s_msg_Co, 
                                           i8, j8, id, s_msg, dataItemId, id_, 
                                           id_R >>

HandleWrite(self) == ProcHandleWrite(self) \/ ProcHandleWriteB_(self)

ProcHandleInquire(self) == /\ pc[self] = "ProcHandleInquire"
                           /\ i_Ha' = [i_Ha EXCEPT ![self] = msg_Hand[self].item]
                           /\ j_Ha' = [j_Ha EXCEPT ![self] = msg_Hand[self].replica]
                           /\ t_Han' = [t_Han EXCEPT ![self] = msg_Hand[self].transaction]
                           /\ readTimestamp' = [readTimestamp EXCEPT ![self] = Transactions[t_Han'[self]].StartTS]
                           /\ IF TransactionStatus[id_Han[self]][t_Han'[self]].status = "ReadSuccess"
                                 THEN /\ readData' = [readData EXCEPT ![self] = CHOOSE v \in {v \in Replicas[i_Ha'[self]][j_Ha'[self]]: v.timestamp <= readTimestamp'[self] /\
                                                                                                 \A u \in Replicas[i_Ha'[self]][j_Ha'[self]] : (u.timestamp <= readTimestamp'[self]) => (u.timestamp <= v.timestamp)}: TRUE]
                                      /\ readValue' = [readValue EXCEPT ![self] = readData'[self].value]
                                      /\ readWTS' = [readWTS EXCEPT ![self] = readData'[self].timestamp]
                                 ELSE /\ readValue' = [readValue EXCEPT ![self] = 0]
                                      /\ readWTS' = [readWTS EXCEPT ![self] = 0]
                                      /\ UNCHANGED readData
                           /\ res_msg_Ha' = [res_msg_Ha EXCEPT ![self] = [item |-> i_Ha'[self], replica |-> j_Ha'[self], transaction |-> t_Han'[self], type |-> "Report", status |-> TransactionStatus[id_Han[self]][t_Han'[self]].status, value |-> readValue'[self], wts |-> readWTS'[self], sendTime |-> CurrentTime, CommitTS |->TransactionStatus[id_Han[self]][t_Han'[self]].CommitTS,  coordinator_id |-> msg_Hand[self].coordinator_id]]
                           /\ CoordinatorMessages' = (CoordinatorMessages \cup {res_msg_Ha'[self]})
                           /\ pc' = [pc EXCEPT ![self] = "ProcHandleWriteB"]
                           /\ UNCHANGED << Replicas, Transactions, Messages, 
                                           WriteLocks, Logs, 
                                           TransactionStatusCoor, 
                                           TransactionStatus, 
                                           CurrentTransaction, SuccessCounts, 
                                           FailureCounts, ReadResults, 
                                           ReadConsistency, DoneCounts, 
                                           NodeStatus, FailedCounts, 
                                           CommitCounts, CommitTS, AbortCounts, 
                                           SyncCounts, CurrentTime, 
                                           SystemTerminated, RecieveACKCount, 
                                           stack, cid_, t_, i1, j1, Test, 
                                           targetReplica_, s_msg_, t, msg_Ca, 
                                           cid_C, cid_Co, quorom_, minority, 
                                           t_C, i7, j7, commitTimestamp, 
                                           targetReplica_C, decision, 
                                           allSuccessful, s_msg_C, cid_Coo, 
                                           quorom, t_Co, targetReplica, 
                                           cid_Coor, msg_, type_, recoveryTid_, 
                                           cid, msg_C, type_C, recoveryTid, 
                                           msg_Ha, id_H, i_, j_, t_H, res_msg_, 
                                           msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                           res_msg_H, msg_Hand, id_Han, 
                                           msg_Handl, id_Hand, i_Han, j_Han, 
                                           t_Hand, idx, res_msg_Han, 
                                           msg_Handle, id_Handl, i_Hand, 
                                           j_Hand, t_Handl, res_msg, msg, 
                                           id_Handle, i, j, id_HandleS, msg_H, 
                                           i13, j13, id_Re, msg_R, type_R, 
                                           id_N, msg_N, type, id_C, s_msg_Co, 
                                           i8, j8, id, s_msg, dataItemId, id_, 
                                           id_R >>

ProcHandleWriteB(self) == /\ pc[self] = "ProcHandleWriteB"
                          /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                          /\ i_Ha' = [i_Ha EXCEPT ![self] = Head(stack[self]).i_Ha]
                          /\ j_Ha' = [j_Ha EXCEPT ![self] = Head(stack[self]).j_Ha]
                          /\ t_Han' = [t_Han EXCEPT ![self] = Head(stack[self]).t_Han]
                          /\ res_msg_Ha' = [res_msg_Ha EXCEPT ![self] = Head(stack[self]).res_msg_Ha]
                          /\ readValue' = [readValue EXCEPT ![self] = Head(stack[self]).readValue]
                          /\ readWTS' = [readWTS EXCEPT ![self] = Head(stack[self]).readWTS]
                          /\ msg_Hand' = [msg_Hand EXCEPT ![self] = Head(stack[self]).msg_Hand]
                          /\ id_Han' = [id_Han EXCEPT ![self] = Head(stack[self]).id_Han]
                          /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                          /\ UNCHANGED << Replicas, Transactions, Messages, 
                                          CoordinatorMessages, WriteLocks, 
                                          Logs, TransactionStatusCoor, 
                                          TransactionStatus, 
                                          CurrentTransaction, SuccessCounts, 
                                          FailureCounts, ReadResults, 
                                          ReadConsistency, DoneCounts, 
                                          NodeStatus, FailedCounts, 
                                          CommitCounts, CommitTS, AbortCounts, 
                                          SyncCounts, CurrentTime, 
                                          SystemTerminated, RecieveACKCount, 
                                          cid_, t_, i1, j1, Test, 
                                          targetReplica_, s_msg_, t, msg_Ca, 
                                          cid_C, cid_Co, quorom_, minority, 
                                          t_C, i7, j7, commitTimestamp, 
                                          targetReplica_C, decision, 
                                          allSuccessful, s_msg_C, cid_Coo, 
                                          quorom, t_Co, targetReplica, 
                                          cid_Coor, msg_, type_, recoveryTid_, 
                                          cid, msg_C, type_C, recoveryTid, 
                                          msg_Ha, id_H, i_, j_, t_H, 
                                          readTimestamp, readData, res_msg_, 
                                          msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                          res_msg_H, msg_Handl, id_Hand, i_Han, 
                                          j_Han, t_Hand, idx, res_msg_Han, 
                                          msg_Handle, id_Handl, i_Hand, j_Hand, 
                                          t_Handl, res_msg, msg, id_Handle, i, 
                                          j, id_HandleS, msg_H, i13, j13, 
                                          id_Re, msg_R, type_R, id_N, msg_N, 
                                          type, id_C, s_msg_Co, i8, j8, id, 
                                          s_msg, dataItemId, id_, id_R >>

HandleInquire(self) == ProcHandleInquire(self) \/ ProcHandleWriteB(self)

ProcHandleCommit(self) == /\ pc[self] = "ProcHandleCommit"
                          /\ i_Han' = [i_Han EXCEPT ![self] = msg_Handl[self].item]
                          /\ j_Han' = [j_Han EXCEPT ![self] = msg_Handl[self].replica]
                          /\ t_Hand' = [t_Hand EXCEPT ![self] = msg_Handl[self].transaction]
                          /\ IF \E log \in Logs[id_Hand[self]] : log.transaction = t_Hand'[self] /\ log.item = i_Han'[self]
                                THEN /\ Logs' = [Logs EXCEPT ![id_Hand[self]] = Logs[id_Hand[self]] \cup {[transaction |-> t_Hand'[self], item |-> i_Han'[self], replica |-> j_Han'[self], type |-> "write-commit", timestamp |-> msg_Handl[self].timestamp, coordinator_id |-> msg_Handl[self].coordinator_id]}]
                                ELSE /\ TRUE
                                     /\ Logs' = Logs
                          /\ IF WriteLocks[i_Han'[self]][j_Han'[self]]
                                THEN /\ WriteLocks' = [WriteLocks EXCEPT ![i_Han'[self]][j_Han'[self]] = FALSE]
                                     /\ Replicas' = [Replicas EXCEPT ![i_Han'[self]][j_Han'[self]] = Replicas[i_Han'[self]][j_Han'[self]] \cup {[value |-> msg_Handl[self].value, timestamp |-> msg_Handl[self].timestamp]}]
                                ELSE /\ TRUE
                                     /\ UNCHANGED << Replicas, WriteLocks >>
                          /\ TransactionStatus' = [TransactionStatus EXCEPT ![id_Hand[self]][t_Hand'[self]] = [status |-> "Committed", CommitTS |-> msg_Handl[self].timestamp]]
                          /\ res_msg_Han' = [res_msg_Han EXCEPT ![self] = [item |-> i_Han'[self], replica |-> j_Han'[self], transaction |-> t_Hand'[self], type |-> "Fin", sendTime |-> CurrentTime, coordinator_id |-> msg_Handl[self].coordinator_id]]
                          /\ CoordinatorMessages' = (CoordinatorMessages \cup {res_msg_Han'[self]})
                          /\ pc' = [pc EXCEPT ![self] = "ProcHandleCommitB"]
                          /\ UNCHANGED << Transactions, Messages, 
                                          TransactionStatusCoor, 
                                          CurrentTransaction, SuccessCounts, 
                                          FailureCounts, ReadResults, 
                                          ReadConsistency, DoneCounts, 
                                          NodeStatus, FailedCounts, 
                                          CommitCounts, CommitTS, AbortCounts, 
                                          SyncCounts, CurrentTime, 
                                          SystemTerminated, RecieveACKCount, 
                                          stack, cid_, t_, i1, j1, Test, 
                                          targetReplica_, s_msg_, t, msg_Ca, 
                                          cid_C, cid_Co, quorom_, minority, 
                                          t_C, i7, j7, commitTimestamp, 
                                          targetReplica_C, decision, 
                                          allSuccessful, s_msg_C, cid_Coo, 
                                          quorom, t_Co, targetReplica, 
                                          cid_Coor, msg_, type_, recoveryTid_, 
                                          cid, msg_C, type_C, recoveryTid, 
                                          msg_Ha, id_H, i_, j_, t_H, 
                                          readTimestamp, readData, res_msg_, 
                                          msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                          res_msg_H, msg_Hand, id_Han, i_Ha, 
                                          j_Ha, t_Han, res_msg_Ha, readValue, 
                                          readWTS, msg_Handl, id_Hand, idx, 
                                          msg_Handle, id_Handl, i_Hand, j_Hand, 
                                          t_Handl, res_msg, msg, id_Handle, i, 
                                          j, id_HandleS, msg_H, i13, j13, 
                                          id_Re, msg_R, type_R, id_N, msg_N, 
                                          type, id_C, s_msg_Co, i8, j8, id, 
                                          s_msg, dataItemId, id_, id_R >>

ProcHandleCommitB(self) == /\ pc[self] = "ProcHandleCommitB"
                           /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                           /\ i_Han' = [i_Han EXCEPT ![self] = Head(stack[self]).i_Han]
                           /\ j_Han' = [j_Han EXCEPT ![self] = Head(stack[self]).j_Han]
                           /\ t_Hand' = [t_Hand EXCEPT ![self] = Head(stack[self]).t_Hand]
                           /\ idx' = [idx EXCEPT ![self] = Head(stack[self]).idx]
                           /\ res_msg_Han' = [res_msg_Han EXCEPT ![self] = Head(stack[self]).res_msg_Han]
                           /\ msg_Handl' = [msg_Handl EXCEPT ![self] = Head(stack[self]).msg_Handl]
                           /\ id_Hand' = [id_Hand EXCEPT ![self] = Head(stack[self]).id_Hand]
                           /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                           /\ UNCHANGED << Replicas, Transactions, Messages, 
                                           CoordinatorMessages, WriteLocks, 
                                           Logs, TransactionStatusCoor, 
                                           TransactionStatus, 
                                           CurrentTransaction, SuccessCounts, 
                                           FailureCounts, ReadResults, 
                                           ReadConsistency, DoneCounts, 
                                           NodeStatus, FailedCounts, 
                                           CommitCounts, CommitTS, AbortCounts, 
                                           SyncCounts, CurrentTime, 
                                           SystemTerminated, RecieveACKCount, 
                                           cid_, t_, i1, j1, Test, 
                                           targetReplica_, s_msg_, t, msg_Ca, 
                                           cid_C, cid_Co, quorom_, minority, 
                                           t_C, i7, j7, commitTimestamp, 
                                           targetReplica_C, decision, 
                                           allSuccessful, s_msg_C, cid_Coo, 
                                           quorom, t_Co, targetReplica, 
                                           cid_Coor, msg_, type_, recoveryTid_, 
                                           cid, msg_C, type_C, recoveryTid, 
                                           msg_Ha, id_H, i_, j_, t_H, 
                                           readTimestamp, readData, res_msg_, 
                                           msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                           res_msg_H, msg_Hand, id_Han, i_Ha, 
                                           j_Ha, t_Han, res_msg_Ha, readValue, 
                                           readWTS, msg_Handle, id_Handl, 
                                           i_Hand, j_Hand, t_Handl, res_msg, 
                                           msg, id_Handle, i, j, id_HandleS, 
                                           msg_H, i13, j13, id_Re, msg_R, 
                                           type_R, id_N, msg_N, type, id_C, 
                                           s_msg_Co, i8, j8, id, s_msg, 
                                           dataItemId, id_, id_R >>

HandleCommit(self) == ProcHandleCommit(self) \/ ProcHandleCommitB(self)

ProcHandleAbort(self) == /\ pc[self] = "ProcHandleAbort"
                         /\ i_Hand' = [i_Hand EXCEPT ![self] = msg_Handle[self].item]
                         /\ j_Hand' = [j_Hand EXCEPT ![self] = msg_Handle[self].replica]
                         /\ t_Handl' = [t_Handl EXCEPT ![self] = msg_Handle[self].transaction]
                         /\ Logs' = [Logs EXCEPT ![id_Handl[self]] = {log \in Logs[id_Handl[self]] : log.transaction /= t_Handl'[self]}]
                         /\ IF WriteLocks[i_Hand'[self]][j_Hand'[self]]
                               THEN /\ WriteLocks' = [WriteLocks EXCEPT ![i_Hand'[self]][j_Hand'[self]] = FALSE]
                               ELSE /\ TRUE
                                    /\ UNCHANGED WriteLocks
                         /\ TransactionStatus' = [TransactionStatus EXCEPT ![id_Handl[self]][t_Handl'[self]].status = "Aborted"]
                         /\ res_msg' = [res_msg EXCEPT ![self] = [item |-> i_Hand'[self], replica |-> j_Hand'[self], transaction |-> t_Handl'[self], type |-> "Fin", sendTime |-> CurrentTime, coordinator_id |-> msg_Handle[self].coordinator_id]]
                         /\ CoordinatorMessages' = (CoordinatorMessages \cup {res_msg'[self]})
                         /\ pc' = [pc EXCEPT ![self] = "ProcHandleAbortB"]
                         /\ UNCHANGED << Replicas, Transactions, Messages, 
                                         TransactionStatusCoor, 
                                         CurrentTransaction, SuccessCounts, 
                                         FailureCounts, ReadResults, 
                                         ReadConsistency, DoneCounts, 
                                         NodeStatus, FailedCounts, 
                                         CommitCounts, CommitTS, AbortCounts, 
                                         SyncCounts, CurrentTime, 
                                         SystemTerminated, RecieveACKCount, 
                                         stack, cid_, t_, i1, j1, Test, 
                                         targetReplica_, s_msg_, t, msg_Ca, 
                                         cid_C, cid_Co, quorom_, minority, t_C, 
                                         i7, j7, commitTimestamp, 
                                         targetReplica_C, decision, 
                                         allSuccessful, s_msg_C, cid_Coo, 
                                         quorom, t_Co, targetReplica, cid_Coor, 
                                         msg_, type_, recoveryTid_, cid, msg_C, 
                                         type_C, recoveryTid, msg_Ha, id_H, i_, 
                                         j_, t_H, readTimestamp, readData, 
                                         res_msg_, msg_Han, id_Ha, i_H, j_H, 
                                         t_Ha, res_msg_H, msg_Hand, id_Han, 
                                         i_Ha, j_Ha, t_Han, res_msg_Ha, 
                                         readValue, readWTS, msg_Handl, 
                                         id_Hand, i_Han, j_Han, t_Hand, idx, 
                                         res_msg_Han, msg_Handle, id_Handl, 
                                         msg, id_Handle, i, j, id_HandleS, 
                                         msg_H, i13, j13, id_Re, msg_R, type_R, 
                                         id_N, msg_N, type, id_C, s_msg_Co, i8, 
                                         j8, id, s_msg, dataItemId, id_, id_R >>

ProcHandleAbortB(self) == /\ pc[self] = "ProcHandleAbortB"
                          /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                          /\ i_Hand' = [i_Hand EXCEPT ![self] = Head(stack[self]).i_Hand]
                          /\ j_Hand' = [j_Hand EXCEPT ![self] = Head(stack[self]).j_Hand]
                          /\ t_Handl' = [t_Handl EXCEPT ![self] = Head(stack[self]).t_Handl]
                          /\ res_msg' = [res_msg EXCEPT ![self] = Head(stack[self]).res_msg]
                          /\ msg_Handle' = [msg_Handle EXCEPT ![self] = Head(stack[self]).msg_Handle]
                          /\ id_Handl' = [id_Handl EXCEPT ![self] = Head(stack[self]).id_Handl]
                          /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                          /\ UNCHANGED << Replicas, Transactions, Messages, 
                                          CoordinatorMessages, WriteLocks, 
                                          Logs, TransactionStatusCoor, 
                                          TransactionStatus, 
                                          CurrentTransaction, SuccessCounts, 
                                          FailureCounts, ReadResults, 
                                          ReadConsistency, DoneCounts, 
                                          NodeStatus, FailedCounts, 
                                          CommitCounts, CommitTS, AbortCounts, 
                                          SyncCounts, CurrentTime, 
                                          SystemTerminated, RecieveACKCount, 
                                          cid_, t_, i1, j1, Test, 
                                          targetReplica_, s_msg_, t, msg_Ca, 
                                          cid_C, cid_Co, quorom_, minority, 
                                          t_C, i7, j7, commitTimestamp, 
                                          targetReplica_C, decision, 
                                          allSuccessful, s_msg_C, cid_Coo, 
                                          quorom, t_Co, targetReplica, 
                                          cid_Coor, msg_, type_, recoveryTid_, 
                                          cid, msg_C, type_C, recoveryTid, 
                                          msg_Ha, id_H, i_, j_, t_H, 
                                          readTimestamp, readData, res_msg_, 
                                          msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                          res_msg_H, msg_Hand, id_Han, i_Ha, 
                                          j_Ha, t_Han, res_msg_Ha, readValue, 
                                          readWTS, msg_Handl, id_Hand, i_Han, 
                                          j_Han, t_Hand, idx, res_msg_Han, msg, 
                                          id_Handle, i, j, id_HandleS, msg_H, 
                                          i13, j13, id_Re, msg_R, type_R, id_N, 
                                          msg_N, type, id_C, s_msg_Co, i8, j8, 
                                          id, s_msg, dataItemId, id_, id_R >>

HandleAbort(self) == ProcHandleAbort(self) \/ ProcHandleAbortB(self)

ProcHandleSync(self) == /\ pc[self] = "ProcHandleSync"
                        /\ i' = [i EXCEPT ![self] = msg[self].item]
                        /\ j' = [j EXCEPT ![self] = msg[self].replica]
                        /\ s_msg' = [s_msg EXCEPT ![self] = [type |-> "SyncAck",
                                                                     item |-> msg[self].item,
                                                                     replica |-> msg[self].replica,
                                                                     log |-> Logs[id_Handle[self]],
                                                                     value |-> Replicas[i'[self]][j'[self]],
                                                            
                                                                     sendTime |-> CurrentTime,
                                                                     targetReplica |-> msg[self].targetReplica]]
                        /\ Messages' = (Messages \cup {s_msg'[self]})
                        /\ pc' = [pc EXCEPT ![self] = "ProcHandleSyncB"]
                        /\ UNCHANGED << Replicas, Transactions, 
                                        CoordinatorMessages, WriteLocks, Logs, 
                                        TransactionStatusCoor, 
                                        TransactionStatus, CurrentTransaction, 
                                        SuccessCounts, FailureCounts, 
                                        ReadResults, ReadConsistency, 
                                        DoneCounts, NodeStatus, FailedCounts, 
                                        CommitCounts, CommitTS, AbortCounts, 
                                        SyncCounts, CurrentTime, 
                                        SystemTerminated, RecieveACKCount, 
                                        stack, cid_, t_, i1, j1, Test, 
                                        targetReplica_, s_msg_, t, msg_Ca, 
                                        cid_C, cid_Co, quorom_, minority, t_C, 
                                        i7, j7, commitTimestamp, 
                                        targetReplica_C, decision, 
                                        allSuccessful, s_msg_C, cid_Coo, 
                                        quorom, t_Co, targetReplica, cid_Coor, 
                                        msg_, type_, recoveryTid_, cid, msg_C, 
                                        type_C, recoveryTid, msg_Ha, id_H, i_, 
                                        j_, t_H, readTimestamp, readData, 
                                        res_msg_, msg_Han, id_Ha, i_H, j_H, 
                                        t_Ha, res_msg_H, msg_Hand, id_Han, 
                                        i_Ha, j_Ha, t_Han, res_msg_Ha, 
                                        readValue, readWTS, msg_Handl, id_Hand, 
                                        i_Han, j_Han, t_Hand, idx, res_msg_Han, 
                                        msg_Handle, id_Handl, i_Hand, j_Hand, 
                                        t_Handl, res_msg, msg, id_Handle, 
                                        id_HandleS, msg_H, i13, j13, id_Re, 
                                        msg_R, type_R, id_N, msg_N, type, id_C, 
                                        s_msg_Co, i8, j8, id, dataItemId, id_, 
                                        id_R >>

ProcHandleSyncB(self) == /\ pc[self] = "ProcHandleSyncB"
                         /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                         /\ i' = [i EXCEPT ![self] = Head(stack[self]).i]
                         /\ j' = [j EXCEPT ![self] = Head(stack[self]).j]
                         /\ msg' = [msg EXCEPT ![self] = Head(stack[self]).msg]
                         /\ id_Handle' = [id_Handle EXCEPT ![self] = Head(stack[self]).id_Handle]
                         /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                         /\ UNCHANGED << Replicas, Transactions, Messages, 
                                         CoordinatorMessages, WriteLocks, Logs, 
                                         TransactionStatusCoor, 
                                         TransactionStatus, CurrentTransaction, 
                                         SuccessCounts, FailureCounts, 
                                         ReadResults, ReadConsistency, 
                                         DoneCounts, NodeStatus, FailedCounts, 
                                         CommitCounts, CommitTS, AbortCounts, 
                                         SyncCounts, CurrentTime, 
                                         SystemTerminated, RecieveACKCount, 
                                         cid_, t_, i1, j1, Test, 
                                         targetReplica_, s_msg_, t, msg_Ca, 
                                         cid_C, cid_Co, quorom_, minority, t_C, 
                                         i7, j7, commitTimestamp, 
                                         targetReplica_C, decision, 
                                         allSuccessful, s_msg_C, cid_Coo, 
                                         quorom, t_Co, targetReplica, cid_Coor, 
                                         msg_, type_, recoveryTid_, cid, msg_C, 
                                         type_C, recoveryTid, msg_Ha, id_H, i_, 
                                         j_, t_H, readTimestamp, readData, 
                                         res_msg_, msg_Han, id_Ha, i_H, j_H, 
                                         t_Ha, res_msg_H, msg_Hand, id_Han, 
                                         i_Ha, j_Ha, t_Han, res_msg_Ha, 
                                         readValue, readWTS, msg_Handl, 
                                         id_Hand, i_Han, j_Han, t_Hand, idx, 
                                         res_msg_Han, msg_Handle, id_Handl, 
                                         i_Hand, j_Hand, t_Handl, res_msg, 
                                         id_HandleS, msg_H, i13, j13, id_Re, 
                                         msg_R, type_R, id_N, msg_N, type, 
                                         id_C, s_msg_Co, i8, j8, id, s_msg, 
                                         dataItemId, id_, id_R >>

HandleSync(self) == ProcHandleSync(self) \/ ProcHandleSyncB(self)

ProcHandleSyncAck(self) == /\ pc[self] = "ProcHandleSyncAck"
                           /\ IF \E m \in Messages : m.targetReplica = id_HandleS[self] /\ m.type = "SyncAck"
                                 THEN /\ msg_H' = [msg_H EXCEPT ![self] = CHOOSE m \in Messages : m.targetReplica = id_HandleS[self] /\ m.type = "SyncAck"]
                                      /\ Messages' = Messages \ {msg_H'[self]}
                                      /\ SyncCounts' = [SyncCounts EXCEPT ![id_HandleS[self]] = SyncCounts[id_HandleS[self]] + 1]
                                      /\ Logs' = [Logs EXCEPT ![id_HandleS[self]] = Logs[id_HandleS[self]] \cup msg_H'[self].Log]
                                      /\ i13' = [i13 EXCEPT ![self] = (id_HandleS[self] - 1) \div M + 1]
                                      /\ j13' = [j13 EXCEPT ![self] = ((id_HandleS[self] - 1) % M) + 1]
                                      /\ Replicas' = [Replicas EXCEPT ![i[self]][j[self]] = Replicas[i[self]][j[self]] \cup msg_H'[self].value]
                                      /\ IF SyncCounts'[id_HandleS[self]] >= Cardinality({i14 \in ((i13'[self] - 1) * M + 1)..(i13'[self] * M) : NodeStatus[i14] = "Active"})
                                            THEN /\ NodeStatus' = [NodeStatus EXCEPT ![id_HandleS[self]] = "Active"]
                                            ELSE /\ TRUE
                                                 /\ UNCHANGED NodeStatus
                                 ELSE /\ TRUE
                                      /\ UNCHANGED << Replicas, Messages, Logs, 
                                                      NodeStatus, SyncCounts, 
                                                      msg_H, i13, j13 >>
                           /\ pc' = [pc EXCEPT ![self] = "ProcHandleSyncAckB"]
                           /\ UNCHANGED << Transactions, CoordinatorMessages, 
                                           WriteLocks, TransactionStatusCoor, 
                                           TransactionStatus, 
                                           CurrentTransaction, SuccessCounts, 
                                           FailureCounts, ReadResults, 
                                           ReadConsistency, DoneCounts, 
                                           FailedCounts, CommitCounts, 
                                           CommitTS, AbortCounts, CurrentTime, 
                                           SystemTerminated, RecieveACKCount, 
                                           stack, cid_, t_, i1, j1, Test, 
                                           targetReplica_, s_msg_, t, msg_Ca, 
                                           cid_C, cid_Co, quorom_, minority, 
                                           t_C, i7, j7, commitTimestamp, 
                                           targetReplica_C, decision, 
                                           allSuccessful, s_msg_C, cid_Coo, 
                                           quorom, t_Co, targetReplica, 
                                           cid_Coor, msg_, type_, recoveryTid_, 
                                           cid, msg_C, type_C, recoveryTid, 
                                           msg_Ha, id_H, i_, j_, t_H, 
                                           readTimestamp, readData, res_msg_, 
                                           msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                           res_msg_H, msg_Hand, id_Han, i_Ha, 
                                           j_Ha, t_Han, res_msg_Ha, readValue, 
                                           readWTS, msg_Handl, id_Hand, i_Han, 
                                           j_Han, t_Hand, idx, res_msg_Han, 
                                           msg_Handle, id_Handl, i_Hand, 
                                           j_Hand, t_Handl, res_msg, msg, 
                                           id_Handle, i, j, id_HandleS, id_Re, 
                                           msg_R, type_R, id_N, msg_N, type, 
                                           id_C, s_msg_Co, i8, j8, id, s_msg, 
                                           dataItemId, id_, id_R >>

ProcHandleSyncAckB(self) == /\ pc[self] = "ProcHandleSyncAckB"
                            /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                            /\ msg_H' = [msg_H EXCEPT ![self] = Head(stack[self]).msg_H]
                            /\ i13' = [i13 EXCEPT ![self] = Head(stack[self]).i13]
                            /\ j13' = [j13 EXCEPT ![self] = Head(stack[self]).j13]
                            /\ id_HandleS' = [id_HandleS EXCEPT ![self] = Head(stack[self]).id_HandleS]
                            /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                            /\ UNCHANGED << Replicas, Transactions, Messages, 
                                            CoordinatorMessages, WriteLocks, 
                                            Logs, TransactionStatusCoor, 
                                            TransactionStatus, 
                                            CurrentTransaction, SuccessCounts, 
                                            FailureCounts, ReadResults, 
                                            ReadConsistency, DoneCounts, 
                                            NodeStatus, FailedCounts, 
                                            CommitCounts, CommitTS, 
                                            AbortCounts, SyncCounts, 
                                            CurrentTime, SystemTerminated, 
                                            RecieveACKCount, cid_, t_, i1, j1, 
                                            Test, targetReplica_, s_msg_, t, 
                                            msg_Ca, cid_C, cid_Co, quorom_, 
                                            minority, t_C, i7, j7, 
                                            commitTimestamp, targetReplica_C, 
                                            decision, allSuccessful, s_msg_C, 
                                            cid_Coo, quorom, t_Co, 
                                            targetReplica, cid_Coor, msg_, 
                                            type_, recoveryTid_, cid, msg_C, 
                                            type_C, recoveryTid, msg_Ha, id_H, 
                                            i_, j_, t_H, readTimestamp, 
                                            readData, res_msg_, msg_Han, id_Ha, 
                                            i_H, j_H, t_Ha, res_msg_H, 
                                            msg_Hand, id_Han, i_Ha, j_Ha, 
                                            t_Han, res_msg_Ha, readValue, 
                                            readWTS, msg_Handl, id_Hand, i_Han, 
                                            j_Han, t_Hand, idx, res_msg_Han, 
                                            msg_Handle, id_Handl, i_Hand, 
                                            j_Hand, t_Handl, res_msg, msg, 
                                            id_Handle, i, j, id_Re, msg_R, 
                                            type_R, id_N, msg_N, type, id_C, 
                                            s_msg_Co, i8, j8, id, s_msg, 
                                            dataItemId, id_, id_R >>

HandleSyncAck(self) == ProcHandleSyncAck(self) \/ ProcHandleSyncAckB(self)

ProcRepRecReq(self) == /\ pc[self] = "ProcRepRecReq"
                       /\ IF \E m \in Messages : m.targetReplica = id_Re[self] /\ m.type /= "SyncAck"
                             THEN /\ msg_R' = [msg_R EXCEPT ![self] = CHOOSE m \in Messages : m.targetReplica = id_Re[self] /\ m.type /= "SyncAck"]
                                  /\ Messages' = Messages \ {msg_R'[self]}
                                  /\ pc' = [pc EXCEPT ![self] = "ProcRepRecReqB"]
                             ELSE /\ pc' = [pc EXCEPT ![self] = "ProcRepRecReqC"]
                                  /\ UNCHANGED << Messages, msg_R >>
                       /\ UNCHANGED << Replicas, Transactions, 
                                       CoordinatorMessages, WriteLocks, Logs, 
                                       TransactionStatusCoor, 
                                       TransactionStatus, CurrentTransaction, 
                                       SuccessCounts, FailureCounts, 
                                       ReadResults, ReadConsistency, 
                                       DoneCounts, NodeStatus, FailedCounts, 
                                       CommitCounts, CommitTS, AbortCounts, 
                                       SyncCounts, CurrentTime, 
                                       SystemTerminated, RecieveACKCount, 
                                       stack, cid_, t_, i1, j1, Test, 
                                       targetReplica_, s_msg_, t, msg_Ca, 
                                       cid_C, cid_Co, quorom_, minority, t_C, 
                                       i7, j7, commitTimestamp, 
                                       targetReplica_C, decision, 
                                       allSuccessful, s_msg_C, cid_Coo, quorom, 
                                       t_Co, targetReplica, cid_Coor, msg_, 
                                       type_, recoveryTid_, cid, msg_C, type_C, 
                                       recoveryTid, msg_Ha, id_H, i_, j_, t_H, 
                                       readTimestamp, readData, res_msg_, 
                                       msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                       res_msg_H, msg_Hand, id_Han, i_Ha, j_Ha, 
                                       t_Han, res_msg_Ha, readValue, readWTS, 
                                       msg_Handl, id_Hand, i_Han, j_Han, 
                                       t_Hand, idx, res_msg_Han, msg_Handle, 
                                       id_Handl, i_Hand, j_Hand, t_Handl, 
                                       res_msg, msg, id_Handle, i, j, 
                                       id_HandleS, msg_H, i13, j13, id_Re, 
                                       type_R, id_N, msg_N, type, id_C, 
                                       s_msg_Co, i8, j8, id, s_msg, dataItemId, 
                                       id_, id_R >>

ProcRepRecReqB(self) == /\ pc[self] = "ProcRepRecReqB"
                        /\ IF msg_R[self].type = "Read"
                              THEN /\ /\ id_H' = [id_H EXCEPT ![self] = id_Re[self]]
                                      /\ msg_Ha' = [msg_Ha EXCEPT ![self] = msg_R[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HandleRead",
                                                                               pc        |->  "ProcRepRecReqC",
                                                                               i_        |->  i_[self],
                                                                               j_        |->  j_[self],
                                                                               t_H       |->  t_H[self],
                                                                               readTimestamp |->  readTimestamp[self],
                                                                               readData  |->  readData[self],
                                                                               res_msg_  |->  res_msg_[self],
                                                                               msg_Ha    |->  msg_Ha[self],
                                                                               id_H      |->  id_H[self] ] >>
                                                                           \o stack[self]]
                                   /\ i_' = [i_ EXCEPT ![self] = 0]
                                   /\ j_' = [j_ EXCEPT ![self] = 0]
                                   /\ t_H' = [t_H EXCEPT ![self] = 0]
                                   /\ readTimestamp' = [readTimestamp EXCEPT ![self] = 0]
                                   /\ readData' = [readData EXCEPT ![self] = {}]
                                   /\ res_msg_' = [res_msg_ EXCEPT ![self] = {}]
                                   /\ pc' = [pc EXCEPT ![self] = "ProcHandleRead"]
                                   /\ UNCHANGED << msg_Han, id_Ha, i_H, j_H, 
                                                   t_Ha, res_msg_H, msg_Hand, 
                                                   id_Han, i_Ha, j_Ha, t_Han, 
                                                   res_msg_Ha, readValue, 
                                                   readWTS, msg_Handl, id_Hand, 
                                                   i_Han, j_Han, t_Hand, idx, 
                                                   res_msg_Han, msg_Handle, 
                                                   id_Handl, i_Hand, j_Hand, 
                                                   t_Handl, res_msg, msg, 
                                                   id_Handle, i, j >>
                              ELSE /\ IF msg_R[self].type = "Write"
                                         THEN /\ /\ id_Ha' = [id_Ha EXCEPT ![self] = id_Re[self]]
                                                 /\ msg_Han' = [msg_Han EXCEPT ![self] = msg_R[self]]
                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HandleWrite",
                                                                                          pc        |->  "ProcRepRecReqC",
                                                                                          i_H       |->  i_H[self],
                                                                                          j_H       |->  j_H[self],
                                                                                          t_Ha      |->  t_Ha[self],
                                                                                          res_msg_H |->  res_msg_H[self],
                                                                                          msg_Han   |->  msg_Han[self],
                                                                                          id_Ha     |->  id_Ha[self] ] >>
                                                                                      \o stack[self]]
                                              /\ i_H' = [i_H EXCEPT ![self] = 0]
                                              /\ j_H' = [j_H EXCEPT ![self] = 0]
                                              /\ t_Ha' = [t_Ha EXCEPT ![self] = 0]
                                              /\ res_msg_H' = [res_msg_H EXCEPT ![self] = {}]
                                              /\ pc' = [pc EXCEPT ![self] = "ProcHandleWrite"]
                                              /\ UNCHANGED << msg_Hand, id_Han, 
                                                              i_Ha, j_Ha, 
                                                              t_Han, 
                                                              res_msg_Ha, 
                                                              readValue, 
                                                              readWTS, 
                                                              msg_Handl, 
                                                              id_Hand, i_Han, 
                                                              j_Han, t_Hand, 
                                                              idx, res_msg_Han, 
                                                              msg_Handle, 
                                                              id_Handl, i_Hand, 
                                                              j_Hand, t_Handl, 
                                                              res_msg, msg, 
                                                              id_Handle, i, j >>
                                         ELSE /\ IF msg_R[self].type = "Commit"
                                                    THEN /\ /\ id_Hand' = [id_Hand EXCEPT ![self] = id_Re[self]]
                                                            /\ msg_Handl' = [msg_Handl EXCEPT ![self] = msg_R[self]]
                                                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HandleCommit",
                                                                                                     pc        |->  "ProcRepRecReqC",
                                                                                                     i_Han     |->  i_Han[self],
                                                                                                     j_Han     |->  j_Han[self],
                                                                                                     t_Hand    |->  t_Hand[self],
                                                                                                     idx       |->  idx[self],
                                                                                                     res_msg_Han |->  res_msg_Han[self],
                                                                                                     msg_Handl |->  msg_Handl[self],
                                                                                                     id_Hand   |->  id_Hand[self] ] >>
                                                                                                 \o stack[self]]
                                                         /\ i_Han' = [i_Han EXCEPT ![self] = 0]
                                                         /\ j_Han' = [j_Han EXCEPT ![self] = 0]
                                                         /\ t_Hand' = [t_Hand EXCEPT ![self] = 0]
                                                         /\ idx' = [idx EXCEPT ![self] = 0]
                                                         /\ res_msg_Han' = [res_msg_Han EXCEPT ![self] = {}]
                                                         /\ pc' = [pc EXCEPT ![self] = "ProcHandleCommit"]
                                                         /\ UNCHANGED << msg_Hand, 
                                                                         id_Han, 
                                                                         i_Ha, 
                                                                         j_Ha, 
                                                                         t_Han, 
                                                                         res_msg_Ha, 
                                                                         readValue, 
                                                                         readWTS, 
                                                                         msg_Handle, 
                                                                         id_Handl, 
                                                                         i_Hand, 
                                                                         j_Hand, 
                                                                         t_Handl, 
                                                                         res_msg, 
                                                                         msg, 
                                                                         id_Handle, 
                                                                         i, j >>
                                                    ELSE /\ IF msg_R[self].type = "Abort"
                                                               THEN /\ /\ id_Handl' = [id_Handl EXCEPT ![self] = id_Re[self]]
                                                                       /\ msg_Handle' = [msg_Handle EXCEPT ![self] = msg_R[self]]
                                                                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HandleAbort",
                                                                                                                pc        |->  "ProcRepRecReqC",
                                                                                                                i_Hand    |->  i_Hand[self],
                                                                                                                j_Hand    |->  j_Hand[self],
                                                                                                                t_Handl   |->  t_Handl[self],
                                                                                                                res_msg   |->  res_msg[self],
                                                                                                                msg_Handle |->  msg_Handle[self],
                                                                                                                id_Handl  |->  id_Handl[self] ] >>
                                                                                                            \o stack[self]]
                                                                    /\ i_Hand' = [i_Hand EXCEPT ![self] = 0]
                                                                    /\ j_Hand' = [j_Hand EXCEPT ![self] = 0]
                                                                    /\ t_Handl' = [t_Handl EXCEPT ![self] = 0]
                                                                    /\ res_msg' = [res_msg EXCEPT ![self] = {}]
                                                                    /\ pc' = [pc EXCEPT ![self] = "ProcHandleAbort"]
                                                                    /\ UNCHANGED << msg_Hand, 
                                                                                    id_Han, 
                                                                                    i_Ha, 
                                                                                    j_Ha, 
                                                                                    t_Han, 
                                                                                    res_msg_Ha, 
                                                                                    readValue, 
                                                                                    readWTS, 
                                                                                    msg, 
                                                                                    id_Handle, 
                                                                                    i, 
                                                                                    j >>
                                                               ELSE /\ IF msg_R[self].type = "Inquire"
                                                                          THEN /\ /\ id_Han' = [id_Han EXCEPT ![self] = id_Re[self]]
                                                                                  /\ msg_Hand' = [msg_Hand EXCEPT ![self] = msg_R[self]]
                                                                                  /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HandleInquire",
                                                                                                                           pc        |->  "ProcRepRecReqC",
                                                                                                                           i_Ha      |->  i_Ha[self],
                                                                                                                           j_Ha      |->  j_Ha[self],
                                                                                                                           t_Han     |->  t_Han[self],
                                                                                                                           res_msg_Ha |->  res_msg_Ha[self],
                                                                                                                           readValue |->  readValue[self],
                                                                                                                           readWTS   |->  readWTS[self],
                                                                                                                           msg_Hand  |->  msg_Hand[self],
                                                                                                                           id_Han    |->  id_Han[self] ] >>
                                                                                                                       \o stack[self]]
                                                                               /\ i_Ha' = [i_Ha EXCEPT ![self] = 0]
                                                                               /\ j_Ha' = [j_Ha EXCEPT ![self] = 0]
                                                                               /\ t_Han' = [t_Han EXCEPT ![self] = 0]
                                                                               /\ res_msg_Ha' = [res_msg_Ha EXCEPT ![self] = {}]
                                                                               /\ readValue' = [readValue EXCEPT ![self] = 0]
                                                                               /\ readWTS' = [readWTS EXCEPT ![self] = 0]
                                                                               /\ pc' = [pc EXCEPT ![self] = "ProcHandleInquire"]
                                                                               /\ UNCHANGED << msg, 
                                                                                               id_Handle, 
                                                                                               i, 
                                                                                               j >>
                                                                          ELSE /\ IF msg_R[self].type = "Sync"
                                                                                     THEN /\ /\ id_Handle' = [id_Handle EXCEPT ![self] = id_Re[self]]
                                                                                             /\ msg' = [msg EXCEPT ![self] = msg_R[self]]
                                                                                             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HandleSync",
                                                                                                                                      pc        |->  "ProcRepRecReqC",
                                                                                                                                      i         |->  i[self],
                                                                                                                                      j         |->  j[self],
                                                                                                                                      msg       |->  msg[self],
                                                                                                                                      id_Handle |->  id_Handle[self] ] >>
                                                                                                                                  \o stack[self]]
                                                                                          /\ i' = [i EXCEPT ![self] = defaultInitValue]
                                                                                          /\ j' = [j EXCEPT ![self] = defaultInitValue]
                                                                                          /\ pc' = [pc EXCEPT ![self] = "ProcHandleSync"]
                                                                                     ELSE /\ pc' = [pc EXCEPT ![self] = "ProcRepRecReqC"]
                                                                                          /\ UNCHANGED << stack, 
                                                                                                          msg, 
                                                                                                          id_Handle, 
                                                                                                          i, 
                                                                                                          j >>
                                                                               /\ UNCHANGED << msg_Hand, 
                                                                                               id_Han, 
                                                                                               i_Ha, 
                                                                                               j_Ha, 
                                                                                               t_Han, 
                                                                                               res_msg_Ha, 
                                                                                               readValue, 
                                                                                               readWTS >>
                                                                    /\ UNCHANGED << msg_Handle, 
                                                                                    id_Handl, 
                                                                                    i_Hand, 
                                                                                    j_Hand, 
                                                                                    t_Handl, 
                                                                                    res_msg >>
                                                         /\ UNCHANGED << msg_Handl, 
                                                                         id_Hand, 
                                                                         i_Han, 
                                                                         j_Han, 
                                                                         t_Hand, 
                                                                         idx, 
                                                                         res_msg_Han >>
                                              /\ UNCHANGED << msg_Han, id_Ha, 
                                                              i_H, j_H, t_Ha, 
                                                              res_msg_H >>
                                   /\ UNCHANGED << msg_Ha, id_H, i_, j_, t_H, 
                                                   readTimestamp, readData, 
                                                   res_msg_ >>
                        /\ UNCHANGED << Replicas, Transactions, Messages, 
                                        CoordinatorMessages, WriteLocks, Logs, 
                                        TransactionStatusCoor, 
                                        TransactionStatus, CurrentTransaction, 
                                        SuccessCounts, FailureCounts, 
                                        ReadResults, ReadConsistency, 
                                        DoneCounts, NodeStatus, FailedCounts, 
                                        CommitCounts, CommitTS, AbortCounts, 
                                        SyncCounts, CurrentTime, 
                                        SystemTerminated, RecieveACKCount, 
                                        cid_, t_, i1, j1, Test, targetReplica_, 
                                        s_msg_, t, msg_Ca, cid_C, cid_Co, 
                                        quorom_, minority, t_C, i7, j7, 
                                        commitTimestamp, targetReplica_C, 
                                        decision, allSuccessful, s_msg_C, 
                                        cid_Coo, quorom, t_Co, targetReplica, 
                                        cid_Coor, msg_, type_, recoveryTid_, 
                                        cid, msg_C, type_C, recoveryTid, 
                                        id_HandleS, msg_H, i13, j13, id_Re, 
                                        msg_R, type_R, id_N, msg_N, type, id_C, 
                                        s_msg_Co, i8, j8, id, s_msg, 
                                        dataItemId, id_, id_R >>

ProcRepRecReqC(self) == /\ pc[self] = "ProcRepRecReqC"
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ msg_R' = [msg_R EXCEPT ![self] = Head(stack[self]).msg_R]
                        /\ type_R' = [type_R EXCEPT ![self] = Head(stack[self]).type_R]
                        /\ id_Re' = [id_Re EXCEPT ![self] = Head(stack[self]).id_Re]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << Replicas, Transactions, Messages, 
                                        CoordinatorMessages, WriteLocks, Logs, 
                                        TransactionStatusCoor, 
                                        TransactionStatus, CurrentTransaction, 
                                        SuccessCounts, FailureCounts, 
                                        ReadResults, ReadConsistency, 
                                        DoneCounts, NodeStatus, FailedCounts, 
                                        CommitCounts, CommitTS, AbortCounts, 
                                        SyncCounts, CurrentTime, 
                                        SystemTerminated, RecieveACKCount, 
                                        cid_, t_, i1, j1, Test, targetReplica_, 
                                        s_msg_, t, msg_Ca, cid_C, cid_Co, 
                                        quorom_, minority, t_C, i7, j7, 
                                        commitTimestamp, targetReplica_C, 
                                        decision, allSuccessful, s_msg_C, 
                                        cid_Coo, quorom, t_Co, targetReplica, 
                                        cid_Coor, msg_, type_, recoveryTid_, 
                                        cid, msg_C, type_C, recoveryTid, 
                                        msg_Ha, id_H, i_, j_, t_H, 
                                        readTimestamp, readData, res_msg_, 
                                        msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                        res_msg_H, msg_Hand, id_Han, i_Ha, 
                                        j_Ha, t_Han, res_msg_Ha, readValue, 
                                        readWTS, msg_Handl, id_Hand, i_Han, 
                                        j_Han, t_Hand, idx, res_msg_Han, 
                                        msg_Handle, id_Handl, i_Hand, j_Hand, 
                                        t_Handl, res_msg, msg, id_Handle, i, j, 
                                        id_HandleS, msg_H, i13, j13, id_N, 
                                        msg_N, type, id_C, s_msg_Co, i8, j8, 
                                        id, s_msg, dataItemId, id_, id_R >>

ReplicaReceiveRequest(self) == ProcRepRecReq(self) \/ ProcRepRecReqB(self)
                                  \/ ProcRepRecReqC(self)

ProcSetNodeFail(self) == /\ pc[self] = "ProcSetNodeFail"
                         /\ IF FailedN /= 0 /\ id_N[self] = FailedN /\ FailedCounts[id_N[self]] < FCount
                               THEN /\ NodeStatus' = [NodeStatus EXCEPT ![id_N[self]] = "Failed"]
                                    /\ FailedCounts' = [FailedCounts EXCEPT ![id_N[self]] = FailedCounts[id_N[self]] + 1]
                                    /\ TransactionStatus' = [TransactionStatus EXCEPT ![id_N[self]] = [tid \in 1..T |-> [status |-> "NotStarted", CommitTS |-> 0]]]
                                    /\ \E i12 \in 1..N:
                                         WriteLocks' = [WriteLocks EXCEPT ![i12][id_N[self]] = FALSE]
                                    /\ Logs' = [Logs EXCEPT ![id_N[self]] = {}]
                                    /\ UNCHANGED << TransactionStatusCoor, 
                                                    CurrentTransaction, 
                                                    SuccessCounts, 
                                                    FailureCounts, ReadResults, 
                                                    ReadConsistency, 
                                                    DoneCounts, CommitCounts, 
                                                    CommitTS, AbortCounts >>
                               ELSE /\ IF FailedC /= 0 /\ id_N[self] = FailedC + (N * M) /\ FailedCounts[id_N[self]] < FCount
                                          THEN /\ NodeStatus' = [NodeStatus EXCEPT ![id_N[self]] = "Failed"]
                                               /\ FailedCounts' = [FailedCounts EXCEPT ![id_N[self]] = FailedCounts[id_N[self]] + 1]
                                               /\ CurrentTransaction' = [CurrentTransaction EXCEPT ![id_N[self]] = 0]
                                               /\ SuccessCounts' = [SuccessCounts EXCEPT ![id_N[self]] = [item \in 1..N |-> 0]]
                                               /\ FailureCounts' = [FailureCounts EXCEPT ![id_N[self]] = [item \in 1..N |-> 0]]
                                               /\ ReadResults' = [ReadResults EXCEPT ![id_N[self]] = [tid \in 1..T |-> [item \in 1..N |-> {}]]]
                                               /\ ReadConsistency' = [ReadConsistency EXCEPT ![id_N[self]] = TRUE]
                                               /\ DoneCounts' = [DoneCounts EXCEPT ![id_N[self]] = [item \in 1..N |-> 0]]
                                               /\ CommitCounts' = [CommitCounts EXCEPT ![id_N[self]] = 0]
                                               /\ CommitTS' = [CommitTS EXCEPT ![id_N[self]] = 0]
                                               /\ AbortCounts' = [AbortCounts EXCEPT ![id_N[self]] = 0]
                                               /\ TransactionStatusCoor' = [TransactionStatusCoor EXCEPT ![id_N[self]] = [ tid \in 1..T
                                                                                                                         |-> IF TransactionStatusCoor[id_N[self]][tid].status /= "Finished" /\ TransactionStatusCoor[id_N[self]][tid].status /= "NotStarted"
                                                                                                                             THEN [TransactionStatusCoor[id_N[self]][tid] EXCEPT !.status = "Recover"]
                                                                                                                             ELSE TransactionStatusCoor[id_N[self]][tid] ]]
                                          ELSE /\ TRUE
                                               /\ UNCHANGED << TransactionStatusCoor, 
                                                               CurrentTransaction, 
                                                               SuccessCounts, 
                                                               FailureCounts, 
                                                               ReadResults, 
                                                               ReadConsistency, 
                                                               DoneCounts, 
                                                               NodeStatus, 
                                                               FailedCounts, 
                                                               CommitCounts, 
                                                               CommitTS, 
                                                               AbortCounts >>
                                    /\ UNCHANGED << WriteLocks, Logs, 
                                                    TransactionStatus >>
                         /\ pc' = [pc EXCEPT ![self] = "ProcSetNodeFailB"]
                         /\ UNCHANGED << Replicas, Transactions, Messages, 
                                         CoordinatorMessages, SyncCounts, 
                                         CurrentTime, SystemTerminated, 
                                         RecieveACKCount, stack, cid_, t_, i1, 
                                         j1, Test, targetReplica_, s_msg_, t, 
                                         msg_Ca, cid_C, cid_Co, quorom_, 
                                         minority, t_C, i7, j7, 
                                         commitTimestamp, targetReplica_C, 
                                         decision, allSuccessful, s_msg_C, 
                                         cid_Coo, quorom, t_Co, targetReplica, 
                                         cid_Coor, msg_, type_, recoveryTid_, 
                                         cid, msg_C, type_C, recoveryTid, 
                                         msg_Ha, id_H, i_, j_, t_H, 
                                         readTimestamp, readData, res_msg_, 
                                         msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                         res_msg_H, msg_Hand, id_Han, i_Ha, 
                                         j_Ha, t_Han, res_msg_Ha, readValue, 
                                         readWTS, msg_Handl, id_Hand, i_Han, 
                                         j_Han, t_Hand, idx, res_msg_Han, 
                                         msg_Handle, id_Handl, i_Hand, j_Hand, 
                                         t_Handl, res_msg, msg, id_Handle, i, 
                                         j, id_HandleS, msg_H, i13, j13, id_Re, 
                                         msg_R, type_R, id_N, msg_N, type, 
                                         id_C, s_msg_Co, i8, j8, id, s_msg, 
                                         dataItemId, id_, id_R >>

ProcSetNodeFailB(self) == /\ pc[self] = "ProcSetNodeFailB"
                          /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                          /\ msg_N' = [msg_N EXCEPT ![self] = Head(stack[self]).msg_N]
                          /\ type' = [type EXCEPT ![self] = Head(stack[self]).type]
                          /\ id_N' = [id_N EXCEPT ![self] = Head(stack[self]).id_N]
                          /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                          /\ UNCHANGED << Replicas, Transactions, Messages, 
                                          CoordinatorMessages, WriteLocks, 
                                          Logs, TransactionStatusCoor, 
                                          TransactionStatus, 
                                          CurrentTransaction, SuccessCounts, 
                                          FailureCounts, ReadResults, 
                                          ReadConsistency, DoneCounts, 
                                          NodeStatus, FailedCounts, 
                                          CommitCounts, CommitTS, AbortCounts, 
                                          SyncCounts, CurrentTime, 
                                          SystemTerminated, RecieveACKCount, 
                                          cid_, t_, i1, j1, Test, 
                                          targetReplica_, s_msg_, t, msg_Ca, 
                                          cid_C, cid_Co, quorom_, minority, 
                                          t_C, i7, j7, commitTimestamp, 
                                          targetReplica_C, decision, 
                                          allSuccessful, s_msg_C, cid_Coo, 
                                          quorom, t_Co, targetReplica, 
                                          cid_Coor, msg_, type_, recoveryTid_, 
                                          cid, msg_C, type_C, recoveryTid, 
                                          msg_Ha, id_H, i_, j_, t_H, 
                                          readTimestamp, readData, res_msg_, 
                                          msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                          res_msg_H, msg_Hand, id_Han, i_Ha, 
                                          j_Ha, t_Han, res_msg_Ha, readValue, 
                                          readWTS, msg_Handl, id_Hand, i_Han, 
                                          j_Han, t_Hand, idx, res_msg_Han, 
                                          msg_Handle, id_Handl, i_Hand, j_Hand, 
                                          t_Handl, res_msg, msg, id_Handle, i, 
                                          j, id_HandleS, msg_H, i13, j13, 
                                          id_Re, msg_R, type_R, id_C, s_msg_Co, 
                                          i8, j8, id, s_msg, dataItemId, id_, 
                                          id_R >>

NodeFail(self) == ProcSetNodeFail(self) \/ ProcSetNodeFailB(self)

ProcCoordinatorRecover(self) == /\ pc[self] = "ProcCoordinatorRecover"
                                /\ NodeStatus' = [NodeStatus EXCEPT ![id_C[self]] = "Recover"]
                                /\ IF \E tid \in 1..T : Transactions[tid].coordinator_id = id_C[self] /\ TransactionStatusCoor[id_C[self]][tid].status = "Recover"
                                      THEN /\ t' = [t EXCEPT ![self] = CHOOSE tid \in 1..T: Transactions[tid].coordinator_id = id_C[self] /\ TransactionStatusCoor[id_C[self]][tid].status = "Recover"]
                                           /\ CurrentTransaction' = [CurrentTransaction EXCEPT ![id_C[self]] = t'[self]]
                                           /\ pc' = [pc EXCEPT ![self] = "ProcCoordinatorRecoverOuterLoop"]
                                      ELSE /\ pc' = [pc EXCEPT ![self] = "ProcCoordinatorRecoverB"]
                                           /\ UNCHANGED << CurrentTransaction, 
                                                           t >>
                                /\ UNCHANGED << Replicas, Transactions, 
                                                Messages, CoordinatorMessages, 
                                                WriteLocks, Logs, 
                                                TransactionStatusCoor, 
                                                TransactionStatus, 
                                                SuccessCounts, FailureCounts, 
                                                ReadResults, ReadConsistency, 
                                                DoneCounts, FailedCounts, 
                                                CommitCounts, CommitTS, 
                                                AbortCounts, SyncCounts, 
                                                CurrentTime, SystemTerminated, 
                                                RecieveACKCount, stack, cid_, 
                                                t_, i1, j1, Test, 
                                                targetReplica_, s_msg_, msg_Ca, 
                                                cid_C, cid_Co, quorom_, 
                                                minority, t_C, i7, j7, 
                                                commitTimestamp, 
                                                targetReplica_C, decision, 
                                                allSuccessful, s_msg_C, 
                                                cid_Coo, quorom, t_Co, 
                                                targetReplica, cid_Coor, msg_, 
                                                type_, recoveryTid_, cid, 
                                                msg_C, type_C, recoveryTid, 
                                                msg_Ha, id_H, i_, j_, t_H, 
                                                readTimestamp, readData, 
                                                res_msg_, msg_Han, id_Ha, i_H, 
                                                j_H, t_Ha, res_msg_H, msg_Hand, 
                                                id_Han, i_Ha, j_Ha, t_Han, 
                                                res_msg_Ha, readValue, readWTS, 
                                                msg_Handl, id_Hand, i_Han, 
                                                j_Han, t_Hand, idx, 
                                                res_msg_Han, msg_Handle, 
                                                id_Handl, i_Hand, j_Hand, 
                                                t_Handl, res_msg, msg, 
                                                id_Handle, i, j, id_HandleS, 
                                                msg_H, i13, j13, id_Re, msg_R, 
                                                type_R, id_N, msg_N, type, 
                                                id_C, s_msg_Co, i8, j8, id, 
                                                s_msg, dataItemId, id_, id_R >>

ProcCoordinatorRecoverOuterLoop(self) == /\ pc[self] = "ProcCoordinatorRecoverOuterLoop"
                                         /\ IF i8[self] < N
                                               THEN /\ i8' = [i8 EXCEPT ![self] = i8[self] + 1]
                                                    /\ IF i8'[self] \in Transactions[t[self]].read \cup Transactions[t[self]].write
                                                          THEN /\ j8' = [j8 EXCEPT ![self] = 0]
                                                               /\ pc' = [pc EXCEPT ![self] = "ProcCoordinatorRecoverInnerLoop"]
                                                          ELSE /\ pc' = [pc EXCEPT ![self] = "ProcCoordinatorRecoverOuterLoop"]
                                                               /\ j8' = j8
                                               ELSE /\ pc' = [pc EXCEPT ![self] = "ProcCoordinatorRecoverB"]
                                                    /\ UNCHANGED << i8, j8 >>
                                         /\ UNCHANGED << Replicas, 
                                                         Transactions, 
                                                         Messages, 
                                                         CoordinatorMessages, 
                                                         WriteLocks, Logs, 
                                                         TransactionStatusCoor, 
                                                         TransactionStatus, 
                                                         CurrentTransaction, 
                                                         SuccessCounts, 
                                                         FailureCounts, 
                                                         ReadResults, 
                                                         ReadConsistency, 
                                                         DoneCounts, 
                                                         NodeStatus, 
                                                         FailedCounts, 
                                                         CommitCounts, 
                                                         CommitTS, AbortCounts, 
                                                         SyncCounts, 
                                                         CurrentTime, 
                                                         SystemTerminated, 
                                                         RecieveACKCount, 
                                                         stack, cid_, t_, i1, 
                                                         j1, Test, 
                                                         targetReplica_, 
                                                         s_msg_, t, msg_Ca, 
                                                         cid_C, cid_Co, 
                                                         quorom_, minority, 
                                                         t_C, i7, j7, 
                                                         commitTimestamp, 
                                                         targetReplica_C, 
                                                         decision, 
                                                         allSuccessful, 
                                                         s_msg_C, cid_Coo, 
                                                         quorom, t_Co, 
                                                         targetReplica, 
                                                         cid_Coor, msg_, type_, 
                                                         recoveryTid_, cid, 
                                                         msg_C, type_C, 
                                                         recoveryTid, msg_Ha, 
                                                         id_H, i_, j_, t_H, 
                                                         readTimestamp, 
                                                         readData, res_msg_, 
                                                         msg_Han, id_Ha, i_H, 
                                                         j_H, t_Ha, res_msg_H, 
                                                         msg_Hand, id_Han, 
                                                         i_Ha, j_Ha, t_Han, 
                                                         res_msg_Ha, readValue, 
                                                         readWTS, msg_Handl, 
                                                         id_Hand, i_Han, j_Han, 
                                                         t_Hand, idx, 
                                                         res_msg_Han, 
                                                         msg_Handle, id_Handl, 
                                                         i_Hand, j_Hand, 
                                                         t_Handl, res_msg, msg, 
                                                         id_Handle, i, j, 
                                                         id_HandleS, msg_H, 
                                                         i13, j13, id_Re, 
                                                         msg_R, type_R, id_N, 
                                                         msg_N, type, id_C, 
                                                         s_msg_Co, id, s_msg, 
                                                         dataItemId, id_, id_R >>

ProcCoordinatorRecoverInnerLoop(self) == /\ pc[self] = "ProcCoordinatorRecoverInnerLoop"
                                         /\ IF j8[self] < M
                                               THEN /\ j8' = [j8 EXCEPT ![self] = j8[self] + 1]
                                                    /\ targetReplica' = [targetReplica EXCEPT ![self] = ((i8[self] - 1) * M) + j8'[self]]
                                                    /\ s_msg_Co' = [s_msg_Co EXCEPT ![self] = [item |-> i8[self], replica |-> j8'[self], transaction |-> t[self], type |-> "Inquire", timestamp |-> 0, coordinator_id |-> Transactions[t[self]].coordinator_id, sendTime |-> CurrentTime, targetReplica |-> targetReplica'[self]]]
                                                    /\ Messages' = (Messages \cup {s_msg_Co'[self]})
                                                    /\ pc' = [pc EXCEPT ![self] = "ProcCoordinatorRecoverInnerLoop"]
                                               ELSE /\ pc' = [pc EXCEPT ![self] = "ProcCoordinatorRecoverOuterLoop"]
                                                    /\ UNCHANGED << Messages, 
                                                                    targetReplica, 
                                                                    s_msg_Co, 
                                                                    j8 >>
                                         /\ UNCHANGED << Replicas, 
                                                         Transactions, 
                                                         CoordinatorMessages, 
                                                         WriteLocks, Logs, 
                                                         TransactionStatusCoor, 
                                                         TransactionStatus, 
                                                         CurrentTransaction, 
                                                         SuccessCounts, 
                                                         FailureCounts, 
                                                         ReadResults, 
                                                         ReadConsistency, 
                                                         DoneCounts, 
                                                         NodeStatus, 
                                                         FailedCounts, 
                                                         CommitCounts, 
                                                         CommitTS, AbortCounts, 
                                                         SyncCounts, 
                                                         CurrentTime, 
                                                         SystemTerminated, 
                                                         RecieveACKCount, 
                                                         stack, cid_, t_, i1, 
                                                         j1, Test, 
                                                         targetReplica_, 
                                                         s_msg_, t, msg_Ca, 
                                                         cid_C, cid_Co, 
                                                         quorom_, minority, 
                                                         t_C, i7, j7, 
                                                         commitTimestamp, 
                                                         targetReplica_C, 
                                                         decision, 
                                                         allSuccessful, 
                                                         s_msg_C, cid_Coo, 
                                                         quorom, t_Co, 
                                                         cid_Coor, msg_, type_, 
                                                         recoveryTid_, cid, 
                                                         msg_C, type_C, 
                                                         recoveryTid, msg_Ha, 
                                                         id_H, i_, j_, t_H, 
                                                         readTimestamp, 
                                                         readData, res_msg_, 
                                                         msg_Han, id_Ha, i_H, 
                                                         j_H, t_Ha, res_msg_H, 
                                                         msg_Hand, id_Han, 
                                                         i_Ha, j_Ha, t_Han, 
                                                         res_msg_Ha, readValue, 
                                                         readWTS, msg_Handl, 
                                                         id_Hand, i_Han, j_Han, 
                                                         t_Hand, idx, 
                                                         res_msg_Han, 
                                                         msg_Handle, id_Handl, 
                                                         i_Hand, j_Hand, 
                                                         t_Handl, res_msg, msg, 
                                                         id_Handle, i, j, 
                                                         id_HandleS, msg_H, 
                                                         i13, j13, id_Re, 
                                                         msg_R, type_R, id_N, 
                                                         msg_N, type, id_C, i8, 
                                                         id, s_msg, dataItemId, 
                                                         id_, id_R >>

ProcCoordinatorRecoverB(self) == /\ pc[self] = "ProcCoordinatorRecoverB"
                                 /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                 /\ s_msg_Co' = [s_msg_Co EXCEPT ![self] = Head(stack[self]).s_msg_Co]
                                 /\ i8' = [i8 EXCEPT ![self] = Head(stack[self]).i8]
                                 /\ j8' = [j8 EXCEPT ![self] = Head(stack[self]).j8]
                                 /\ id_C' = [id_C EXCEPT ![self] = Head(stack[self]).id_C]
                                 /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                 /\ UNCHANGED << Replicas, Transactions, 
                                                 Messages, CoordinatorMessages, 
                                                 WriteLocks, Logs, 
                                                 TransactionStatusCoor, 
                                                 TransactionStatus, 
                                                 CurrentTransaction, 
                                                 SuccessCounts, FailureCounts, 
                                                 ReadResults, ReadConsistency, 
                                                 DoneCounts, NodeStatus, 
                                                 FailedCounts, CommitCounts, 
                                                 CommitTS, AbortCounts, 
                                                 SyncCounts, CurrentTime, 
                                                 SystemTerminated, 
                                                 RecieveACKCount, cid_, t_, i1, 
                                                 j1, Test, targetReplica_, 
                                                 s_msg_, t, msg_Ca, cid_C, 
                                                 cid_Co, quorom_, minority, 
                                                 t_C, i7, j7, commitTimestamp, 
                                                 targetReplica_C, decision, 
                                                 allSuccessful, s_msg_C, 
                                                 cid_Coo, quorom, t_Co, 
                                                 targetReplica, cid_Coor, msg_, 
                                                 type_, recoveryTid_, cid, 
                                                 msg_C, type_C, recoveryTid, 
                                                 msg_Ha, id_H, i_, j_, t_H, 
                                                 readTimestamp, readData, 
                                                 res_msg_, msg_Han, id_Ha, i_H, 
                                                 j_H, t_Ha, res_msg_H, 
                                                 msg_Hand, id_Han, i_Ha, j_Ha, 
                                                 t_Han, res_msg_Ha, readValue, 
                                                 readWTS, msg_Handl, id_Hand, 
                                                 i_Han, j_Han, t_Hand, idx, 
                                                 res_msg_Han, msg_Handle, 
                                                 id_Handl, i_Hand, j_Hand, 
                                                 t_Handl, res_msg, msg, 
                                                 id_Handle, i, j, id_HandleS, 
                                                 msg_H, i13, j13, id_Re, msg_R, 
                                                 type_R, id_N, msg_N, type, id, 
                                                 s_msg, dataItemId, id_, id_R >>

CoordinatorRecover(self) == ProcCoordinatorRecover(self)
                               \/ ProcCoordinatorRecoverOuterLoop(self)
                               \/ ProcCoordinatorRecoverInnerLoop(self)
                               \/ ProcCoordinatorRecoverB(self)

ProcReplicaRecover(self) == /\ pc[self] = "ProcReplicaRecover"
                            /\ NodeStatus' = [NodeStatus EXCEPT ![id[self]] = "Recover"]
                            /\ dataItemId' = [dataItemId EXCEPT ![self] = (id[self] - 1) \div M + 1]
                            /\ \E j11 \in 1..M:
                                 /\ targetReplica' = [targetReplica EXCEPT ![self] = ((dataItemId'[self] - 1) * M) + j11]
                                 /\ IF targetReplica'[self] /= id[self]
                                       THEN /\ s_msg' = [s_msg EXCEPT ![self] = [type |-> "Sync",
                                                                                         item |-> dataItemId'[self],
                                                                                         replica |-> j11,
                                                                                         sendTime |-> CurrentTime,
                                                                                         targetReplica |-> targetReplica'[self]]]
                                            /\ Messages' = (Messages \cup {s_msg'[self]})
                                       ELSE /\ TRUE
                                            /\ UNCHANGED << Messages, s_msg >>
                            /\ pc' = [pc EXCEPT ![self] = "ProcReplicaRecoverB"]
                            /\ UNCHANGED << Replicas, Transactions, 
                                            CoordinatorMessages, WriteLocks, 
                                            Logs, TransactionStatusCoor, 
                                            TransactionStatus, 
                                            CurrentTransaction, SuccessCounts, 
                                            FailureCounts, ReadResults, 
                                            ReadConsistency, DoneCounts, 
                                            FailedCounts, CommitCounts, 
                                            CommitTS, AbortCounts, SyncCounts, 
                                            CurrentTime, SystemTerminated, 
                                            RecieveACKCount, stack, cid_, t_, 
                                            i1, j1, Test, targetReplica_, 
                                            s_msg_, t, msg_Ca, cid_C, cid_Co, 
                                            quorom_, minority, t_C, i7, j7, 
                                            commitTimestamp, targetReplica_C, 
                                            decision, allSuccessful, s_msg_C, 
                                            cid_Coo, quorom, t_Co, cid_Coor, 
                                            msg_, type_, recoveryTid_, cid, 
                                            msg_C, type_C, recoveryTid, msg_Ha, 
                                            id_H, i_, j_, t_H, readTimestamp, 
                                            readData, res_msg_, msg_Han, id_Ha, 
                                            i_H, j_H, t_Ha, res_msg_H, 
                                            msg_Hand, id_Han, i_Ha, j_Ha, 
                                            t_Han, res_msg_Ha, readValue, 
                                            readWTS, msg_Handl, id_Hand, i_Han, 
                                            j_Han, t_Hand, idx, res_msg_Han, 
                                            msg_Handle, id_Handl, i_Hand, 
                                            j_Hand, t_Handl, res_msg, msg, 
                                            id_Handle, i, j, id_HandleS, msg_H, 
                                            i13, j13, id_Re, msg_R, type_R, 
                                            id_N, msg_N, type, id_C, s_msg_Co, 
                                            i8, j8, id, id_, id_R >>

ProcReplicaRecoverB(self) == /\ pc[self] = "ProcReplicaRecoverB"
                             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                             /\ s_msg' = [s_msg EXCEPT ![self] = Head(stack[self]).s_msg]
                             /\ dataItemId' = [dataItemId EXCEPT ![self] = Head(stack[self]).dataItemId]
                             /\ id' = [id EXCEPT ![self] = Head(stack[self]).id]
                             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                             /\ UNCHANGED << Replicas, Transactions, Messages, 
                                             CoordinatorMessages, WriteLocks, 
                                             Logs, TransactionStatusCoor, 
                                             TransactionStatus, 
                                             CurrentTransaction, SuccessCounts, 
                                             FailureCounts, ReadResults, 
                                             ReadConsistency, DoneCounts, 
                                             NodeStatus, FailedCounts, 
                                             CommitCounts, CommitTS, 
                                             AbortCounts, SyncCounts, 
                                             CurrentTime, SystemTerminated, 
                                             RecieveACKCount, cid_, t_, i1, j1, 
                                             Test, targetReplica_, s_msg_, t, 
                                             msg_Ca, cid_C, cid_Co, quorom_, 
                                             minority, t_C, i7, j7, 
                                             commitTimestamp, targetReplica_C, 
                                             decision, allSuccessful, s_msg_C, 
                                             cid_Coo, quorom, t_Co, 
                                             targetReplica, cid_Coor, msg_, 
                                             type_, recoveryTid_, cid, msg_C, 
                                             type_C, recoveryTid, msg_Ha, id_H, 
                                             i_, j_, t_H, readTimestamp, 
                                             readData, res_msg_, msg_Han, 
                                             id_Ha, i_H, j_H, t_Ha, res_msg_H, 
                                             msg_Hand, id_Han, i_Ha, j_Ha, 
                                             t_Han, res_msg_Ha, readValue, 
                                             readWTS, msg_Handl, id_Hand, 
                                             i_Han, j_Han, t_Hand, idx, 
                                             res_msg_Han, msg_Handle, id_Handl, 
                                             i_Hand, j_Hand, t_Handl, res_msg, 
                                             msg, id_Handle, i, j, id_HandleS, 
                                             msg_H, i13, j13, id_Re, msg_R, 
                                             type_R, id_N, msg_N, type, id_C, 
                                             s_msg_Co, i8, j8, id_, id_R >>

ReplicaRecover(self) == ProcReplicaRecover(self)
                           \/ ProcReplicaRecoverB(self)

CoordinatorMain(self) == /\ pc[self] = "CoordinatorMain"
                         /\ IF SystemTerminated = FALSE
                               THEN /\ IF NodeStatus[id_[self]] = "Active"
                                          THEN /\ /\ cid_' = [cid_ EXCEPT ![self] = id_[self]]
                                                  /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "SendTransactions",
                                                                                           pc        |->  "CoordinatorMainB",
                                                                                           t_        |->  t_[self],
                                                                                           i1        |->  i1[self],
                                                                                           j1        |->  j1[self],
                                                                                           Test      |->  Test[self],
                                                                                           targetReplica_ |->  targetReplica_[self],
                                                                                           s_msg_    |->  s_msg_[self],
                                                                                           cid_      |->  cid_[self] ] >>
                                                                                       \o stack[self]]
                                               /\ t_' = [t_ EXCEPT ![self] = 0]
                                               /\ i1' = [i1 EXCEPT ![self] = 0]
                                               /\ j1' = [j1 EXCEPT ![self] = 0]
                                               /\ Test' = [Test EXCEPT ![self] = {}]
                                               /\ targetReplica_' = [targetReplica_ EXCEPT ![self] = 0]
                                               /\ s_msg_' = [s_msg_ EXCEPT ![self] = {}]
                                               /\ pc' = [pc EXCEPT ![self] = "ProcSendTransaction"]
                                               /\ UNCHANGED << cid, msg_C, 
                                                               type_C, 
                                                               recoveryTid, 
                                                               id_C, s_msg_Co, 
                                                               i8, j8 >>
                                          ELSE /\ IF NodeStatus[id_[self]] = "Recover"
                                                     THEN /\ /\ cid' = [cid EXCEPT ![self] = id_[self]]
                                                             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "CoordinatorHandleReports",
                                                                                                      pc        |->  "CoordinatorMain",
                                                                                                      msg_C     |->  msg_C[self],
                                                                                                      type_C    |->  type_C[self],
                                                                                                      recoveryTid |->  recoveryTid[self],
                                                                                                      cid       |->  cid[self] ] >>
                                                                                                  \o stack[self]]
                                                          /\ msg_C' = [msg_C EXCEPT ![self] = {}]
                                                          /\ type_C' = [type_C EXCEPT ![self] = ""]
                                                          /\ recoveryTid' = [recoveryTid EXCEPT ![self] = 0]
                                                          /\ pc' = [pc EXCEPT ![self] = "ProcCoorHandleReport"]
                                                          /\ UNCHANGED << id_C, 
                                                                          s_msg_Co, 
                                                                          i8, 
                                                                          j8 >>
                                                     ELSE /\ /\ id_C' = [id_C EXCEPT ![self] = id_[self]]
                                                             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "CoordinatorRecover",
                                                                                                      pc        |->  "CoordinatorMain",
                                                                                                      s_msg_Co  |->  s_msg_Co[self],
                                                                                                      i8        |->  i8[self],
                                                                                                      j8        |->  j8[self],
                                                                                                      id_C      |->  id_C[self] ] >>
                                                                                                  \o stack[self]]
                                                          /\ s_msg_Co' = [s_msg_Co EXCEPT ![self] = {}]
                                                          /\ i8' = [i8 EXCEPT ![self] = 0]
                                                          /\ j8' = [j8 EXCEPT ![self] = 0]
                                                          /\ pc' = [pc EXCEPT ![self] = "ProcCoordinatorRecover"]
                                                          /\ UNCHANGED << cid, 
                                                                          msg_C, 
                                                                          type_C, 
                                                                          recoveryTid >>
                                               /\ UNCHANGED << cid_, t_, i1, 
                                                               j1, Test, 
                                                               targetReplica_, 
                                                               s_msg_ >>
                               ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                    /\ UNCHANGED << stack, cid_, t_, i1, j1, 
                                                    Test, targetReplica_, 
                                                    s_msg_, cid, msg_C, type_C, 
                                                    recoveryTid, id_C, 
                                                    s_msg_Co, i8, j8 >>
                         /\ UNCHANGED << Replicas, Transactions, Messages, 
                                         CoordinatorMessages, WriteLocks, Logs, 
                                         TransactionStatusCoor, 
                                         TransactionStatus, CurrentTransaction, 
                                         SuccessCounts, FailureCounts, 
                                         ReadResults, ReadConsistency, 
                                         DoneCounts, NodeStatus, FailedCounts, 
                                         CommitCounts, CommitTS, AbortCounts, 
                                         SyncCounts, CurrentTime, 
                                         SystemTerminated, RecieveACKCount, t, 
                                         msg_Ca, cid_C, cid_Co, quorom_, 
                                         minority, t_C, i7, j7, 
                                         commitTimestamp, targetReplica_C, 
                                         decision, allSuccessful, s_msg_C, 
                                         cid_Coo, quorom, t_Co, targetReplica, 
                                         cid_Coor, msg_, type_, recoveryTid_, 
                                         msg_Ha, id_H, i_, j_, t_H, 
                                         readTimestamp, readData, res_msg_, 
                                         msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                         res_msg_H, msg_Hand, id_Han, i_Ha, 
                                         j_Ha, t_Han, res_msg_Ha, readValue, 
                                         readWTS, msg_Handl, id_Hand, i_Han, 
                                         j_Han, t_Hand, idx, res_msg_Han, 
                                         msg_Handle, id_Handl, i_Hand, j_Hand, 
                                         t_Handl, res_msg, msg, id_Handle, i, 
                                         j, id_HandleS, msg_H, i13, j13, id_Re, 
                                         msg_R, type_R, id_N, msg_N, type, id, 
                                         s_msg, dataItemId, id_, id_R >>

CoordinatorMainB(self) == /\ pc[self] = "CoordinatorMainB"
                          /\ /\ cid_Coor' = [cid_Coor EXCEPT ![self] = id_[self]]
                             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "CoordinatorHandleACKs",
                                                                      pc        |->  "CoordinatorMainC",
                                                                      msg_      |->  msg_[self],
                                                                      type_     |->  type_[self],
                                                                      recoveryTid_ |->  recoveryTid_[self],
                                                                      cid_Coor  |->  cid_Coor[self] ] >>
                                                                  \o stack[self]]
                          /\ msg_' = [msg_ EXCEPT ![self] = {}]
                          /\ type_' = [type_ EXCEPT ![self] = ""]
                          /\ recoveryTid_' = [recoveryTid_ EXCEPT ![self] = 0]
                          /\ pc' = [pc EXCEPT ![self] = "ProcCoorHandleACK"]
                          /\ UNCHANGED << Replicas, Transactions, Messages, 
                                          CoordinatorMessages, WriteLocks, 
                                          Logs, TransactionStatusCoor, 
                                          TransactionStatus, 
                                          CurrentTransaction, SuccessCounts, 
                                          FailureCounts, ReadResults, 
                                          ReadConsistency, DoneCounts, 
                                          NodeStatus, FailedCounts, 
                                          CommitCounts, CommitTS, AbortCounts, 
                                          SyncCounts, CurrentTime, 
                                          SystemTerminated, RecieveACKCount, 
                                          cid_, t_, i1, j1, Test, 
                                          targetReplica_, s_msg_, t, msg_Ca, 
                                          cid_C, cid_Co, quorom_, minority, 
                                          t_C, i7, j7, commitTimestamp, 
                                          targetReplica_C, decision, 
                                          allSuccessful, s_msg_C, cid_Coo, 
                                          quorom, t_Co, targetReplica, cid, 
                                          msg_C, type_C, recoveryTid, msg_Ha, 
                                          id_H, i_, j_, t_H, readTimestamp, 
                                          readData, res_msg_, msg_Han, id_Ha, 
                                          i_H, j_H, t_Ha, res_msg_H, msg_Hand, 
                                          id_Han, i_Ha, j_Ha, t_Han, 
                                          res_msg_Ha, readValue, readWTS, 
                                          msg_Handl, id_Hand, i_Han, j_Han, 
                                          t_Hand, idx, res_msg_Han, msg_Handle, 
                                          id_Handl, i_Hand, j_Hand, t_Handl, 
                                          res_msg, msg, id_Handle, i, j, 
                                          id_HandleS, msg_H, i13, j13, id_Re, 
                                          msg_R, type_R, id_N, msg_N, type, 
                                          id_C, s_msg_Co, i8, j8, id, s_msg, 
                                          dataItemId, id_, id_R >>

CoordinatorMainC(self) == /\ pc[self] = "CoordinatorMainC"
                          /\ /\ id_N' = [id_N EXCEPT ![self] = id_[self]]
                             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NodeFail",
                                                                      pc        |->  "CoordinatorMainD",
                                                                      msg_N     |->  msg_N[self],
                                                                      type      |->  type[self],
                                                                      id_N      |->  id_N[self] ] >>
                                                                  \o stack[self]]
                          /\ msg_N' = [msg_N EXCEPT ![self] = {}]
                          /\ type' = [type EXCEPT ![self] = ""]
                          /\ pc' = [pc EXCEPT ![self] = "ProcSetNodeFail"]
                          /\ UNCHANGED << Replicas, Transactions, Messages, 
                                          CoordinatorMessages, WriteLocks, 
                                          Logs, TransactionStatusCoor, 
                                          TransactionStatus, 
                                          CurrentTransaction, SuccessCounts, 
                                          FailureCounts, ReadResults, 
                                          ReadConsistency, DoneCounts, 
                                          NodeStatus, FailedCounts, 
                                          CommitCounts, CommitTS, AbortCounts, 
                                          SyncCounts, CurrentTime, 
                                          SystemTerminated, RecieveACKCount, 
                                          cid_, t_, i1, j1, Test, 
                                          targetReplica_, s_msg_, t, msg_Ca, 
                                          cid_C, cid_Co, quorom_, minority, 
                                          t_C, i7, j7, commitTimestamp, 
                                          targetReplica_C, decision, 
                                          allSuccessful, s_msg_C, cid_Coo, 
                                          quorom, t_Co, targetReplica, 
                                          cid_Coor, msg_, type_, recoveryTid_, 
                                          cid, msg_C, type_C, recoveryTid, 
                                          msg_Ha, id_H, i_, j_, t_H, 
                                          readTimestamp, readData, res_msg_, 
                                          msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                          res_msg_H, msg_Hand, id_Han, i_Ha, 
                                          j_Ha, t_Han, res_msg_Ha, readValue, 
                                          readWTS, msg_Handl, id_Hand, i_Han, 
                                          j_Han, t_Hand, idx, res_msg_Han, 
                                          msg_Handle, id_Handl, i_Hand, j_Hand, 
                                          t_Handl, res_msg, msg, id_Handle, i, 
                                          j, id_HandleS, msg_H, i13, j13, 
                                          id_Re, msg_R, type_R, id_C, s_msg_Co, 
                                          i8, j8, id, s_msg, dataItemId, id_, 
                                          id_R >>

CoordinatorMainD(self) == /\ pc[self] = "CoordinatorMainD"
                          /\ IF \A tid1 \in 1..T : TransactionStatusCoor[Transactions[tid1].coordinator_id][tid1].status = "Finished"
                                THEN /\ SystemTerminated' = TRUE
                                ELSE /\ TRUE
                                     /\ UNCHANGED SystemTerminated
                          /\ pc' = [pc EXCEPT ![self] = "CoordinatorMain"]
                          /\ UNCHANGED << Replicas, Transactions, Messages, 
                                          CoordinatorMessages, WriteLocks, 
                                          Logs, TransactionStatusCoor, 
                                          TransactionStatus, 
                                          CurrentTransaction, SuccessCounts, 
                                          FailureCounts, ReadResults, 
                                          ReadConsistency, DoneCounts, 
                                          NodeStatus, FailedCounts, 
                                          CommitCounts, CommitTS, AbortCounts, 
                                          SyncCounts, CurrentTime, 
                                          RecieveACKCount, stack, cid_, t_, i1, 
                                          j1, Test, targetReplica_, s_msg_, t, 
                                          msg_Ca, cid_C, cid_Co, quorom_, 
                                          minority, t_C, i7, j7, 
                                          commitTimestamp, targetReplica_C, 
                                          decision, allSuccessful, s_msg_C, 
                                          cid_Coo, quorom, t_Co, targetReplica, 
                                          cid_Coor, msg_, type_, recoveryTid_, 
                                          cid, msg_C, type_C, recoveryTid, 
                                          msg_Ha, id_H, i_, j_, t_H, 
                                          readTimestamp, readData, res_msg_, 
                                          msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                          res_msg_H, msg_Hand, id_Han, i_Ha, 
                                          j_Ha, t_Han, res_msg_Ha, readValue, 
                                          readWTS, msg_Handl, id_Hand, i_Han, 
                                          j_Han, t_Hand, idx, res_msg_Han, 
                                          msg_Handle, id_Handl, i_Hand, j_Hand, 
                                          t_Handl, res_msg, msg, id_Handle, i, 
                                          j, id_HandleS, msg_H, i13, j13, 
                                          id_Re, msg_R, type_R, id_N, msg_N, 
                                          type, id_C, s_msg_Co, i8, j8, id, 
                                          s_msg, dataItemId, id_, id_R >>

Coordinator(self) == CoordinatorMain(self) \/ CoordinatorMainB(self)
                        \/ CoordinatorMainC(self) \/ CoordinatorMainD(self)

ReplicaMain(self) == /\ pc[self] = "ReplicaMain"
                     /\ IF SystemTerminated = FALSE
                           THEN /\ IF NodeStatus[id_R[self]] = "Active"
                                      THEN /\ /\ id_Re' = [id_Re EXCEPT ![self] = id_R[self]]
                                              /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "ReplicaReceiveRequest",
                                                                                       pc        |->  "ReplicaMainB",
                                                                                       msg_R     |->  msg_R[self],
                                                                                       type_R    |->  type_R[self],
                                                                                       id_Re     |->  id_Re[self] ] >>
                                                                                   \o stack[self]]
                                           /\ msg_R' = [msg_R EXCEPT ![self] = {}]
                                           /\ type_R' = [type_R EXCEPT ![self] = ""]
                                           /\ pc' = [pc EXCEPT ![self] = "ProcRepRecReq"]
                                           /\ UNCHANGED << id_HandleS, msg_H, 
                                                           i13, j13, id, s_msg, 
                                                           dataItemId >>
                                      ELSE /\ IF NodeStatus[id_R[self]] = "Recover"
                                                 THEN /\ /\ id_HandleS' = [id_HandleS EXCEPT ![self] = id_R[self]]
                                                         /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HandleSyncAck",
                                                                                                  pc        |->  "ReplicaMain",
                                                                                                  msg_H     |->  msg_H[self],
                                                                                                  i13       |->  i13[self],
                                                                                                  j13       |->  j13[self],
                                                                                                  id_HandleS |->  id_HandleS[self] ] >>
                                                                                              \o stack[self]]
                                                      /\ msg_H' = [msg_H EXCEPT ![self] = {}]
                                                      /\ i13' = [i13 EXCEPT ![self] = 0]
                                                      /\ j13' = [j13 EXCEPT ![self] = 0]
                                                      /\ pc' = [pc EXCEPT ![self] = "ProcHandleSyncAck"]
                                                      /\ UNCHANGED << id, 
                                                                      s_msg, 
                                                                      dataItemId >>
                                                 ELSE /\ /\ id' = [id EXCEPT ![self] = id_R[self]]
                                                         /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "ReplicaRecover",
                                                                                                  pc        |->  "ReplicaMain",
                                                                                                  s_msg     |->  s_msg[self],
                                                                                                  dataItemId |->  dataItemId[self],
                                                                                                  id        |->  id[self] ] >>
                                                                                              \o stack[self]]
                                                      /\ s_msg' = [s_msg EXCEPT ![self] = {}]
                                                      /\ dataItemId' = [dataItemId EXCEPT ![self] = 0]
                                                      /\ pc' = [pc EXCEPT ![self] = "ProcReplicaRecover"]
                                                      /\ UNCHANGED << id_HandleS, 
                                                                      msg_H, 
                                                                      i13, j13 >>
                                           /\ UNCHANGED << id_Re, msg_R, 
                                                           type_R >>
                           ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                /\ UNCHANGED << stack, id_HandleS, msg_H, i13, 
                                                j13, id_Re, msg_R, type_R, id, 
                                                s_msg, dataItemId >>
                     /\ UNCHANGED << Replicas, Transactions, Messages, 
                                     CoordinatorMessages, WriteLocks, Logs, 
                                     TransactionStatusCoor, TransactionStatus, 
                                     CurrentTransaction, SuccessCounts, 
                                     FailureCounts, ReadResults, 
                                     ReadConsistency, DoneCounts, NodeStatus, 
                                     FailedCounts, CommitCounts, CommitTS, 
                                     AbortCounts, SyncCounts, CurrentTime, 
                                     SystemTerminated, RecieveACKCount, cid_, 
                                     t_, i1, j1, Test, targetReplica_, s_msg_, 
                                     t, msg_Ca, cid_C, cid_Co, quorom_, 
                                     minority, t_C, i7, j7, commitTimestamp, 
                                     targetReplica_C, decision, allSuccessful, 
                                     s_msg_C, cid_Coo, quorom, t_Co, 
                                     targetReplica, cid_Coor, msg_, type_, 
                                     recoveryTid_, cid, msg_C, type_C, 
                                     recoveryTid, msg_Ha, id_H, i_, j_, t_H, 
                                     readTimestamp, readData, res_msg_, 
                                     msg_Han, id_Ha, i_H, j_H, t_Ha, res_msg_H, 
                                     msg_Hand, id_Han, i_Ha, j_Ha, t_Han, 
                                     res_msg_Ha, readValue, readWTS, msg_Handl, 
                                     id_Hand, i_Han, j_Han, t_Hand, idx, 
                                     res_msg_Han, msg_Handle, id_Handl, i_Hand, 
                                     j_Hand, t_Handl, res_msg, msg, id_Handle, 
                                     i, j, id_N, msg_N, type, id_C, s_msg_Co, 
                                     i8, j8, id_, id_R >>

ReplicaMainB(self) == /\ pc[self] = "ReplicaMainB"
                      /\ /\ id_N' = [id_N EXCEPT ![self] = id_R[self]]
                         /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NodeFail",
                                                                  pc        |->  "ReplicaMainC",
                                                                  msg_N     |->  msg_N[self],
                                                                  type      |->  type[self],
                                                                  id_N      |->  id_N[self] ] >>
                                                              \o stack[self]]
                      /\ msg_N' = [msg_N EXCEPT ![self] = {}]
                      /\ type' = [type EXCEPT ![self] = ""]
                      /\ pc' = [pc EXCEPT ![self] = "ProcSetNodeFail"]
                      /\ UNCHANGED << Replicas, Transactions, Messages, 
                                      CoordinatorMessages, WriteLocks, Logs, 
                                      TransactionStatusCoor, TransactionStatus, 
                                      CurrentTransaction, SuccessCounts, 
                                      FailureCounts, ReadResults, 
                                      ReadConsistency, DoneCounts, NodeStatus, 
                                      FailedCounts, CommitCounts, CommitTS, 
                                      AbortCounts, SyncCounts, CurrentTime, 
                                      SystemTerminated, RecieveACKCount, cid_, 
                                      t_, i1, j1, Test, targetReplica_, s_msg_, 
                                      t, msg_Ca, cid_C, cid_Co, quorom_, 
                                      minority, t_C, i7, j7, commitTimestamp, 
                                      targetReplica_C, decision, allSuccessful, 
                                      s_msg_C, cid_Coo, quorom, t_Co, 
                                      targetReplica, cid_Coor, msg_, type_, 
                                      recoveryTid_, cid, msg_C, type_C, 
                                      recoveryTid, msg_Ha, id_H, i_, j_, t_H, 
                                      readTimestamp, readData, res_msg_, 
                                      msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                      res_msg_H, msg_Hand, id_Han, i_Ha, j_Ha, 
                                      t_Han, res_msg_Ha, readValue, readWTS, 
                                      msg_Handl, id_Hand, i_Han, j_Han, t_Hand, 
                                      idx, res_msg_Han, msg_Handle, id_Handl, 
                                      i_Hand, j_Hand, t_Handl, res_msg, msg, 
                                      id_Handle, i, j, id_HandleS, msg_H, i13, 
                                      j13, id_Re, msg_R, type_R, id_C, 
                                      s_msg_Co, i8, j8, id, s_msg, dataItemId, 
                                      id_, id_R >>

ReplicaMainC(self) == /\ pc[self] = "ReplicaMainC"
                      /\ IF \A tid1 \in 1..T : TransactionStatusCoor[Transactions[tid1].coordinator_id][tid1].status = "Finished"
                            THEN /\ SystemTerminated' = TRUE
                            ELSE /\ TRUE
                                 /\ UNCHANGED SystemTerminated
                      /\ pc' = [pc EXCEPT ![self] = "ReplicaMain"]
                      /\ UNCHANGED << Replicas, Transactions, Messages, 
                                      CoordinatorMessages, WriteLocks, Logs, 
                                      TransactionStatusCoor, TransactionStatus, 
                                      CurrentTransaction, SuccessCounts, 
                                      FailureCounts, ReadResults, 
                                      ReadConsistency, DoneCounts, NodeStatus, 
                                      FailedCounts, CommitCounts, CommitTS, 
                                      AbortCounts, SyncCounts, CurrentTime, 
                                      RecieveACKCount, stack, cid_, t_, i1, j1, 
                                      Test, targetReplica_, s_msg_, t, msg_Ca, 
                                      cid_C, cid_Co, quorom_, minority, t_C, 
                                      i7, j7, commitTimestamp, targetReplica_C, 
                                      decision, allSuccessful, s_msg_C, 
                                      cid_Coo, quorom, t_Co, targetReplica, 
                                      cid_Coor, msg_, type_, recoveryTid_, cid, 
                                      msg_C, type_C, recoveryTid, msg_Ha, id_H, 
                                      i_, j_, t_H, readTimestamp, readData, 
                                      res_msg_, msg_Han, id_Ha, i_H, j_H, t_Ha, 
                                      res_msg_H, msg_Hand, id_Han, i_Ha, j_Ha, 
                                      t_Han, res_msg_Ha, readValue, readWTS, 
                                      msg_Handl, id_Hand, i_Han, j_Han, t_Hand, 
                                      idx, res_msg_Han, msg_Handle, id_Handl, 
                                      i_Hand, j_Hand, t_Handl, res_msg, msg, 
                                      id_Handle, i, j, id_HandleS, msg_H, i13, 
                                      j13, id_Re, msg_R, type_R, id_N, msg_N, 
                                      type, id_C, s_msg_Co, i8, j8, id, s_msg, 
                                      dataItemId, id_, id_R >>

Replica(self) == ReplicaMain(self) \/ ReplicaMainB(self)
                    \/ ReplicaMainC(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in ProcSet:  \/ SendTransactions(self)
                               \/ CalculateACKCounts(self)
                               \/ CoordinatorDecide(self)
                               \/ CoordinatorFinDecide(self)
                               \/ CoordinatorHandleACKs(self)
                               \/ CoordinatorHandleReports(self)
                               \/ HandleRead(self) \/ HandleWrite(self)
                               \/ HandleInquire(self) \/ HandleCommit(self)
                               \/ HandleAbort(self) \/ HandleSync(self)
                               \/ HandleSyncAck(self)
                               \/ ReplicaReceiveRequest(self)
                               \/ NodeFail(self) \/ CoordinatorRecover(self)
                               \/ ReplicaRecover(self))
           \/ (\E self \in (1+(N * M))..(C+(N * M)): Coordinator(self))
           \/ (\E self \in 1..(N * M): Replica(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

=============================================================================
\* Modification History
\* Last modified Sat Aug 31 22:38:49 CST 2024 by yqekzb
\* Created Thu Jul 11 16:13:35 CST 2024 by yqekzb
