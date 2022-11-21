#ifndef _CONFIG_H_

#define _CONFIG_H_

/***tictoc****/
/*
#define WRITE_PERMISSION_LOCK         false
#define MULTI_VERSION                 false
#define ENABLE_LOCAL_CACHING          false
#define OCC_LOCK_TYPE                 WAIT_DIE
#define TICTOC_MV                     false
#define OCC_WAW_LOCK                  true
#define RO_LEASE                      false
#define ATOMIC_WORD                   false
#define TRACK_LAST                    false
#define UPDATE_TABLE_TS               true
#define WRITE_PERMISSION_LOCK         false
#define LOCK_ALL_BEFORE_COMMIT        false
#define LOCK_ALL_DEBUG                false
#define PAUSE __asm__ ( "pause;" );
#define COMPILER_BARRIER asm volatile("" ::: "memory");
*/

#define SIT_TCP         0
#define SIT_TWO_SIDE    1
#define SIT_ONE_SIDE    2
#define SIT_COROUTINE   3
#define SIT_DBPA        4
#define SIT_ALL         5
#define RDMA_SIT SIT_COROUTINE
// #if RDMA_SIT == SIT_TCP
//   #define RDMA_ONE_SIDE false
//   #define RDMA_TWO_SIDE false
//   #define USE_COROUTINE false
//   #define USE_DBPAOR false
// #elif RDMA_SIT == SIT_TWO_SIDE
//   #define RDMA_ONE_SIDE false
//   #define RDMA_TWO_SIDE true
//   #define USE_COROUTINE false
//   #define USE_DBPAOR false
// #elif RDMA_SIT == SIT_ONE_SIDE
//   #define RDMA_ONE_SIDE true
//   #define RDMA_TWO_SIDE true
//   #define USE_COROUTINE false
//   #define USE_DBPAOR false
// #elif RDMA_SIT == SIT_COROUTINE
//   #define RDMA_ONE_SIDE true
//   #define RDMA_TWO_SIDE true
//   #define USE_COROUTINE true
//   #define USE_DBPAOR false
// #elif RDMA_SIT == SIT_DBPA
//   #define RDMA_ONE_SIDE true
//   #define RDMA_TWO_SIDE true
//   #define USE_COROUTINE false
//   #define USE_DBPAOR true
// #elif RDMA_SIT == SIT_ALL
  #define RDMA_ONE_SIDE true
  #define RDMA_TWO_SIDE false
  #define USE_COROUTINE false
  #define USE_DBPAOR false
// #endif
/************RDMA TYPE**************/
#define CHANGE_TCP_ONLY 0
#define CHANGE_MSG_QUEUE 1

#define HIS_CHAIN_NUM 4
#define USE_CAS
// #define USE_COROUTINE false
#define MAX_SEND_SIZE 1

#define CENTER_MASTER true  //hg-network without replica stage 2
#define PARAL_SUBTXN true  //hg-network without replica stage 3
#define USE_REPLICA true
#define THOMAS_WRITE true  //if false, wait and sort
#define INTER_DC_CONTROL true
#define RDMA_DBPAOR false //concurrent logging

#if USE_REPLICA
#define ASYNC_REDO_THREAD_CNT 1
#else
#define ASYNC_REDO_THREAD_CNT 0
#endif
/***********************************************/
// DA Trans Creator
/***********************************************/
//which creator to use
#define CREATOR_USE_T false

//TraversalActionSequenceCreator
#define TRANS_CNT 2
#define ITEM_CNT 4
#define SUBTASK_NUM 1
#define SUBTASK_ID 0
#define MAX_DML 4
#define WITH_ABORT false
#define TAIL_DTL false
#define SAVE_HISTROY_WITH_EMPTY_OPT false
#define DYNAMIC_SEQ_LEN false
#define ONLY_ONE_HOME false
//InputActionSequenceCreator
#define INPUT_FILE_PATH "./input.txt"

// ! Parameters used to locate distributed performance bottlenecks.

#define SECOND 200 // Set the queue monitoring time.
// #define THD_ID_QUEUE
#define ONE_NODE_RECIEVE 0 // only node 0 will receive the txn query
#define USE_WORK_NUM_THREAD true
#if 1
// #define LESS_DIS // Reduce the number of yCSB remote data to 1
// #define LESS_DIS_NUM 0 // Reduce the number of yCSB remote data to 1
// #define NEW_WORK_QUEUE  // The workQueue data structure has been modified to perform 10,000 better than the original implementation.
// #define NO_2PC  // Removing 2PC, of course, would be problematic in distributed transactions.
// #define FAKE_PROCESS  // Io_thread returns as soon as it gets the request from the remote. Avoid waiting in the WORK_queue.
// #define NO_REMOTE // remove all remote txn
#endif 
#define TXN_QUEUE_PERCENT 0.0 // The proportion of the transaction to take from txn_queue firstly.
#define MALLOC_TYPE 0 // 0 represent normal malloc. 1 represent je-malloc
// ! end of these parameters
// ! Parameters used to locate distributed performance bottlenecks.
#define SEND_TO_SELF_PAHSE 0 // 0 means do not send to self, 1 will execute the phase1, 2 will execute phase2, 3 will exeute phase1 and phase 2
// msg send can be split into three stage, stage1 encapsulates msg; stage2 send msg; stgae3 parse msg;
#define SEND_STAGE 1 // 1 will execute the stage1, 2 will execute stage1 and 3, 3 will exeute all
// ! end of these parameters
/***********************************************/
// Simulation + Hardware
/***********************************************/
#define CENTER_CNT 4
#define NODE_CNT 8
#define THREAD_CNT 40
#define REM_THREAD_CNT 1
#define SEND_THREAD_CNT 1
#define COROUTINE_CNT 4
#define CORE_CNT 2
// PART_CNT should be at least NODE_CNT
#define PART_CNT NODE_CNT
#define CLIENT_NODE_CNT 1
#define CLIENT_THREAD_CNT 4
#define CLIENT_REM_THREAD_CNT 1
#define CLIENT_SEND_THREAD_CNT 1
#define CLIENT_RUNTIME false

#define LOAD_METHOD LOAD_MAX
#define LOAD_PER_SERVER 100

// Replication
#define REPLICA_CNT 0
// AA (Active-Active), AP (Active-Passive)
#define REPL_TYPE AP

// each transaction only accesses only 1 virtual partition. But the lock/ts manager and index are
// not aware of such partitioning. VIRTUAL_PART_CNT describes the request distribution and is only
// used to generate queries. For HSTORE, VIRTUAL_PART_CNT should be the same as PART_CNT.
#define VIRTUAL_PART_CNT    PART_CNT
#define PAGE_SIZE         4096
#define CL_SIZE           64
#define CPU_FREQ          2.6
// enable hardware migration.
#define HW_MIGRATE          false

// # of transactions to run for warmup
#define WARMUP            0
// YCSB or TPCC or PPS or DA
#define WORKLOAD YCSB
// print the transaction latency distribution
#define PRT_LAT_DISTR false
#define STATS_ENABLE        true
#define TIME_ENABLE         true //STATS_ENABLE

#define FIN_BY_TIME true
#define MAX_TXN_IN_FLIGHT 320

#define SERVER_GENERATE_QUERIES false

/***********************************************/
// Memory System
/***********************************************/
// Three different memory allocation methods are supported.
// 1. default libc malloc
// 2. per-thread malloc. each thread has a private local memory
//    pool
// 3. per-partition malloc. each partition has its own memory pool
//    which is mapped to a unique tile on the chip.
#define MEM_ALLIGN          8

// [THREAD_ALLOC]
