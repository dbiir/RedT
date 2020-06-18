#ifndef _CONFIG_H_

#define _CONFIG_H_

/***********************************************/
// Simulation + Hardware
/***********************************************/
#define NODE_CNT 16
#define THREAD_CNT 4
#define REM_THREAD_CNT THREAD_CNT
#define SEND_THREAD_CNT THREAD_CNT
#define CORE_CNT 2
// PART_CNT should be at least NODE_CNT
#define PART_CNT NODE_CNT
#define CLIENT_NODE_CNT NODE_CNT
#define CLIENT_THREAD_CNT 4
#define CLIENT_REM_THREAD_CNT 2
#define CLIENT_SEND_THREAD_CNT 2
#define CLIENT_RUNTIME false

#define LOAD_METHOD LOAD_MAX
#define LOAD_PER_SERVER 100

// Replication
#define REPLICA_CNT 0
// AA (Active-Active), AP (Active-Passive)
#define REPL_TYPE AP

// each transaction only accesses only 1 virtual partition. But the lock/ts manager and index are not aware of such partitioning. VIRTUAL_PART_CNT describes the request distribution and is only used to generate queries. For HSTORE, VIRTUAL_PART_CNT should be the same as PART_CNT.
#define VIRTUAL_PART_CNT    PART_CNT  
#define PAGE_SIZE         4096 
#define CL_SIZE           64
#define CPU_FREQ          2.6
// enable hardware migration.
#define HW_MIGRATE          false

// # of transactions to run for warmup
#define WARMUP            0
// YCSB or TPCC or PPS
#define WORKLOAD YCSB
// print the transaction latency distribution
#define PRT_LAT_DISTR false
#define STATS_ENABLE        true
#define TIME_ENABLE         true //STATS_ENABLE

#define FIN_BY_TIME true
#define MAX_TXN_IN_FLIGHT 10000

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
#define THREAD_ALLOC        false
#define THREAD_ARENA_SIZE     (1UL << 22) 
#define MEM_PAD           true

// [PART_ALLOC] 
#define PART_ALLOC          false
#define MEM_SIZE          (1UL << 30) 
#define NO_FREE           false

/***********************************************/
// Message Passing
/***********************************************/
#define TPORT_TYPE tcp
#define TPORT_PORT 7000
#define SET_AFFINITY true
#define TPORT_TYPE tcp
#define TPORT_PORT 7000
#define SET_AFFINITY true

#define MAX_TPORT_NAME 128
#define MSG_SIZE 128 // in bytes
#define HEADER_SIZE sizeof(uint32_t)*2 // in bits 
#define MSG_TIMEOUT 5000000000UL // in ns
#define NETWORK_TEST false
#define NETWORK_DELAY_TEST false
#define NETWORK_DELAY 0UL

#define MAX_QUEUE_LEN NODE_CNT * 2

#define PRIORITY_WORK_QUEUE false
#define PRIORITY PRIORITY_ACTIVE
#define MSG_SIZE_MAX 4096
#define MSG_TIME_LIMIT 0

/***********************************************/
// Concurrency Control
/***********************************************/
// WAIT_DIE, NO_WAIT, TIMESTAMP, MVCC, CALVIN, MAAT, WOOKONG
#define CC_ALG NO_WAIT
#define ISOLATION_LEVEL SERIALIZABLE
#define YCSB_ABORT_MODE false

// all transactions acquire tuples according to the primary key order.
#define KEY_ORDER         false
// transaction roll back changes after abort
#define ROLL_BACK         true
// per-row lock/ts management or central lock/ts management
#define CENTRAL_MAN         false
#define BUCKET_CNT          31
#define ABORT_PENALTY 10 * 1000000UL   // in ns.
#define ABORT_PENALTY_MAX 5 * 100 * 1000000UL   // in ns.
#define BACKOFF true
// [ INDEX ]
#define ENABLE_LATCH        false
#define CENTRAL_INDEX       false
#define CENTRAL_MANAGER       false
#define INDEX_STRUCT        IDX_HASH
#define BTREE_ORDER         16

// [TIMESTAMP]
#define TS_TWR            false
#define TS_ALLOC          TS_CLOCK
#define TS_BATCH_ALLOC        false
#define TS_BATCH_NUM        1
// [MVCC]
// when read/write history is longer than HIS_RECYCLE_LEN
// the history should be recycled.
#define HIS_RECYCLE_LEN       10
#define MAX_PRE_REQ         MAX_TXN_IN_FLIGHT * NODE_CNT//1024