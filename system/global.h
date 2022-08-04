/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _GLOBAL_H_
#define _GLOBAL_H_

#define __STDC_LIMIT_MACROS
#include <stdint.h>
#include <unistd.h>
#include <cstddef>
#include <cstdlib>
#include <cassert>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string.h>
#include <typeinfo>
#include <list>
#include <map>
#include <set>
#include <queue>
#include <string>
#include <vector>
#include <sstream>
#include <time.h>
#include <sys/time.h>
#include <math.h>

#include "pthread.h"
#include "config.h"
#include "stats.h"
//#include "work_queue.h"
#include "pool.h"
#include "txn_table.h"
#include "logger.h"
#include "sim_manager.h"

#include <boost/lockfree/queue.hpp>
#include "da_block_queue.h"
#include "src/allocator_master.hh"
#include "lib.hh"
using namespace std;

#ifdef USE_RDMA
  #include "qps/rc_recv_manager.hh"
  #include "qps/recv_iter.hh"
  #include "qps/mod.hh"
  using namespace rdmaio;
  using namespace rdmaio::rmem;
  using namespace rdmaio::qp;
#endif
//#endif

class mem_alloc;
class Stats;
class SimManager;
class Manager;
class Query_queue;
class OptCC;
class Dli;
class Focc;
class Bocc;
class ssi;
class wsi;
class Cicada;
class Maat;
class Dta;
class Wkdb;
class Tictoc;
class Transport;
class Rdma;
#if CC_ALG == RDMA_SILO
class RDMA_silo;
#elif CC_ALG == RDMA_MOCC
class RDMA_mocc;
#elif CC_ALG == RDMA_MVCC
class rdma_mvcc;
#endif
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
class RDMA_2pl;
#endif
#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT || RDMA_TS
class RdmaTxnTable;
#endif
#if CC_ALG == RDMA_DSLR_NO_WAIT
class RDMA_dslr_no_wait;
#endif
#if CC_ALG == RDMA_MAAT
class RDMA_Maat;
class RdmaTxnTable;
#endif
#if CC_ALG == RDMA_TS1
class RDMA_ts1;
#endif
#if CC_ALG == RDMA_TS
class RDMA_ts;
#endif
#if CC_ALG == RDMA_CICADA
class RDMA_Cicada;
#endif
#if CC_ALG == RDMA_CALVIN
class RDMA_calvin;
#endif
#if CC_ALG == RDMA_CNULL
class RDMA_Null;
#endif
class Remote_query;
class TxnManPool;
class TxnPool;
class AccessPool;
class TxnTablePool;
class MsgPool;
class RowPool;
class QryPool;
class TxnTable;
class QWorkQueue;
class AbortQueue;
class MessageQueue;
class Client_query_queue;
class Client_txn;
class Sequencer;
class Logger;
class TimeTable;
class InOutTable;
class WkdbTimeTable;
class DAQuery;
class DABlockQueue;
class DtaTimeTable;
class KeyXidCache;
class RtsCache;
class Workload;
// class QTcpQueue;
// class TcpTimestamp;

typedef uint32_t UInt32;
typedef int32_t SInt32;
typedef uint64_t UInt64;
typedef int64_t SInt64;

typedef uint64_t ts_t; // time stamp type

/******************************************/
// Global Data Structure
/******************************************/
extern mem_alloc mem_allocator;
extern Stats stats;
extern SimManager * simulation;
extern Manager glob_manager;
extern Query_queue query_queue;
extern Client_query_queue client_query_queue;
extern OptCC occ_man;
extern Dli dli_man;
extern Focc focc_man;
extern Bocc bocc_man;
extern ssi ssi_man;
extern wsi wsi_man;
extern Cicada cicada_man;
extern Maat maat_man;
extern Dta dta_man;
extern Wkdb wkdb_man;
extern Tictoc tictoc_man;
extern Transport tport_man;
extern Rdma rdma_man;
#if CC_ALG == RDMA_SILO
extern RDMA_silo rsilo_man;
#elif CC_ALG == RDMA_MOCC
extern RDMA_mocc rmocc_man;
#elif CC_ALG == RDMA_MVCC
extern rdma_mvcc rmvcc_man;
#endif
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
extern RDMA_2pl r2pl_man;
#endif
#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT || CC_ALG == RDMA_TS || CC_ALG == RDMA_TS1
extern RdmaTxnTable rdma_txn_table;
#endif
#if CC_ALG == RDMA_DSLR_NO_WAIT
extern  RDMA_dslr_no_wait dslr_man;
#endif
#if CC_ALG == RDMA_MAAT
extern RDMA_Maat rmaat_man;
extern RdmaTxnTable rdma_txn_table;
#endif
#if CC_ALG ==RDMA_TS1
extern RDMA_ts1 rdmats_man;
#endif
#if CC_ALG ==RDMA_TS
extern RDMA_ts rdmats_man;
#endif
#if CC_ALG == RDMA_CICADA
extern RDMA_Cicada rcicada_man;
#endif
#if CC_ALG == RDMA_CALVIN
extern RDMA_calvin calvin_man;
#endif
#if CC_ALG == RDMA_CNULL
extern RDMA_Null rcnull_man;
#endif
extern Workload * m_wl;
extern TxnManPool txn_man_pool;
extern TxnPool txn_pool;
extern AccessPool access_pool;
extern TxnTablePool txn_table_pool;
extern MsgPool msg_pool;
extern RowPool row_pool;
extern QryPool qry_pool;
extern TxnTable txn_table;
extern QWorkQueue work_queue;
extern AbortQueue abort_queue;
extern MessageQueue msg_queue;
extern Client_txn client_man;
extern Sequencer seq_man;
extern Logger logger;
extern TimeTable time_table;
extern DtaTimeTable dta_time_table;
extern KeyXidCache dta_key_xid_cache;
extern RtsCache dta_rts_cache;
extern InOutTable inout_table;
extern WkdbTimeTable wkdb_time_table;
extern KeyXidCache wkdb_key_xid_cache;
extern RtsCache wkdb_rts_cache;
// extern QTcpQueue tcp_queue;
// extern TcpTimestamp tcp_ts;

extern map<string, string> g_params;

extern char *rdma_global_buffer;
extern char *rdma_txntable_buffer;
// CALVIN share memory
extern char *rdma_calvin_buffer;
//extern rdmaio::Arc<rdmaio::rmem::RMem> rdma_global_buffer;
extern rdmaio::Arc<rdmaio::rmem::RMem> rdma_rm;
extern rdmaio::Arc<rdmaio::rmem::RMem> client_rdma_rm;
extern rdmaio::Arc<rdmaio::rmem::RegHandler> rm_handler;
extern rdmaio::Arc<rdmaio::rmem::RegHandler> client_rm_handler;

extern std::vector<rdmaio::ConnectManager> cm;
extern rdmaio::Arc<rdmaio::RCtrl> rm_ctrl;
extern rdmaio::Arc<rdmaio::RNic> nic;
// extern rdmaio::Arc<rdmaio::qp::RDMARC> rc_qp[NODE_CNT][THREAD_CNT * (COROUTINE_CNT + 1)];
extern rdmaio::Arc<rdmaio::qp::RDMARC> rc_qp[NODE_CNT][RDMA_MAX_CLIENT_QP * (COROUTINE_CNT + 1)];
extern pthread_mutex_t * RDMA_QP_LATCH;

extern rdmaio::rmem::RegAttr remote_mr_attr[NODE_CNT];

extern string rdma_server_add[NODE_CNT];
// #if USE_COROUTINE
extern string qp_name[NODE_CNT][RDMA_MAX_CLIENT_QP * (COROUTINE_CNT + 1)];
// else
// extern string qp_name[NODE_CNT][THREAD_CNT * (COROUTINE_CNT + 1)];
// #endif

//extern rdmaio::ConnectManager cm[NODE_CNT];
//extern r2::Allocator *r2_allocator;

extern int rdma_server_port[NODE_CNT];

extern bool volatile warmup_done;
extern bool volatile enable_thread_mem_pool;
extern pthread_barrier_t warmup_bar;
#if USE_REPLICA
extern pthread_mutex_t log_lock;
#endif
/******************************************/
// Client Global Params
/******************************************/
extern UInt32 g_client_thread_cnt;
extern UInt32 g_client_rem_thread_cnt;
extern UInt32 g_client_send_thread_cnt;
extern UInt32 g_client_node_cnt;
extern UInt32 g_servers_per_client;
extern UInt32 g_clients_per_server;
extern UInt32 g_server_start_node;
/******************************************/
// INIT Global Params
/******************************************/
extern bool g_init_done[50];
extern int g_init_cnt;

/******************************************/
// Global Parameter
/******************************************/
extern volatile UInt64 g_row_id;
extern bool g_part_alloc;
extern bool g_mem_pad;
extern bool g_prt_lat_distr;
extern UInt32 g_node_id;
extern UInt32 g_node_cnt;
extern UInt32 g_center_id;
extern UInt32 g_center_cnt;
extern UInt32 g_part_cnt;
extern UInt32 g_virtual_part_cnt;
extern UInt32 g_core_cnt;
extern UInt32 g_total_node_cnt;
extern UInt32 g_total_thread_cnt;
extern UInt32 g_total_client_thread_cnt;
extern UInt32 g_this_thread_cnt;
extern UInt32 g_this_rem_thread_cnt;
extern UInt32 g_this_send_thread_cnt;
extern UInt32 g_this_total_thread_cnt;
extern UInt32 g_thread_cnt;
extern UInt32 g_abort_thread_cnt;
extern UInt32 g_logger_thread_cnt;
extern UInt32 g_work_thread_cnt;
extern UInt32 g_tcp_thread_cnt;
extern UInt32 g_send_thread_cnt;
extern UInt32 g_rem_thread_cnt;
extern ts_t g_abort_penalty;
extern ts_t g_abort_penalty_max;
extern bool g_central_man;
extern UInt32 g_ts_alloc;
extern bool g_key_order;
extern bool g_ts_batch_alloc;
extern UInt32 g_ts_batch_num;
extern int32_t g_inflight_max;
extern uint64_t g_msg_size;
extern uint64_t g_log_buf_max;
extern uint64_t g_log_flush_timeout;

extern UInt64 memory_count;
extern UInt64 tuple_count;
extern UInt64 max_tuple_size;
extern pthread_mutex_t * RDMA_MEMORY_LATCH;

extern uint64_t rdma_buffer_size;
extern uint64_t client_rdma_buffer_size;
extern uint64_t rdma_index_size;
// MAAT
extern uint64_t rdma_txntable_size;
extern uint64_t row_set_length;

extern UInt32 g_max_txn_per_part;
extern int32_t g_load_per_server;

extern bool g_hw_migrate;
extern UInt32 g_network_delay;
extern UInt64 g_done_timer;
extern UInt64 g_batch_time_limit;
extern UInt64 g_seq_batch_time_limit;
extern UInt64 g_prog_timer;
extern UInt64 g_warmup_timer;
extern UInt64 g_msg_time_limit;

// MVCC
extern UInt64 g_max_read_req;
extern UInt64 g_max_pre_req;
extern UInt64 g_his_recycle_len;

extern uint64_t count_max;
// YCSB
extern UInt32 g_cc_alg;
extern ts_t g_query_intvl;
extern UInt32 g_part_per_txn;
extern double g_perc_multi_part;
extern double g_txn_read_perc;
extern double g_txn_write_perc;
extern double g_tup_read_perc;
extern double g_tup_write_perc;
extern double g_zipf_theta;
extern double g_data_perc;
extern double g_access_perc;
extern UInt64 g_synth_table_size;
extern UInt32 g_req_per_query;
extern bool g_strict_ppt;
extern UInt32 g_field_per_tuple;
extern UInt32 g_init_parallelism;
extern double g_mpr;
extern double g_mpitem;
extern double g_cross_dc_txn_perc;

// TPCC
extern UInt32 g_num_wh;
extern double g_perc_payment;
extern bool g_wh_update;
extern char * output_file;
extern char * input_file;
extern char * txn_file;
extern UInt32 g_max_items;
extern UInt32 g_dist_per_wh;
extern UInt32 g_cust_per_dist;
extern UInt32 g_max_items_per_txn;

extern uint64_t tpcc_idx_per_num;
extern uint64_t item_idx_num;
extern uint64_t wh_idx_num;
extern uint64_t stock_idx_num;
extern uint64_t dis_idx_num;
extern uint64_t cust_idx_num;
extern uint64_t order_idx_num ;
extern uint64_t ol_idx_num ;

extern uint64_t item_index_size ;
extern uint64_t wh_index_size ;
extern uint64_t stock_index_size ;
extern uint64_t dis_index_size ;
extern uint64_t cust_index_size ;
extern uint64_t cl_index_size ;
extern uint64_t order_index_size ;
extern uint64_t ol_index_size ;

// PPS (Product-Part-Supplier)
extern UInt32 g_max_parts_per;
extern UInt32 g_max_part_key;
extern UInt32 g_max_product_key;
extern UInt32 g_max_supplier_key;
extern double g_perc_getparts;
extern double g_perc_getproducts;
extern double g_perc_getsuppliers;
extern double g_perc_getpartbyproduct;
extern double g_perc_getpartbysupplier;
extern double g_perc_orderproduct;
extern double g_perc_updateproductpart;
extern double g_perc_updatepart;

extern boost::lockfree::queue<DAQuery*, boost::lockfree::fixed_sized<true>> da_query_queue;
extern DABlockQueue da_gen_qry_queue;
extern bool is_server;
extern map<uint64_t, ts_t> da_start_stamp_tab;
extern set<uint64_t> da_start_trans_tab;
extern map<uint64_t, ts_t> da_stamp_tab;
extern set<uint64_t> already_abort_tab;
extern string DA_history_mem;
extern bool abort_history;
extern ofstream commit_file;
extern ofstream abort_file;
// CALVIN
extern UInt32 g_seq_thread_cnt;
extern UInt64 rdma_calvin_buffer_size;

// TICTOC
extern uint32_t g_max_num_waits;

// Replication
extern UInt32 g_repl_type;
extern UInt32 g_repl_cnt;

enum RC { RCOK=0, Commit, Abort, WAIT, WAIT_REM, ERROR, FINISH, NONE};
enum RemReqType {
  INIT_DONE = 0,
    RLK,
    RULK,
    CL_QRY,
    CL_QRY_O,//one server but use the msg queue
    RQRY,
    RQRY_CONT,
    RFIN,
    RLK_RSP,
    RULK_RSP,
    RQRY_RSP,
    RACK,
    RACK_PREP,
    RLOG,
    RACK_LOG,
    RFIN_LOG,
    RACK_FIN_LOG,
    RACK_FIN,
    RTXN,
    RTXN_CONT,
    RINIT,
    RPREPARE,
    RPASS,
    RFWD,
    RDONE,
    CL_RSP,
    LOG_MSG,
    LOG_MSG_RSP,
    LOG_FLUSHED,
    CALVIN_ACK,
  NO_MSG
};

// Calvin
enum CALVIN_PHASE {
  CALVIN_RW_ANALYSIS = 0,
  CALVIN_LOC_RD,
  CALVIN_SERVE_RD,
  CALVIN_COLLECT_RD,
  CALVIN_EXEC_WR,
  CALVIN_DONE
};

//WOUND_WAIT
enum TxnStatus {
  RUNNING = 0,
  WOUNDED,
  STARTCOMMIT
};

/* Thread */
typedef uint64_t txnid_t;

/* Txn */
typedef uint64_t txn_t;

/* Table and Row */
typedef uint64_t rid_t; // row id
typedef uint64_t pgid_t; // page id



/* INDEX */
enum latch_t {LATCH_EX, LATCH_SH, LATCH_NONE};
// accessing type determines the latch type on nodes
enum idx_acc_t {INDEX_INSERT, INDEX_READ, INDEX_NONE};
typedef uint64_t idx_key_t; // key id for index
typedef uint64_t (*func_ptr)(idx_key_t);	// part_id func_ptr(index_key);

/* general concurrency control */
enum access_t {RD, WR, XP, SCAN};
/* LOCK */
enum lock_t {DLOCK_EX = 0, DLOCK_SH, LOCK_NONE };
/* TIMESTAMP */
enum TsType {R_REQ = 0, W_REQ, P_REQ, XP_REQ};
/* CICADA */
enum RecordStatus {COMMITED = 0, ABORTED, PENDING};

/*DA query build queue*/
//queue<DAQuery> query_build_queue;

#define GET_NODE_ID(id)	(id % g_node_cnt) //get id of the leader node. id: transaction id or partition id
// #define GET_FOLLOWER1_NODE(pid)	((pid + (g_node_cnt/g_center_cnt)) % g_node_cnt) //leader and follower1 are in the same data center
#define GET_FOLLOWER1_NODE(pid)	((pid + 2) % g_node_cnt) 
#define GET_FOLLOWER2_NODE(pid)	((pid + 1) % g_node_cnt) 
#define GET_CENTER_ID(nid) (nid % g_center_cnt) //nid: the id of node

#define GET_THREAD_ID(id)	(id % g_thread_cnt)
#define GET_PART_ID(t,n)	(n)
#define GET_PART_ID_FROM_IDX(idx)	(g_node_id + idx * g_node_cnt)
#define GET_PART_ID_IDX(p)	(p / g_node_cnt)
#define ISSERVER (g_node_id < g_node_cnt)
#define ISSERVERN(id) (id < g_node_cnt)
#define ISCLIENT (g_node_id >= g_node_cnt && g_node_id < g_node_cnt + g_client_node_cnt)
#define ISREPLICA                                 \
  (g_node_id >= g_node_cnt + g_client_node_cnt && \
   g_node_id < g_node_cnt + g_client_node_cnt + g_repl_cnt * g_node_cnt)
#define ISREPLICAN(id)                     \
  (id >= g_node_cnt + g_client_node_cnt && \
   id < g_node_cnt + g_client_node_cnt + g_repl_cnt * g_node_cnt)
#define ISCLIENTN(id) (id >= g_node_cnt && id < g_node_cnt + g_client_node_cnt)
#define IS_LOCAL(tid) (tid % g_node_cnt == g_node_id || CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN)
#define IS_REMOTE(tid) (tid % g_node_cnt != g_node_id || CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN)
#define IS_LOCAL_KEY(key) (key % g_node_cnt == g_node_id)

/*
#define GET_THREAD_ID(id)	(id % g_thread_cnt)
#define GET_NODE_ID(id)	(id / g_thread_cnt)
#define GET_PART_ID(t,n)	(n*g_thread_cnt + t)
*/

#define MSG(str, args...) \
  { printf("[%s : %d] " str, __FILE__, __LINE__, args); }  //	printf(args); }

// principal index structure. The workload may decide to use a different
// index structure for specific purposes. (e.g. non-primary key access should use hash)
#if (INDEX_STRUCT == IDX_BTREE)
#define INDEX		index_btree
#elif (INDEX_STRUCT == IDX_HASH)
#define  INDEX		IndexHash
// #elif (INDEX_STRUCT == IDX_RDMA_TPCC)
// #define INDEX       IndexRdmaTpcc
#else
#define INDEX		IndexRdma
#endif

/************************************************/
// constants
/************************************************/
#ifndef UINT64_MAX
#define UINT64_MAX 		18446744073709551615UL
#endif // UINT64_MAX

#endif

extern int total_num_atomic_retry;  
extern int max_num_atomic_retry;

extern int max_batch_index;

extern uint64_t extra_wait_time;

extern uint64_t total_local_txn_commit;
extern uint64_t total_num_msgs_rw;
extern uint64_t total_num_msgs_prep;
extern uint64_t total_num_msgs_commit;
extern uint64_t max_num_msgs_rw;
extern uint64_t max_num_msgs_prep;
extern uint64_t max_num_msgs_commit;