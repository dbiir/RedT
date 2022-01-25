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

#include "global.h"
#include "mem_alloc.h"
#include "stats.h"
#include "sim_manager.h"
#include "manager.h"
#include "query.h"
#include "client_query.h"
#include "occ.h"
#include "bocc.h"
#include "focc.h"
#include "ssi.h"
#include "wsi.h"
#include "cicada.h"
#include "transport.h"
#include "rdma.h"
#include "work_queue.h"
#include "abort_queue.h"
#include "client_query.h"
#include "client_txn.h"
#include "logger.h"
#include "maat.h"
#include "manager.h"
#include "mem_alloc.h"
#include "msg_queue.h"
#include "pool.h"
#include "query.h"
#include "sequencer.h"
#include "dli.h"
#include "sim_manager.h"
#include "stats.h"
#include "transport.h"
#include "txn_table.h"
#include "work_queue.h"
#include "dta.h"
#include "client_txn.h"
#include "sequencer.h"
#include "logger.h"
#include "maat.h"
#include "wkdb.h"
#include "tictoc.h"
#include "rdma_silo.h"
#include "rdma_mocc.h"
#include "rdma_mvcc.h"
#include "rdma_2pl.h"
#include "rdma_maat.h"
#include "rdma_ts1.h"
#include "rdma_ts.h"
#include "rdma_cicada.h"
#include "rdma_calvin.h"
#include "rdma_null.h"
#include "rdma_dslr_no_wait.h"
#include "key_xid.h"
#include "rts_cache.h"
#include "src/allocator_master.hh"
//#include "rdma_ctrl.hpp"
#include "lib.hh"
#include <boost/lockfree/queue.hpp>
#include "da_block_queue.h"
#include "wl.h"
#ifdef USE_RDMA
  #include "qps/rc_recv_manager.hh"
  #include "qps/recv_iter.hh"
  #include "qps/mod.hh"
  using namespace rdmaio;
  using namespace rdmaio::rmem;
  using namespace rdmaio::qp;
  using namespace std;
#endif

mem_alloc mem_allocator;
Stats stats;
SimManager * simulation;
Manager glob_manager;
Query_queue query_queue;
Client_query_queue client_query_queue;
OptCC occ_man;
Dli dli_man;
Focc focc_man;
Bocc bocc_man;
Maat maat_man;
Dta dta_man;
Wkdb wkdb_man;
ssi ssi_man;
wsi wsi_man;
Cicada cicada_man;
Tictoc tictoc_man;
Transport tport_man;
Rdma rdma_man;
#if CC_ALG == RDMA_SILO
RDMA_silo rsilo_man;
#elif CC_ALG == RDMA_MOCC
RDMA_mocc rmocc_man;
#elif CC_ALG == RDMA_MVCC
rdma_mvcc rmvcc_man;
#endif
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
RDMA_2pl r2pl_man;
#endif
#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT || CC_ALG == RDMA_TS || CC_ALG == RDMA_TS1
RdmaTxnTable rdma_txn_table;
#endif
#if CC_ALG == RDMA_DSLR_NO_WAIT
RDMA_dslr_no_wait dslr_man;
#endif
#if CC_ALG == RDMA_MAAT
RDMA_Maat rmaat_man;
RdmaTxnTable rdma_txn_table;
#endif
#if CC_ALG == RDMA_TS1
RDMA_ts1 rdmats_man;
#endif
#if CC_ALG == RDMA_TS
RDMA_ts rdmats_man;
#endif
#if CC_ALG == RDMA_CICADA
RDMA_Cicada rcicada_man;
#endif
#if CC_ALG == RDMA_CALVIN
RDMA_calvin calvin_man;
#endif
#if CC_ALG == RDMA_CNULL
RDMA_Null rcnull_man;
#endif
Workload * m_wl;
TxnManPool txn_man_pool;
TxnPool txn_pool;
AccessPool access_pool;
TxnTablePool txn_table_pool;
MsgPool msg_pool;
RowPool row_pool;
QryPool qry_pool;
TxnTable txn_table;
QWorkQueue work_queue;
AbortQueue abort_queue;
MessageQueue msg_queue;
Client_txn client_man;
Sequencer seq_man;
Logger logger;
TimeTable time_table;
DtaTimeTable dta_time_table;
KeyXidCache dta_key_xid_cache;
RtsCache dta_rts_cache;
InOutTable inout_table;
WkdbTimeTable wkdb_time_table;
KeyXidCache wkdb_key_xid_cache;
RtsCache wkdb_rts_cache;
// QTcpQueue tcp_queue;
// TcpTimestamp tcp_ts;

boost::lockfree::queue<DAQuery*, boost::lockfree::fixed_sized<true>> da_query_queue{100};
DABlockQueue da_gen_qry_queue(50);
bool is_server=false;
map<uint64_t, ts_t> da_start_stamp_tab;
set<uint64_t> da_start_trans_tab;
map<uint64_t, ts_t> da_stamp_tab;
set<uint64_t> already_abort_tab;
string DA_history_mem;
bool abort_history;
ofstream commit_file;
ofstream abort_file;

bool volatile warmup_done = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t warmup_bar;

ts_t g_abort_penalty = ABORT_PENALTY;
ts_t g_abort_penalty_max = ABORT_PENALTY_MAX;
bool g_central_man = CENTRAL_MAN;
UInt32 g_ts_alloc = TS_ALLOC;
bool g_key_order = KEY_ORDER;
bool g_ts_batch_alloc = TS_BATCH_ALLOC;
UInt32 g_ts_batch_num = TS_BATCH_NUM;
int32_t g_inflight_max = MAX_TXN_IN_FLIGHT;
//int32_t g_inflight_max = MAX_TXN_IN_FLIGHT/NODE_CNT;
uint64_t g_msg_size = MSG_SIZE_MAX;
int32_t g_load_per_server = LOAD_PER_SERVER;

bool g_hw_migrate = HW_MIGRATE;

volatile UInt64 g_row_id = 0;
bool g_part_alloc = PART_ALLOC;
bool g_mem_pad = MEM_PAD;
UInt32 g_cc_alg = CC_ALG;
ts_t g_query_intvl = QUERY_INTVL;
UInt32 g_part_per_txn = PART_PER_TXN;
double g_perc_multi_part = PERC_MULTI_PART;
double g_txn_read_perc = 1.0 - TXN_WRITE_PERC;
double g_txn_write_perc = TXN_WRITE_PERC;
double g_tup_read_perc = 1.0 - TUP_WRITE_PERC;
double g_tup_write_perc = TUP_WRITE_PERC;
double g_zipf_theta = ZIPF_THETA;
double g_data_perc = DATA_PERC;
double g_access_perc = ACCESS_PERC;
bool g_prt_lat_distr = PRT_LAT_DISTR;
UInt32 g_node_id = 0;
UInt32 g_node_cnt = NODE_CNT;
UInt32 g_part_cnt = PART_CNT;
UInt32 g_virtual_part_cnt = VIRTUAL_PART_CNT;
UInt32 g_core_cnt = CORE_CNT;

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
UInt32 g_thread_cnt = PART_CNT/NODE_CNT;
#else
UInt32 g_thread_cnt = THREAD_CNT;
#endif
UInt32 g_rem_thread_cnt = REM_THREAD_CNT;
UInt32 g_abort_thread_cnt = 1;
#if LOGGING
UInt32 g_logger_thread_cnt = 1;
#else
UInt32 g_logger_thread_cnt = 0;
#endif
#if USE_WORK_NUM_THREAD
UInt32 g_work_thread_cnt = 1;
#else
UInt32 g_work_thread_cnt = 0;
#endif
UInt32 g_send_thread_cnt = SEND_THREAD_CNT;
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
// sequencer + scheduler thread
UInt32 g_total_thread_cnt = g_thread_cnt + g_rem_thread_cnt + g_send_thread_cnt + g_abort_thread_cnt + g_logger_thread_cnt + 2 + g_work_thread_cnt;
#else
UInt32 g_total_thread_cnt = g_thread_cnt + g_rem_thread_cnt + g_send_thread_cnt + g_abort_thread_cnt + g_logger_thread_cnt + g_work_thread_cnt;
#endif

UInt32 g_total_client_thread_cnt = g_client_thread_cnt + g_client_rem_thread_cnt + g_client_send_thread_cnt;
UInt32 g_total_node_cnt = g_node_cnt + g_client_node_cnt + g_repl_cnt*g_node_cnt;
UInt64 g_synth_table_size = SYNTH_TABLE_SIZE;
UInt32 g_req_per_query = REQ_PER_QUERY;
bool g_strict_ppt = STRICT_PPT == 1;
UInt32 g_field_per_tuple = FIELD_PER_TUPLE;
UInt32 g_init_parallelism = INIT_PARALLELISM;
UInt32 g_client_node_cnt = CLIENT_NODE_CNT;
UInt32 g_client_thread_cnt = CLIENT_THREAD_CNT;
UInt32 g_client_rem_thread_cnt = CLIENT_REM_THREAD_CNT;
UInt32 g_client_send_thread_cnt = CLIENT_SEND_THREAD_CNT;
UInt32 g_servers_per_client = 0;
UInt32 g_clients_per_server = 0;
UInt32 g_server_start_node = 0;

UInt32 g_this_thread_cnt = ISCLIENT ? g_client_thread_cnt : g_thread_cnt;
UInt32 g_this_rem_thread_cnt = ISCLIENT ? g_client_rem_thread_cnt : g_rem_thread_cnt;
UInt32 g_this_send_thread_cnt = ISCLIENT ? g_client_send_thread_cnt : g_send_thread_cnt;
UInt32 g_this_total_thread_cnt = ISCLIENT ? g_total_client_thread_cnt : g_total_thread_cnt;

UInt32 g_max_txn_per_part = MAX_TXN_PER_PART;
UInt32 g_network_delay = NETWORK_DELAY;
UInt64 g_done_timer = DONE_TIMER;
UInt64 g_batch_time_limit = BATCH_TIMER;
UInt64 g_seq_batch_time_limit = SEQ_BATCH_TIMER;
UInt64 g_prog_timer = PROG_TIMER;
UInt64 g_warmup_timer = WARMUP_TIMER;
UInt64 g_msg_time_limit = MSG_TIME_LIMIT;

UInt64 g_log_buf_max = LOG_BUF_MAX;
UInt64 g_log_flush_timeout = LOG_BUF_TIMEOUT;

UInt64 memory_count = 0;
UInt64 tuple_count = 0;
UInt64 max_tuple_size = 0;
pthread_mutex_t * RDMA_MEMORY_LATCH;

UInt64 rdma_buffer_size = 16*(1024*1024*1024L);
UInt64 client_rdma_buffer_size = 600*(1024*1024L);
UInt64 rdma_index_size = (1024*1024*1024L);

// MAAT
UInt64 rdma_txntable_size = 30*1024*1024; //4*(1024*1024*1024L);//30*1024*1024;
UInt64 row_set_length = floor(ROW_SET_LENGTH);

// MVCC
UInt64 g_max_read_req = MAX_READ_REQ;
UInt64 g_max_pre_req = MAX_PRE_REQ;
UInt64 g_his_recycle_len = HIS_RECYCLE_LEN;

// CALVIN
UInt32 g_seq_thread_cnt = SEQ_THREAD_CNT;
UInt64 rdma_calvin_buffer_size = 400*1024*1024;

// TICTOC
uint32_t g_max_num_waits = MAX_NUM_WAITS;

double g_mpr = MPR;
double g_mpitem = MPIR;

uint64_t count_max = 32768;

// PPS (Product-Part-Supplier)
UInt32 g_max_parts_per = MAX_PPS_PARTS_PER;
UInt32 g_max_part_key = MAX_PPS_PART_KEY;
UInt32 g_max_product_key = MAX_PPS_PRODUCT_KEY;
UInt32 g_max_supplier_key = MAX_PPS_SUPPLIER_KEY;
double g_perc_getparts = PERC_PPS_GETPART;
double g_perc_getproducts = PERC_PPS_GETPRODUCT;
double g_perc_getsuppliers = PERC_PPS_GETSUPPLIER;
double g_perc_getpartbyproduct = PERC_PPS_GETPARTBYPRODUCT;
double g_perc_getpartbysupplier = PERC_PPS_GETPARTBYSUPPLIER;
double g_perc_orderproduct = PERC_PPS_ORDERPRODUCT;
double g_perc_updateproductpart = PERC_PPS_UPDATEPRODUCTPART;
double g_perc_updatepart = PERC_PPS_UPDATEPART;

// TPCC
UInt32 g_num_wh = NUM_WH;
double g_perc_payment = PERC_PAYMENT;
bool g_wh_update = WH_UPDATE;
char * output_file = NULL;
char * input_file = NULL;
char * txn_file = NULL;

#if TPCC_SMALL
UInt32 g_max_items = MAX_ITEMS_SMALL;
UInt32 g_cust_per_dist = CUST_PER_DIST_SMALL;
#else
UInt32 g_max_items = MAX_ITEMS_NORM;
UInt32 g_cust_per_dist = CUST_PER_DIST_NORM;
#endif
UInt32 g_max_items_per_txn = MAX_ITEMS_PER_TXN;
UInt32 g_dist_per_wh = DIST_PER_WH;

UInt32 g_repl_type = REPL_TYPE;
UInt32 g_repl_cnt = REPLICA_CNT;

uint64_t tpcc_idx_per_num = (700000 * NUM_WH)/PART_CNT ;

uint64_t item_idx_num = 100000 * g_node_cnt;//A copy of the item table is stored on each server, *g_node_cnt easy to caculate
uint64_t wh_idx_num = NUM_WH;
uint64_t stock_idx_num = 100000 * NUM_WH;
uint64_t dis_idx_num = 10 * NUM_WH;
uint64_t cust_idx_num = 30000 * NUM_WH;
// uint64_t his_idx_num = ;
// uint64_t new_o_idx_num = ;
uint64_t order_idx_num = 30000 * NUM_WH;
uint64_t ol_idx_num = 300000 * NUM_WH;

uint64_t item_index_size = (10 * 1024 *1024L) ;
uint64_t wh_index_size = (1024 * 1024L);
uint64_t stock_index_size = (10 * 1024 *1024L);
uint64_t dis_index_size = (1024 *1024L);
uint64_t cust_index_size = (2 * 1024 *1024L);
uint64_t cl_index_size = (2 * 1024 *1024L);
uint64_t order_index_size = (2 * 1024 *1024L);
uint64_t ol_index_size = (20 * 1024 *1024L);



map<string, string> g_params;

char *rdma_global_buffer;
char *rdma_txntable_buffer;
// CALVIN shared memory
char *rdma_calvin_buffer;
//rdmaio::Arc<rdmaio::rmem::RMem> rdma_global_buffer;
rdmaio::Arc<rdmaio::rmem::RMem> rdma_rm;
//Each thread uses only its own piece of client memory address
/* client_rdma_rm suppose there are 4 work thd 
 * size:IndexInfo //thd 0 read index
 * size:IndexInfo //thd 1 read index
 * size:IndexInfo //thd 2 read index
 * size:IndexInfo //thd 3 read index
 * size:Row //thd 0 read row
 * size:Row //thd 1 read row
 * size:Row //thd 2 read row
 * size:Row //thd 3 read row
 */
rdmaio::Arc<rdmaio::rmem::RMem> client_rdma_rm;
rdmaio::Arc<rdmaio::rmem::RegHandler> rm_handler;
rdmaio::Arc<rdmaio::rmem::RegHandler> client_rm_handler;

std::vector<rdmaio::ConnectManager> cm;
rdmaio::Arc<rdmaio::RCtrl> rm_ctrl;
rdmaio::Arc<rdmaio::RNic> nic;
// rdmaio::Arc<rdmaio::qp::RDMARC> rc_qp[NODE_CNT][THREAD_CNT * (COROUTINE_CNT + 1)];
rdmaio::Arc<rdmaio::qp::RDMARC> rc_qp[NODE_CNT][RDMA_MAX_CLIENT_QP * (COROUTINE_CNT + 1)];
pthread_mutex_t * RDMA_QP_LATCH;
rdmaio::rmem::RegAttr remote_mr_attr[NODE_CNT];

string rdma_server_add[NODE_CNT];
// string qp_name[NODE_CNT][THREAD_CNT * (COROUTINE_CNT + 1)];
string qp_name[NODE_CNT][RDMA_MAX_CLIENT_QP * (COROUTINE_CNT + 1)];
//rdmaio::ConnectManager cm[NODE_CNT];

int rdma_server_port[NODE_CNT];

//rdmaio::Arc<rdmaio::rmem::RegHandler> local_mr[NODE_CNT][THREAD_CNT];



//r2::Allocator *r2_allocator;
//rdmaio::RCQP *qp[NODE_CNT][THREAD_CNT];
bool g_init_done[50] = {false};
int g_init_cnt = 0;

int total_num_atomic_retry = 0;  
int max_num_atomic_retry = 0;

//the maximum number of doorbell batched row
int max_batch_index = REQ_PER_QUERY; 
