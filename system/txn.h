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

#ifndef _TXN_H_
#define _TXN_H_

#include "global.h"
#include "query.h"
#include "helper.h"
#include "semaphore.h"
#include "array.h"
#include "transport/message.h"
#include "worker_thread.h"
#include "routine.h"
#include <unordered_map>
#include "work_queue.h"
#include "index_rdma.h"
//#include "wl.h"

class Workload;
class Thread;
class WorkerThread;
class row_t;
class table_t;
class BaseQuery;
class INDEX;
class TxnQEntry;
class YCSBQuery;
class TPCCQuery;
//class r_query;

enum TxnState {START,INIT,EXEC,PREP,FIN,DONE};
enum BatchReadType {R_INDEX=0,R_ROW};

class Access {
public:
	access_t 	type;
	row_t * 	orig_row;
	row_t * 	data;
	row_t * 	orig_data;
	uint64_t    version;
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3
	uint64_t    location;   //node id of server the data location
	uint64_t    offset;
#endif
	uint64_t	key;
	uint64_t	partition_id;
	void cleanup();
};

class Transaction {
public:
	void init();
	void reset(uint64_t thd_id);
	void release_accesses(uint64_t thd_id);
	void release_inserts(uint64_t thd_id);
	void release(uint64_t thd_id);
	//vector<Access*> accesses;
	Array<Access*> accesses;
	uint64_t timestamp;
	// For OCC and SSI
	uint64_t start_timestamp;
	uint64_t end_timestamp;

	uint64_t write_cnt;
	uint64_t row_cnt;
	// Internal state
	TxnState twopc_state;
	Array<row_t*> insert_rows;
	txnid_t         txn_id;
	uint64_t batch_id;
	RC rc;
};

enum TxnPhase {NULL_PHASE,EXECUTION_PHASE, FINISH_PHASE, DONE_PHASE};

class TxnStats {
public:
	void init();
	void clear_short();
	void reset();
	void abort_stats(uint64_t thd_id);
	void commit_stats(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id, uint64_t timespan_long,
										uint64_t timespan_short);
	uint64_t starttime;
	uint64_t restart_starttime;
	// for redt
	uint64_t wait_for_rsp_time;
	TxnPhase current_states; // EXECUTION_PHASE, PREPARE_PHASE, FINISH_PHASE

  	uint64_t init_complete_time;
	uint64_t wait_starttime;
	uint64_t write_cnt;
	uint64_t abort_cnt;
	uint64_t prepare_start_time;
	uint64_t finish_start_time;
	double total_process_time;
	double process_time;
	double total_local_wait_time;
	double local_wait_time;
	double total_remote_wait_time;  // time waiting for a remote response, to help calculate network
																	// time
	double remote_wait_time;
	double total_twopc_time;
	double twopc_time;
	double total_abort_time; // time spent in aborted query land
	double total_msg_queue_time; // time spent on outgoing queue
	double msg_queue_time;
	double total_work_queue_time; // time spent on work queue
	double work_queue_time;
	double total_cc_block_time; // time spent blocking on a cc resource
	double cc_block_time;
	double total_cc_time; // time spent actively doing cc
	double cc_time;
	uint64_t total_work_queue_cnt;
	uint64_t work_queue_cnt;

	// short stats
	double work_queue_time_short;
	double cc_block_time_short;
	double cc_time_short;
	double msg_queue_time_short;
	double process_time_short;
	double network_time_short;

	double lat_network_time_start;
	double lat_other_time_start;

	int unlock_atomic_failed_count;
	int lock_atomic_failed_count;
};

/*
	 Execution of transactions
	 Manipulates/manages Transaction (contains txn-specific data)
	 Maintains BaseQuery (contains input args, info about query)
	 */
class TxnManager {
public:
	virtual ~TxnManager() {}
	virtual void init(uint64_t thd_id,Workload * h_wl);
	virtual void reset();
	void clear();
	void reset_query();
	void release();
#if USE_COROUTINE
    WorkerThread * h_thd;
	uint64_t _cor_id;
#else
	Thread * h_thd;
#endif
	Workload * h_wl;

	virtual RC      run_txn(yield_func_t &yield, uint64_t cor_id) = 0;
#if USE_REPLICA
	virtual RC 		redo_log(yield_func_t &yield, RC status, uint64_t cor_id) = 0;
	virtual RC 		redo_commit_log(yield_func_t &yield, RC status, uint64_t cor_id) = 0;
#endif


	virtual RC      run_txn_post_wait() = 0;
	virtual RC      run_calvin_txn(yield_func_t &yield,uint64_t cor_id) = 0;
	virtual RC      acquire_locks() = 0;
	virtual RC 		send_remote_request() = 0;
	#if !USE_COROUTINE
	void            register_thread(Thread * h_thd);
	#else
	void            register_thread(WorkerThread * h_thd, uint64_t cor_id);
	#endif
	void            register_coroutine(WorkerThread * h_thd, uint64_t cor_id);
	uint64_t        get_thd_id();
	Workload *      get_wl();
	void            set_txn_id(txnid_t txn_id);
	txnid_t         get_txn_id();
	void            set_query(BaseQuery * qry);
	BaseQuery *     get_query();
	bool            is_done();
	void            commit_stats();
	bool            is_multi_part();

	void            set_timestamp(ts_t timestamp);
	ts_t            get_timestamp();
	void            set_start_timestamp(uint64_t start_timestamp);
	ts_t            get_start_timestamp();
	uint64_t        get_rsp_cnt() {return rsp_cnt;}
	bool			need_extra_wait() {
		#if WORKLOAD == YCSB
		for(int i=0;i<REQ_PER_QUERY;i++){
			if(req_need_wait[i]) return true;
		}
		return false;
		#else 
		if (cus_need_wait || wh_need_wait) return true;
		for(int i=0;i<MAX_ITEMS_PER_TXN;i++){
			if(req_need_wait[i]) return true;
		}
		return false;
		#endif
	}
	uint64_t        incr_rsp(int i);
	uint64_t        decr_rsp(int i);
	uint64_t        incr_lr();
	uint64_t        decr_lr();

	RC commit(yield_func_t &yield, uint64_t cor_id);
	RC start_commit(yield_func_t &yield, uint64_t cor_id);
	RC start_abort(yield_func_t &yield, uint64_t cor_id);
	RC abort(yield_func_t &yield, uint64_t cor_id);

	void release_locks(yield_func_t &yield, RC rc, uint64_t cor_id);

	bool rdma_one_side() {
	if (CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3) return true;
	else return false;
	}

    uint64_t get_part_num(uint64_t num,uint64_t part);
	RC get_remote_row(yield_func_t &yield, access_t type, uint64_t key, uint64_t loc, itemid_t *m_item, row_t *& row_local, uint64_t cor_id);
    RC preserve_access(row_t *&row_local,itemid_t* m_item,row_t *test_row,access_t type,uint64_t key,uint64_t loc,uint64_t part_id);

	//---- Base RDMA primitives ----//
	RC read_remote_content(yield_func_t &yield, uint64_t target_server, uint64_t remote_offset, uint64_t operate_size, char* local_buf, uint64_t cor_id);
	RC write_remote_content(yield_func_t &yield, uint64_t target_server, uint64_t operate_size, uint64_t remote_offset, char *write_content, char* local_buf, uint64_t cor_id, bool outstanding = false);
	//---- Next step RDMA primitives ----//
	RC read_remote_index(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t key, itemid_t * &item, uint64_t cor_id);
	RC read_remote_row(yield_func_t &yield, uint64_t target_server, uint64_t remote_offset, row_t * &row, uint64_t cor_id, uint64_t key = UINT64_MAX);
	RC write_remote_index(yield_func_t &yield, uint64_t target_server, uint64_t operate_size, uint64_t remote_offset, char *write_content, uint64_t cor_id);
    RC write_remote_row(yield_func_t &yield, uint64_t target_server, uint64_t operate_size, uint64_t remote_offset, char *write_content, uint64_t cor_id);
    RC cas_remote_content(yield_func_t &yield, uint64_t target_server, uint64_t remote_offset, uint64_t old_value, uint64_t new_value, uint64_t *result, uint64_t cor_id);
    RC faa_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset, uint64_t value_to_add, uint64_t *result, uint64_t cor_id, int num = 1, bool outstanding = false);
	RC loop_cas_remote(yield_func_t &yield,uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value, uint64_t cor_id);
#if USE_REPLICA
	RC read_remote_log_head(yield_func_t &yield, uint64_t target_server, uint64_t* result, uint64_t cor_id);
 	RC check_log_in_remote_buffer(yield_func_t &yield, uint64_t target_server, uint64_t remote_offset, uint64_t *result_offset, uint64_t cor_id);
 	RC write_remote_log(yield_func_t &yield, uint64_t target_server, uint64_t operate_size, uint64_t remote_offset, char *write_content, uint64_t cor_id, int num = 1, bool outstanding = false);
#endif

	// For check timeout
	bool is_time_out() {
		uint64_t now = get_wall_clock();
		uint64_t last_ts = last_send_msg;
		// DEBUG_T("txn %ld check time out, %lu:%lu\n",get_txn_id(),last_ts,now);
		if (now > last_ts && (now - last_ts) > (EXECUTOR_FAILED_TIME)) {
			DEBUG_T("txn %ld is time out, %lu:%lu\n",get_txn_id(),last_ts,now);
			return true;
		} else return false;
	}
	void update_send_time() {
		last_send_msg = get_wall_clock();
		DEBUG_T("txn %ld update last send time %lu\n",get_txn_id(),last_send_msg);
	}
	bool is_enqueue = false;
	wait_list_entry* wait_queue_entry;
	uint64_t new_coordinator = -1;
	bool is_recover = false;
	bool is_logged = false;
	// For recover
	Array<uint64_t> failed_partition;
	virtual void insert_failed_partition(uint64_t key) = 0;
	bool all_partition_alive(); 
	// Check response
	inline void update_single_query_status(execute_node &node,uint64_t return_id, OpStatus status) {
		if (node.execute_node == return_id) {
			for (int j = 0; j < failed_partition.size(); j++) {
				if (failed_partition[j] == node.stored_node) {
					// 当目前状态还没进入准备阶段之前，节点状态设置为PREP_ABORT
					// 否则，设置为COM_ABORT。
					node.status = status < PREP_ABORT ? PREP_ABORT : COM_ABORT;
					break;
				}
			}	
			if (node.status != PREP_ABORT &&
				node.status != COM_ABORT &&
				node.status < status) node.status = status;
		}
	}
	virtual RC check_query_status(OpStatus status) = 0;
	virtual void update_query_status(uint64_t return_id, OpStatus status) = 0;
	virtual void update_query_status(bool timeout_check) = 0;
  	virtual RC resend_remote_subtxn() = 0;
	virtual RC agent_check_commit() = 0;

	bool isRecon() {
		assert(CC_ALG == CALVIN || !recon);
		return recon;
	};
	bool recon;

	row_t * volatile cur_row;
	// [DL_DETECT, NO_WAIT, WAIT_DIE]
	int volatile   lock_ready;
	// [TIMESTAMP, MVCC]
	bool volatile   ts_ready;
	// [HSTORE, HSTORE_SPEC]
	int volatile    ready_part;
	int volatile    ready_ulk;
	std::map<uint64_t, uint64_t> center_master;
	bool is_primary[CENTER_CNT]; //is center_master has primary replica or not
	uint64_t num_msgs_rw_prep;
	uint64_t num_msgs_commit;	

	int wh_extra_wait[2];
	bool wh_need_wait;
	//for payment
	int cus_extra_wait[2];
	bool cus_need_wait;
	//for neworder

	#if WORKLOAD == YCSB
	int extra_wait[REQ_PER_QUERY][2];
	bool req_need_wait[REQ_PER_QUERY];
	#elif WORKLOAD == TPCC
	int extra_wait[MAX_ITEMS_PER_TXN][2];
	bool req_need_wait[MAX_ITEMS_PER_TXN];
	#endif
	bool send_RQRY_RSP;

#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3
    int             write_set[100];
    int*            read_set;
	int				num_atomic_retry; //num of txn atomic_retry
	int				num_locks;
#endif

#if CC_ALG == WOUND_WAIT
	TxnStatus		txn_state;
#endif
	bool aborted;
	uint64_t return_id;
	RC        		validate(yield_func_t &yield, uint64_t cor_id);
	void            cleanup(yield_func_t &yield, RC rc, uint64_t cor_id);
	void            cleanup_row(yield_func_t &yield, RC rc,uint64_t rid, vector<vector<uint64_t>>&remote_access, uint64_t cor_id);
	void 			release_last_row_lock();
	RC 				send_remote_reads();

	void set_end_timestamp(uint64_t timestamp) {txn->end_timestamp = timestamp;}

	uint64_t get_end_timestamp() {return txn->end_timestamp;}
	uint64_t get_access_cnt() {return txn->row_cnt;}
	uint64_t get_write_set_size() {return txn->write_cnt;}
	uint64_t get_read_set_size() {return txn->row_cnt - txn->write_cnt;}
	
	access_t get_access_type(uint64_t access_id) {return txn->accesses[access_id]->type;}
	uint64_t get_access_version(uint64_t access_id) { return txn->accesses[access_id]->version; }
	row_t * get_access_original_row(uint64_t access_id) {return txn->accesses[access_id]->orig_row;}
	void swap_accesses(uint64_t a, uint64_t b) { txn->accesses.swap(a, b); }

	uint64_t get_batch_id() {return txn->batch_id;}
	void set_batch_id(uint64_t batch_id) {txn->batch_id = batch_id;}

	// For MaaT
	uint64_t commit_timestamp;
	uint64_t get_commit_timestamp() {return commit_timestamp;}
	void set_commit_timestamp(uint64_t timestamp) {commit_timestamp = timestamp;}
	uint64_t greatest_write_timestamp;
	uint64_t greatest_read_timestamp;
	std::set<uint64_t> * uncommitted_reads;
	std::set<uint64_t> * uncommitted_writes;
	std::set<uint64_t> * uncommitted_writes_y;

	uint64_t twopl_wait_start;

	uint64_t _timestamp;
	uint64_t get_priority() { return _timestamp; }
	// debug time
	uint64_t _start_wait_time;
	uint64_t _lock_acquire_time;
	uint64_t _lock_acquire_time_commit;
	uint64_t _lock_acquire_time_abort;
	////////////////////////////////
	// LOGGING
	////////////////////////////////
	bool log_flushed;
	bool repl_finished;
	Transaction * txn;
	BaseQuery * query;
	uint64_t client_startts;
	uint64_t client_id;
	uint64_t get_abort_cnt() {return abort_cnt;}
	uint64_t abort_cnt;
	int received_response(RC rc);
	int received_response(AckMessage* msg, OpStatus state);
	bool waiting_for_response();
	RC get_rc() {return txn->rc;}
	void set_rc(RC rc) {txn->rc = rc;}
	//void send_rfin_messages(RC rc) {assert(false);}
	void send_finish_messages();
	void send_prepare_messages();

	TxnStats txn_stats;

	bool set_ready() {return ATOM_CAS(txn_ready,0,1);}
	bool unset_ready() {return ATOM_CAS(txn_ready,1,0);}
	bool is_ready() {return txn_ready == true;}
	volatile int txn_ready;
	volatile bool finish_logging;

	// Calvin
	uint32_t lock_ready_cnt;
	uint32_t calvin_expected_rsp_cnt;
	bool locking_done;
	CALVIN_PHASE phase;
	Array<row_t*> calvin_locked_rows;
	bool calvin_exec_phase_done();
	bool calvin_collect_phase_done();

	int last_batch_id;
	int last_txn_id;
	Message* last_msg;
	uint64_t log_idx[NODE_CNT]; //redo_log_buf.get_size() if no log 

	IndexInfo* last_index;

protected:
	int rsp_cnt;
	void            insert_row(row_t * row, table_t * table);

	itemid_t *      index_read(INDEX * index, idx_key_t key, int part_id);
	itemid_t *      index_read(INDEX * index, idx_key_t key, int part_id, int count);
	RC get_lock(row_t * row, access_t type);
	//RC get_row(row_t * row, access_t type, row_t *& row_rtn);
    RC get_row(yield_func_t &yield,row_t * row, access_t type, row_t *& row_rtn,uint64_t cor_id);
	RC get_row_post_wait(row_t *& row_rtn);

	// For Waiting
	row_t * last_row;
	row_t * last_row_rtn;
	access_t last_type;

	// For check timeout
	//! For coordinator, it will be updated after each message is sent
	//! For agent coordinator/executor, it will be updated after return RACK_PREP
	uint64_t last_send_msg; 

	sem_t rsp_mutex;
	bool registed_;
#if BATCH_INDEX_AND_READ
  	map<int, itemid_t*> reqId_index;
  	map<int, row_t*> reqId_row;
#endif
};

#endif

