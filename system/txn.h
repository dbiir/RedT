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
#include "helper.h"
#include "semaphore.h"
#include "array.h"
#include "row_rdma_maat.h"
#include "rdma_maat.h"
#include "transport/message.h"
#include "worker_thread.h"
#include "routine.h"
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
#if CC_ALG == TICTOC
	uint64_t    orig_wts;
	uint64_t    orig_rts;
	bool         locked;
#endif
#if CC_ALG == SILO
	ts_t 		tid;
	// ts_t 		epoch;
#endif
#if CC_ALG == RDMA_SILO || CC_ALG == RDMA_MVCC
    uint64_t 	key;
	ts_t 		tid;
	ts_t		timestamp;
    row_t * 	test_row;
	uint64_t    location;
	uint64_t    offset;
    uint64_t    old_version_num;
#endif
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
	uint64_t    location;   //node id of server the data location
	uint64_t    offset;
#endif
#if CC_ALG == RDMA_MAAT || CC_ALG ==RDMA_TS1 || CC_ALG == RDMA_CICADA || CC_ALG == RDMA_CNULL
	uint64_t 	key;
    uint64_t	location;
	uint64_t	offset;
#endif
#if CC_ALG == CICADA
	uint64_t	recordId;	//already readed record id
#endif
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
	RC 		redo_log(yield_func_t &yield, RC status, uint64_t cor_id);
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
  if (CC_ALG == RDMA_SILO || CC_ALG == RDMA_MVCC || CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_MAAT || CC_ALG ==RDMA_TS1 || CC_ALG == RDMA_CICADA || CC_ALG == RDMA_CNULL || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT) return true;
  else return false;
}

    row_t * read_remote_row(uint64_t target_server,uint64_t remote_offset);
    itemid_t * read_remote_index(uint64_t target_server,uint64_t remote_offset,uint64_t key);
// #if CC_ALG == RDMA_MAAT
    RdmaTxnTableNode * read_remote_timetable(uint64_t target_server,uint64_t remote_offset);
// #endif

    bool write_remote_row(uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content);
    bool write_remote_index(uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content);
    bool write_unlock_remote_content(uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *local_buf);//TODO

    bool get_version(row_t * temp_row,uint64_t * change_num,Transaction *txn);
    uint64_t cas_remote_content(uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value );
	bool loop_cas_remote(uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value);
    RC preserve_access(row_t *&row_local,itemid_t* m_item,row_t *test_row,access_t type,uint64_t key,uint64_t loc);
#if USE_DBPAOR == true
	void batch_unlock_remote(yield_func_t &yield, uint64_t cor_id, int loc, RC rc, TxnManager * txnMng , vector<vector<uint64_t>> remote_index_origin,ts_t time = 0, vector<vector<uint64_t>> remote_num = {{0}});
	row_t * cas_and_read_remote(yield_func_t &yield, uint64_t& try_lock, uint64_t target_server, uint64_t cas_offset, uint64_t read_offset, uint64_t compare, uint64_t swap, uint64_t cor_id);
#endif
#if BATCH_INDEX_AND_READ
    void batch_read(yield_func_t &yield, BatchReadType rtype,int loc, vector<vector<uint64_t>> remote_index_origin, uint64_t cor_id);
	void get_batch_read(yield_func_t &yield, BatchReadType rtype,int loc, vector<vector<uint64_t>> remote_index_origin, uint64_t cor_id);
#endif
//***********coroutine**********//

	row_t * read_remote_row(yield_func_t &yield, uint64_t target_server, uint64_t remote_offset, uint64_t cor_id);
    itemid_t * read_remote_index(yield_func_t &yield, uint64_t target_server, uint64_t remote_offset, uint64_t key, uint64_t cor_id);
	uint64_t read_remote_log_head(yield_func_t &yield, uint64_t target_server, uint64_t cor_id);
	void read_remote_content(yield_func_t &yield, uint64_t target_server, uint64_t remote_offset, uint64_t operate_size, char* local_buf, uint64_t cor_id);

// #if CC_ALG == RDMA_MAAT
    RdmaTxnTableNode * read_remote_timetable(yield_func_t &yield, uint64_t target_server, uint64_t remote_offset, uint64_t cor_id);
	char * read_remote_txntable(yield_func_t &yield, uint64_t target_server, uint64_t remote_offset, uint64_t cor_id);
// #endif

    bool write_remote_index(yield_func_t &yield, uint64_t target_server, uint64_t operate_size, uint64_t remote_offset, char *write_content, uint64_t cor_id);
    bool write_remote_row(yield_func_t &yield, uint64_t target_server, uint64_t operate_size, uint64_t remote_offset, char *write_content, uint64_t cor_id);
 	bool write_remote_log(yield_func_t &yield, uint64_t target_server, uint64_t operate_size, uint64_t remote_offset, char *write_content, uint64_t cor_id);
	bool write_remote_content(yield_func_t &yield, uint64_t target_server, uint64_t operate_size, uint64_t remote_offset, char *write_content, char* local_buf, uint64_t cor_id);

    uint64_t cas_remote_content(yield_func_t &yield, uint64_t target_server, uint64_t remote_offset, uint64_t old_value, uint64_t new_value, uint64_t cor_id);
    uint64_t faa_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset, uint64_t value_to_add, uint64_t cor_id);

	bool loop_cas_remote(yield_func_t &yield,uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value, uint64_t cor_id);


	bool isRecon() {
		assert(CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN || !recon);
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
#if CC_ALG == SILO
	ts_t 			last_tid;
    ts_t            max_tid;
    uint64_t        num_locks;
    // int*            write_set;
    int             write_set[100];
    int*            read_set;
    RC              find_tid_silo(ts_t max_tid);
    RC              finish(RC rc);
#endif
#if CC_ALG == RDMA_MVCC 
    int             write_set[100];
    int*            read_set;
    uint64_t        num_locks;
#endif
	bool send_RQRY_RSP;


#if CC_ALG == RDMA_SILO
	ts_t 			last_tid;
    ts_t            max_tid;
	uint64_t        num_locks;
    int             write_set[100];
    int*            read_set;
	// RC              find_tid_silo(ts_t max_tid);
    // RC              finish(RC rc);
#endif
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
    int             write_set[100];
    int*            read_set;
	int				num_atomic_retry; //num of txn atomic_retry
#endif

#if CC_ALG == SILO || CC_ALG == RDMA_SILO
	bool 			_pre_abort;
	bool 			_validation_no_wait;
	ts_t 			_cur_tid;
	RC				validate_silo();
#endif
#if CC_ALG == WOUND_WAIT
	TxnStatus		txn_state;
#endif
	bool aborted;
	uint64_t return_id;
	RC        validate(yield_func_t &yield, uint64_t cor_id);
	void            cleanup(yield_func_t &yield, RC rc, uint64_t cor_id);
	void            cleanup_row(yield_func_t &yield, RC rc,uint64_t rid, vector<vector<uint64_t>>&remote_access, uint64_t cor_id);
	void release_last_row_lock();
	RC send_remote_reads();

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
#if CC_ALG == RDMA_MAAT
	std::set<uint64_t> uncommitted_reads;
	std::set<uint64_t> uncommitted_writes;
	std::set<uint64_t> uncommitted_writes_y;
	int             write_set[100];
    int*            read_set;
	std::set<uint64_t> unread_set;
	std::set<uint64_t> unwrite_set;
#else
	std::set<uint64_t> * uncommitted_reads;
	std::set<uint64_t> * uncommitted_writes;
	std::set<uint64_t> * uncommitted_writes_y;
#endif

#if CC_ALG == RDMA_CICADA
	uint64_t start_ts;
	std::map<uint64_t, uint64_t> uncommitted_set;
	int write_set[100];
	std::vector<uint64_t> version_num; 
#endif

	uint64_t twopl_wait_start;

	// For Tictoc
	uint64_t _min_commit_ts;
	uint64_t _max_commit_ts;
	volatile uint32_t _num_lock_waits;
	bool _signal_abort;
	bool _is_sub_txn;

	uint64_t _timestamp;
	uint64_t     get_priority() { return _timestamp; }
	// debug time
	uint64_t _start_wait_time;
	uint64_t _lock_acquire_time;
	uint64_t _lock_acquire_time_commit;
	uint64_t _lock_acquire_time_abort;
	////////////////////////////////
	// LOGGING
	////////////////////////////////
//	void 			gen_log_entry(int &length, void * log);
	bool log_flushed;
	bool repl_finished;
	Transaction * txn;
	BaseQuery * query;
	uint64_t client_startts;
	uint64_t client_id;
	uint64_t get_abort_cnt() {return abort_cnt;}
	uint64_t abort_cnt;
	int received_response(RC rc);
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
  	vector<vector<uint64_t>> log_idx{g_node_cnt};

#if CC_ALG == DLI_MVCC || CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || \
	CC_ALG == DLI_OCC
	std::atomic<bool>* is_abort = nullptr;
#endif

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

	sem_t rsp_mutex;
	bool registed_;
#if BATCH_INDEX_AND_READ
  	map<int, itemid_t*> reqId_index;
  	map<int, row_t*> reqId_row;
#endif
};

#endif

