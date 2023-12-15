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

#include "helper.h"
#include "txn.h"
#include "row.h"
#include "wl.h"
#include "query.h"
#include "thread.h"
#include "mem_alloc.h"
#include "occ.h"
#include "row_occ.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "index_rdma.h"
#include "maat.h"
#include "manager.h"
#include "mem_alloc.h"
#include "message.h"
#include "msg_queue.h"
#include "occ.h"
#include "pool.h"
#include "message.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "pps_query.h"
#include "array.h"
#include "maat.h"
#include "manager.h"
#include "row_rdma_2pl.h"
#include "rdma_2pl.h"
#include "transport.h"
#include "dbpa.hpp"
#include "routine.h"
#include "lib.hh"
#include "qps/op.hh"
#include "transport/rdma.h"
#include "src/rdma/sop.hh"
#include "src/sshed.hh"
#include "global.h"
#include "log_rdma.h"
#include "work_queue.h"

void TxnStats::init() {
	starttime=0;
	wait_starttime=get_sys_clock();
	total_process_time=0;
	process_time=0;
	total_local_wait_time=0;
	local_wait_time=0;
	total_remote_wait_time=0;
	remote_wait_time=0;
	total_twopc_time=0;
	twopc_time=0;
	write_cnt = 0;
	abort_cnt = 0;

	total_work_queue_time = 0;
	work_queue_time = 0;
	total_cc_block_time = 0;
	cc_block_time = 0;
	total_cc_time = 0;
	cc_time = 0;
	total_work_queue_cnt = 0;
	work_queue_cnt = 0;
	total_msg_queue_time = 0;
	msg_queue_time = 0;
	total_abort_time = 0;

	clear_short();
}

void TxnStats::clear_short() {

	work_queue_time_short = 0;
	cc_block_time_short = 0;
	cc_time_short = 0;
	msg_queue_time_short = 0;
	process_time_short = 0;
	network_time_short = 0;
	current_states = EXECUTION_PHASE;
}

void TxnStats::reset() {
	wait_starttime=get_sys_clock();
	total_process_time += process_time;
	process_time = 0;
	total_local_wait_time += local_wait_time;
	local_wait_time = 0;
	total_remote_wait_time += remote_wait_time;
	remote_wait_time = 0;
	total_twopc_time += twopc_time;
	twopc_time = 0;
	write_cnt = 0;

	total_work_queue_time += work_queue_time;
	work_queue_time = 0;
	total_cc_block_time += cc_block_time;
	cc_block_time = 0;
	total_cc_time += cc_time;
	cc_time = 0;
	total_work_queue_cnt += work_queue_cnt;
	work_queue_cnt = 0;
	total_msg_queue_time += msg_queue_time;
	msg_queue_time = 0;

	clear_short();

}

void TxnStats::abort_stats(uint64_t thd_id) {
	total_process_time += process_time;
	total_local_wait_time += local_wait_time;
	total_remote_wait_time += remote_wait_time;
	total_twopc_time += twopc_time;
	total_work_queue_time += work_queue_time;
	total_msg_queue_time += msg_queue_time;
	total_cc_block_time += cc_block_time;
	total_cc_time += cc_time;
	total_work_queue_cnt += work_queue_cnt;
	assert(total_process_time >= process_time);

	INC_STATS(thd_id,lat_s_rem_work_queue_time,total_work_queue_time);
	INC_STATS(thd_id,lat_s_rem_msg_queue_time,total_msg_queue_time);
	INC_STATS(thd_id,lat_s_rem_cc_block_time,total_cc_block_time);
	INC_STATS(thd_id,lat_s_rem_cc_time,total_cc_time);
	INC_STATS(thd_id,lat_s_rem_process_time,total_process_time);
}

void TxnStats::commit_stats(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id,
														uint64_t timespan_long, uint64_t timespan_short) {
	total_process_time += process_time;
	total_local_wait_time += local_wait_time;
	total_remote_wait_time += remote_wait_time;
	total_twopc_time += twopc_time;
	total_work_queue_time += work_queue_time;
	total_msg_queue_time += msg_queue_time;
	total_cc_block_time += cc_block_time;
	total_cc_time += cc_time;
	total_work_queue_cnt += work_queue_cnt;
	assert(total_process_time >= process_time);

#if CC_ALG == CALVIN

	INC_STATS(thd_id,lat_s_loc_work_queue_time,work_queue_time);
	INC_STATS(thd_id,lat_s_loc_msg_queue_time,msg_queue_time);
	INC_STATS(thd_id,lat_s_loc_cc_block_time,cc_block_time);
	INC_STATS(thd_id,lat_s_loc_cc_time,cc_time);
	INC_STATS(thd_id,lat_s_loc_process_time,process_time);
	// latency from start of transaction at this node
	PRINT_LATENCY("lat_l %ld %ld %ld %f %f %f %f %f %f\n", txn_id, batch_id, total_work_queue_cnt,
								(double)timespan_long / BILLION, (double)total_work_queue_time / BILLION,
								(double)total_msg_queue_time / BILLION, (double)total_cc_block_time / BILLION,
								(double)total_cc_time / BILLION, (double)total_process_time / BILLION);
#else
	// latency from start of transaction
	if (IS_LOCAL(txn_id)) {
	INC_STATS(thd_id,lat_l_loc_work_queue_time,total_work_queue_time);
	INC_STATS(thd_id,lat_l_loc_msg_queue_time,total_msg_queue_time);
	INC_STATS(thd_id,lat_l_loc_cc_block_time,total_cc_block_time);
	INC_STATS(thd_id,lat_l_loc_cc_time,total_cc_time);
	INC_STATS(thd_id,lat_l_loc_process_time,total_process_time);
	INC_STATS(thd_id,lat_l_loc_abort_time,total_abort_time);

	INC_STATS(thd_id,lat_s_loc_work_queue_time,work_queue_time);
	INC_STATS(thd_id,lat_s_loc_msg_queue_time,msg_queue_time);
	INC_STATS(thd_id,lat_s_loc_cc_block_time,cc_block_time);
	INC_STATS(thd_id,lat_s_loc_cc_time,cc_time);
	INC_STATS(thd_id,lat_s_loc_process_time,process_time);

	INC_STATS(thd_id,lat_short_work_queue_time,work_queue_time_short);
	INC_STATS(thd_id,lat_short_msg_queue_time,msg_queue_time_short);
	INC_STATS(thd_id,lat_short_cc_block_time,cc_block_time_short);
	INC_STATS(thd_id,lat_short_cc_time,cc_time_short);
	INC_STATS(thd_id,lat_short_process_time,process_time_short);
	INC_STATS(thd_id,lat_short_network_time,network_time_short);
	} else {
	INC_STATS(thd_id,lat_l_rem_work_queue_time,total_work_queue_time);
	INC_STATS(thd_id,lat_l_rem_msg_queue_time,total_msg_queue_time);
	INC_STATS(thd_id,lat_l_rem_cc_block_time,total_cc_block_time);
	INC_STATS(thd_id,lat_l_rem_cc_time,total_cc_time);
	INC_STATS(thd_id,lat_l_rem_process_time,total_process_time);
	}
	if (IS_LOCAL(txn_id)) {
		PRINT_LATENCY("lat_s %ld %ld %f %f %f %f %f %f\n", txn_id, work_queue_cnt,
									(double)timespan_short / BILLION, (double)work_queue_time / BILLION,
									(double)msg_queue_time / BILLION, (double)cc_block_time / BILLION,
									(double)cc_time / BILLION, (double)process_time / BILLION);
	 /*
	PRINT_LATENCY("lat_l %ld %ld %ld %f %f %f %f %f %f %f\n"
			, txn_id
			, total_work_queue_cnt
			, abort_cnt
			, (double) timespan_long / BILLION
			, (double) total_work_queue_time / BILLION
			, (double) total_msg_queue_time / BILLION
			, (double) total_cc_block_time / BILLION
			, (double) total_cc_time / BILLION
			, (double) total_process_time / BILLION
			, (double) total_abort_time / BILLION
			);
			*/
	} else {
		PRINT_LATENCY("lat_rs %ld %ld %f %f %f %f %f %f\n", txn_id, work_queue_cnt,
									(double)timespan_short / BILLION, (double)total_work_queue_time / BILLION,
									(double)total_msg_queue_time / BILLION, (double)total_cc_block_time / BILLION,
									(double)total_cc_time / BILLION, (double)total_process_time / BILLION);
	}
	/*
	if (!IS_LOCAL(txn_id) || timespan_short < timespan_long) {
	// latency from most recent start or restart of transaction
	PRINT_LATENCY("lat_s %ld %ld %f %f %f %f %f %f\n"
			, txn_id
			, work_queue_cnt
			, (double) timespan_short / BILLION
			, (double) work_queue_time / BILLION
			, (double) msg_queue_time / BILLION
			, (double) cc_block_time / BILLION
			, (double) cc_time / BILLION
			, (double) process_time / BILLION
			);
	}
	*/
#endif

	if (!IS_LOCAL(txn_id)) {
		return;
	}

	INC_STATS(thd_id,txn_total_process_time,total_process_time);
	INC_STATS(thd_id,txn_process_time,process_time);
	INC_STATS(thd_id,txn_total_local_wait_time,total_local_wait_time);
	INC_STATS(thd_id,txn_local_wait_time,local_wait_time);
	INC_STATS(thd_id,txn_total_remote_wait_time,total_remote_wait_time);
	INC_STATS(thd_id,txn_remote_wait_time,remote_wait_time);
	INC_STATS(thd_id,txn_total_twopc_time,total_twopc_time);
	INC_STATS(thd_id,txn_twopc_time,twopc_time);
	if(write_cnt > 0) {
	INC_STATS(thd_id,txn_write_cnt,1);
	}
	if(abort_cnt > 0) {
	INC_STATS(thd_id,unique_txn_abort_cnt,1);
	}

}


void Transaction::init() {
	timestamp = UINT64_MAX;
	start_timestamp = UINT64_MAX;
	end_timestamp = UINT64_MAX;
	txn_id = UINT64_MAX;
	batch_id = UINT64_MAX;
	DEBUG_M("Transaction::init array insert_rows\n");
	insert_rows.init(g_max_items_per_txn + 80);
	DEBUG_M("Transaction::reset array accesses\n");
	accesses.init(MAX_ROW_PER_TXN);

	reset(0);
}

void Transaction::reset(uint64_t thd_id) {
	release_accesses(thd_id);
	accesses.clear();
	release_inserts(thd_id);
	insert_rows.clear();
	write_cnt = 0;
	row_cnt = 0;
	twopc_state = START;
	rc = RCOK;
}

void Transaction::release_accesses(uint64_t thd_id) {
	for(uint64_t i = 0; i < accesses.size(); i++) {
	access_pool.put(thd_id,accesses[i]);
	}
}

void Transaction::release_inserts(uint64_t thd_id) {
	for(uint64_t i = 0; i < insert_rows.size(); i++) {
	row_t * row = insert_rows[i];
#if CC_ALG != MAAT && CC_ALG != OCC
		DEBUG_M("TxnManager::cleanup row->manager free\n");
		mem_allocator.free(row->manager, 0);
#endif

		row->free_row();
#if RDMA_ONE_SIDE == true
		// r2::AllocatorMaster<>::get_thread_allocator()->free(row);
        uint64_t size = row_t::get_row_size(row->get_schema()->get_tuple_size());
        mem_allocator.free(row,row_t::get_row_size(size));
#else
		DEBUG_M("Transaction::release insert_rows free\n")
        // mem_allocator.free(row,row_t::get_row_size(ROW_DEFAULT_SIZE));
		row_pool.put(thd_id,row);
#endif
	}
}

void Transaction::release(uint64_t thd_id) {
	DEBUG("Transaction release\n");
	release_accesses(thd_id);
	DEBUG_M("Transaction::release array accesses free\n")
	accesses.release();
	release_inserts(thd_id);
	DEBUG_M("Transaction::release array insert_rows free\n")
	insert_rows.release();
}

void TxnManager::init(uint64_t thd_id, Workload * h_wl) {
	uint64_t prof_starttime = get_sys_clock();
	if(!txn)  {
	DEBUG_M("Transaction alloc\n");
	txn_pool.get(thd_id,txn);
	
	}
	INC_STATS(get_thd_id(),mtx[15],get_sys_clock()-prof_starttime);
	prof_starttime = get_sys_clock();
	//txn->init();
	if(!query) {
	DEBUG_M("TxnManager::init Query alloc\n");
	qry_pool.get(thd_id,query);
	}
	INC_STATS(get_thd_id(),mtx[16],get_sys_clock()-prof_starttime);
	//query->init();
	//reset();
	sem_init(&rsp_mutex, 0, 1);
	return_id = UINT64_MAX;

	this->h_wl = h_wl;
#if CC_ALG == MAAT
	uncommitted_writes = new std::set<uint64_t>();
	uncommitted_writes_y = new std::set<uint64_t>();
	uncommitted_reads = new std::set<uint64_t>();
#endif
#if CC_ALG == CALVIN 
	phase = CALVIN_RW_ANALYSIS;
	locking_done = false;
	calvin_locked_rows.init(MAX_ROW_PER_TXN);
#endif
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3
	num_atomic_retry = 0;
#endif
#if CC_ALG == WOUND_WAIT
	txn_state = RUNNING;
#endif
	registed_ = false;
	txn_ready = true;
	twopl_wait_start = 0;
	txn_stats.init();
	failed_partition.init(g_part_cnt);
	finish_logging = false;
	
	num_msgs_commit = 0;
	num_msgs_rw_prep = 0;
	for(int i=0;i<CENTER_CNT;i++){
		is_primary[i] = false; 	
	}
	#if WORKLOAD == YCSB
	#else 
	wh_need_wait = false;
	wh_extra_wait[0] = -1;
	wh_extra_wait[1] = -1;
	cus_need_wait = false;
	cus_extra_wait[0] = -1;
	cus_extra_wait[1] = -1;
	for(int i=0;i<MAX_ITEMS_PER_TXN;i++){
		for(int j=0;j<2;j++){
			extra_wait[i][j] = -1;
		}
	}
	for(int i=0;i<MAX_ITEMS_PER_TXN;i++){
		req_need_wait[i] = false;	
	}
	#endif
	commit_timestamp = 0;
	rsp_cnt = 0;
	abort_cnt = 0;
}

// reset after abort
void TxnManager::reset() {
	lock_ready = false;
	lock_ready_cnt = 0;
	locking_done = true;
	ready_part = 0;
	rsp_cnt = 0;
	// aborted = false;
	return_id = UINT64_MAX;
	twopl_wait_start = 0;
	finish_logging = false;

	num_msgs_commit = 0;
	num_msgs_rw_prep = 0;
	for(int i=0;i<CENTER_CNT;i++){
		is_primary[i] = false; 	
	}
	for(int i=0;i<NODE_CNT;i++){
		log_idx[i] = -1; 	
	}
	is_logged = false;
	#if WORKLOAD == YCSB
	#else 
	wh_need_wait = false;
	wh_extra_wait[0] = -1;
	wh_extra_wait[1] = -1;
	cus_need_wait = false;
	cus_extra_wait[0] = -1;
	cus_extra_wait[1] = -1;
	for(int i=0;i<MAX_ITEMS_PER_TXN;i++){
		for(int j=0;j<2;j++){
			extra_wait[i][j] = -1;
		}
	}
	for(int i=0;i<MAX_ITEMS_PER_TXN;i++){
		req_need_wait[i] = false;	
	}
	#endif


	//ready = true;

	// MaaT & DTA & WKDB
	greatest_write_timestamp = 0;
	greatest_read_timestamp = 0;
	commit_timestamp = 0;
#if CC_ALG == MAAT
	uncommitted_writes->clear();
	uncommitted_writes_y->clear();
	uncommitted_reads->clear();
#endif
#if CC_ALG == CALVIN
	phase = CALVIN_RW_ANALYSIS;
	locking_done = false;
	calvin_locked_rows.clear();
#endif
	assert(txn);
	assert(query);
	query->reset_query_status();
	txn->reset(get_thd_id());
	center_master.clear();
	failed_partition.clear();
	// Stats
	txn_stats.reset();
}

void TxnManager::release() {
	uint64_t prof_starttime = get_sys_clock();
	if (!aborted) {
		qry_pool.put(get_thd_id(),query);
	}
	aborted = false;
	// qry_pool.put(get_thd_id(),query);
	INC_STATS(get_thd_id(),mtx[0],get_sys_clock()-prof_starttime);
	query = NULL;
	prof_starttime = get_sys_clock();
	txn_pool.put(get_thd_id(),txn);
	INC_STATS(get_thd_id(),mtx[1],get_sys_clock()-prof_starttime);
	txn = NULL;

#if CC_ALG == MAAT 
	delete uncommitted_writes;
	delete uncommitted_writes_y;
	delete uncommitted_reads;
#endif
#if CC_ALG == MAAT
	uncommitted_writes->clear();
	uncommitted_writes_y->clear();
	uncommitted_reads->clear();
#endif
#if CC_ALG == CALVIN
	calvin_locked_rows.release();
#endif
	txn_ready = true;
	num_msgs_commit = 0;
	num_msgs_rw_prep = 0;
	for(int i=0;i<CENTER_CNT;i++){
		is_primary[i] = false; 	
	}
	#if WORKLOAD == YCSB
	#else 
	wh_need_wait = false;
	wh_extra_wait[0] = -1;
	wh_extra_wait[1] = -1;
	cus_need_wait = false;
	cus_extra_wait[0] = -1;
	cus_extra_wait[1] = -1;
	for(int i=0;i<MAX_ITEMS_PER_TXN;i++){
		for(int j=0;j<2;j++){
			extra_wait[i][j] = -1;
		}
	}
	for(int i=0;i<MAX_ITEMS_PER_TXN;i++){
		req_need_wait[i] = false;	
	}
	#endif
}

void TxnManager::reset_query() {
#if WORKLOAD == YCSB
	((YCSBQuery*)query)->reset();
#elif WORKLOAD == TPCC
	((TPCCQuery*)query)->reset();
#elif WORKLOAD == PPS
	((PPSQuery*)query)->reset();
#endif
}

RC TxnManager::commit(yield_func_t &yield, uint64_t cor_id) {
	DEBUG_T("Commit %ld\n",get_txn_id());
#if CC_ALG == WOUND_WAIT
    txn_state = STARTCOMMIT;    
#endif
	release_locks(yield, RCOK, cor_id);
#if CC_ALG == MAAT
	time_table.release(get_thd_id(),get_txn_id());
#endif
	commit_stats();
#if LOGGING
	LogRecord * record = logger.createRecord(get_txn_id(),L_NOTIFY,0,0);
	if(g_repl_cnt > 0) {
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), g_node_id + g_node_cnt + g_client_node_cnt, Message::create_message(record, LOG_MSG));
#else
		msg_queue.enqueue(get_thd_id(), Message::create_message(record, LOG_MSG),
											g_node_id + g_node_cnt + g_client_node_cnt);
#endif
	}
	logger.enqueueRecord(record);
	return WAIT;
#endif
	return Commit;
}

RC TxnManager::abort(yield_func_t &yield, uint64_t cor_id) {
	if (aborted) return Abort;
	DEBUG("Abort %ld\n",get_txn_id());
	//printf("Abort %ld\n",get_txn_id());
	txn->rc = Abort;
	INC_STATS(get_thd_id(),total_txn_abort_cnt, 1);
	txn_stats.abort_cnt++;
	if(IS_LOCAL(get_txn_id())) {
	    INC_STATS(get_thd_id(), local_txn_abort_cnt, 1);
	} else {
        INC_STATS(get_thd_id(), remote_txn_abort_cnt, 1);
        txn_stats.abort_stats(get_thd_id());
	}
	aborted = true;
	//RDMA_SILO - ADD remote release lock by rdma
	release_locks(yield, Abort, cor_id);
#if CC_ALG == MAAT
	//assert(time_table.get_state(get_txn_id()) == MAAT_ABORTED);
	time_table.release(get_thd_id(),get_txn_id());
#endif

	uint64_t timespan = get_sys_clock() - txn_stats.restart_starttime;
	if (IS_LOCAL(get_txn_id()) && warmup_done) {
		INC_STATS_ARR(get_thd_id(),start_abort_commit_latency, timespan);
	}

	/*
	// latency from most recent start or restart of transaction
	PRINT_LATENCY("lat_s %ld %ld 0 %f %f %f %f %f %f 0.0\n"
			, get_txn_id()
			, txn_stats.work_queue_cnt
			, (double) timespan / BILLION
			, (double) txn_stats.work_queue_time / BILLION
			, (double) txn_stats.msg_queue_time / BILLION
			, (double) txn_stats.cc_block_time / BILLION
			, (double) txn_stats.cc_time / BILLION
			, (double) txn_stats.process_time / BILLION
			);
			*/
	//commit_stats();
	is_logged = false;
	return Abort;
}

RC TxnManager::start_abort(yield_func_t &yield, uint64_t cor_id) {
	// ! trans process time
	uint64_t prepare_start_time = get_sys_clock();
	txn_stats.prepare_start_time = prepare_start_time;
	uint64_t process_time_span  = prepare_start_time - txn_stats.restart_starttime;
	INC_STATS(get_thd_id(), trans_process_time, process_time_span);
    INC_STATS(get_thd_id(), trans_process_count, 1);
	txn->rc = Abort;
	DEBUG_T("%ld start_abort\n",get_txn_id());

	uint64_t finish_start_time = get_sys_clock();
	txn_stats.finish_start_time = finish_start_time;
	// uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
	// INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
    // INC_STATS(get_thd_id(), trans_prepare_count, 1);
	//RDMA_SILO:keep message or not
#if CENTER_MASTER == true
    if(query->centers_touched.size() > 1 && CC_ALG != RDMA_NO_WAIT && CC_ALG != RDMA_NO_WAIT3) {
#else
	if(query->partitions_touched.size() > 1 && CC_ALG != RDMA_NO_WAIT && CC_ALG != RDMA_NO_WAIT3) {
#endif
		send_finish_messages();
		abort(yield, cor_id);
		return Abort;
	}
	return abort(yield, cor_id);
}

#ifdef NO_2PC
RC TxnManager::start_commit() {
	RC rc = RCOK;
	DEBUG("%ld start_commit RO?%d\n",get_txn_id(),query->readonly());
	_is_sub_txn = false;

	rc = validate();
	if(rc == RCOK)
		rc = commit();
	else
		start_abort();

		return rc;
}
#else
RC TxnManager::start_commit(yield_func_t &yield, uint64_t cor_id) {
	// ! trans process time
	uint64_t prepare_start_time = get_sys_clock();
	txn_stats.prepare_start_time = prepare_start_time;
	uint64_t process_time_span  = prepare_start_time - txn_stats.restart_starttime;
	INC_STATS(get_thd_id(), trans_process_time, process_time_span);
  	INC_STATS(get_thd_id(), trans_process_count, 1);
	RC rc = RCOK;
	DEBUG("%ld start_commit RO?%d\n",get_txn_id(),query->readonly());
	if(true) {
		if (!query->readonly() || CC_ALG == OCC || CC_ALG == MAAT || CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3) {
			// send prepare messages
			send_prepare_messages();
			rc = WAIT_REM;
		} else {
			uint64_t finish_start_time = get_sys_clock();
			txn_stats.finish_start_time = finish_start_time;
			// uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
			// INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
      		// INC_STATS(get_thd_id(), trans_prepare_count, 1);
			send_finish_messages();
			rsp_cnt = 0;
			rc = commit(yield, cor_id);
		}
	} 
	else { 
		rc = validate(yield, cor_id);
		// rc = RCOK;
		uint64_t finish_start_time = get_sys_clock();
		txn_stats.finish_start_time = finish_start_time;

		// uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
		// INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
    	// INC_STATS(get_thd_id(), trans_prepare_count, 1);
		if(rc == RCOK){   //for NO_WAIT , rc == RCOK
			rc = commit(yield, cor_id);
		}		
		else {
			txn->rc = Abort;
			DEBUG("%ld start_abort\n",get_txn_id());
			if(query->partitions_touched.size() > 1 && CC_ALG != RDMA_NO_WAIT && CC_ALG != RDMA_NO_WAIT3) {
				send_finish_messages();
				abort(yield, cor_id);
				rc = Abort;
			}
			rc = abort(yield, cor_id);
		}
	}
	return rc;
}
#endif
void TxnManager::send_prepare_messages() {
#if PARAL_SUBTXN == true && CENTER_MASTER == true
	{
#elif CENTER_MASTER == true
	rsp_cnt = query->centers_touched.size() - 1;	
	DEBUG("%ld Send PREPARE messages to %d\n",get_txn_id(),rsp_cnt);
	for(uint64_t i = 0; i < query->centers_touched.size(); i++) {
		if(this->center_master[query->centers_touched[i]] == g_node_id || query->centers_touched[i] == g_center_id) {
			continue;
 	    }
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), this->center_master[query->centers_touched[i]]), Message::create_message(this, RPREPARE));
#else
		msg_queue.enqueue(get_thd_id(), Message::create_message(this, RPREPARE),
											this->center_master[query->centers_touched[i]]);
#endif
	#if RECOVERY_TXN_MECHANISM
	update_send_time();
	if (!is_enqueue && wait_queue_entry == nullptr) {
		work_queue.waittxn_enqueue(get_thd_id(),Message::create_message(this,WAIT_TXN),wait_queue_entry);
		DEBUG_T("Txn %ld enqueue wait queue.\n",get_txn_id());
		is_enqueue = true;
	}else {
		DEBUG_T("Txn %ld has already enqueue wait queue %ld.\n",get_txn_id(), wait_queue_entry->txn_id);
	}
	
	#endif
#else
	rsp_cnt = query->partitions_touched.size() - 1;
	DEBUG("%ld Send PREPARE messages to %d\n",get_txn_id(),rsp_cnt);
	for(uint64_t i = 0; i < query->partitions_touched.size(); i++) {
		if(GET_NODE_ID(query->partitions_touched[i]) == g_node_id || GET_CENTER_ID(query->partitions_touched[i]) == g_center_id) {
		continue;
	}
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), GET_NODE_ID(query->partitions_touched[i]), Message::create_message(this, RPREPARE));
#else
		msg_queue.enqueue(get_thd_id(), Message::create_message(this, RPREPARE),
											GET_NODE_ID(query->partitions_touched[i]));
#endif
	#if RECOVERY_TXN_MECHANISM
	update_send_time();
	if (!is_enqueue && wait_queue_entry == nullptr) {
		work_queue.waittxn_enqueue(get_thd_id(),Message::create_message(this,WAIT_TXN),wait_queue_entry);
		DEBUG_T("Txn %ld enqueue wait queue.\n",get_txn_id());
		is_enqueue = true;
	}else {
		DEBUG_T("Txn %ld has already enqueue wait queue %ld.\n",get_txn_id(), wait_queue_entry->txn_id);
	}
	#endif
#endif
	}
	
}

void TxnManager::send_finish_messages() {
	txn_stats.current_states = FINISH_PHASE;
#if CENTER_MASTER == true
	//get rsp_cnt
	rsp_cnt = 0;
	for(int i=0;i<query->centers_touched.size();i++){
		if(is_primary[query->centers_touched[i]]) ++rsp_cnt;
	}
	--rsp_cnt; //exclude this center
	// rsp_cnt = query->centers_touched.size() - 1;	
	// printf("c_rsp_cnt: %d\n", rsp_cnt);

	//get req_need_wait

	#if WORKLOAD == YCSB
	#else 
	if (wh_extra_wait[0] != -1) {
		assert(wh_extra_wait[1] != -1);
		wh_need_wait = true;
	}
	if (cus_extra_wait[0] != -1) {
		assert(cus_extra_wait[1] != -1);
		cus_need_wait = true;
	}
	for(int i=0;i<MAX_ITEMS_PER_TXN;i++){
		if (extra_wait[i][0] != -1) {
			assert(extra_wait[i][1] != -1);
			req_need_wait[i] = true;
		}
	}
	#endif
	assert(num_msgs_commit==0);
	assert(IS_LOCAL(get_txn_id()));
	DEBUG_T("%ld Send FINISH messages to %d\n",get_txn_id(),rsp_cnt);
	for(uint64_t i = 0; i < query->centers_touched.size(); i++) {
		if(this->center_master[query->centers_touched[i]] == g_node_id || query->centers_touched[i] == g_center_id) {
			continue;
    	}
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), this->center_master[query->centers_touched[i]]), Message::create_message(this, RFIN));
#else
		num_msgs_commit++;
		msg_queue.enqueue(get_thd_id(), Message::create_message(this, RFIN),
											this->center_master[query->centers_touched[i]]);
		DEBUG_T("txn %lu, send inter-finish to %ld\n", get_txn_id(),this->center_master[query->centers_touched[i]]);
#endif
#else
	rsp_cnt = query->partitions_touched.size() - 1;

	assert(IS_LOCAL(get_txn_id()));
	DEBUG("%ld Send FINISH messages to %d\n",get_txn_id(),rsp_cnt);
	for(uint64_t i = 0; i < query->partitions_touched.size(); i++) {
		if(GET_NODE_ID(query->partitions_touched[i]) == g_node_id || GET_CENTER_ID(query->partitions_touched[i]) == g_center_id) {
			continue;
    }
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), GET_NODE_ID(query->partitions_touched[i]), Message::create_message(this, RFIN));
#else
		msg_queue.enqueue(get_thd_id(), Message::create_message(this, RFIN),
											GET_NODE_ID(query->partitions_touched[i]));
#endif
#endif
	}
	assert(get_rc() == Abort || num_msgs_commit==num_msgs_rw_prep);
	
	#if RECOVERY_TXN_MECHANISM
	update_send_time();
	if (!is_enqueue && wait_queue_entry == nullptr) {
		work_queue.waittxn_enqueue(get_thd_id(),Message::create_message(this,WAIT_TXN),wait_queue_entry);
		DEBUG_T("Txn %ld enqueue wait queue.\n",get_txn_id());
		is_enqueue = true;
	}else {
		DEBUG_T("Txn %ld has already enqueue wait queue %ld.\n",get_txn_id(), wait_queue_entry->txn_id);
	}
	// 
	#endif
}

int TxnManager::received_response(RC rc) {
	assert(txn->rc == RCOK || txn->rc == Abort);
	if (txn->rc == RCOK) txn->rc = rc;
#if CC_ALG == CALVIN
	++rsp_cnt;
#else
	if (rsp_cnt > 0)
		--rsp_cnt;
	else return -1;
#endif
	return rsp_cnt;
}

int TxnManager::received_response(AckMessage* msg, OpStatus state) {
	uint64_t return_id = msg->return_node_id;
	OpStatus change_state = state;
	if (((AckMessage*)msg)->rc == Abort &&
		txn->rc == RCOK) txn->rc = Abort;
	if (txn->rc == RCOK) txn->rc = msg->rc;
	if (((AckMessage*)msg)->rc == Abort && state == PREPARE) change_state = PREP_ABORT;
	update_query_status(return_id,change_state);
	RC result = check_query_status(state);
	if (result == Abort) {
		set_rc(Abort);
		return 0;
	}
	if (result == RCOK) return 0;
	return 1;
}

bool TxnManager::waiting_for_response() { return rsp_cnt > 0; }

bool TxnManager::is_multi_part() {
#if CENTER_MASTER == true
	return query->centers_touched.size() > 1;
#else
	return query->partitions_touched.size() > 1;
#endif
	//return query->partitions.size() > 1;
}

void TxnManager::commit_stats() {
	uint64_t commit_time = get_sys_clock();
	uint64_t timespan_short = commit_time - txn_stats.restart_starttime;
	uint64_t timespan_long  = commit_time - txn_stats.starttime;
	INC_STATS(get_thd_id(),total_txn_commit_cnt,1);

	uint64_t warmuptime = get_sys_clock() - simulation->run_starttime;
	DEBUG("Commit_stats execute_time %ld warmup_time %ld\n",warmuptime,g_warmup_timer);
	if (simulation->is_warmup_done())
		DEBUG("Commit_stats total_txn_commit_cnt %ld\n",stats._stats[get_thd_id()]->total_txn_commit_cnt);
	if(!IS_LOCAL(get_txn_id()) && (CC_ALG != CALVIN )) {
		INC_STATS(get_thd_id(),remote_txn_commit_cnt,1);
		txn_stats.commit_stats(get_thd_id(), get_txn_id(), get_batch_id(), timespan_long,
													 timespan_short);
		return;
	}


	INC_STATS(get_thd_id(),txn_cnt,1);
	INC_STATS(get_thd_id(),local_txn_commit_cnt,1);
	INC_STATS(get_thd_id(), txn_run_time, timespan_long);
	if(query->partitions_touched.size() > 1) {
		INC_STATS(get_thd_id(),multi_part_txn_cnt,1);
		INC_STATS(get_thd_id(),multi_part_txn_run_time,timespan_long);
	} else {
		INC_STATS(get_thd_id(),single_part_txn_cnt,1);
		INC_STATS(get_thd_id(),single_part_txn_run_time,timespan_long);
	}
	/*if(cflt) {
		INC_STATS(get_thd_id(),cflt_cnt_txn,1);
	}*/
	txn_stats.commit_stats(get_thd_id(),get_txn_id(),get_batch_id(),timespan_long, timespan_short);
	#if CC_ALG == CALVIN
	return;
	#endif

	INC_STATS_ARR(get_thd_id(),start_abort_commit_latency, timespan_short);
	INC_STATS_ARR(get_thd_id(),last_start_commit_latency, timespan_short);
	INC_STATS_ARR(get_thd_id(),first_start_commit_latency, timespan_long);

	assert(query->partitions_touched.size() > 0);
	INC_STATS(get_thd_id(),parts_touched,query->partitions_touched.size());
	INC_STATS(get_thd_id(),part_cnt[query->partitions_touched.size()-1],1);
	for(uint64_t i = 0 ; i < query->partitions_touched.size(); i++) {
		INC_STATS(get_thd_id(),part_acc[query->partitions_touched[i]],1);
	}
}
#if !USE_COROUTINE
void TxnManager::register_thread(Thread * h_thd) {
	this->h_thd = h_thd;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
	this->active_part = GET_PART_ID_FROM_IDX(get_thd_id());
#endif
}
#else 
void TxnManager::register_thread(WorkerThread * h_thd, uint64_t cor_id) {
	this->h_thd = h_thd;
	this->_cor_id = cor_id;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
	this->active_part = GET_PART_ID_FROM_IDX(get_thd_id());
#endif
}
#endif
void TxnManager::set_txn_id(txnid_t txn_id) { txn->txn_id = txn_id; }

txnid_t TxnManager::get_txn_id() { return txn->txn_id; }

Workload *TxnManager::get_wl() { return h_wl; }

uint64_t TxnManager::get_thd_id() {
	if(h_thd)
	return h_thd->get_thd_id();
	else
	return 0;
}

BaseQuery *TxnManager::get_query() { return query; }
void TxnManager::set_query(BaseQuery *qry) { query = qry; }

void TxnManager::set_timestamp(ts_t timestamp) { txn->timestamp = timestamp; }

ts_t TxnManager::get_timestamp() { return txn->timestamp; }

void TxnManager::set_start_timestamp(uint64_t start_timestamp) {
	txn->start_timestamp = start_timestamp;
}

ts_t TxnManager::get_start_timestamp() { return txn->start_timestamp; }

uint64_t TxnManager::incr_lr() {
	//ATOM_ADD(this->rsp_cnt,i);
	uint64_t result;
	sem_wait(&rsp_mutex);
	result = ++this->lock_ready_cnt;
	sem_post(&rsp_mutex);
	return result;
}

uint64_t TxnManager::decr_lr() {
	//ATOM_SUB(this->rsp_cnt,i);
	uint64_t result;
	sem_wait(&rsp_mutex);
	result = --this->lock_ready_cnt;
	sem_post(&rsp_mutex);
	return result;
}
uint64_t TxnManager::incr_rsp(int i) {
	//ATOM_ADD(this->rsp_cnt,i);
	uint64_t result;
	sem_wait(&rsp_mutex);
	result = ++this->rsp_cnt;
	sem_post(&rsp_mutex);
	return result;
}
uint64_t TxnManager::decr_rsp(int i) {
	//ATOM_SUB(this->rsp_cnt,i);
	uint64_t result;
	sem_wait(&rsp_mutex);
	result = --this->rsp_cnt;
	sem_post(&rsp_mutex);
	return result;
}

void TxnManager::release_last_row_lock() {
	assert(txn->row_cnt > 0);
	row_t * orig_r = txn->accesses[txn->row_cnt-1]->orig_row;
	access_t type = txn->accesses[txn->row_cnt-1]->type;
	orig_r->return_row(RCOK, type, this, NULL);
	//txn->accesses[txn->row_cnt-1]->orig_row = NULL;
}

void TxnManager::cleanup_row(yield_func_t &yield, RC rc, uint64_t rid, vector<vector<uint64_t>>& remote_access, uint64_t cor_id) {
	access_t type = txn->accesses[rid]->type;
	if (type == WR && rc == Abort && CC_ALG != MAAT) {
		type = XP;
	}
    bool is_local = true;
	uint64_t version = 0;
	// Handle calvin elsewhere

#if CC_ALG != CALVIN 
#if ISOLATION_LEVEL != READ_UNCOMMITTED
	row_t * orig_r = txn->accesses[rid]->orig_row;
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3
	if(txn->accesses[rid]->location == g_node_id) is_local=true;
	else is_local=false;

	//no wait abort write
  	if (ROLL_BACK && type == XP && is_local){
       orig_r->return_row(rc,type, this, txn->accesses[rid]->orig_data); 
	} else {
#if ISOLATION_LEVEL == READ_COMMITTED
		if(type == WR) {
		version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
		}
#endif
	}
#else
  if (ROLL_BACK && type == XP &&
      (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3 || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == WOUND_WAIT )) {
    orig_r->return_row(rc,type, this, txn->accesses[rid]->orig_data); 
  } else {
#if ISOLATION_LEVEL == READ_COMMITTED
    if(type == WR) {
      version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
    }
#else
    version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);  
#endif
  }
#endif
#endif

#if ROLL_BACK && \
		(CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3 || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == WOUND_WAIT)
	if (type == WR && is_local) {
		//printf("free 10 %ld\n",get_txn_id());
		txn->accesses[rid]->orig_data->free_row();
		DEBUG_M("TxnManager::cleanup row_t free\n");
		row_pool.put(get_thd_id(),txn->accesses[rid]->orig_data);
		if(rc == RCOK) {
			INC_STATS(get_thd_id(),record_write_cnt,1);
			++txn_stats.write_cnt;
		}
	}
#endif
#endif

	if (type == WR) txn->accesses[rid]->version = version;

#if CC_ALG != RDMA_NO_WAIT && CC_ALG != RDMA_NO_WAIT3
  txn->accesses[rid]->data = NULL;
#endif
}

void TxnManager::cleanup(yield_func_t &yield, RC rc, uint64_t cor_id) {
#if CC_ALG == OCC && MODE == NORMAL_MODE
	occ_man.finish(rc,this);
#endif

	ts_t starttime = get_sys_clock();
	uint64_t row_cnt = txn->accesses.get_count();
	assert(txn->accesses.get_count() == txn->row_cnt);
	assert((WORKLOAD == YCSB && row_cnt <= g_req_per_query) || (WORKLOAD == TPCC && row_cnt <=
	g_max_items_per_txn*2 + 3));

	DEBUG("Cleanup %ld %ld\n",get_txn_id(),row_cnt);


	vector<vector<uint64_t>> remote_access(g_node_cnt); //for DBPA, collect remote abort write
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		cleanup_row(yield, rc,rid,remote_access,cor_id);  //return abort write row
	}

#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3
    r2pl_man.finish(yield,rc,this,cor_id);
#endif
#if CC_ALG == CALVIN 
	// cleanup locked rows
	for (uint64_t i = 0; i < calvin_locked_rows.size(); i++) {
		row_t * row = calvin_locked_rows[i];
		row->return_row(rc,RD,this,row);
	}
#endif
	if (rc == Abort) {
		txn->release_inserts(get_thd_id());
		txn->insert_rows.clear();

		INC_STATS(get_thd_id(), abort_time, get_sys_clock() - starttime);
	}
}

RC TxnManager::get_lock(row_t * row, access_t type) {
	if (calvin_locked_rows.contains(row)) {
		return RCOK;
	}
	calvin_locked_rows.add(row);
	RC rc = row->get_lock(type, this);
	if(rc == WAIT) {
		INC_STATS(get_thd_id(), txn_wait_cnt, 1);
	}
	return rc;
}

RC TxnManager::get_row(yield_func_t &yield,row_t * row, access_t type, row_t *& row_rtn,uint64_t cor_id) {
	uint64_t starttime = get_sys_clock();
	uint64_t timespan;
	RC rc = RCOK;
	DEBUG_M("TxnManager::get_row access alloc\n");
	Access * access = NULL;
	this->last_row = row;
	this->last_type = type;
  	uint64_t get_access_end_time = 0;

	access_pool.get(get_thd_id(),access);
	get_access_end_time = get_sys_clock();
	INC_STATS(get_thd_id(), trans_get_access_time, get_access_end_time - starttime);
	INC_STATS(get_thd_id(), trans_get_access_count, 1);
	//uint64_t row_cnt = txn->row_cnt;
	//assert(txn->accesses.get_count() - 1 == row_cnt);
  // uint64_t start_time = get_sys_clock();
  //for NO_WAIT, lock and preserve access
#if CC_ALG == WOUND_WAIT
	if (txn_state == WOUNDED) 
		rc = Abort;
	else 
		rc = row->get_row(yield,type, this, access,cor_id);
#else
	rc = row->get_row(yield,type, this, access,cor_id);
#endif
	INC_STATS(get_thd_id(), trans_get_row_time, get_sys_clock() - get_access_end_time);
	INC_STATS(get_thd_id(), trans_get_row_count, 1);
  	uint64_t middle_time = get_sys_clock();
	if (rc == Abort || rc == WAIT) {
		row_rtn = NULL;
		DEBUG_M("TxnManager::get_row(abort) access free\n");
		access_pool.put(get_thd_id(),access);
		timespan = get_sys_clock() - starttime;
    INC_STATS(get_thd_id(), trans_store_access_time, timespan + starttime - middle_time);
    INC_STATS(get_thd_id(), trans_store_access_count, 1);
		INC_STATS(get_thd_id(), txn_manager_time, timespan);
		INC_STATS(get_thd_id(), txn_conflict_cnt, 1);
		//cflt = true;
#if DEBUG_TIMELINE
		printf("CONFLICT %ld %ld\n",get_txn_id(),get_sys_clock());
#endif
		return rc;
	}
	++num_locks;
	access->type = type;
	access->orig_row = row; //access->data == access->orig_row
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3
	access->location = g_node_id;
	access->offset = (char*)row - rdma_global_buffer;
#endif
	access->key = row->get_primary_key();
	access->partition_id = row->get_part_id();

#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3 || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == WOUND_WAIT)
	if (type == WR) {
		uint64_t part_id = row->get_part_id();
		DEBUG_M("TxnManager::get_row row_t alloc\n")
		row_pool.get(get_thd_id(),access->orig_data);
		access->orig_data->init(row->get_table(), part_id, 0);
		access->orig_data->copy(row);
		assert(access->orig_data->get_schema() == row->get_schema());

		// ARIES-style physiological logging
#if LOGGING
		// LogRecord * record =
		// logger.createRecord(LRT_UPDATE,L_UPDATE,get_txn_id(),part_id,row->get_table()->get_table_id(),row->get_primary_key());
		LogRecord *record = logger.createRecord(
				get_txn_id(), L_UPDATE, row->get_table()->get_table_id(), row->get_primary_key());
		if(g_repl_cnt > 0) {
#if USE_RDMA == CHANGE_MSG_QUEUE
            tport_man.rdma_thd_send_msg(get_thd_id(), g_node_id + g_node_cnt + g_client_node_cnt, Message::create_message(record, LOG_MSG));
#else
			msg_queue.enqueue(get_thd_id(), Message::create_message(record, LOG_MSG),
												g_node_id + g_node_cnt + g_client_node_cnt);
#endif
		}
		logger.enqueueRecord(record);
#endif
	}
#endif

	++txn->row_cnt;
	if (type == WR) ++txn->write_cnt;
	txn->accesses.add(access);
   
	timespan = get_sys_clock() - starttime;
    INC_STATS(get_thd_id(), trans_store_access_time, timespan + starttime - middle_time);
  	INC_STATS(get_thd_id(), trans_store_access_count, 1);
	INC_STATS(get_thd_id(), txn_manager_time, timespan);
	row_rtn  = access->data;

	if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == CALVIN) assert(rc == RCOK);
	assert(rc == RCOK);
	return rc;
}

RC TxnManager::get_row_post_wait(row_t *& row_rtn) {
	assert(CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC);

	uint64_t starttime = get_sys_clock();
	row_t * row = this->last_row;
	access_t type = this->last_type;
	assert(row != NULL);
	DEBUG_M("TxnManager::get_row_post_wait access alloc\n")
	Access * access;
	access_pool.get(get_thd_id(),access);

	row->get_row_post_wait(type,this,access->data);

	access->type = type;
	access->orig_row = row;
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT)
	if (type == WR) {
		uint64_t part_id = row->get_part_id();
		//printf("alloc 10 %ld\n",get_txn_id());
		DEBUG_M("TxnManager::get_row_post_wait row_t alloc\n")
		row_pool.get(get_thd_id(),access->orig_data);
		access->orig_data->init(row->get_table(), part_id, 0);
		access->orig_data->copy(row);
		// for(int i = 0;i < this->txn->row_cnt;i++){
		// 	if(txn->accesses[i]->type == WR)
        //     // printf("txn %ld orgin_d[%ld] table %ld",this->get_txn_id(),i,this->txn->accesses[i]->orig_data->table_idx);
        // }
	}
#endif

	++txn->row_cnt;
	if (type == WR) ++txn->write_cnt;

	txn->accesses.add(access);
	uint64_t timespan = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_manager_time, timespan);
	this->last_row_rtn  = access->data;
	row_rtn  = access->data;
	return RCOK;
}

uint64_t TxnManager::get_part_num(uint64_t num,uint64_t part){
    uint64_t result = 0;
    switch(part){
        case 1:
            result = num>>48;
            break;
        case 2:
            result = (num<<16)>>48;
            break;
        case 3:
            result = (num<<32)>>48;
            break;
        case 4:
            result = (num<<48)>>48;
            break;
        default:
            assert(false);    
    }
    return result;
}


RC TxnManager::get_remote_row(yield_func_t &yield, access_t type, uint64_t key, uint64_t loc, itemid_t *m_item, row_t *& row_local, uint64_t cor_id) {
	RC rc = RCOK;

	#if CC_ALG == RDMA_NO_WAIT
	if(type == RD || type == WR){
		uint64_t new_lock_info;
		uint64_t lock_info;
		row_t * test_row = NULL;
		row_t * lock_read;

		if(type == RD){ //读集元素
			//第一次rdma read，得到数据项的锁信息
			rc = read_remote_row(yield,loc,m_item->offset,lock_read,cor_id, key);
			if (rc != RCOK) {
				rc = rc == NODE_FAILED ? Abort : rc;
				return rc;
			}
			new_lock_info = 0;
			lock_info = lock_read->_tid_word;
			#if !DEBUG_PRINTF
			mem_allocator.free(lock_read, row_t::get_row_size(ROW_DEFAULT_SIZE));
			#endif
remote_atomic_retry_lock:
			bool conflict = Row_rdma_2pl::conflict_lock(lock_info, DLOCK_SH, new_lock_info);

			if(conflict){
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				return Abort;
			}
			assert(new_lock_info!=0);
            uint64_t try_lock = -1;
            rc = cas_remote_content(yield,loc,m_item->offset,lock_info,new_lock_info, &try_lock, cor_id);
			if (rc != RCOK) {
				rc = rc == NODE_FAILED ? Abort : rc;
				#if DEBUG_PRINTF
					printf("---thd %lu, remote lock read failed!!!!!!!! because node failed, lock location: %u; %lu, txn: %lu, old lock_info: %lu\n", get_thd_id(), loc, lock_read->get_primary_key(), get_txn_id(), try_lock);
				#endif
				return rc;
			}
            if(try_lock != lock_info){
                num_atomic_retry++;
                total_num_atomic_retry++;
				#if DEBUG_PRINTF
					printf("---thd %lu, remote lock read failed!!!!!!!!, lock location: %u; %lu, txn: %lu, old lock_info: %lu, lock owner %ld\n", get_thd_id(), loc, lock_read->get_primary_key(), get_txn_id(), try_lock);
				#endif
                if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;
                lock_info = try_lock;
                if (!simulation->is_done()) goto remote_atomic_retry_lock;
                else {
                  DEBUG_M("TxnManager::get_row(abort) access free\n");
                  row_local = NULL;
                  txn->rc = Abort;
                  mem_allocator.free(m_item, sizeof(itemid_t));
                  // mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                  return Abort; //原子性被破坏，CAS失败	
                }
            }
			#if DEBUG_PRINTF
				printf("---thd %lu, remote lock read succ, lock location: %u; %lu, txn: %lu, old lock_info: %lu\n", get_thd_id(), loc, lock_read->get_primary_key(), get_txn_id(), try_lock);
			#endif
			//read remote data
        	rc = read_remote_row(yield,loc,m_item->offset,test_row,cor_id,key);
			if (rc != RCOK) {
				rc = rc == NODE_FAILED ? Abort : rc;
				return rc;
			}
        	// assert(test_row->get_primary_key() == req->key);
		}
		else{ //写集元素直接CAS即可，不需要RDMA READ
			lock_info = 0; //只有lock_info==0时才可以CAS，否则加写锁失败，Abort
			new_lock_info = 3; //二进制11，即1个写锁
			#if DEBUG_PRINTF
			rc = read_remote_row(yield,loc,m_item->offset,lock_read,cor_id,key);
			if (rc != RCOK) {
				rc = rc == NODE_FAILED ? Abort : rc;
				return rc;
			}
			#endif
			//RDMA CAS加锁
			uint64_t try_lock = -1;
			rc = cas_remote_content(yield,loc,m_item->offset,lock_info,new_lock_info,&try_lock, cor_id);
			if (rc != RCOK) {
				#if DEBUG_PRINTF
					printf("---thd %lu, local lock write failed!!!!!!!! because node failed, lock location: %u; %lu, txn: %lu, old lock_info: %lu\n", get_thd_id(), loc, lock_read->get_primary_key(), get_txn_id(), try_lock);
				#endif
				rc = rc == NODE_FAILED ? Abort : rc;
				return rc;
			}
			if(try_lock != lock_info){ //如果CAS失败:对写集元素来说,即已经有锁;
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				#if DEBUG_PRINTF
					printf("---thd %lu, local lock write failed!!!!!!!!, lock location: %u; %lu, txn: %lu, old lock_info: %lu\n", get_thd_id(), loc, lock_read->get_primary_key(), get_txn_id(), try_lock);
				#endif
				mem_allocator.free(m_item, sizeof(itemid_t));
				return Abort; //原子性被破坏，CAS失败			
			}	
			#if DEBUG_PRINTF
				printf("---thd %lu, remote lock write succ, lock location: %u; %lu, txn: %lu, old lock_info: %lu\n", get_thd_id(), loc, lock_read->get_primary_key(), get_txn_id(), try_lock);
			#endif
			//read remote data
			rc = read_remote_row(yield,loc,m_item->offset,test_row,cor_id,key);
			if (rc != RCOK) {
				rc = rc == NODE_FAILED ? Abort : rc;
				return rc;
			}
			// assert(test_row->get_primary_key() == req->key);   
		}
		#if DEBUG_PRINTF
		mem_allocator.free(lock_read, row_t::get_row_size(ROW_DEFAULT_SIZE));
		#endif
        //preserve the txn->access
		++num_locks;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc,test_row->get_part_id());
        return rc;
	}		
		rc = RCOK;
		return rc;
	#endif
	#if CC_ALG == RDMA_NO_WAIT3
	if(type == RD || type == WR){
		// uint64_t new_lock_info;
		// uint64_t lock_info;
		row_t * test_row = NULL;
		uint64_t retry_time = 0;
	retry_lock:
		uint64_t try_lock = -1;
		uint64_t lock_type = 0;
		if (is_recover) {
			rc = read_remote_row(yield, loc, m_item->offset, test_row, cor_id);
			if (rc != RCOK) {
				rc = rc == NODE_FAILED ? Abort : rc;
				return rc;
			}
		} else {
			rc = cas_remote_content(yield, loc, m_item->offset, 0, txn->txn_id, &try_lock, cor_id);
			if (rc != RCOK) {
				rc = rc == NODE_FAILED ? Abort : rc;
				return rc;
			}
			if(try_lock != 0 && !simulation->is_done()) {
				// printf("retry cas lock \n");
				retry_time++;
				if (retry_time > 5) {
					DEBUG_T("txn %d add remote mutx lock on item %d failed !!!!!\n", txn->txn_id, key);
					return Abort;
				}
				goto retry_lock;
			}
			rc = read_remote_row(yield, loc, m_item->offset, test_row, cor_id, key);
			#if WORKLOAD == YCSB
			assert(test_row->get_primary_key() == key);
			#endif
			if (rc != RCOK) {
				rc = rc == NODE_FAILED ? Abort : rc;
				return rc;
			}
			lock_type = test_row->lock_type;
			if(lock_type == 0) {
				uint64_t lock_index = txn->txn_id % LOCK_LENGTH;
				test_row->lock_owner[lock_index] = txn->txn_id;
				test_row->lock_type = type == RD? 2:1;
				test_row->_tid_word = 0;
				rc = write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id);
				if (rc != RCOK) {
					rc = rc == NODE_FAILED ? Abort : rc;
					return rc;
				}
			} else if(lock_type == 1 || type == WR) {
				test_row->_tid_word = 0;
				DEBUG_T("txn %d add remote lock on item %d failed !!!!! because lock type %s, lock type %s, lock owner %ld\n", txn->txn_id, test_row->get_primary_key(),lock_type == 1 ? "EX":"SH", type == WR ? "EX":"SH", test_row->lock_owner[0]);

				rc = write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id);
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				mem_allocator.free(m_item, sizeof(itemid_t));
				rc = Abort;
				return rc;
			} else {
				uint64_t lock_index = txn->txn_id % LOCK_LENGTH;
				uint64_t try_time = 0;
				while(try_time <= LOCK_LENGTH) {
					if(test_row->lock_owner[lock_index] == 0) {
						test_row->lock_owner[lock_index] = txn->txn_id;
						test_row->lock_type = test_row->lock_type + 1;
						test_row->_tid_word = 0;
						rc = write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id);
						if (rc != RCOK) {
							rc = rc == NODE_FAILED ? Abort : rc;
							return rc;
						}
						rc = RCOK;
						break;
					}
					lock_index = (lock_index + 1) % LOCK_LENGTH;
					try_time ++;
				}
				if(try_time > LOCK_LENGTH) {
					test_row->_tid_word = 0;
					rc = write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id);
					DEBUG_T("txn %d add remote lock on item %d failed !!!!! because lock owner is too long\n", txn->txn_id, test_row->get_primary_key());
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					mem_allocator.free(m_item, sizeof(itemid_t));
					rc = Abort;
					return rc;
				}
			}
		}
		
		DEBUG_T("txn %d add remote lock on item %d, lock_type: %d\n", txn->txn_id, test_row->get_primary_key(), lock_type);
        //preserve the txn->access
		++num_locks;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc,test_row->get_part_id());
        return rc;
	}		
	rc = RCOK;
	return rc;
	#endif
}

// This function is useless
void TxnManager::insert_row(row_t * row, table_t * table) {
	if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) return;
	assert(txn->insert_rows.size() < MAX_ROW_PER_TXN);
	txn->insert_rows.add(row);
}

itemid_t *TxnManager::index_read(INDEX *index, idx_key_t key, int part_id) {
	uint64_t starttime = get_sys_clock();

	itemid_t * item;
	index->index_read(key, item, part_id, get_thd_id());

	uint64_t t = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_index_time, t);
	//txn_time_idx += t;

	return item;
}

itemid_t *TxnManager::index_read(INDEX *index, idx_key_t key, int part_id, int count) {
	uint64_t starttime = get_sys_clock();

	itemid_t * item;
	index->index_read(key, count, item, part_id);

	uint64_t t = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_index_time, t);
	//txn_time_idx += t;

	return item;
}

RC TxnManager::validate(yield_func_t &yield, uint64_t cor_id) {
#if MODE != NORMAL_MODE
	return RCOK;
#endif
	if (CC_ALG != OCC && CC_ALG != MAAT) {
		return RCOK; //no validate in NO_WAIT
	}
	RC rc = RCOK;
	uint64_t starttime = get_sys_clock();
	if (CC_ALG == OCC && rc == RCOK) rc = occ_man.validate(this);
	if(CC_ALG == MAAT  && rc == RCOK) {
		rc = maat_man.validate(this);
		// Note: home node must be last to validate
		if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
			rc = maat_man.find_bound(this);
		}
	}
	INC_STATS(get_thd_id(),txn_validate_time,get_sys_clock() - starttime);
	INC_STATS(get_thd_id(),trans_validate_time,get_sys_clock() - starttime);
    INC_STATS(get_thd_id(),trans_validate_count, 1);
	return rc;
}

RC TxnManager::send_remote_reads() {
	assert(CC_ALG == CALVIN);
#if !YCSB_ABORT_MODE && WORKLOAD == YCSB
	return RCOK;
#endif
	assert(query->active_nodes.size() == g_node_cnt);
	for(uint64_t i = 0; i < query->active_nodes.size(); i++) {
		if (i == g_node_id) continue;
	if(query->active_nodes[i] == 1) {
		DEBUG("(%ld,%ld) send_remote_read to %ld\n",get_txn_id(),get_batch_id(),i);
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), i, Message::create_message(this,RFWD));
#else
		msg_queue.enqueue(get_thd_id(),Message::create_message(this,RFWD),i);
#endif
	}
	}
	return RCOK;

}

bool TxnManager::calvin_exec_phase_done() {
	bool ready =  (phase == CALVIN_DONE) && (get_rc() != WAIT);
	if(ready) {
	DEBUG("(%ld,%ld) calvin exec phase done!\n",txn->txn_id,txn->batch_id);
	}
	return ready;
}

bool TxnManager::calvin_collect_phase_done() {
	bool ready =  (phase == CALVIN_COLLECT_RD) && (get_rsp_cnt() == calvin_expected_rsp_cnt);
	if(ready) {
	DEBUG("(%ld,%ld) calvin collect phase done!\n",txn->txn_id,txn->batch_id);
	}
	return ready;
}

void TxnManager::release_locks(yield_func_t &yield, RC rc, uint64_t cor_id) {
	uint64_t starttime = get_sys_clock();
	uint64_t endtime;
	cleanup(yield, rc, cor_id);

	uint64_t timespan = (get_sys_clock() - starttime);
	INC_STATS(get_thd_id(), txn_cleanup_time,  timespan);
#if DEBUG_PRINTF
	if(rc == Abort) printf("---thd %lu txn %lu, Abort end.\n",get_thd_id(), get_txn_id());
	else if(rc == RCOK) printf("---thd %lu txn %lu, Commit end.\n",get_thd_id(), get_txn_id());
#endif
}

RC TxnManager::read_remote_index(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t key, itemid_t * &item, uint64_t cor_id){
    uint64_t operate_size = sizeof(IndexInfo);
    uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_index_client_memory(thd_id);

	RC rc = read_remote_content(yield, target_server, remote_offset, operate_size, local_buf, cor_id);
	if (rc != RCOK) return rc;
    // itemid_t* 
	last_index = (IndexInfo *)mem_allocator.alloc(sizeof(IndexInfo));
	memcpy((char*)last_index, local_buf, operate_size);
	item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
	memset(item, 0, sizeof(itemid_t));
	assert(((IndexInfo*)local_buf)->key == key);
	item->key = ((IndexInfo*)local_buf)->key;
	assert(item->key == key);
	item->location = ((IndexInfo*)local_buf)->address;
	item->type = ((IndexInfo*)local_buf)->type;
	item->valid = ((IndexInfo*)local_buf)->valid;
	item->offset = ((IndexInfo*)local_buf)->offset;
  	item->table_offset = ((IndexInfo*)local_buf)->table_offset;
	

    return rc;
}

RC TxnManager::read_remote_row(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,row_t * &test_row, uint64_t cor_id, uint64_t key){
    uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
    uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_row_client_memory(thd_id);
   
	RC rc = read_remote_content(yield, target_server, remote_offset, operate_size, local_buf, cor_id);
	if (rc != RCOK) return rc;
	test_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
	memset(test_row, 0, operate_size);
    memcpy(test_row, local_buf, operate_size);
	#if WORKLOAD == YCSB
	if (key != UINT64_MAX) assert(test_row->get_primary_key() == key);
	#endif
    return rc;
}

#if USE_REPLICA
RC TxnManager::read_remote_log_head(yield_func_t &yield, uint64_t target_server, uint64_t* result, uint64_t cor_id){
    uint64_t operate_size = sizeof(uint64_t);
    uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_row_client_memory(thd_id);
	uint64_t remote_offset = rdma_buffer_size-rdma_log_size;

	RC rc = read_remote_content(yield, target_server, remote_offset, operate_size, local_buf, cor_id);
	if (rc != RCOK) return rc;
	*result = *((uint64_t *)local_buf);
	return RCOK;
}
#endif

RC TxnManager::read_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset, uint64_t operate_size, char* local_buf, uint64_t cor_id){
	uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    memset(local_buf, 0, operate_size);

    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));

	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp(RDMA_CALLS_TIMEOUT);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1); //include index, row, log,...etc read.
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	// assert(res_p == rdmaio::IOCode::Ok);
	if (res_p != rdmaio::IOCode::Ok) {
		node_status.set_node_status(target_server, NS::Failure, get_thd_id());
		DEBUG_T("Thd %ld send RDMA one-sided failed--read %ld.\n", get_thd_id(),target_server);
		return NODE_FAILED;
	}
	return RCOK;
#endif

}

RC TxnManager::write_remote_index(yield_func_t &yield,uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content,uint64_t cor_id){
	uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_index_client_memory(thd_id);
	return write_remote_content(yield, target_server, operate_size,remote_offset,write_content,local_buf,cor_id);

 }

RC TxnManager::write_remote_row(yield_func_t &yield, uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content,uint64_t cor_id){
	uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
	char *local_buf = Rdma::get_row_client_memory(thd_id);
	return write_remote_content(yield, target_server, operate_size,remote_offset,write_content,local_buf,cor_id);
 }

#if USE_REPLICA
RC TxnManager::write_remote_log(yield_func_t &yield,uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content,uint64_t cor_id, int num, bool outstanding){
	uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_log_client_memory(thd_id, num);
	return write_remote_content(yield, target_server, operate_size,remote_offset,write_content,local_buf,cor_id,outstanding);
}
#endif

RC TxnManager::write_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content, char* local_buf,uint64_t cor_id, bool outstanding){
	uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    memset(local_buf, 0, operate_size);
    memcpy(local_buf, write_content , operate_size);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	if(!outstanding){
		INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
		// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));

		uint64_t waitcomp_time;
		std::pair<int,ibv_wc> res_p;
		INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
		do {
			h_thd->start_wait_time = get_sys_clock();
			h_thd->last_yield_time = get_sys_clock();
			// printf("do\n");
			yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
			uint64_t yield_endtime = get_sys_clock();
			INC_STATS(get_thd_id(), worker_yield_cnt, 1);
			INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
			INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
			res_p = rc_qp[target_server][thd_id]->poll_send_comp();
			waitcomp_time = get_sys_clock();
			
			INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
			INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
		} while (res_p.first == 0);
		h_thd->cor_process_starttime[cor_id] = get_sys_clock();
		// yield(h_thd->_routines[0]);
#else
		auto res_p = rc_qp[target_server][thd_id]->wait_one_comp(RDMA_CALLS_TIMEOUT);
		endtime = get_sys_clock();
		INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
		INC_STATS(get_thd_id(), rdma_read_cnt, 1);
		INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
		DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
		// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
		if (res_p != rdmaio::IOCode::Ok) {
			node_status.set_node_status(target_server, NS::Failure, get_thd_id());
			DEBUG_T("Thd %ld send RDMA one-sided failed--write %ld.\n", get_thd_id(), target_server);
			return NODE_FAILED;
		}
#endif
	}
    return RCOK;
}

//the value prior to being incremented is returned to the caller.
RC TxnManager::faa_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset, uint64_t value_to_add, uint64_t *result, uint64_t cor_id, int num, bool outstanding){
    
    rdmaio::qp::Op<> op;
	uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id,num);
	*local_buf = -1;
    auto mr = client_rm_handler->get_reg_attr().value();
    
    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[target_server].buf + remote_offset), remote_mr_attr[target_server].key).set_fetch_add(value_to_add);
    assert(op.set_payload(local_buf, sizeof(uint64_t), mr.key) == true);
    auto res_s2 = op.execute(rc_qp[target_server][thd_id], IBV_SEND_SIGNALED);

    RDMA_ASSERT(res_s2 == IOCode::Ok);
	if(!outstanding){
		INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
		uint64_t waitcomp_time;
		std::pair<int,ibv_wc> res_p;
		INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
		do {
			h_thd->start_wait_time = get_sys_clock();
			h_thd->last_yield_time = get_sys_clock();
			// printf("do\n");
			yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
			uint64_t yield_endtime = get_sys_clock();
			INC_STATS(get_thd_id(), worker_yield_cnt, 1);
			INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
			INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
			res_p = rc_qp[target_server][thd_id]->poll_send_comp();
			waitcomp_time = get_sys_clock();
			
			INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
			INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
		} while (res_p.first == 0);
		h_thd->cor_process_starttime[cor_id] = get_sys_clock();

#else
		auto res_p = rc_qp[target_server][thd_id]->wait_one_comp(RDMA_CALLS_TIMEOUT);
		// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
		endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
		DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
		// assert(res_p == rdmaio::IOCode::Ok);
		if (res_p != rdmaio::IOCode::Ok) {
			node_status.set_node_status(target_server, NS::Failure, get_thd_id());
			DEBUG_T("Thd %ld send RDMA one-sided failed--faa %ld.\n", get_thd_id(),target_server);
			return NODE_FAILED;
		}
#endif
		*result = *local_buf;
		return RCOK;
	}else {
		*result = 0;
		return RCOK;
	}
}

RC TxnManager::cas_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value, uint64_t *try_lock, uint64_t cor_id){
    
    rdmaio::qp::Op<> op;
    uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
    auto mr = client_rm_handler->get_reg_attr().value();
    
    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[target_server].buf + remote_offset), remote_mr_attr[target_server].key).set_cas(old_value, new_value);
    assert(op.set_payload(local_buf, sizeof(uint64_t), mr.key) == true);
    auto res_s2 = op.execute(rc_qp[target_server][thd_id], IBV_SEND_SIGNALED);

    RDMA_ASSERT(res_s2 == IOCode::Ok);
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
		
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(get_thd_id(), worker_yield_cnt, 1);
		INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();

#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp(RDMA_CALLS_TIMEOUT);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	// assert(res_p == rdmaio::IOCode::Ok);
	if (res_p != rdmaio::IOCode::Ok) {
		node_status.set_node_status(target_server, NS::Failure, get_thd_id());
		DEBUG_T("Thd %ld send RDMA one-sided failed--cas %ld.\n", get_thd_id(),target_server);
		return NODE_FAILED;
	}
#endif
	*try_lock = *local_buf;
    return RCOK;
}

RC TxnManager::loop_cas_remote(yield_func_t &yield,uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value, uint64_t cor_id){
    uint64_t cas_result = -1;
    do{
        RC rc = cas_remote_content(yield,target_server,remote_offset,old_value,new_value,&cas_result,cor_id);
		if (rc != RCOK) {
			return rc;
		}
    }
    while(cas_result != old_value && cas_result != new_value && !simulation->is_done());

    return RCOK;
}

RC TxnManager::preserve_access(row_t *&row_local,itemid_t* m_item,row_t *test_row,access_t type,uint64_t key,uint64_t loc,uint64_t part_id){
    Access * access = NULL;
	access_pool.get(get_thd_id(),access);

	this->last_row = test_row;
    this->last_type = type;

    RC rc = RCOK;
	rc = test_row->remote_copy_row(test_row, this, access);
    assert(test_row->get_primary_key() == access->data->get_primary_key());
    if (rc == Abort || rc == WAIT) {
        DEBUG_T("TxnManager::get_row(abort) access free\n");
        access_pool.put(get_thd_id(),access);
        return rc;
    }

    access->type = type;
	access->key = key;
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3
  	access->orig_row = test_row;
	access->location = loc;
	access->offset = m_item->offset;
#endif
	access->partition_id = part_id;

    row_local = access->data;
    ++txn->row_cnt;
	DEBUG_T("txn %ld put remote row %ld offset %ld type %d in access\n", get_txn_id(), key, access->offset, access->type);
    mem_allocator.free(m_item,0);

    if (type == WR) ++txn->write_cnt;//this->last_type = WR
    txn->accesses.add(access);

    return rc;
}

