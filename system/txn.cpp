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
#include "focc.h"
#include "bocc.h"
#include "row_occ.h"
#include "table.h"
#include "catalog.h"
#include "dli.h"
#include "dta.h"
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
#include "wkdb.h"
#include "tictoc.h"
#include "ssi.h"
#include "wsi.h"
#include "manager.h"
#include "row_rdma_2pl.h"
#include "rdma_silo.h"
#include "rdma_mvcc.h"
#include "rdma_2pl.h"
#include "rdma_maat.h"
#include "rdma_ts1.h"
#include "rdma_cicada.h"
#include "cicada.h"
#include "row_cicada.h"
#include "rdma_null.h"
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

#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN

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
	insert_rows.init(g_max_items_per_txn + 10);
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
#if CC_ALG != MAAT && CC_ALG != OCC && CC_ALG != WOOKONG && \
		CC_ALG != TICTOC && CC_ALG != BOCC && CC_ALG != FOCC && CC_ALG != DTA && CC_ALG != DLI_MVCC_OCC && \
		CC_ALG != DLI_MVCC_BASE && CC_ALG != DLI_DTA && CC_ALG != DLI_DTA2 && CC_ALG != DLI_DTA3 && \
		CC_ALG != DLI_BASE && CC_ALG != DLI_OCC && CC_ALG != RDMA_MVCC  && CC_ALG != RDMA_MAAT  && CC_ALG != RDMA_CICADA && CC_ALG != RDMA_CNULL
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
#if CC_ALG == RDMA_MAAT
	// uncommitted_writes = new std::set<uint64_t>();
	// uncommitted_writes_y = new std::set<uint64_t>();
	// uncommitted_reads = new std::set<uint64_t>();
	memset(write_set, 0, 100);
#endif
#if CC_ALG == RDMA_CICADA
	start_ts = get_sys_clock();
	memset(write_set, 0, 100);
	// uncommit_set = new std::set<uint64_t>();
#endif
#if CC_ALG == TICTOC
	_is_sub_txn = true;
	_min_commit_ts = glob_manager.get_max_cts();;
	_num_lock_waits = 0;
	_signal_abort = false;
	_timestamp = glob_manager.get_ts(get_thd_id());
#endif
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	phase = CALVIN_RW_ANALYSIS;
	locking_done = false;
	calvin_locked_rows.init(MAX_ROW_PER_TXN);
#endif
#if CC_ALG == DLI_MVCC || CC_ALG == DLI_MVCC_OCC
	is_abort = nullptr;
#endif
#if CC_ALG == SILO
	_pre_abort = (g_params["pre_abort"] == "true");
	if (g_params["validation_lock"] == "no-wait")
		_validation_no_wait = true;
	else if (g_params["validation_lock"] == "waiting")
		_validation_no_wait = false;
	else
		assert(false);
  _cur_tid = 0;
  num_locks = 0;
  memset(write_set, 0, 100);
  // write_set = (int *) mem_allocator.alloc(sizeof(int) * 100);
#endif
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
	num_atomic_retry = 0;
#endif
#if CC_ALG == WOUND_WAIT
	txn_state = RUNNING;
#endif
#if CC_ALG == RDMA_MAAT || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
    rdma_txn_table.release(get_thd_id(), get_txn_id());
#endif
	registed_ = false;
	txn_ready = true;
	twopl_wait_start = 0;
	txn_stats.init();
}

// reset after abort
void TxnManager::reset() {
	lock_ready = false;
	lock_ready_cnt = 0;
	locking_done = true;
#if CC_ALG == DLI_MVCC || CC_ALG == DLI_MVCC_OCC
	is_abort = nullptr;
#endif
	ready_part = 0;
	rsp_cnt = 0;
	aborted = false;
	return_id = UINT64_MAX;
	twopl_wait_start = 0;

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
#if CC_ALG == RDMA_MAAT
	uncommitted_writes.clear();
	uncommitted_writes_y.clear();
	uncommitted_reads.clear();
	unread_set.clear();
	unwrite_set.clear();
#endif
#if CC_ALG == RDMA_CICADA
	uncommitted_set.clear();
	start_ts = 0;
	version_num.clear();
#endif

#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	phase = CALVIN_RW_ANALYSIS;
	locking_done = false;
	calvin_locked_rows.clear();
#endif
#if CC_ALG == RDMA_MAAT || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
    rdma_txn_table.release(get_thd_id(), get_txn_id());
#endif
	assert(txn);
	assert(query);
	txn->reset(get_thd_id());
	center_master.clear();
	// Stats
	txn_stats.reset();
}

void TxnManager::release() {
	uint64_t prof_starttime = get_sys_clock();
	qry_pool.put(get_thd_id(),query);
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
#if CC_ALG == RDMA_MAAT
	uncommitted_writes.clear();
	uncommitted_writes_y.clear();
	uncommitted_reads.clear();
	unread_set.clear();
	unwrite_set.clear();
#endif
#if CC_ALG == RDMA_CICADA
	uncommitted_set.clear();
	start_ts = 0;
	version_num.clear();
#endif
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	calvin_locked_rows.release();
#endif
#if CC_ALG == SILO
  num_locks = 0;
  memset(write_set, 0, 100);
  // mem_allocator.free(write_set, sizeof(int) * 100);
#endif
	txn_ready = true;
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
	DEBUG("Commit %ld\n",get_txn_id());
#if CC_ALG == WOUND_WAIT
    txn_state = STARTCOMMIT;    
#endif
	release_locks(yield, RCOK, cor_id);
#if CC_ALG == MAAT
	time_table.release(get_thd_id(),get_txn_id());
#endif
#if CC_ALG == RDMA_MAAT || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
    rdma_txn_table.release(get_thd_id(), get_txn_id());
#endif
#if CC_ALG == WOOKONG
	wkdb_time_table.release(get_thd_id(),get_txn_id());
#endif
#if CC_ALG == TICTOC
	tictoc_man.cleanup(RCOK, this);
#endif
#if CC_ALG == SSI
	inout_table.set_commit_ts(get_thd_id(), get_txn_id(), get_commit_timestamp());
	inout_table.set_state(get_thd_id(), get_txn_id(), SSI_COMMITTED);
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
#if CC_ALG == SSI
	inout_table.set_state(get_thd_id(), get_txn_id(), SSI_ABORTED);
	inout_table.clear_Conflict(get_thd_id(), get_txn_id());
#endif
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
#if CC_ALG == RDMA_MAAT || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
    rdma_txn_table.release(get_thd_id(), get_txn_id());
#endif
#if CC_ALG == WOOKONG
	wkdb_time_table.release(get_thd_id(),get_txn_id());
#endif
#if CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
	//assert(time_table.get_state(get_txn_id()) == MAAT_ABORTED);
	dta_time_table.release(get_thd_id(), get_txn_id());
#endif
#if CC_ALG == RDMA_SILO
    //TODO
	//rdam_rlease(get_thd_id(), get_txn_id());
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
	DEBUG("%ld start_abort\n",get_txn_id());

	uint64_t finish_start_time = get_sys_clock();
	txn_stats.finish_start_time = finish_start_time;
	uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
	INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
    INC_STATS(get_thd_id(), trans_prepare_count, 1);
	//RDMA_SILO:keep message or not
#if CENTER_MASTER == true
    if(query->centers_touched.size() > 1 && CC_ALG != RDMA_NO_WAIT && CC_ALG != RDMA_NO_WAIT2 && CC_ALG != RDMA_WAIT_DIE2  && CC_ALG != RDMA_MAAT && CC_ALG != RDMA_CICADA && CC_ALG != RDMA_WOUND_WAIT2 && CC_ALG != RDMA_WAIT_DIE && CC_ALG != RDMA_WOUND_WAIT) {
#else
	if(query->partitions_touched.size() > 1 && CC_ALG != RDMA_NO_WAIT && CC_ALG != RDMA_NO_WAIT2 && CC_ALG != RDMA_WAIT_DIE2  && CC_ALG != RDMA_MAAT && CC_ALG != RDMA_CICADA && CC_ALG != RDMA_WOUND_WAIT2 && CC_ALG != RDMA_WAIT_DIE && CC_ALG != RDMA_WOUND_WAIT) {
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
	if(CC_ALG == SSI) {
		ssi_man.gene_finish_ts(this);
	}
	if(CC_ALG == WSI) {
		wsi_man.gene_finish_ts(this);
	}
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
#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
        // printf("read local WOUNDState:%ld\n", rdma_txn_table.local_get_state(get_thd_id(),txn->txn_id));
		if(rdma_txn_table.local_get_state(get_thd_id(),txn->txn_id) == WOUND_RUNNING) {
			rdma_txn_table.local_set_state(get_thd_id(),txn->txn_id, WOUND_COMMITTING);
		}
#endif
	if(is_multi_part() && CC_ALG != RDMA_WAIT_DIE2 && CC_ALG != RDMA_MAAT  && CC_ALG != RDMA_CICADA && CC_ALG !=RDMA_TS1 && CC_ALG != RDMA_WOUND_WAIT2 && CC_ALG != RDMA_WAIT_DIE && CC_ALG != RDMA_WOUND_WAIT) {
		if(CC_ALG == TICTOC) {
			rc = validate(yield, cor_id);
			if (rc != Abort) {
				send_prepare_messages();
				rc = WAIT_REM;
			}
		} else if (!query->readonly() || CC_ALG == OCC || CC_ALG == MAAT || CC_ALG == DLI_BASE ||
				CC_ALG == DLI_OCC || CC_ALG == SILO || CC_ALG == BOCC || CC_ALG == SSI || CC_ALG == CICADA || 
				CC_ALG == RDMA_SILO || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_NO_WAIT) {
			// send prepare messages
			send_prepare_messages();
			rc = WAIT_REM;
		} else {
			uint64_t finish_start_time = get_sys_clock();
			txn_stats.finish_start_time = finish_start_time;
			uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
			INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
      		INC_STATS(get_thd_id(), trans_prepare_count, 1);
			if(CC_ALG == WSI) {
				wsi_man.gene_finish_ts(this);
			}
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

		uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
		INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
    	INC_STATS(get_thd_id(), trans_prepare_count, 1);
		if(CC_ALG == SSI) {
			ssi_man.gene_finish_ts(this);
		}
		if(CC_ALG == WSI) {
			wsi_man.gene_finish_ts(this);
		}
		if(rc == RCOK){   //for NO_WAIT , rc == RCOK
			rc = commit(yield, cor_id);
		}		
		else {
			txn->rc = Abort;
			DEBUG("%ld start_abort\n",get_txn_id());
			if(query->partitions_touched.size() > 1 && CC_ALG != RDMA_SILO &&  CC_ALG != RDMA_NO_WAIT && CC_ALG != RDMA_NO_WAIT2 && CC_ALG != RDMA_WAIT_DIE2 && CC_ALG != RDMA_MAAT && CC_ALG != RDMA_CICADA && CC_ALG != RDMA_WOUND_WAIT2 && CC_ALG != RDMA_WOUND_WAIT && CC_ALG != RDMA_WAIT_DIE) {
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
#endif
	}
}

void TxnManager::send_finish_messages() {
#if CENTER_MASTER == true
	rsp_cnt = query->centers_touched.size() - 1;	
	assert(IS_LOCAL(get_txn_id()));
	DEBUG("%ld Send FINISH messages to %d\n",get_txn_id(),rsp_cnt);
	for(uint64_t i = 0; i < query->centers_touched.size(); i++) {
		if(this->center_master[query->centers_touched[i]] == g_node_id || query->centers_touched[i] == g_center_id) {
			continue;
    	}
#if USE_RDMA == CHANGE_MSG_QUEUE
        tport_man.rdma_thd_send_msg(get_thd_id(), this->center_master[query->centers_touched[i]]), Message::create_message(this, RFIN));
#else
		msg_queue.enqueue(get_thd_id(), Message::create_message(this, RFIN),
											this->center_master[query->centers_touched[i]]);
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
}

int TxnManager::received_response(RC rc) {
	assert(txn->rc == RCOK || txn->rc == Abort);
	if (txn->rc == RCOK) txn->rc = rc;
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	++rsp_cnt;
#else
  if (rsp_cnt > 0)
	  --rsp_cnt;
#endif
	return rsp_cnt;
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
	if(!IS_LOCAL(get_txn_id()) && (CC_ALG != CALVIN && CC_ALG != RDMA_CALVIN)) {
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
	#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
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
	if (type == WR && rc == Abort && CC_ALG != MAAT && CC_ALG != RDMA_MAAT) {
		type = XP;
	}
    bool is_local = true;
	uint64_t version = 0;
	// Handle calvin elsewhere

#if CC_ALG != CALVIN && CC_ALG != RDMA_CALVIN
#if ISOLATION_LEVEL != READ_UNCOMMITTED
	row_t * orig_r = txn->accesses[rid]->orig_row;
#if CC_ALG == RDMA_SILO
  if (txn->accesses[rid]->location == g_node_id) {
    if (ROLL_BACK && type == XP &&
        (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE ||
        CC_ALG == HSTORE_SPEC)) {
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
  }
#elif CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
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
#elif CC_ALG == RDMA_MAAT || CC_ALG == RDMA_CICADA
    if(txn->accesses[rid]->location == g_node_id) is_local = true;
	else is_local = false;
#if ISOLATION_LEVEL == READ_COMMITTED
	if(type == WR) {
		version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
	}
#else
    version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
#endif
#elif CC_ALG ==RDMA_TS1 
	if (type == RD || type == SCAN) {
		version = orig_r->return_row(yield, type, this, txn->accesses[rid], cor_id);
	} else if (type == WR || type == XP) { 
		if(type == WR)
			assert(txn->accesses[rid]->data != NULL);
		
		//1.No distinction is made between local and remote
		// rdmats_man.commit_write(this, rid, type); //COMMIT

		//2.Distinguish between local and remote
		if (txn->accesses[rid]->location != g_node_id)
			is_local = false;
		if (is_local) {
			version = orig_r->return_row(yield, type, this, txn->accesses[rid], cor_id);
		} else {
			if(type ==  XP) remote_access[txn->accesses[rid]->location].push_back(rid);
			rdmats_man.commit_write(yield, this, rid, type, cor_id);
		}
	} else {
		assert(false);
	}
#elif CC_ALG == CICADA
	version = orig_r->manager->commit(this, type);
#else
  if (ROLL_BACK && type == XP &&
      (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == WAIT_DIE || CC_ALG == HSTORE ||
      CC_ALG == HSTORE_SPEC || CC_ALG == WOUND_WAIT || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT)) {
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
		(CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_NO_WAIT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == WOUND_WAIT || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT)
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
#if CC_ALG == TICTOC
	if (_min_commit_ts > glob_manager.get_max_cts())
		glob_manager.set_max_cts(_min_commit_ts);
#endif

#if CC_ALG != SILO && CC_ALG != RDMA_NO_WAIT && CC_ALG != RDMA_NO_WAIT2 && CC_ALG != RDMA_WAIT_DIE2 && CC_ALG != RDMA_WOUND_WAIT2 && CC_ALG != RDMA_WAIT_DIE && CC_ALG != RDMA_WOUND_WAIT
  txn->accesses[rid]->data = NULL;
#endif
}

void TxnManager::cleanup(yield_func_t &yield, RC rc, uint64_t cor_id) {
#if CC_ALG == SILO
    finish(rc);
#endif
#if CC_ALG == OCC && MODE == NORMAL_MODE
	occ_man.finish(rc,this);
#endif
#if CC_ALG == BOCC && MODE == NORMAL_MODE
	bocc_man.finish(rc,this);
#endif
#if CC_ALG == FOCC && MODE == NORMAL_MODE
	focc_man.finish(rc,this);
#endif
#if (CC_ALG == WSI) && MODE == NORMAL_MODE
	wsi_man.finish(rc,this);
#endif
//TODO-relase lock
#if CC_ALG == RDMA_SILO
    rsilo_man.finish(yield,rc,this, cor_id);
#endif

#if CC_ALG == RDMA_CNULL
    rcnull_man.finish(rc,this);
#endif

#if CC_ALG == RDMA_MVCC
    rmvcc_man.finish(yield, rc, this, cor_id);
#endif

#if CC_ALG == RDMA_MAAT
    rmaat_man.finish(yield, rc, this, cor_id);
#endif
#if CC_ALG == RDMA_CICADA
	rcicada_man.finish(yield, rc, this, cor_id);
#endif
#if CC_ALG == RDMA_TS1
    rdmats_man.finish(rc,this);
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
	
#if USE_DBPAOR == true && CC_ALG == RDMA_TS1 
	starttime = get_sys_clock();
	uint64_t endtime;
    for(int i=0;i<g_node_cnt;i++){ //for the same node, batch unlock remote
        if(remote_access[i].size() > 0){
            batch_unlock_remote(yield, cor_id, i, Abort, this, remote_access);
        }
    }
    for(int i=0;i<g_node_cnt;i++){ //poll result
        if(remote_access[i].size() > 0){
        	//to do: add coroutine
		INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
			uint64_t waitcomp_time;
			std::pair<int,ibv_wc> dbres1;
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
				dbres1 = rc_qp[i][get_thd_id() + cor_id * g_thread_cnt]->poll_send_comp();
				waitcomp_time = get_sys_clock();
				
				INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
				INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
			} while (dbres1.first == 0);
			h_thd->cor_process_starttime[cor_id] = get_sys_clock();
			// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
#else
            auto dbres1 = rc_qp[i][get_thd_id() + cor_id * g_thread_cnt]->wait_one_comp();
            RDMA_ASSERT(dbres1 == IOCode::Ok);
			endtime = get_sys_clock();
			INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
			INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
			DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif    
        }
    }
#endif 
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
    r2pl_man.finish(yield,rc,this,cor_id);
#endif
#if CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || \
		CC_ALG == DLI_MVCC_BASE
	dli_man.finish_trans(rc, this);
#endif
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	// cleanup locked rows
	for (uint64_t i = 0; i < calvin_locked_rows.size(); i++) {
		row_t * row = calvin_locked_rows[i];
		row->return_row(rc,RD,this,row);
	}
#endif

#if CC_ALG == DTA
	dta_man.finish(rc, this);
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
#if CC_ALG == TICTOC
	bool isexist = false;
	uint64_t size = get_write_set_size();
	size += get_read_set_size();
	// UInt32 n = 0, m = 0;
	for (uint64_t i = 0; i < size; i++) {
		if (txn->accesses[i]->orig_row == row) {
			access = txn->accesses[i];
			access->orig_row->get_ts(access->orig_wts, access->orig_rts);
			isexist = true;
			// DEBUG("TxnManagerTictoc::find the exist access \n", access->orig_data, access->orig_row, access->data, access->orig_rts, access->orig_wts);
			break;
		}
	}
	if (!access) {
		access_pool.get(get_thd_id(),access);

    get_access_end_time = get_sys_clock();
    INC_STATS(get_thd_id(), trans_get_access_count, 1);
    INC_STATS(get_thd_id(), trans_get_access_time, get_access_end_time - starttime);

		rc = row->get_row(type, this, access->data, access->orig_wts, access->orig_rts);
		if (!OCC_WAW_LOCK || type == RD) {
			_min_commit_ts = _min_commit_ts > access->orig_wts ? _min_commit_ts : access->orig_wts;
		} else {
			if (rc == WAIT)
						ATOM_ADD_FETCH(_num_lock_waits, 1);
						if (rc == Abort || rc == WAIT)
								return rc;
		}
    INC_STATS(get_thd_id(), trans_get_row_time, get_sys_clock() - get_access_end_time);
    INC_STATS(get_thd_id(), trans_get_row_count, 1);
	} else {
    get_access_end_time = get_sys_clock();
    INC_STATS(get_thd_id(), trans_get_access_count, 1);
    INC_STATS(get_thd_id(), trans_get_access_time, get_access_end_time - starttime);
  }
	if (!OCC_WAW_LOCK || type == RD) {
		access->locked = false;
	} else {
		_min_commit_ts = _min_commit_ts > access->orig_rts + 1 ? _min_commit_ts : access->orig_rts + 1;
		access->locked = true;
	}
#else
	access_pool.get(get_thd_id(),access);
	get_access_end_time = get_sys_clock();
	INC_STATS(get_thd_id(), trans_get_access_time, get_access_end_time - starttime);
	INC_STATS(get_thd_id(), trans_get_access_count, 1);
#endif
	//uint64_t row_cnt = txn->row_cnt;
	//assert(txn->accesses.get_count() - 1 == row_cnt);
#if CC_ALG == RDMA_TS1 || CC_ALG == RDMA_MVCC
	access->location = g_node_id;
	access->offset = (char*)row - rdma_global_buffer;
#endif
#if CC_ALG != TICTOC
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
#endif
#if CC_ALG == FOCC
	focc_man.active_storage(type, this, access);
#endif
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
#if CC_ALG == SILO
	access->tid = last_tid;
#endif
#if CC_ALG ==RDMA_NO_WAIT || CC_ALG ==RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
	access->location = g_node_id;
	access->offset = (char*)row - rdma_global_buffer;
#endif
#if CC_ALG == RDMA_SILO
	access->key = row->get_primary_key();
	access->tid = last_tid;
	access->timestamp = row->timestamp;
	access->offset = (char*)row - rdma_global_buffer;
	access->location = g_node_id;
#endif

#if CC_ALG == RDMA_MVCC
   access->offset = (char*)row - rdma_global_buffer;
   access->old_version_num = row->version_num;
#endif

#if CC_ALG == RDMA_MAAT
	access->location = g_node_id;
	access->offset = (char*)row - rdma_global_buffer;
#endif

#if CC_ALG == RDMA_CICADA
	access->location = g_node_id;
	access->offset = (char*)row - rdma_global_buffer;
	
#endif

#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == WOUND_WAIT || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT)
	if (type == WR) {
	//printf("alloc 10 %ld\n",get_txn_id());
	uint64_t part_id = row->get_part_id();
	DEBUG_M("TxnManager::get_row row_t alloc\n")
	row_pool.get(get_thd_id(),access->orig_data);
	access->orig_data->init(row->get_table(), part_id, 0);
	access->orig_data->copy(row);
    //  for(int i = 0;i < this->txn->row_cnt; i++){
    //      if(txn->accesses[i]->type == WR)
    //             printf("txn %ld o_d[%ld] table_idx = %ld \n",this->get_txn_id(),i,txn->accesses[i]->orig_data->table_idx);
    // }
    //printf("\n");
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

#if CC_ALG == TICTOC
	if (!isexist) {
		++txn->row_cnt;
		if (type == WR)
			++txn->write_cnt;
			txn->accesses.add(access);
	}
#else
	++txn->row_cnt;
	if (type == WR) ++txn->write_cnt;
	txn->accesses.add(access);
#endif
   
	timespan = get_sys_clock() - starttime;
    INC_STATS(get_thd_id(), trans_store_access_time, timespan + starttime - middle_time);
  	INC_STATS(get_thd_id(), trans_store_access_count, 1);
	INC_STATS(get_thd_id(), txn_manager_time, timespan);
	row_rtn  = access->data;

	if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN) assert(rc == RCOK);
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
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == WOUND_WAIT || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT)
	if (type == WR) {
		uint64_t part_id = row->get_part_id();
	//printf("alloc 10 %ld\n",get_txn_id());
	DEBUG_M("TxnManager::get_row_post_wait row_t alloc\n")
	row_pool.get(get_thd_id(),access->orig_data);
		access->orig_data->init(row->get_table(), part_id, 0);
		access->orig_data->copy(row);
         for(int i = 0;i < this->txn->row_cnt;i++){
             if(txn->accesses[i]->type == WR)
            printf("txn %ld orgin_d[%ld] table %ld",this->get_txn_id(),i,this->txn->accesses[i]->orig_data->table_idx);
        }
	}
#endif

	++txn->row_cnt;
	if (type == WR) ++txn->write_cnt;
#if CC_ALG == RDMA_SILO
	access->offset = (char*)row - rdma_global_buffer;
#endif

#if CC_ALG == RDMA_MVCC
   access->offset = (char*)row - rdma_global_buffer;
   access->old_version_num = row->version_num;
#endif

	txn->accesses.add(access);
	uint64_t timespan = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_manager_time, timespan);
	this->last_row_rtn  = access->data;
	row_rtn  = access->data;
	return RCOK;
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
	if (CC_ALG != OCC && CC_ALG != MAAT  && CC_ALG != WOOKONG &&
			CC_ALG != TICTOC && CC_ALG != BOCC && CC_ALG != FOCC && CC_ALG != WSI &&
			CC_ALG != SSI && CC_ALG != DLI_BASE && CC_ALG != DLI_OCC &&
			CC_ALG != DLI_MVCC_OCC && CC_ALG != DTA && CC_ALG != DLI_DTA &&
			CC_ALG != DLI_DTA2 && CC_ALG != DLI_DTA3 && CC_ALG != DLI_MVCC && CC_ALG != SILO &&
			CC_ALG != RDMA_SILO && CC_ALG != RDMA_MVCC && CC_ALG != RDMA_MAAT && 
			CC_ALG != RDMA_CICADA && CC_ALG != CICADA) {
		return RCOK; //no validate in NO_WAIT
	}
	RC rc = RCOK;
	uint64_t starttime = get_sys_clock();
	if (CC_ALG == OCC && rc == RCOK) rc = occ_man.validate(this);
	if(CC_ALG == BOCC && rc == RCOK) rc = bocc_man.validate(this);
	if(CC_ALG == FOCC && rc == RCOK) rc = focc_man.validate(this);
	if(CC_ALG == SSI && rc == RCOK) rc = ssi_man.validate(this);
	if(CC_ALG == WSI && rc == RCOK) rc = wsi_man.validate(this);
	if(CC_ALG == CICADA && rc == RCOK) rc = cicada_man.validate(this);
	if(CC_ALG == MAAT  && rc == RCOK) {
		rc = maat_man.validate(this);
		// Note: home node must be last to validate
		if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
			rc = maat_man.find_bound(this);
		}
	}
#if CC_ALG == RDMA_MAAT

	if(CC_ALG == RDMA_MAAT && rc == RCOK) {
		rc = rmaat_man.validate(yield, this, cor_id);
		if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
			rc = rmaat_man.find_bound(this);
		}
	}
#endif
	if(CC_ALG == TICTOC  && rc == RCOK) {
		rc = tictoc_man.validate(this);
		// Note: home node must be last to validate
		// if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
		//   rc = tictoc_man.find_bound(this);
		// }
	}
	if ((CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 ||
			 CC_ALG == DLI_MVCC) &&
			rc == RCOK) {
		rc = dli_man.validate(this);
		if (IS_LOCAL(get_txn_id()) && rc == RCOK) {
#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
			rc = dli_man.find_bound(this);
#else
			set_commit_timestamp(glob_manager.get_ts(get_thd_id()));
#endif
		}
	}
	if(CC_ALG == WOOKONG  && rc == RCOK) {
		rc = wkdb_man.validate(this);
		// Note: home node must be last to validate
		if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
			rc = wkdb_man.find_bound(this);
		}
	}
	if ((CC_ALG == DTA) && rc == RCOK) {
		rc = dta_man.validate(this);
		// Note: home node must be last to validate
		if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
				rc = dta_man.find_bound(this);
		}
	}
#if CC_ALG == SILO
  if(CC_ALG == SILO && rc == RCOK) {
    rc = validate_silo();
	//rc = RCOK;
    if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
      _cur_tid ++;
      commit_timestamp = _cur_tid;
      DEBUG("Validate success: %ld, cts: %ld \n", get_txn_id(), commit_timestamp);
    }
  }
#endif
#if CC_ALG == RDMA_SILO
  if(CC_ALG == RDMA_SILO && rc == RCOK) {
    rc = rsilo_man.validate_rdma_silo(yield, this, cor_id);
	//rc = RCOK;
    if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
      _cur_tid ++;
      commit_timestamp = _cur_tid;
      DEBUG("Validate success: %ld, cts: %ld \n", get_txn_id(), commit_timestamp);
    }
  }
#endif

#if CC_ALG == RDMA_MVCC
    //rc = rmvcc_man.lock_row(this);
    // if(CC_ALG == RDMA_MVCC && rc == RCOK){
    //      rc = rmvcc_man.validate_local(this);
    // }
	//validate done in read-write phase
	return RCOK;
#endif

#if CC_ALG == RDMA_CICADA
  if(CC_ALG == RDMA_CICADA && rc == RCOK) {
	  rc = rcicada_man.validate(yield, this, cor_id); 
  }
#endif
	INC_STATS(get_thd_id(),txn_validate_time,get_sys_clock() - starttime);
	INC_STATS(get_thd_id(),trans_validate_time,get_sys_clock() - starttime);
    INC_STATS(get_thd_id(),trans_validate_count, 1);
	return rc;
}

RC TxnManager::send_remote_reads() {
	assert(CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN);
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
#if USE_DBPAOR == true
row_t * TxnManager::cas_and_read_remote(yield_func_t &yield, uint64_t& try_lock, uint64_t target_server, uint64_t cas_offset, uint64_t read_offset, uint64_t compare, uint64_t swap, uint64_t cor_id){
	uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
	uint64_t *local_buf1 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	char *local_buf2 = Rdma::get_row_client_memory(thd_id,2);
	uint64_t read_size = row_t::get_row_size(ROW_DEFAULT_SIZE);

	uint64_t starttime = get_sys_clock(), endtime;

	DBrequests dbreq(2);
	dbreq.init();
	dbreq.set_atomic_meta(0,compare,swap,local_buf1,(uint64_t)(remote_mr_attr[target_server].buf + cas_offset));
	dbreq.set_rdma_meta(1, IBV_WR_RDMA_READ, read_size, local_buf2, (uint64_t)(remote_mr_attr[target_server].buf + read_offset));
	auto dbres = dbreq.post_reqs(rc_qp[target_server][thd_id]);

	//only one signaled request need to be polled
	RDMA_ASSERT(dbres == IOCode::Ok);
    //to do: add coroutine
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> dbres1;
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
		dbres1 = rc_qp[target_server][get_thd_id() + cor_id * g_thread_cnt]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (dbres1.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
	// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
#else
	auto dbres1 = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(dbres1 == IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif
	try_lock = *local_buf1;
	row_t *test_row = (row_t *)mem_allocator.alloc(read_size);
    memcpy(test_row, local_buf2, read_size);
    return test_row;
}
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_SILO || CC_ALG == RDMA_TS1 || CC_ALG == RDMA_MVCC || CC_ALG == RDMA_CICADA || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
void TxnManager::batch_unlock_remote(yield_func_t &yield, uint64_t cor_id, int loc, RC rc, TxnManager * txnMng , vector<vector<uint64_t>> remote_index_origin, ts_t time,vector<vector<uint64_t>> remote_num){
	vector<uint64_t> remote_access_noorder = remote_index_origin[loc];
	//when USE_OR==true, use count to avoid overlap write
	int count = 0;
	if(loc>=1){
		for(int i=0;i<loc;i++){
			count += remote_index_origin[i].size();
		}
	}
    DBrequests dbreq(remote_access_noorder.size());   
    uint64_t thd_id = txnMng->get_thd_id()  + cor_id * g_thread_cnt;
    dbreq.init(); 
    vector<uint64_t> remote_need_cas,remote_access;
    for(int i=0;i<remote_access_noorder.size();i++){
        Access *access = txnMng->txn->accesses[remote_access_noorder[i]];
        if(access->type == WR){
            remote_access.push_back(remote_access_noorder[i]);
        }
        else remote_access.insert(remote_access.begin(),remote_access_noorder[i]);
    } //sort: read before write
    for(int i=0; i<remote_access.size();i++){
        Access *access = txnMng->txn->accesses[remote_access[i]];
        uint64_t off = access->offset;
        uint64_t operate_size;
        if(access->type == WR){
            row_t *data = access->data;
            if(rc != Abort) operate_size = row_t::get_row_size(data->tuple_size);
            else operate_size = sizeof(uint64_t);
            char *local_buf = Rdma::get_row_client_memory(thd_id,count+i+1);
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_SILO || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
            data->_tid_word = 0; //write data and unlock
#if CC_ALG == RDMA_SILO
			data->timestamp = time;
#endif
#if CC_ALG == RDMA_WOUND_WAIT2
			data->lock_owner = 0;
#endif
            memcpy(local_buf, (char*)data, operate_size);
			uint64_t remote_off = (uint64_t)(remote_mr_attr[loc].buf + off);
#endif
#if CC_ALG == RDMA_TS1
			uint64_t *temp_tid = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
			*temp_tid = 0;
            memcpy(local_buf, (char*)(temp_tid), operate_size);
			mem_allocator.free(temp_tid, sizeof(uint64_t));
			uint64_t remote_off = (uint64_t)(remote_mr_attr[loc].buf + off + sizeof(uint64_t));
#endif 
#if CC_ALG == RDMA_MVCC
			uint64_t *temp_txn_id = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
			*temp_txn_id = 0;
            memcpy(local_buf, (char*)(temp_txn_id), operate_size);
			mem_allocator.free(temp_txn_id, sizeof(uint64_t));
        	int version = access->old_version_num % HIS_CHAIN_NUM ;//version be locked
			uint64_t remote_off = (uint64_t)(remote_mr_attr[loc].buf + off + 2*sizeof(uint64_t) + version*sizeof(uint64_t));
#endif
#if CC_ALG == RDMA_CICADA
			vector<uint64_t> remote_num_current = remote_num[loc];
			operate_size = sizeof(uint64_t);
			CicadaState *temp_state = (CicadaState *)mem_allocator.alloc(sizeof(CicadaState));
			if(rc == Abort) *temp_state = Cicada_ABORTED;
			else *temp_state = Cicada_COMMITTED;
            memcpy(local_buf, (char*)(temp_state), operate_size);
			mem_allocator.free(temp_state, sizeof(uint64_t));
			uint64_t num = remote_num_current[i];
			uint64_t remote_off = (uint64_t)(remote_mr_attr[loc].buf + off + sizeof(uint64_t) * 4 + sizeof(RdmaCicadaVersion) * (num % HIS_CHAIN_NUM));
#endif
            dbreq.set_rdma_meta(i,IBV_WR_RDMA_WRITE,operate_size,local_buf,remote_off);
        }
        else{
            operate_size = sizeof(uint64_t);
            uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id,count+i+1);            
#if CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
            *local_buf = 0;
            dbreq.set_rdma_meta(i,IBV_WR_RDMA_WRITE,operate_size,(char*)local_buf,(uint64_t)(remote_mr_attr[loc].buf + off));
#endif
#if CC_ALG == RDMA_NO_WAIT
            remote_need_cas.push_back(remote_access[i]);
            dbreq.set_rdma_meta(i,IBV_WR_RDMA_READ,operate_size,(char*)local_buf,(uint64_t)(remote_mr_attr[loc].buf + off));            
#endif
        }
    }
    auto dbres = dbreq.post_reqs(rc_qp[loc][thd_id]);

	//only one signaled request need to be polled
	RDMA_ASSERT(dbres == IOCode::Ok);
//not use outstanding requests here for RDMA_NO_WAIT
#if CC_ALG == RDMA_NO_WAIT  //otherwise USE_OR is always true
    auto dbres1 = rc_qp[loc][thd_id]->wait_one_comp();
    RDMA_ASSERT(dbres1 == IOCode::Ok);       
#endif

#if CC_ALG == RDMA_NO_WAIT
    vector<uint64_t> orig_lock_info;
    for(int i = 0;i<remote_need_cas.size();i++){
        uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id,count+i+1);            
        orig_lock_info.push_back(*local_buf);
    }
    while(remote_need_cas.size()>0){
        DBrequests dbreq(remote_need_cas.size());   
        dbreq.init(); 
        for(int i=0;i<remote_need_cas.size();i++){
            Access *access = txnMng->txn->accesses[remote_need_cas[i]];
            uint64_t off = access->offset;
            uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id,count+i+1);            
            uint64_t lock_info = orig_lock_info[i];
            uint64_t new_lock_info,lock_type,lock_num;
            Row_rdma_2pl::info_decode(lock_info,lock_type,lock_num);
            Row_rdma_2pl::info_encode(new_lock_info,lock_type,lock_num-1);
            assert((lock_type == 0)&&(lock_num > 0)); //must have at least 1 read lock
            dbreq.set_atomic_meta(i,lock_info,new_lock_info,local_buf,remote_mr_attr[loc].buf + off);            
        }
        auto dbres = dbreq.post_reqs(rc_qp[loc][thd_id]);

        //only one signaled request need to be polled
        RDMA_ASSERT(dbres == IOCode::Ok);
        auto dbres1 = rc_qp[loc][thd_id]->wait_one_comp();
        RDMA_ASSERT(dbres1 == IOCode::Ok);
        
        vector<uint64_t> tmp;
        vector<uint64_t> tmp_lock_info;
        for(int i=0;i<remote_need_cas.size();i++){
            uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id,count+i+1);            
            if(*local_buf != orig_lock_info[i]){ //CAS fail, atomicity violated
                txnMng->num_atomic_retry++;
                total_num_atomic_retry++;
                if(txnMng->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txnMng->num_atomic_retry;
                tmp_lock_info.push_back(*local_buf);
                tmp.push_back(remote_need_cas[i]);
            }        
        }
        remote_need_cas = tmp;
        orig_lock_info = tmp_lock_info;
    }
#endif
}
#endif
#endif

#if BATCH_INDEX_AND_READ
 void TxnManager::batch_read(yield_func_t &yield, BatchReadType rtype,int loc, vector<vector<uint64_t>> remote_index_origin, uint64_t cor_id){
	 vector<uint64_t> remote_index = remote_index_origin[loc];
	 int count = 0;
	 if(loc>=1){
		for(int i=0;i<loc;i++){
			count += remote_index_origin[i].size();
		}
	 }
	 
	 DBrequests dbreq(remote_index.size());
	 dbreq.init();
	 uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
	 YCSBQuery* ycsb_query = (YCSBQuery*) query;
	 for(int i=0;i<remote_index.size();i++){
		 if(rtype == R_INDEX){
			ycsb_request * req = ycsb_query->requests[remote_index[i]];
			uint64_t index_key = req->key / g_node_cnt;
			uint64_t index_addr = (index_key) * sizeof(IndexInfo);
			char *local_buf = Rdma::get_index_client_memory(thd_id,count+i+1);
			dbreq.set_rdma_meta(i,IBV_WR_RDMA_READ,sizeof(IndexInfo),local_buf,(uint64_t)(remote_mr_attr[loc].buf + index_addr));
		 }
		 else if(rtype == R_ROW){
			uint64_t read_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
			itemid_t * m_item = reqId_index.find(remote_index[i])->second;
			char *local_buf = Rdma::get_row_client_memory(thd_id,count+i+1);
			dbreq.set_rdma_meta(i,IBV_WR_RDMA_READ,read_size,local_buf,(uint64_t)(remote_mr_attr[loc].buf + m_item->offset));
		 }
	 }
	 auto dbres = dbreq.post_reqs(rc_qp[loc][thd_id]);

 	 //only one signaled request need to be polled
	 RDMA_ASSERT(dbres == IOCode::Ok);
#if 0 //USE_OR is always true
	//  auto dbres1 = rc_qp[loc][thd_id]->wait_one_comp();
	//  RDMA_ASSERT(dbres1 == IOCode::Ok);
	 
	//  for(int i=0;i<remote_index.size();i++){
	// 	if(rtype == R_INDEX){
	// 		char *local_buf = Rdma::get_index_client_memory(thd_id,count+i+1);
	// 		ycsb_request * req = ycsb_query->requests[remote_index[i]];
	// //		uint64_t index_key = req->key / g_node_cnt;
	// //		uint64_t index_addr = (index_key) * sizeof(IndexInfo);
	// 		assert(((IndexInfo*)local_buf)->key == req->key);
	// 		itemid_t* item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));

	// 		item->location = ((IndexInfo*)local_buf)->address;
	// 		item->type = ((IndexInfo*)local_buf)->type;
	// 		item->valid = ((IndexInfo*)local_buf)->valid;
	// 		item->offset = ((IndexInfo*)local_buf)->offset;
	// 		item->table_offset = ((IndexInfo*)local_buf)->table_offset;
	// 		reqId_index.insert(pair<int, itemid_t*>(remote_index[i],item));
	// 	}
	// 	else if(rtype == R_ROW){
	// 		uint64_t read_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
	// 		char *local_buf = Rdma::get_row_client_memory(thd_id,count+i+1);
	// 		row_t *test_row = (row_t *)mem_allocator.alloc(read_size);
    // 		memcpy(test_row, local_buf, read_size);
	// 		reqId_row.insert(pair<int, row_t*>(remote_index[i],test_row));
	// 	}
	//  }
#endif
 }
 void TxnManager::get_batch_read(yield_func_t &yield, BatchReadType rtype,int loc, vector<vector<uint64_t>> remote_index_origin, uint64_t cor_id){
	 int count = 0;
	 if(loc>=1){
		for(int i=0;i<loc;i++){
			count += remote_index_origin[i].size();
		}
	 }
	 uint64_t starttime = get_sys_clock(), endtime;
	 vector<uint64_t> remote_index = remote_index_origin[loc];
	 uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
	 YCSBQuery* ycsb_query = (YCSBQuery*) query;
     //to do: add coroutine
	 INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> dbres1;
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
		dbres1 = rc_qp[loc][get_thd_id() + cor_id * g_thread_cnt]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (dbres1.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
	// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
#else
	auto dbres1 = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(dbres1 == IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif
	 for(int i=0;i<remote_index.size();i++){
		if(rtype == R_INDEX){
			char *local_buf = Rdma::get_index_client_memory(thd_id,count+i+1);
			ycsb_request * req = ycsb_query->requests[remote_index[i]];
	//		uint64_t index_key = req->key / g_node_cnt;
	//		uint64_t index_addr = (index_key) * sizeof(IndexInfo);
			assert(((IndexInfo*)local_buf)->key == req->key);
			itemid_t* item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));

			item->location = ((IndexInfo*)local_buf)->address;
			item->type = ((IndexInfo*)local_buf)->type;
			item->valid = ((IndexInfo*)local_buf)->valid;
			item->offset = ((IndexInfo*)local_buf)->offset;
			item->table_offset = ((IndexInfo*)local_buf)->table_offset;
			reqId_index.insert(pair<int, itemid_t*>(remote_index[i],item));
		}
		else if(rtype == R_ROW){
			uint64_t read_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
			char *local_buf = Rdma::get_row_client_memory(thd_id,count+i+1);
			row_t *test_row = (row_t *)mem_allocator.alloc(read_size);
    		memcpy(test_row, local_buf, read_size);
			reqId_row.insert(pair<int, row_t*>(remote_index[i],test_row));
		}
	 }
 
 }
#endif

 itemid_t * TxnManager::read_remote_index(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t key, uint64_t cor_id){
    uint64_t operate_size = sizeof(IndexInfo);
    uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_index_client_memory(thd_id);

	read_remote_content(yield, target_server, remote_offset, operate_size, local_buf, cor_id);

    itemid_t* item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
	assert(((IndexInfo*)local_buf)->key == key);
	item->location = ((IndexInfo*)local_buf)->address;
	item->type = ((IndexInfo*)local_buf)->type;
	item->valid = ((IndexInfo*)local_buf)->valid;
	item->offset = ((IndexInfo*)local_buf)->offset;
  	item->table_offset = ((IndexInfo*)local_buf)->table_offset;

    return item;
}

row_t * TxnManager::read_remote_row(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset, uint64_t cor_id){
    uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
    uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_row_client_memory(thd_id);
   
	read_remote_content(yield, target_server, remote_offset, operate_size, local_buf, cor_id);

    row_t *test_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
    memcpy(test_row, local_buf, operate_size);
    return test_row;
}

#if USE_REPLICA
uint64_t TxnManager::read_remote_log_head(yield_func_t &yield, uint64_t target_server, uint64_t cor_id){
    uint64_t operate_size = sizeof(uint64_t);
    uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_row_client_memory(thd_id);
	uint64_t remote_offset = rdma_buffer_size-rdma_log_size;

	read_remote_content(yield, target_server, remote_offset, operate_size, local_buf, cor_id);

	return *((uint64_t *)local_buf);
}
char* TxnManager::read_remote_log(yield_func_t &yield, uint64_t target_server, uint64_t remote_offset, uint64_t cor_id){
    uint64_t operate_size = sizeof(LogEntry);
    uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_log_client_memory(thd_id);
	
	read_remote_content(yield, target_server, remote_offset, operate_size, local_buf, cor_id);
	
	return local_buf;
}
#endif

void TxnManager::read_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset, uint64_t operate_size, char* local_buf, uint64_t cor_id){
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
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1); //include index, row, log,...etc read.
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif

}

 bool TxnManager::write_remote_index(yield_func_t &yield,uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content,uint64_t cor_id){
	uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_index_client_memory(thd_id);
	return write_remote_content(yield, target_server, operate_size,remote_offset,write_content,local_buf,cor_id);

 }

 bool TxnManager::write_remote_row(yield_func_t &yield, uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content,uint64_t cor_id){
	uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
	char *local_buf = Rdma::get_row_client_memory(thd_id);
	return write_remote_content(yield, target_server, operate_size,remote_offset,write_content,local_buf,cor_id);
 }

#if USE_REPLICA
 bool TxnManager::write_remote_log(yield_func_t &yield,uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content,uint64_t cor_id){
	uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_log_client_memory(thd_id);
	return write_remote_content(yield, target_server, operate_size,remote_offset,write_content,local_buf,cor_id);
 }
#endif

 bool TxnManager::write_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content, char* local_buf,uint64_t cor_id){
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
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
#endif
    return true;
}

//the value prior to being incremented is returned to the caller.
uint64_t TxnManager::faa_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset, uint64_t value_to_add, uint64_t cor_id){
    
    rdmaio::qp::Op<> op;
	uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
    auto mr = client_rm_handler->get_reg_attr().value();
    
    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[target_server].buf + remote_offset), remote_mr_attr[target_server].key).set_fetch_add(value_to_add);
    assert(op.set_payload(local_buf, sizeof(uint64_t), mr.key) == true);
    auto res_s2 = op.execute(rc_qp[target_server][thd_id], IBV_SEND_SIGNALED);

    RDMA_ASSERT(res_s2 == IOCode::Ok);
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
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
#endif

    return *local_buf;
}

uint64_t TxnManager::cas_remote_content(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value, uint64_t cor_id){
    
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
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
#endif

    return *local_buf;
}

bool TxnManager::loop_cas_remote(yield_func_t &yield,uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value, uint64_t cor_id){
    uint64_t cas_result = -1;
    do{
        cas_result = cas_remote_content(yield,target_server,remote_offset,old_value,new_value,cor_id);
    }
    while(cas_result != old_value && !simulation->is_done());

    return true;
}

#if USE_REPLICA
RC TxnManager::redo_log(yield_func_t &yield,RC status, uint64_t cor_id) {
	if(CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_NO_WAIT) assert(status == RCOK);
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	RC rc = RCOK;
	ts_t ts = get_sys_clock(); //for RDMA_SILO, which is problematic

	//for every node
	int change_cnt[g_node_cnt] = {0};
	vector<vector<ChangeInfo>> change(g_node_cnt);
	

	//construct log 
	for(int i=0;i<((YCSBQuery*)query)->requests.size();i++){
		ycsb_request * req = ycsb_query->requests[i];
		if(req->acctype!=WR) continue; //log is only for write operation
		// key_to_part should be used here! but its only for YCSB workload, so manually set part_id
		// this part should be considered and changed for other workload!
		// uint64_t part_id = _wl->key_to_part( req->key );
		uint64_t part_id = req->key % g_part_cnt;
		vector<uint64_t> node_id;
		if(status == RCOK){ //validate success, log all replicas
			node_id.push_back(GET_FOLLOWER1_NODE(part_id));
			node_id.push_back(GET_FOLLOWER2_NODE(part_id));
			node_id.push_back(GET_NODE_ID(part_id));
		}
		else if(status == Abort){ //validate fail, only log the primary replicas that have been locked
#if CC_ALG == RDMA_SILO
			int sum = 0;
			for(int i=0;i<g_node_cnt;i++) sum += change_cnt[i];
			if(sum>=num_locks) break;
			node_id.push_back(GET_NODE_ID(part_id));	
#elif CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_NO_WAIT
			// int sum = 0;
			// for(int i=0;i<g_node_cnt;i++) sum += change_cnt[i];
			// if(sum>=num_locks) break;
			// node_id.push_back(GET_NODE_ID(part_id));				
#endif
		}else{
			assert(false);
		}

		for(int i=0;i<node_id.size();i++){
			uint32_t center_id = GET_CENTER_ID(node_id[i]);
			if(center_id != g_center_id) continue; //log is only for row in the same center
			++change_cnt[node_id[i]];
			ChangeInfo newChange;
			
			//fill in ChangeInfo here
			row_t* temp_row = (row_t *) mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
			//for simulation purpose, only write back metadata here
			//in actual application, data in req should also be written back
			temp_row->_tid_word = 0;
#if CC_ALG == RDMA_SILO
			temp_row->timestamp = ts;
			uint64_t op_size = sizeof(temp_row->_tid_word)+sizeof(temp_row->timestamp);
			newChange.set_change_info(req->key,op_size,(char *)temp_row); //local 
#elif CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_NO_WAIT
			uint64_t op_size = sizeof(temp_row->_tid_word);
			bool is_primary = (node_id[i] == GET_NODE_ID(part_id));
			newChange.set_change_info(req->key,op_size,(char *)temp_row,is_primary); //local 
#endif
			mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));

			change[node_id[i]].push_back(newChange);
		}
	}
	
	//write log
	for(int i=0;i<g_node_cnt&&rc==RCOK;i++){
		if(change_cnt[i]>0){
			int num_of_entry = 1;
			//use RDMA_FAA for local and remote
			uint64_t start_idx = faa_remote_content(yield, i, rdma_buffer_size-rdma_log_size+sizeof(uint64_t), num_of_entry, cor_id);

			LogEntry* newEntry = (LogEntry*)mem_allocator.alloc(sizeof(LogEntry));

			newEntry->set_entry(change_cnt[i],change[i],get_start_timestamp());


			if(i == g_node_id){ //local log
				//consider possible overwritten:if no space, wait until cleaned 
				//for head: there is local read and write, and remote RDMA read, conflicts are ignored here, which might be problematic
				// while(start_idx + num_of_entry < *(redo_log_buf.get_head())){
				// 	//wait for head to reset
				// 	assert(false);
				// }
				while((start_idx + num_of_entry - *(redo_log_buf.get_head()) >= redo_log_buf.get_size()) && !simulation->is_done()){
					//wait for AsyncRedoThread to clean buffer
				}
				if(simulation->is_done()) break;
				
				start_idx = start_idx % redo_log_buf.get_size();

				char* start_addr = (char*)redo_log_buf.get_entry(start_idx);

				assert(((LogEntry *)start_addr)->state == EMPTY);					
				assert(((LogEntry *)start_addr)->change_cnt == 0);					

				memcpy(start_addr, (char *)newEntry, sizeof(LogEntry));
				assert(((LogEntry *)start_addr)->state == LOGGED);						
			}else{ //remote log
				//consider possible overwritten: if no space, wait until cleaned 
				//first prevent concurrent read and write among threads
				pthread_mutex_lock( LOG_HEAD_LATCH[i] );
				// while(start_idx + num_of_entry < log_head[i]){
				// 	//wait for head to reset
				// 	assert(false);
				// 	log_head[i] = read_remote_log_head(yield, i, cor_id);					
				// }
				while(start_idx + num_of_entry - log_head[i] >= redo_log_buf.get_size() && !simulation->is_done()){
					//wait for AsyncRedoThread to clean buffer
					log_head[i] = read_remote_log_head(yield, i, cor_id);
				}
				pthread_mutex_unlock( LOG_HEAD_LATCH[i] );
				if(simulation->is_done()) break;
				
				start_idx = start_idx % redo_log_buf.get_size();

				uint64_t start_offset = redo_log_buf.get_entry_offset(start_idx);
				
				// //for debug purpose
				// LogEntry* le= (LogEntry*)read_remote_log(yield,i,start_offset,cor_id);
				// assert(le->state == EMPTY);
				// assert(le->change_cnt == 0);

				write_remote_log(yield, i, sizeof(LogEntry), start_offset, (char *)newEntry, cor_id);
				//for debug purpose
				// LogEntry* le= (LogEntry*)read_remote_log(yield,i,start_offset,cor_id);
				// assert(le->state == LOGGED);

			}
			log_idx[i] = start_idx;
			mem_allocator.free(newEntry, sizeof(LogEntry));
		}
	}
	return rc;
}
#endif


row_t * TxnManager::read_remote_row(uint64_t target_server,uint64_t remote_offset){
    uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
    uint64_t thd_id = get_thd_id();
    char *local_buf = Rdma::get_row_client_memory(thd_id);

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
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);

    row_t *test_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
    memcpy(test_row, local_buf, operate_size);

    return test_row;
}

RdmaTxnTableNode * TxnManager::read_remote_timetable(uint64_t target_server,uint64_t remote_offset){
	uint64_t operate_size = sizeof(RdmaTxnTableNode);

    uint64_t thd_id = get_thd_id();
    char *local_buf = Rdma::get_row_client_memory(thd_id);

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
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);

    RdmaTxnTableNode *test_row = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
    memcpy(test_row, local_buf, operate_size);

    return test_row;
}

RdmaTxnTableNode * TxnManager::read_remote_timetable(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t cor_id){
	uint64_t operate_size = sizeof(RdmaTxnTableNode);

    uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_row_client_memory(thd_id);

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
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
#endif

    RdmaTxnTableNode *test_row = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
    memcpy(test_row, local_buf, operate_size);

    return test_row;
}

char * TxnManager::read_remote_txntable(yield_func_t &yield, uint64_t target_server,uint64_t remote_offset,uint64_t cor_id){
	uint64_t operate_size = sizeof(RdmaTxnTableNode);

    uint64_t thd_id = get_thd_id() + cor_id * g_thread_cnt;
    char *local_buf = Rdma::get_row_client_memory(thd_id);

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
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
#endif

    // RdmaTxnTableNode *test_row = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
    // memcpy(test_row, local_buf, operate_size);

    return local_buf;
}
itemid_t * TxnManager::read_remote_index(uint64_t target_server,uint64_t remote_offset,uint64_t key){
    uint64_t operate_size = sizeof(IndexInfo);
    uint64_t thd_id = get_thd_id();
    char *test_buf = Rdma::get_index_client_memory(thd_id);
    memset(test_buf, 0, operate_size);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);

    itemid_t* item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
	assert(((IndexInfo*)test_buf)->key == key);

	item->location = ((IndexInfo*)test_buf)->address;
	item->type = ((IndexInfo*)test_buf)->type;
	item->valid = ((IndexInfo*)test_buf)->valid;
	item->offset = ((IndexInfo*)test_buf)->offset;
  	item->table_offset = ((IndexInfo*)test_buf)->table_offset;

    return item;
}

bool TxnManager::write_remote_row(uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content){
    uint64_t thd_id = get_thd_id();

    char *local_buf = Rdma::get_row_client_memory(thd_id);
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
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();
    INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);

    return true;
}

bool TxnManager::write_remote_index(uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *write_content){
    uint64_t thd_id = get_thd_id();

    char *local_buf = Rdma::get_index_client_memory(thd_id);
    ::memset(local_buf, 0, operate_size);
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
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);

    return true;
}

bool TxnManager::write_unlock_remote_content(uint64_t target_server,uint64_t operate_size,uint64_t remote_offset,char *local_buf){
    
}

#if CC_ALG == RDMA_MVCC
bool TxnManager::get_version(row_t * temp_row,uint64_t * change_num,Transaction *txn){
    bool result = false;
    uint64_t check_num = temp_row->version_num < HIS_CHAIN_NUM ?  temp_row->version_num : HIS_CHAIN_NUM;
               
    uint64_t k = 0;
    if(temp_row->version_num < HIS_CHAIN_NUM){//find txn matches the version
        for(k = 0;k <= check_num ; k++){
            if((temp_row->start_ts[k] < txn->timestamp || temp_row->start_ts[k] == 0)&& (temp_row->end_ts[k] > txn->timestamp || temp_row->end_ts[k] == UINT64_MAX)){
                result = true;
                *change_num = k;
                break;
            }
        }
    }else{
        for( k = 0 ; k < HIS_CHAIN_NUM ; k++){
            uint64_t j = 0;
            j = (temp_row->version_num + k)%HIS_CHAIN_NUM;
            if((temp_row->start_ts[j] < txn->timestamp || temp_row->start_ts[j] == 0) && (temp_row->end_ts[j] > txn->timestamp || temp_row->end_ts[k] == UINT64_MAX)){
                result = true;
                *change_num = j;
                break;
            }
        }
    }

    return result;
}
#endif

uint64_t TxnManager::cas_remote_content(uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value ){
    
    rdmaio::qp::Op<> op;
    uint64_t thd_id = get_thd_id();
    uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
    auto mr = client_rm_handler->get_reg_attr().value();

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[target_server].buf + remote_offset), remote_mr_attr[target_server].key).set_cas(old_value, new_value);
    assert(op.set_payload(local_buf, sizeof(uint64_t), mr.key) == true);
    auto res_s2 = op.execute(rc_qp[target_server][thd_id], IBV_SEND_SIGNALED);

    INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
    RDMA_ASSERT(res_s2 == IOCode::Ok);
    auto res_p2 = rc_qp[target_server][thd_id]->wait_one_comp();
    RDMA_ASSERT(res_p2 == IOCode::Ok);

    endtime = get_sys_clock();
	INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
	INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
	DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);

    return *local_buf;
}

bool TxnManager::loop_cas_remote(uint64_t target_server,uint64_t remote_offset,uint64_t old_value,uint64_t new_value){
    uint64_t cas_result = -1;
    do{
        cas_result = cas_remote_content(target_server,remote_offset,old_value,new_value);
    }
    while(cas_result != old_value && !simulation->is_done());

    return true;
}

RC TxnManager::preserve_access(row_t *&row_local,itemid_t* m_item,row_t *test_row,access_t type,uint64_t key,uint64_t loc){
    Access * access = NULL;
	access_pool.get(get_thd_id(),access);

	this->last_row = test_row;
    this->last_type = type;

    RC rc = RCOK;
	rc = test_row->remote_copy_row(test_row, this, access);
    assert(test_row->get_primary_key() == access->data->get_primary_key());
	// printf("preserve_access %s %s\n", test_row->table_name, access->data->table_name);
    if (rc == Abort || rc == WAIT) {
        DEBUG_M("TxnManager::get_row(abort) access free\n");
        access_pool.put(get_thd_id(),access);
        return rc;
    }

    access->type = type;

#if CC_ALG == RDMA_SILO
    access->orig_row = test_row;
	access->tid = last_tid;
	access->timestamp = test_row->timestamp;
    access->key = test_row->get_primary_key();
    access->location = loc;
	access->offset = m_item->offset;	
#endif

#if CC_ALG == RDMA_MVCC
    access->orig_row = test_row;
    access->old_version_num = test_row->version_num;//record the locked version by txn_id when write
    access->key = test_row->get_primary_key();
    access->location =loc;
	access->offset = m_item->offset;
#endif

#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_TS1 || CC_ALG == RDMA_CNULL || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT
  	access->orig_row = test_row;
	access->location = loc;
	access->offset = m_item->offset;
#endif

#if CC_ALG == RDMA_MAAT || CC_ALG == RDMA_CICADA
	access->orig_row = test_row;
	access->key = key;
	access->location = loc;
	access->offset = m_item->offset;
#endif

    row_local = access->data;
    ++txn->row_cnt;

    mem_allocator.free(m_item,0);

    if (type == WR) ++txn->write_cnt;//this->last_type = WR
    txn->accesses.add(access);

    return rc;
}

