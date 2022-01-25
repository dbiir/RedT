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

#include "row.h"
#include "txn.h"
#include "row_rdma_maat.h"
#include "mem_alloc.h"
#include "manager.h"
#include "helper.h"
#include "rdma_maat.h"
#include "qps/op.hh"
#include "rdma.h"
#include "routine.h"
#if CC_ALG == RDMA_MAAT
void Row_rdma_maat::init(row_t * row) {
	_row = row;

	maat_avail = true;
	

	//uncommitted_writes = new std::set<uint64_t>();
	//uncommitted_reads = new std::set<uint64_t>();
	//assert(uncommitted_writes->begin() == uncommitted_writes->end());
	//assert(uncommitted_writes->size() == 0);

}

bool Row_rdma_maat::local_cas_lock(TxnManager * txnMng , uint64_t info, uint64_t new_info){
   // INC_STATS(txnMng->get_thd_id(), cas_cnt, 1);
	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    uint64_t loc = g_node_id;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto mr = client_rm_handler->get_reg_attr().value();

	rdmaio::qp::Op<> op;
	op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + (char*)_row - rdma_global_buffer), remote_mr_attr[loc].key).set_cas(info,new_info);
	assert(op.set_payload(tmp_buf2, sizeof(uint64_t), mr.key) == true);
	auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

	RDMA_ASSERT(res_s2 == IOCode::Ok);
	auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p2 == IOCode::Ok);
	INC_STATS(txnMng->get_thd_id(), worker_oneside_cnt, 1);
	endtime = get_sys_clock();
	// INC_STATS(txnMng->get_thd_id(), rdma_cas_time, endtime-starttime);
	// INC_STATS(txnMng->get_thd_id(), rdma_cas_cnt, 1);
	INC_STATS(txnMng->get_thd_id(), worker_idle_time, endtime-starttime);
	DEL_STATS(txnMng->get_thd_id(), worker_process_time, endtime-starttime);
	INC_STATS(txnMng->get_thd_id(), worker_waitcomp_time, endtime-starttime);
    if(*tmp_buf2 != info) return false;
    return true;
  // return 0;
}

RC Row_rdma_maat::access(access_t type, TxnManager * txn) {
	uint64_t starttime = get_sys_clock();
#if WORKLOAD == TPCC
	read_and_prewrite(txn);
#else
	if (type == RD) read(txn);
	if (type == WR) prewrite(txn);
#endif
	uint64_t timespan = get_sys_clock() - starttime;
	txn->txn_stats.cc_time += timespan;
	txn->txn_stats.cc_time_short += timespan;
	return RCOK;
}

RC Row_rdma_maat::read_and_prewrite(TxnManager * txn) {
	assert (CC_ALG == RDMA_MAAT);
	RC rc = RCOK;

	uint64_t mtx_wait_starttime = get_sys_clock();
	if(WORKLOAD == TPCC &&(_row->ucreads_len >= row_set_length - 1 || _row->ucwrites_len >= row_set_length - 1)) {
		// printf("txn %lu abort due to 91\n", txn->get_txn_id());
        return Abort;
    }
#ifdef USE_CAS
	//while (!ATOM_CAS(_row->_tid_word, 0, 1)) {
    if(!local_cas_lock(txn, 0, txn->get_txn_id() + 1)){
		// printf("txn %lu abort due to 96\n", txn->get_txn_id());
		return Abort;
	}
#endif
	INC_STATS(txn->get_thd_id(),mtx[30],get_sys_clock() - mtx_wait_starttime);
	INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - mtx_wait_starttime);
	DEBUG("READ + PREWRITE %ld -- %ld: lw %ld\n", txn->get_txn_id(), _row->get_primary_key(),
			_row->timestamp_last_write);
    
	// Copy uncommitted writes
	for(uint64_t i = 0, j = 0; i < row_set_length && j < _row->ucwrites_len; i++) {
		// assert(i <= row_set_length - 1);
		if(_row->uncommitted_writes[i] == 0 || _row->uncommitted_writes[i] == txn->get_txn_id()) {
			continue;
		} else {
			txn->uncommitted_writes.insert(_row->uncommitted_writes[i]);
			txn->uncommitted_writes_y.insert(_row->uncommitted_writes[i]);
			j++;
		}
		DEBUG("    UW %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),_row->uncommitted_writes[i]);
	}

	// Copy uncommitted reads
	for(auto i = 0, j = 0; i < row_set_length && j < _row->ucreads_len; i++) {
		if(_row->uncommitted_reads[i] == 0 || _row->uncommitted_reads[i] == txn->get_txn_id()) {
			continue;
		} else {
			txn->uncommitted_reads.insert(_row->uncommitted_reads[i]);
			j++;
		}
		DEBUG("    UR %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),_row->uncommitted_reads[i]);
	}

	// Copy read timestamp
	if(txn->greatest_read_timestamp < _row->timestamp_last_read)
		txn->greatest_read_timestamp = _row->timestamp_last_read;


	// Copy write timestamp
	if(txn->greatest_write_timestamp < _row->timestamp_last_write)
		txn->greatest_write_timestamp = _row->timestamp_last_write;

	//Add to uncommitted reads (soft lock)
	for(uint64_t i = 0; i < row_set_length; i++) {
		if(_row->uncommitted_reads[i] == txn->get_txn_id() || txn->unread_set.find(_row->get_primary_key()) != txn->unread_set.end()) {//!notice
			break;
		}
		if(_row->uncommitted_reads[i] == 0) {
            _row->ucreads_len += 1;
			txn->unread_set.insert(_row->get_primary_key());
			_row->uncommitted_reads[i] = txn->get_txn_id();
			break;
		}
	}
	//Add to uncommitted writes (soft lock)
	bool in_set = false;
	for(auto i = 0, j = 0; i < row_set_length && j < _row->ucwrites_len; i++) {
		if(_row->uncommitted_writes[i] == txn->get_txn_id() || txn->unwrite_set.find(_row->get_primary_key()) != txn->unwrite_set.end()) {
			in_set = true;//!notice
			continue;
		}
		if(_row->uncommitted_writes[i] == 0 && in_set == false) {
			_row->ucwrites_len += 1;
			txn->unwrite_set.insert(_row->get_primary_key());
			_row->uncommitted_writes[i] = txn->get_txn_id();
            in_set = true;
			j++;
			break;
		}
		if(_row->uncommitted_writes[i] != 0) {
			// txn->uncommitted_writes_y.insert(_row->uncommitted_writes[i]);
			j++;
		}
		DEBUG("    UW %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),_row->uncommitted_writes[i]);
	}
#ifdef USE_CAS
	//ATOM_CAS(_row->_tid_word,1,0);
	local_cas_lock(txn, txn->get_txn_id() + 1, 0);
#endif

	return rc;
}


RC Row_rdma_maat::read(TxnManager * txn) {
	assert (CC_ALG == RDMA_MAAT);
	RC rc = RCOK;

	uint64_t mtx_wait_starttime = get_sys_clock();
	if(_row->ucreads_len >= row_set_length - 1) {
		INC_STATS(txn->get_thd_id(), maat_case6_cnt, 1);
        return Abort;
    }
#ifdef USE_CAS
	//while (!ATOM_CAS(_row->_tid_word, 0, 1)) {
    if(!local_cas_lock(txn, 0, txn->get_txn_id() + 1)){
		return Abort;
	}
#endif
	INC_STATS(txn->get_thd_id(),mtx[30],get_sys_clock() - mtx_wait_starttime);
	INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - mtx_wait_starttime);
	DEBUG("READ %ld -- %ld: lw %ld\n", txn->get_txn_id(), _row->get_primary_key(),
			_row->timestamp_last_write);

	// Copy uncommitted writes
	for(uint64_t i = 0, j = 0; i < row_set_length && j < _row->ucwrites_len; i++) {
		// assert(i <= row_set_length - 1);
		if(_row->uncommitted_writes[i] == 0 || _row->uncommitted_writes[i] == txn->get_txn_id()) {
			continue;
		} else {
			txn->uncommitted_writes.insert(_row->uncommitted_writes[i]);
			j++;
		}
		DEBUG("    UW %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),_row->uncommitted_writes[i]);
	}

	// Copy write timestamp
	if(txn->greatest_write_timestamp < _row->timestamp_last_write)
		txn->greatest_write_timestamp = _row->timestamp_last_write;

	//Add to uncommitted reads (soft lock)
	for(uint64_t i = 0; i < row_set_length; i++) {
		if(_row->uncommitted_reads[i] == txn->get_txn_id() || txn->unread_set.find(_row->get_primary_key()) != txn->unread_set.end()) {
			break;
		}
		if(_row->uncommitted_reads[i] == 0) {
            _row->ucreads_len += 1;
			txn->unread_set.insert(_row->get_primary_key());
			_row->uncommitted_reads[i] = txn->get_txn_id();
			break;
		}
	}

#ifdef USE_CAS
	//ATOM_CAS(_row->_tid_word,1,0);
	// local_cas_lock(txn, txn->get_txn_id() + 1, 0);
	_row->_tid_word = 0;
#endif
	return rc;
}

RC Row_rdma_maat::prewrite(TxnManager * txn) {
	assert (CC_ALG == RDMA_MAAT);
	RC rc = RCOK;

	uint64_t mtx_wait_starttime = get_sys_clock();
	if(_row->ucwrites_len >= row_set_length - 1) {
		INC_STATS(txn->get_thd_id(), maat_case6_cnt, 1);
        return Abort;
    }
#ifdef USE_CAS
	//while (!ATOM_CAS(_row->_tid_word, 0, 1)) {
	if(!local_cas_lock(txn, 0, txn->get_txn_id()+1)){
		return Abort;
	}
#endif
	INC_STATS(txn->get_thd_id(),mtx[31],get_sys_clock() - mtx_wait_starttime);
	INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - mtx_wait_starttime);
	DEBUG("PREWRITE %ld -- %ld: lw %ld, lr %ld\n", txn->get_txn_id(), _row->get_primary_key(),
			_row->timestamp_last_write, _row->timestamp_last_read);

	// Copy uncommitted reads
	for(auto i = 0, j = 0; i < row_set_length && j < _row->ucreads_len; i++) {
		if(_row->uncommitted_reads[i] == 0 || _row->uncommitted_reads[i] == txn->get_txn_id()) {
			continue;
		} else {
			txn->uncommitted_reads.insert(_row->uncommitted_reads[i]);
			j++;
		}
		DEBUG("    UR %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),_row->uncommitted_reads[i]);
	}
	// Copy read timestamp
	if(txn->greatest_read_timestamp < _row->timestamp_last_read)
		txn->greatest_read_timestamp = _row->timestamp_last_read;

	// Copy write timestamp
	if(txn->greatest_write_timestamp < _row->timestamp_last_write)
		txn->greatest_write_timestamp = _row->timestamp_last_write;

	// Copy uncommitted writes
	bool in_set = false;
	for(auto i = 0, j = 0; i < row_set_length && j < _row->ucwrites_len; i++) {
		if(_row->uncommitted_writes[i] == txn->get_txn_id() || txn->unwrite_set.find(_row->get_primary_key()) != txn->unwrite_set.end()) {
			in_set = true;
			continue;
		}
		if(_row->uncommitted_writes[i] == 0 && in_set == false) {
			_row->ucwrites_len += 1;
			txn->unwrite_set.insert(_row->get_primary_key());
			_row->uncommitted_writes[i] = txn->get_txn_id();
            in_set = true;
			j++;
			break;
		}
		if(_row->uncommitted_writes[i] != 0) {
			txn->uncommitted_writes_y.insert(_row->uncommitted_writes[i]);
			j++;
		}
		DEBUG("    UW %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),_row->uncommitted_writes[i]);
	}
#ifdef USE_CAS
	//ATOM_CAS(_row->_tid_word,1,0);
	// local_cas_lock(txn, txn->get_txn_id() + 1, 0);
	_row->_tid_word = 0;
#endif
	return rc;
}

void Row_rdma_maat::ucread_erase(uint64_t txn_id) {
	
	for(uint64_t i = 0; i < row_set_length; i++) {
		uint64_t last = _row->uncommitted_reads[i];
		if(txn_id == 0) break;
		if(last == txn_id) {
            _row->ucreads_len -= 1;
			_row->uncommitted_reads[i] = 0;
			break;
		}
	}
    // printf("row %p clean txn %ld\n", _row, txn_id);
	
}
void Row_rdma_maat::ucwrite_erase(uint64_t txn_id) {
	
	for(uint64_t i = 0; i < row_set_length; i++) {
		// printf("%ld ", _row->uncommitted_writes[i]);
		uint64_t last = _row->uncommitted_writes[i];
		// assert(i <= row_set_length - 1);
		if(txn_id  == 0) break;
		if(last == txn_id) {
            _row->ucwrites_len -= 1;
			_row->uncommitted_writes[i] = 0;
			break;
		}
	}
}
RC Row_rdma_maat::abort(yield_func_t &yield, access_t type, TxnManager * txn, uint64_t cor_id) {
	uint64_t mtx_wait_starttime = get_sys_clock();
#ifdef USE_CAS
	// while(!simulation->is_done() && !local_cas_lock(txn, 0, txn->get_txn_id() + 1)){
	// 	total_num_atomic_retry++;
	// }
	uint64_t off = (char*)_row - rdma_global_buffer;
	uint64_t loc = g_node_id;
	uint64_t thd_id = txn->get_thd_id();
	uint64_t lock = txn->get_txn_id() + 1;
	while(!simulation->is_done() && !txn->cas_remote_content(yield, loc,off,0,lock, cor_id)) {
	// local_cas_lock(txn, 0, txn->get_txn_id() + 1)){
		total_num_atomic_retry++;
	}
#endif
	INC_STATS(txn->get_thd_id(),mtx[32],get_sys_clock() - mtx_wait_starttime);
	DEBUG("Maat Abort %ld: %d -- %ld\n",txn->get_txn_id(),type,_row->get_primary_key());
#if WORKLOAD == TPCC
		ucread_erase(txn->get_txn_id());
		ucwrite_erase(txn->get_txn_id());
#else
    if(type == RD) {
		//uncommitted_reads->erase(txn->get_txn_id());
		ucread_erase(txn->get_txn_id());
	}

	if(type == WR) {
		ucwrite_erase(txn->get_txn_id());
		//uncommitted_writes->erase(txn->get_txn_id());
	}
#endif
#ifdef USE_CAS
	//ATOM_CAS(_row->_tid_word,1,0);
	// local_cas_lock(txn, txn->get_txn_id() + 1, 0);
	_row->_tid_word = 0;
#endif
	return Abort;
}

RC Row_rdma_maat::commit(yield_func_t &yield, access_t type, TxnManager * txn, row_t * data, uint64_t cor_id) {
	//printf("the first txn will commit %d\n", txn->get_txn_id());
	uint64_t mtx_wait_starttime = get_sys_clock();
	RC rc = RCOK;
#ifdef USE_CAS
	uint64_t off = (char*)_row - rdma_global_buffer;
	uint64_t loc = g_node_id;
	uint64_t thd_id = txn->get_thd_id();
	uint64_t lock = txn->get_txn_id() + 1;
	while(!simulation->is_done() && !txn->cas_remote_content(yield, loc,off,0,lock, cor_id)) {
	// local_cas_lock(txn, 0, txn->get_txn_id() + 1)){
		total_num_atomic_retry++;
	}
#endif
	INC_STATS(txn->get_thd_id(),mtx[33],get_sys_clock() - mtx_wait_starttime);
	DEBUG("Maat Commit %ld: %d,%lu -- %ld\n", txn->get_txn_id(), type, txn->get_commit_timestamp(),
			_row->get_primary_key());

	uint64_t txn_commit_ts = txn->get_commit_timestamp();
	if(type == RD || WORKLOAD == TPCC) {
		if (txn_commit_ts > _row->timestamp_last_read) _row->timestamp_last_read = txn_commit_ts;
		//uncommitted_reads->erase(txn->get_txn_id());
		ucread_erase(txn->get_txn_id());
		// Forward validation
		// Check uncommitted writes against this txn's
		#if COMMIT_ADJUST
			for(uint64_t i = 0; i < row_set_length; i++) {
				if (_row->uncommitted_writes[i] == 0) {
					continue;
				}
				//printf("row->uncommitted_writes has txn: %u\n", _row->uncommitted_writes[i]);
				//exit(0);
				else {
					if(txn->uncommitted_writes.count(_row->uncommitted_writes[i]) == 0) {
						if(_row->uncommitted_writes[i] % g_node_cnt == g_node_id) {
							while(!rdma_txn_table.local_cas_timeNode(yield, txn, _row->uncommitted_writes[i], cor_id) && !simulation->is_done()) {
								rc = Abort;
							}
							uint64_t it_lower = rdma_txn_table.local_get_lower(_row->uncommitted_writes[i]);
							if(it_lower <= txn_commit_ts) {
								rdma_txn_table.local_set_lower(txn,_row->uncommitted_writes[i],txn_commit_ts+1);
								
							}
							rdma_txn_table.local_release_timeNode(txn, _row->uncommitted_writes[i]);
						} else {
							if(!rdma_txn_table.remote_cas_timeNode(yield, txn, _row->uncommitted_writes[i], cor_id)) {
								rc = Abort;
							}
							RdmaTxnTableNode* item = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
							item = rdma_txn_table.remote_get_timeNode(yield, txn, _row->uncommitted_writes[i], cor_id);
							uint64_t it_lower = item->lower;
							if(it_lower <= txn_commit_ts) {
								item->lower = txn_commit_ts+1;
								if (_row->uncommitted_writes[i] != 0)
								rdma_txn_table.remote_set_timeNode(yield, txn, _row->uncommitted_writes[i], item, cor_id);
							}
							mem_allocator.free(item, sizeof(RdmaTxnTableNode));
							rdma_txn_table.remote_release_timeNode(yield, txn,  _row->uncommitted_writes[i], cor_id);
						}
						DEBUG("MAAT forward val set lower %ld: %lu\n",_row->uncommitted_writes[i],txn_commit_ts+1);
					} 
				}
			}
		#endif
	}
	/*
	#if WORKLOAD == TPCC
		if(txn_commit_ts >  timestamp_last_read)
		timestamp_last_read = txn_commit_ts;
	#endif
	*/

	if(type == WR || WORKLOAD == TPCC) {
		if (txn_commit_ts > _row->timestamp_last_write) _row->timestamp_last_write = txn_commit_ts;
		//uncommitted_writes->erase(txn->get_txn_id());
		ucwrite_erase(txn->get_txn_id());
		// Apply write to DB
		write(data);
		#if COMMIT_ADJUST
			uint64_t lower = rdma_txn_table.local_get_lower(txn->get_txn_id());
			for(uint64_t i = 0; i < row_set_length; i++) {
				if (_row->uncommitted_writes[i] == 0) {
					continue;
				}
				if(txn->uncommitted_writes_y.count(_row->uncommitted_writes[i]) == 0) {
					if(_row->uncommitted_writes[i] % g_node_cnt == g_node_id) {
						if(!rdma_txn_table.local_cas_timeNode(yield, txn, _row->uncommitted_writes[i], cor_id)) {
							rc = Abort;
						}
						uint64_t it_upper = rdma_txn_table.local_get_upper(_row->uncommitted_writes[i]);
						if(it_upper >= txn_commit_ts) {
							rdma_txn_table.local_set_upper(txn,_row->uncommitted_writes[i],txn_commit_ts-1);
						}
						rdma_txn_table.local_release_timeNode(txn, _row->uncommitted_writes[i]);
					} else {
						if(!rdma_txn_table.remote_cas_timeNode(yield, txn, _row->uncommitted_writes[i], cor_id)) {
							rc = Abort;
						}
						RdmaTxnTableNode* item = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
						item = rdma_txn_table.remote_get_timeNode(yield, txn, _row->uncommitted_writes[i], cor_id);
						uint64_t it_upper = item->upper;
						if(it_upper >= txn_commit_ts) {
							item->upper = txn_commit_ts-1;
							if(_row->uncommitted_writes[i] != 0)
							rdma_txn_table.remote_set_timeNode(yield, txn, _row->uncommitted_writes[i], item, cor_id);
						}
						mem_allocator.free(item, sizeof(RdmaTxnTableNode));
						rdma_txn_table.remote_release_timeNode(yield, txn,  _row->uncommitted_writes[i], cor_id);
					}
					DEBUG("MAAT forward val set upper %ld: %lu\n",_row->uncommitted_writes[i],txn_commit_ts -1);
				} 
			}
			for(uint64_t i = 0; i < row_set_length; i++) {
				if (_row->uncommitted_reads[i] == 0) {
					continue;
				} else {
					if(txn->uncommitted_reads.count(_row->uncommitted_reads[i]) == 0) {
						if(_row->uncommitted_reads[i] % g_node_cnt == g_node_id) {
							if(!rdma_txn_table.local_cas_timeNode(yield, txn, _row->uncommitted_reads[i], cor_id)) {
								rc = Abort;
							}
							uint64_t it_upper = rdma_txn_table.local_get_upper(_row->uncommitted_reads[i]);
							if(it_upper >= lower) {
								rdma_txn_table.local_set_upper(txn,_row->uncommitted_reads[i],lower-1);
							}
							rdma_txn_table.local_release_timeNode(txn, _row->uncommitted_reads[i]);
						} else {
							if(!rdma_txn_table.remote_cas_timeNode(yield, txn, _row->uncommitted_reads[i], cor_id)) {
								rc = Abort;
							}
							RdmaTxnTableNode* item = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
							item = rdma_txn_table.remote_get_timeNode(yield, txn, _row->uncommitted_reads[i], cor_id);
							uint64_t it_upper = item->upper;
							if(it_upper >= lower) {
								item->upper = lower-1;
								if(_row->uncommitted_reads[i] != 0)
								rdma_txn_table.remote_set_timeNode(yield, txn, _row->uncommitted_reads[i], item, cor_id);
							}
							mem_allocator.free(item, sizeof(RdmaTxnTableNode));
							rdma_txn_table.remote_release_timeNode(yield, txn,  _row->uncommitted_reads[i], cor_id);
						}
						DEBUG("MAAT forward val set upper %ld: %lu\n",_row->uncommitted_reads[i],lower -1);
					} 
				}
			}
		#endif
	}
#ifdef USE_CAS
	//ATOM_CAS(_row->_tid_word,1,0);
	// local_cas_lock(txn, txn->get_txn_id() + 1, 0);
	_row->_tid_word = 0;
#endif
	 return rc;
}

void Row_rdma_maat::write(row_t* data) { _row->copy(data); }
#endif

