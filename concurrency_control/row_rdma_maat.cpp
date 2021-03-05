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

void Row_rdma_maat::init(row_t * row) {
	_row = row;

	timestamp_last_read = 0;
	timestamp_last_write = 0;
	maat_avail = true;
	for (int i = 0; i < row_set_length; i++) {
		uncommitted_writes[i] = -1;
		uncommitted_reads[i] = -1;
	}
	//uncommitted_writes = new std::set<uint64_t>();
	//uncommitted_reads = new std::set<uint64_t>();
	//assert(uncommitted_writes->begin() == uncommitted_writes->end());
	//assert(uncommitted_writes->size() == 0);

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
	assert (CC_ALG == MAAT);
	RC rc = RCOK;

	uint64_t mtx_wait_starttime = get_sys_clock();
	while (!ATOM_CAS(maat_avail, true, false)) {
	}
	INC_STATS(txn->get_thd_id(),mtx[30],get_sys_clock() - mtx_wait_starttime);
	INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - mtx_wait_starttime);
	DEBUG("READ + PREWRITE %ld -- %ld: lw %ld\n", txn->get_txn_id(), _row->get_primary_key(),
			timestamp_last_write);

	// Copy uncommitted writes
	for(uint64_t i = 0; i < row_set_length; i++) {
		if(uncommitted_writes[i] == -1) {
			break;
		} else {
			txn->uncommitted_writes.insert(uncommitted_writes[i]);
			txn->uncommitted_writes_y.insert(uncommitted_writes[i]);
		}
		DEBUG("    UW %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),uncommitted_writes[i]);
	}

	// Copy uncommitted reads
	for(uint64_t i = 0; i < row_set_length; i++) {
		if(uncommitted_reads[i] == -1) {
			break;
		} else {
			txn->uncommitted_reads.insert(uncommitted_reads[i]);
		}
		DEBUG("    UR %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),uncommitted_reads[i]);
	}

	// Copy read timestamp
	if(txn->greatest_read_timestamp < timestamp_last_read)
		txn->greatest_read_timestamp = timestamp_last_read;


	// Copy write timestamp
	if(txn->greatest_write_timestamp < timestamp_last_write)
		txn->greatest_write_timestamp = timestamp_last_write;

	//Add to uncommitted reads (soft lock)
	for(uint64_t i = 0; i < row_set_length; i++) {
		if(uncommitted_reads[i] == -1) {
			uncommitted_reads[i] = txn->get_txn_id();
			break;
		} 
	}
	//Add to uncommitted writes (soft lock)
	for(uint64_t i = 0; i < row_set_length; i++) {
		if(uncommitted_writes[i] == -1) {
			uncommitted_writes[i] = txn->get_txn_id();
			break;
		} 
	}
	ATOM_CAS(maat_avail,false,true);

	return rc;
}

RC Row_rdma_maat::read(TxnManager * txn) {
	assert (CC_ALG == MAAT);
	RC rc = RCOK;

	uint64_t mtx_wait_starttime = get_sys_clock();
	while (!ATOM_CAS(maat_avail, true, false)) {
	}
	INC_STATS(txn->get_thd_id(),mtx[30],get_sys_clock() - mtx_wait_starttime);
	INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - mtx_wait_starttime);
	DEBUG("READ %ld -- %ld: lw %ld\n", txn->get_txn_id(), _row->get_primary_key(),
			timestamp_last_write);

	// Copy uncommitted writes
	for(uint64_t i = 0; i < row_set_length; i++) {
		uint64_t last_write = uncommitted_writes[i];
		assert(i <= row_set_length - 1);
		if(last_write == -1) {
			break;
		}
		txn->uncommitted_writes.insert(last_write);
		DEBUG("    UW %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),last_write);
	}

	// Copy write timestamp
	if(txn->greatest_write_timestamp < timestamp_last_write)
		txn->greatest_write_timestamp = timestamp_last_write;

	//Add to uncommitted reads (soft lock)
	for(uint64_t i = 0; i < row_set_length; i++) {
		uint64_t last_read = uncommitted_reads[i];
		assert(i <= row_set_length - 1);
		if(last_read == -1) {
			last_read = txn->get_txn_id();
			break;
		}

	}


	ATOM_CAS(maat_avail,false,true);

	return rc;
}

RC Row_rdma_maat::prewrite(TxnManager * txn) {
	assert (CC_ALG == RDMA_MAAT);
	RC rc = RCOK;

	uint64_t mtx_wait_starttime = get_sys_clock();
	while (!ATOM_CAS(maat_avail, true, false)) {
	}
	INC_STATS(txn->get_thd_id(),mtx[31],get_sys_clock() - mtx_wait_starttime);
	INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - mtx_wait_starttime);
	DEBUG("PREWRITE %ld -- %ld: lw %ld, lr %ld\n", txn->get_txn_id(), _row->get_primary_key(),
			timestamp_last_write, timestamp_last_read);

	// Copy uncommitted reads
	for(auto i = 0; i < row_set_length; i++) {
		uint64_t last_read = uncommitted_reads[i];
		assert(i <= row_set_length - 1);
		if(last_read == -1) {
			break;
		}
		txn->uncommitted_reads.insert(last_read);
		DEBUG("    UR %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),last_read);
	}

	// Copy uncommitted writes
	for(auto i = 0; i < row_set_length; i++) {
		uint64_t last_write = uncommitted_writes[i];
		assert(i <= row_set_length - 1);
		if(last_write == -1) {
			uncommitted_writes[i] = txn->get_txn_id();
			break;
		}
		txn->uncommitted_writes_y.insert(last_write);
		DEBUG("    UW %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),last_write);
	}

	// Copy read timestamp
	if(txn->greatest_read_timestamp < timestamp_last_read)
		txn->greatest_read_timestamp = timestamp_last_read;

	// Copy write timestamp
	if(txn->greatest_write_timestamp < timestamp_last_write)
		txn->greatest_write_timestamp = timestamp_last_write;


	ATOM_CAS(maat_avail,false,true);

	return rc;
}

void Row_rdma_maat::ucread_erase(uint64_t txn_id) {
	for(uint64_t i = 0; i < row_set_length; i++) {
		uint64_t last = uncommitted_reads[i];
		assert(i < row_set_length - 1);
		if(last == -1) break;
		if(last == txn_id) {
			if(uncommitted_reads[i+1] == -1) {
				uncommitted_reads[i] = -1;
				break;
			}
			for(uint64_t j = i; j < row_set_length; j++) {
				uncommitted_reads[j] = uncommitted_reads[j+1];
				if(uncommitted_reads[j+1] == -1) break;
			}
		}
	}
}
void Row_rdma_maat::ucwrite_erase(uint64_t txn_id) {
	for(uint64_t i = 0; i < row_set_length; i++) {
		uint64_t last = uncommitted_writes[i];
		assert(i < row_set_length - 1);
		if(last == -1) break;
		if(last == txn_id) {
			if(uncommitted_writes[i+1] == -1) {
				uncommitted_writes[i] = -1;
				break;
			}
			for(uint64_t j = i; j < row_set_length; j++) {
				uncommitted_writes[j] = uncommitted_writes[j+1];
				if(uncommitted_writes[j+1] == -1) break;
			}
		}
	}
}
RC Row_rdma_maat::abort(access_t type, TxnManager * txn) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	while (!ATOM_CAS(maat_avail, true, false)) {
	}
	INC_STATS(txn->get_thd_id(),mtx[32],get_sys_clock() - mtx_wait_starttime);
	DEBUG("Maat Abort %ld: %d -- %ld\n",txn->get_txn_id(),type,_row->get_primary_key());
#if WORKLOAD == TPCC
	uncommitted_reads->erase(txn->get_txn_id());
	uncommitted_writes->erase(txn->get_txn_id());
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

	ATOM_CAS(maat_avail,false,true);
	return Abort;
}

// RC Row_rdma_maat::commit(access_t type, TxnManager * txn, row_t * data) {
// 	uint64_t mtx_wait_starttime = get_sys_clock();
// 	while (!ATOM_CAS(maat_avail, true, false)) {
// 	}
// 	INC_STATS(txn->get_thd_id(),mtx[33],get_sys_clock() - mtx_wait_starttime);
// 	DEBUG("Maat Commit %ld: %d,%lu -- %ld\n", txn->get_txn_id(), type, txn->get_commit_timestamp(),
// 			_row->get_primary_key());

// #if WORKLOAD == TPCC
// 	if(txn->get_commit_timestamp() >  timestamp_last_read)
// 	timestamp_last_read = txn->get_commit_timestamp();
// 	uncommitted_reads->erase(txn->get_txn_id());
// 	if(txn->get_commit_timestamp() >  timestamp_last_write)
// 	timestamp_last_write = txn->get_commit_timestamp();
// 	uncommitted_writes->erase(txn->get_txn_id());
// 	// Apply write to DB
// 	write(data);

// 	uint64_t txn_commit_ts = txn->get_commit_timestamp();
// 	// Forward validation
// 	// Check uncommitted writes against this txn's
// 		for(auto it = uncommitted_writes->begin(); it != uncommitted_writes->end();it++) {
// 		if(txn->uncommitted_writes->count(*it) == 0) {
// 			// apply timestamps
// 			// these write txns need to come AFTER this txn
// 			uint64_t it_lower = time_table.get_lower(txn->get_thd_id(),*it);
// 			if(it_lower <= txn_commit_ts) {
// 			time_table.set_lower(txn->get_thd_id(),*it,txn_commit_ts+1);
// 			DEBUG("MAAT forward val set lower %ld: %lu\n",*it,txn_commit_ts+1);
// 			}
// 		}
// 	}

// 	uint64_t lower =  time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
// 	for(auto it = uncommitted_writes->begin(); it != uncommitted_writes->end();it++) {
// 		if(txn->uncommitted_writes_y->count(*it) == 0) {
// 			// apply timestamps
// 			// these write txns need to come BEFORE this txn
// 			uint64_t it_upper = time_table.get_upper(txn->get_thd_id(),*it);
// 			if(it_upper >= txn_commit_ts) {
// 			time_table.set_upper(txn->get_thd_id(),*it,txn_commit_ts-1);
// 			DEBUG("MAAT forward val set upper %ld: %lu\n",*it,txn_commit_ts-1);
// 			}
// 		}
// 	}

// 	for(auto it = uncommitted_reads->begin(); it != uncommitted_reads->end();it++) {
// 		if(txn->uncommitted_reads->count(*it) == 0) {
// 			// apply timestamps
// 			// these write txns need to come BEFORE this txn
// 			uint64_t it_upper = time_table.get_upper(txn->get_thd_id(),*it);
// 			if(it_upper >= lower) {
// 			time_table.set_upper(txn->get_thd_id(),*it,lower-1);
// 			DEBUG("MAAT forward val set upper %ld: %lu\n",*it,lower-1);
// 			}
// 		}
// 	}

// #else
// 	uint64_t txn_commit_ts = txn->get_commit_timestamp();
// 	if(type == RD) {
// 		if (txn_commit_ts > timestamp_last_read) timestamp_last_read = txn_commit_ts;
// 		//uncommitted_reads->erase(txn->get_txn_id());
// 		ucread_erase(txn->get_txn_id());
// 		// Forward validation
// 		// Check uncommitted writes against this txn's
// 		for(auto it = uncommitted_writes->begin(); it != uncommitted_writes->end();it++) {
// 			if(txn->uncommitted_writes->count(*it) == 0) {
// 				// apply timestamps
// 				// these write txns need to come AFTER this txn
// 				uint64_t it_lower = time_table.get_lower(txn->get_thd_id(),*it);
// 				if(it_lower <= txn_commit_ts) {
// 					time_table.set_lower(txn->get_thd_id(),*it,txn_commit_ts+1);
// 					DEBUG("MAAT forward val set lower %ld: %lu\n",*it,txn_commit_ts+1);
// 				}
// 			}
// 		}

// 	}
// 	/*
// 	#if WORKLOAD == TPCC
// 		if(txn_commit_ts >  timestamp_last_read)
// 		timestamp_last_read = txn_commit_ts;
// 	#endif
// 	*/

// 	if(type == WR) {
// 		if (txn_commit_ts > timestamp_last_write) timestamp_last_write = txn_commit_ts;
// 		//uncommitted_writes->erase(txn->get_txn_id());
// 		ucwrite_erase(txn->get_txn_id());
// 		// Apply write to DB
// 		write(data);
// 		uint64_t lower =  time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
// 		for(auto it = uncommitted_writes->begin(); it != uncommitted_writes->end();it++) {
// 			if(txn->uncommitted_writes_y->count(*it) == 0) {
// 				// apply timestamps
// 				// these write txns need to come BEFORE this txn
// 				uint64_t it_upper = time_table.get_upper(txn->get_thd_id(),*it);
// 				if(it_upper >= txn_commit_ts) {
// 					time_table.set_upper(txn->get_thd_id(),*it,txn_commit_ts-1);
// 					DEBUG("MAAT forward val set upper %ld: %lu\n",*it,txn_commit_ts-1);
// 				}
// 			}
// 		}

// 		for(auto it = uncommitted_reads->begin(); it != uncommitted_reads->end();it++) {
// 			if(txn->uncommitted_reads->count(*it) == 0) {
// 				// apply timestamps
// 				// these write txns need to come BEFORE this txn
// 				uint64_t it_upper = time_table.get_upper(txn->get_thd_id(),*it);
// 				if(it_upper >= lower) {
// 					time_table.set_upper(txn->get_thd_id(),*it,lower-1);
// 					DEBUG("MAAT forward val set upper %ld: %lu\n",*it,lower-1);
// 				}
// 			}
// 		}

// 	}
// 	#endif

// 	ATOM_CAS(maat_avail,false,true);
// 	return RCOK;
// }

void Row_rdma_maat::write(row_t* data) { _row->copy(data); }
