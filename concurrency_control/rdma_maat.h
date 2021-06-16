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

#ifndef _RDMA_MAAT_H_
#define _RDMA_MAAT_H_

#include "row.h"
#include "semaphore.h"
#include "maat.h"
#include "row_rdma_maat.h"
#include "routine.h"

#if CC_ALG == RDMA_MAAT

class TxnManager;

// enum MAATState {
//   MAAT_RUNNING = 0,
//   MAAT_VALIDATED,
//   MAAT_COMMITTED,
//   MAAT_ABORTED
// };

struct RdmaTimeTableNode{
	uint64_t _lock;
	uint64_t lower;
	uint64_t upper;
	uint64_t key;
	MAATState state;


	void init(uint64_t key) {
		// printf("init table index: %ld\n", key);
		lower = 0;
		upper = UINT64_MAX;
		this->key = 0;
		state = MAAT_RUNNING;
		_lock = 0;
	}
};

class RDMA_Maat {
public:
	void init();
	RC validate(yield_func_t &yield, TxnManager * txn, uint64_t cor_id);
	RC finish(yield_func_t &yield, RC rc, TxnManager *txnMng, uint64_t cor_id);
	RC find_bound(TxnManager * txn);
	RC remote_abort(yield_func_t &yield, TxnManager * txn, Access * data, uint64_t cor_id);
	RC remote_commit(yield_func_t &yield, TxnManager * txn, Access * data, uint64_t cor_id);
	// RdmaTimeTableNode * read_remote_timetable(yield_func_t &yield, TxnManager * txn, uint64_t node_id, uint64_t cor_id);
	// RdmaTimeTableNode * read_remote_timetable(TxnManager * txn, uint64_t node_id);
private:
	sem_t 	_semaphore;
};



class RdmaTimeTable {
public:
	void init();
	void init(uint64_t thd_id, uint64_t key);
	void release(uint64_t thd_id, uint64_t key);
	uint64_t local_get_lower(uint64_t thd_id, uint64_t key);
	uint64_t local_get_upper(uint64_t thd_id, uint64_t key);
	MAATState local_get_state(uint64_t thd_id, uint64_t key);
	void local_set_lower(uint64_t thd_id, uint64_t key, uint64_t value);
	void local_set_upper(uint64_t thd_id, uint64_t key, uint64_t value);
	void local_set_state(uint64_t thd_id, uint64_t key, MAATState value);

	RdmaTimeTableNode * remote_get_timeNode(yield_func_t &yield, TxnManager *txnMng, uint64_t key, uint64_t cor_id);
	void remote_set_timeNode(yield_func_t &yield, TxnManager *txnMng, uint64_t key, RdmaTimeTableNode * value, uint64_t cor_id);
private:
	// hash table
	uint64_t hash(uint64_t key);
	uint64_t table_size;
	RdmaTimeTableNode *table;
	sem_t 	_semaphore;
};

#endif

#endif

