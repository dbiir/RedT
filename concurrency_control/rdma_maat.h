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

enum WOUNDState {
  WOUND_RUNNING = 0,
  WOUND_WOUNDED,
  WOUND_WAITING,
  WOUND_COMMITTING,
  WOUND_ABORTING
};

enum TSState {
  TS_RUNNING = 0,
  TS_WAITING,
  TS_COMMITTING,
  TS_ABORTING,
  TS_NULL
};
struct MAATTableNode {
	uint64_t lower;
	uint64_t upper;
	uint64_t key;
	MAATState state;
};
struct RdmaTxnTableNode{
#if CC_ALG == RDMA_MAAT
	uint64_t _lock;
	MAATTableNode nodes[MAAT_NODES_COUNT];
	uint64_t index;
	
	void init() {
		// printf("init table index: %ld\n", key);
		for (uint64_t i = 0; i < MAAT_NODES_COUNT; i++) {
			nodes[i].lower = 0;
			nodes[i].upper = UINT64_MAX;
			nodes[i].key = 0;
			nodes[i].state = MAAT_RUNNING;
			// nodes[i]._lock = 0;
		}
		index = 0;
		_lock = 0;
	}
	void init(uint64_t key) {
		nodes[index % MAAT_NODES_COUNT].lower = 0;
		nodes[index % MAAT_NODES_COUNT].upper = UINT64_MAX;
		nodes[index % MAAT_NODES_COUNT].key = key;
		nodes[index % MAAT_NODES_COUNT].state = MAAT_RUNNING;
		// nodes[index % MAAT_NODES_COUNT]._lock = 0;
		// printf("%lu init %ld: [%lu,%lu]\n",key,nodes[index % MAAT_NODES_COUNT].key,nodes[index % MAAT_NODES_COUNT].lower,nodes[index % MAAT_NODES_COUNT].upper);
		index ++;
	}
	void release(uint64_t key) {
		for (uint64_t i = 0; i < MAAT_NODES_COUNT; i++) {
			if (nodes[i].key != key)continue;
			nodes[i].lower = 0;
			nodes[i].upper = UINT64_MAX;
			nodes[i].key = 0;
			nodes[i].state = MAAT_RUNNING;
			// nodes[i]._lock = 0;
		}
	}	
#endif
#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
	uint64_t _lock;
	uint64_t key;
	WOUNDState state;

	void init(uint64_t key) {
		// printf("init table index: %ld\n", key);
		this->key = key;
		state = WOUND_RUNNING;
		_lock = 0;
	}
#endif
#if CC_ALG == RDMA_TS
	uint64_t _lock;
	uint64_t key;
	TSState state;

	void init(uint64_t key) {
		// printf("init table index: %ld\n", key);
		this->key = key;
		state = TS_RUNNING;
		_lock = 0;
	}
#endif
#if CC_ALG == RDMA_TS1
	uint64_t _lock;
	uint64_t key[RDMA_TSSTATE_COUNT];
	// uint64_t txn_id[RDMA_TSSTATE_COUNT];
	TSState state[RDMA_TSSTATE_COUNT];

	void init(uint64_t key_) {
		uint64_t min_key = 0, min_idx = 0;
		for (int i = 0; i < RDMA_TSSTATE_COUNT; i++) {
			if (key[i] < min_key) {
				min_key = key[i];
				min_idx = i;
			}
		}
		key[min_idx] = key_;
		state[min_idx] = TS_RUNNING;
	}
#endif
};

class RdmaTxnTable {
public:
	void init();
	void init(uint64_t thd_id, uint64_t key);
	void release(uint64_t thd_id, uint64_t key);
#if CC_ALG == RDMA_MAAT
	bool local_is_key(uint64_t key);
	uint64_t local_get_lower(uint64_t key);
	uint64_t local_get_upper(uint64_t key);
	MAATState local_get_state(uint64_t key);
	bool local_set_lower(TxnManager *txnMng, uint64_t key, uint64_t value);
	bool local_set_upper(TxnManager *txnMng, uint64_t key, uint64_t value);
	bool local_set_state(TxnManager *txnMng, uint64_t key, MAATState value);

	bool remote_is_key(RdmaTxnTableNode * root,uint64_t key);
	uint64_t remote_get_lower(RdmaTxnTableNode * root,uint64_t key);
	uint64_t remote_get_upper(RdmaTxnTableNode * root,uint64_t key);
	MAATState remote_get_state(RdmaTxnTableNode * root,uint64_t key);
	bool remote_set_lower(RdmaTxnTableNode * root, uint64_t key, uint64_t value);
	bool remote_set_upper(RdmaTxnTableNode * root, uint64_t key, uint64_t value);
	bool remote_set_state(RdmaTxnTableNode * root, uint64_t key, MAATState value);

	bool local_cas_timeNode(yield_func_t &yield,TxnManager *txnMng, uint64_t key, uint64_t cor_id);
	void local_release_timeNode(TxnManager *txnMng, uint64_t key);
	RdmaTxnTableNode * remote_get_timeNode(yield_func_t &yield, TxnManager *txnMng, uint64_t key, uint64_t cor_id);
	bool remote_set_timeNode(yield_func_t &yield, TxnManager *txnMng, uint64_t key, RdmaTxnTableNode * value, uint64_t cor_id);
	bool remote_cas_timeNode(yield_func_t &yield,TxnManager *txnMng, uint64_t key, uint64_t cor_id);
	bool remote_release_timeNode(yield_func_t &yield,TxnManager *txnMng, uint64_t key, uint64_t cor_id);
#endif
#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
	WOUNDState local_get_state(uint64_t thd_id, uint64_t key);
	bool local_set_state(TxnManager *txnMng, uint64_t thd_id, uint64_t key, WOUNDState value);

	char * remote_get_state(yield_func_t &yield, TxnManager *txnMng, uint64_t key, uint64_t cor_id);
	bool remote_set_state(yield_func_t &yield, TxnManager *txnMng, uint64_t key, WOUNDState value, uint64_t cor_id);
#endif
#if CC_ALG == RDMA_TS
	TSState local_get_state(uint64_t thd_id, uint64_t key);
	void local_set_state(uint64_t thd_id, uint64_t key, TSState value);

	char * remote_get_state(yield_func_t &yield, TxnManager *txnMng, uint64_t key, uint64_t cor_id);
	void remote_set_state(yield_func_t &yield, TxnManager *txnMng, uint64_t key, RdmaTxnTableNode * value, uint64_t cor_id);
#endif
#if CC_ALG == RDMA_TS1
	TSState local_get_state(uint64_t thd_id, uint64_t key);
	void local_set_state(uint64_t thd_id, uint64_t key, TSState value);
	TSState remote_get_state(yield_func_t &yield, TxnManager *txnMng, uint64_t key, uint64_t cor_id);
#endif
private:
	// hash table
	uint64_t hash(uint64_t key);
	uint64_t table_size;
	RdmaTxnTableNode *table;
	sem_t 	_semaphore;
};
#if CC_ALG == RDMA_MAAT

class TxnManager;

class RDMA_Maat {
public:
	void init();
	RC validate(yield_func_t &yield, TxnManager * txn, uint64_t cor_id);
	RC finish(yield_func_t &yield, RC rc, TxnManager *txnMng, uint64_t cor_id);
	RC find_bound(TxnManager * txn);
	RC remote_abort(yield_func_t &yield, TxnManager * txn, Access * data, uint64_t cor_id);
	RC remote_commit(yield_func_t &yield, TxnManager * txn, Access * data, uint64_t cor_id);
	// RdmaTxnTableNode * read_remote_timetable(yield_func_t &yield, TxnManager * txn, uint64_t node_id, uint64_t cor_id);
	// RdmaTxnTableNode * read_remote_timetable(TxnManager * txn, uint64_t node_id);
private:
	sem_t 	_semaphore;
};
#endif

#endif

