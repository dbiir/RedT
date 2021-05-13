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

#ifndef _SSI_H_
#define _SSI_H_

#include "row.h"
#include "semaphore.h"

class TxnManager;
enum SSIState { SSI_RUNNING=0,SSI_COMMITTED,SSI_ABORTED};
struct InOutTableEntry{
	std::set<uint64_t> inConflict;
	std::set<uint64_t> outConflict;
	// bool inConflict;
	// bool outConflict;
	SSIState state;
	uint64_t commit_ts;
	uint64_t key;
	//   MAATState state;
	InOutTableEntry * next;
	InOutTableEntry * prev;
	void init(uint64_t key) {
		// inConflict = false;
		// outConflict = false;
		this->key = key;
		state = SSI_RUNNING;
		commit_ts = 0;
		next = NULL;
		prev = NULL;
	}
};

struct InOutTableNode {
	InOutTableEntry * head;
	InOutTableEntry * tail;
	pthread_mutex_t mtx;
	void init() {
		head = NULL;
		tail = NULL;
		pthread_mutex_init(&mtx,NULL);
	}
};

class InOutTable {
public:
	void init();
	void init(uint64_t thd_id, uint64_t key);
	void release(uint64_t thd_id, uint64_t key);
	bool get_inConflict(uint64_t thd_id, uint64_t key);
	bool get_outConflict(uint64_t thd_id, uint64_t key);
	void set_inConflict(uint64_t thd_id, uint64_t key, uint64_t value);
	void down_inConflict(uint64_t thd_id, uint64_t key, uint64_t value);

	void set_outConflict(uint64_t thd_id, uint64_t key, uint64_t value);
	void down_outConflict(uint64_t thd_id, uint64_t key, uint64_t value);

	void clear_Conflict(uint64_t thd_id, uint64_t key);

	SSIState get_state(uint64_t thd_id, uint64_t key);
	void set_state(uint64_t thd_id, uint64_t key, SSIState value);
	uint64_t get_commit_ts(uint64_t thd_id, uint64_t key);
	void set_commit_ts(uint64_t thd_id, uint64_t key, uint64_t value);
	// MAATState get_state(uint64_t thd_id, uint64_t key);
	// void set_state(uint64_t thd_id, uint64_t key, MAATState value);
	private:
	// hash table
	uint64_t hash(uint64_t key);
	uint64_t table_size;
	InOutTableNode* table;
	InOutTableEntry* find(uint64_t key);
	pthread_mutex_t mtx;
	InOutTableEntry * find_entry(uint64_t id);

		sem_t 	_semaphore;
	};

class ssi {
public:
	void init();
	RC validate(TxnManager * txn);
	void gene_finish_ts(TxnManager * txn);
private:
	sem_t 	_semaphore;
};

#endif
