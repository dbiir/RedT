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

#ifndef _WKDB_H_
#define _WKDB_H_

#include "row.h"
#include "semaphore.h"


class TxnManager;

enum WKDBState { WKDB_RUNNING=0,WKDB_VALIDATED,WKDB_COMMITTED,WKDB_ABORTED};

class wkdb_set_ent{
public:
	wkdb_set_ent();
	UInt64 tn;
	TxnManager * txn;
	UInt32 set_size;
	row_t ** rows; //[MAX_WRITE_SET];
	wkdb_set_ent * next;
};

class Wkdb {
public:
  void init();
  RC validate(TxnManager * txn);
  RC find_bound(TxnManager * txn);
private:
  RC get_rw_set(TxnManager * txni, wkdb_set_ent * &rset, wkdb_set_ent *& wset);
 	sem_t 	_semaphore;
};

struct WkdbTimeTableEntry{
  uint64_t lower;
  uint64_t upper;
  uint64_t key;
  WKDBState state;
  WkdbTimeTableEntry * next;
  WkdbTimeTableEntry * prev;
  void init(uint64_t key, uint64_t ts) {
    lower = ts;
    upper = UINT64_MAX;
    this->key = key;
    state = WKDB_RUNNING;
    next = NULL;
    prev = NULL;
  }
};

struct WkdbTimeTableNode {
  WkdbTimeTableEntry * head;
  WkdbTimeTableEntry * tail;
  pthread_mutex_t mtx;
  void init() {
    head = NULL;
    tail = NULL;
    pthread_mutex_init(&mtx,NULL);
  }
};

class WkdbTimeTable {
public:
	void init();
	void init(uint64_t thd_id, uint64_t key, uint64_t ts);
	void release(uint64_t thd_id, uint64_t key);
  uint64_t get_lower(uint64_t thd_id, uint64_t key);
  uint64_t get_upper(uint64_t thd_id, uint64_t key);
  void set_lower(uint64_t thd_id, uint64_t key, uint64_t value);
  void set_upper(uint64_t thd_id, uint64_t key, uint64_t value);
  WKDBState get_state(uint64_t thd_id, uint64_t key);
  void set_state(uint64_t thd_id, uint64_t key, WKDBState value);
private:
  // hash table
  uint64_t hash(uint64_t key);
  uint64_t table_size;
  WkdbTimeTableNode* table;
  WkdbTimeTableEntry* find(uint64_t key);

  WkdbTimeTableEntry * find_entry(uint64_t id);

 	sem_t 	_semaphore;
};

#endif
