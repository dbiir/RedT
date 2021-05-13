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

#include "global.h"
#include "helper.h"
#include "txn.h"
#include "ssi.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_ssi.h"

void ssi::init() {
  sem_init(&_semaphore, 0, 1);
}

RC ssi::validate(TxnManager * txn) {
  uint64_t start_time = get_sys_clock();
  uint64_t timespan;
  sem_wait(&_semaphore);

  timespan = get_sys_clock() - start_time;
  txn->txn_stats.cc_block_time += timespan;
  txn->txn_stats.cc_block_time_short += timespan;
  // INC_STATS(txn->get_thd_id(),maat_cs_wait_time,timespan);
  start_time = get_sys_clock();
  RC rc = RCOK;

  DEBUG("SSI Validate Start %ld\n",txn->get_txn_id());
  std::set<uint64_t> after;
  std::set<uint64_t> before;
  if (inout_table.get_inConflict(txn->get_thd_id(), txn->get_txn_id()) &&
  	  inout_table.get_outConflict(txn->get_thd_id(), txn->get_txn_id()))
  {
    DEBUG("ssi Validate abort, %ld\n",txn->get_txn_id());
	  rc = Abort;
  } else {
    DEBUG("ssi Validate ok %ld\n",txn->get_txn_id());
	  rc = RCOK;
  }
  // INC_STATS(txn->get_thd_id(),ssi_commit_cnt,1);
  // INC_STATS(txn->get_thd_id(),ssi_validate_cnt,1);
  // timespan = get_sys_clock() - start_time;
  // INC_STATS(txn->get_thd_id(),ssi_validate_time,timespan);
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;
  DEBUG("SSI Validate End %ld: %d\n",txn->get_txn_id(),rc==RCOK);
  sem_post(&_semaphore);
  return rc;
}

void ssi::gene_finish_ts(TxnManager * txn) {
	txn->set_commit_timestamp(glob_manager.get_ts(txn->get_thd_id()));
}

void InOutTable::init() {
  //table_size = g_inflight_max * g_node_cnt * 2 + 1;
  table_size = g_inflight_max + 1;
  pthread_mutex_init(&mtx,NULL);
  DEBUG_M("InOutTable::init table alloc\n");
  table = (InOutTableNode*) mem_allocator.alloc(sizeof(InOutTableNode) * table_size);
  for(uint64_t i = 0; i < table_size;i++) {
    table[i].init();
  }
}

uint64_t InOutTable::hash(uint64_t key) {
  return key % table_size;
}

InOutTableEntry* InOutTable::find(uint64_t key) {
  InOutTableEntry * entry = table[hash(key)].head;
  while(entry) {
    if(entry->key == key)
      break;
    entry = entry->next;
  }
  return entry;

}

void InOutTable::init(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[34],get_sys_clock() - mtx_wait_starttime);
  InOutTableEntry* entry = find(key);
  if(!entry) {
    DEBUG_M("InOutTable::init entry alloc\n");
    entry = (InOutTableEntry*) mem_allocator.alloc(sizeof(InOutTableEntry));
    entry->init(key);
    LIST_PUT_TAIL(table[idx].head,table[idx].tail,entry);
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

void InOutTable::release(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[35],get_sys_clock() - mtx_wait_starttime);
  InOutTableEntry* entry = find(key);
  if(entry) {
    LIST_REMOVE_HT(entry,table[idx].head,table[idx].tail);
    DEBUG_M("InOutTable::release entry free\n");
    mem_allocator.free(entry,sizeof(InOutTableEntry));
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

bool InOutTable::get_inConflict(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  bool value = 0;
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[36],get_sys_clock() - mtx_wait_starttime);
  InOutTableEntry* entry = find(key);
  if(entry) {
    value = !entry->inConflict.empty();
  }
  pthread_mutex_unlock(&table[idx].mtx);
  return value;
}

bool InOutTable::get_outConflict(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  bool value = UINT64_MAX;
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_wait_starttime);
  InOutTableEntry* entry = find(key);
  if(entry) {
    value = !entry->outConflict.empty();
  }
  pthread_mutex_unlock(&table[idx].mtx);
  return value;
}


void InOutTable::set_inConflict(uint64_t thd_id, uint64_t key, uint64_t value) {
  uint64_t idx = hash(key);
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[38],get_sys_clock() - mtx_wait_starttime);
  InOutTableEntry* entry = find(key);
  if(entry) {
    entry->inConflict.insert(value);
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

void InOutTable::set_outConflict(uint64_t thd_id, uint64_t key, uint64_t value) {
  uint64_t idx = hash(key);
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[39],get_sys_clock() - mtx_wait_starttime);
  InOutTableEntry* entry = find(key);
  if(entry) {
    entry->outConflict.insert(value);
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

void InOutTable::down_inConflict(uint64_t thd_id, uint64_t key, uint64_t value) {
  uint64_t idx = hash(key);
  uint64_t v_idx = hash(value);
  if (idx != v_idx)
    pthread_mutex_lock(&table[idx].mtx);
  InOutTableEntry* entry = find(key);
  if(entry) {
    entry->inConflict.erase(value);
  }
  if (idx != v_idx)
    pthread_mutex_unlock(&table[idx].mtx);
}

void InOutTable::down_outConflict(uint64_t thd_id, uint64_t key, uint64_t value) {
  uint64_t idx = hash(key);
  uint64_t v_idx = hash(value);
  if (idx != v_idx)
    pthread_mutex_lock(&table[idx].mtx);
  InOutTableEntry* entry = find(key);
  if(entry) {
    entry->outConflict.erase(value);
  }
  if (idx != v_idx)
    pthread_mutex_unlock(&table[idx].mtx);
}

void InOutTable::clear_Conflict(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&mtx);
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[38],get_sys_clock() - mtx_wait_starttime);
  InOutTableEntry* entry = find(key);
  if(entry) {
    set<uint64_t>::iterator iter = entry->inConflict.begin();
    while (iter != entry->inConflict.end())
    {
      down_outConflict(thd_id, *iter, key);
      iter++;
    }
    iter = entry->outConflict.begin();
    while (iter != entry->outConflict.end())
    {
      down_inConflict(thd_id, *iter, key);
      iter++;
    }
  }
  pthread_mutex_unlock(&table[idx].mtx);
  pthread_mutex_unlock(&mtx);
}

SSIState InOutTable::get_state(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  SSIState value = SSI_RUNNING;
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_wait_starttime);
  InOutTableEntry* entry = find(key);
  if(entry) {
    value = entry->state;
  }
  pthread_mutex_unlock(&table[idx].mtx);
  return value;
}

void InOutTable::set_state(uint64_t thd_id, uint64_t key, SSIState value) {
  uint64_t idx = hash(key);
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[39],get_sys_clock() - mtx_wait_starttime);
  InOutTableEntry* entry = find(key);
  if(entry) {
    entry->state = value;
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

uint64_t InOutTable::get_commit_ts(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  uint64_t value = SSI_RUNNING;
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_wait_starttime);
  InOutTableEntry* entry = find(key);
  if(entry) {
    value = entry->commit_ts;
  }
  pthread_mutex_unlock(&table[idx].mtx);
  return value;
}

void InOutTable::set_commit_ts(uint64_t thd_id, uint64_t key, uint64_t value) {
  uint64_t idx = hash(key);
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[39],get_sys_clock() - mtx_wait_starttime);
  InOutTableEntry* entry = find(key);
  if(entry) {
    entry->commit_ts = value;
  }
  pthread_mutex_unlock(&table[idx].mtx);
}
