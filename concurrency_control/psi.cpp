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
#include "psi.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_psi.h"
#include "maat.h"

#if CC_ALG == PSI
void psi::init() { sem_init(&_semaphore, 0, 1); }

RC psi::validate_4a(TxnManager * txn) {
  uint64_t start_time = get_sys_clock();
  uint64_t timespan;
  // sem_wait(&_semaphore);

  timespan = get_sys_clock() - start_time;
  txn->txn_stats.cc_block_time += timespan;
  txn->txn_stats.cc_block_time_short += timespan;
  INC_STATS(txn->get_thd_id(),maat_cs_wait_time,timespan);
  start_time = get_sys_clock();
  RC rc = RCOK;

  uint64_t commit_timestamp, lower, upper;

  // 处理写操作，对写加锁
  // std::set<uint64_t> * reads_before;
  // for (auto access : txn->txn->accesses) {
  for (int i = 0; i < txn->get_access_cnt(); i++) {
    Access * access = txn->get_access(i);
    if (access->type == WR) {
      RC rc = access->orig_row->manager->prewrite(txn, access->pversion);
      if (rc == Abort) {
        rc = Abort;
        goto VALIDATE_END;
      }
      for (auto it = access->pversion->visitor_list->begin(); it != access->pversion->visitor_list->end();it++) {
        txn->reads_before->insert(*it);
        // 收集需要记录反依赖的
      }
    }
  }
  
  // 然后开始验证开始时间戳范围
  //local time_table
  lower = time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
  upper = time_table.get_upper(txn->get_thd_id(),txn->get_txn_id());
  DEBUG("PSI Validate Start %ld: [%lu,%lu]\n",txn->get_txn_id(),lower,upper);
  if (lower > upper) {
    // Abort
    time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),MAAT_ABORTED);
    rc = Abort;
    goto VALIDATE_END;
  }
  // txn->set_start_timestamp(lower);

  // Rule 4a，检查反依赖表
  commit_timestamp = time_table.get_cts(txn->get_thd_id(),txn->get_txn_id());
  for(auto it = txn->reads_before->begin(); it != txn->reads_before->end();it++) {
    uint64_t it_lower = time_table.get_lower(txn->get_thd_id(),*it);
    commit_timestamp = commit_timestamp > it_lower ? commit_timestamp : it_lower;
  }
  // commit_timestamp = commit_timestamp + 1;
  time_table.set_cts(txn->get_thd_id(),txn->get_txn_id(),commit_timestamp);
  // txn->set_commit_timestamp(commit_timestamp);

  sem_wait(&ad_table._semaphore);
  for (auto txn_id : ad_table.reverse_table[txn->get_txn_id()]) {
    txn->writes_after->insert(txn_id);
  }
  sem_post(&ad_table._semaphore);
VALIDATE_END:

  time_table.set_lower(txn->get_thd_id(),txn->get_txn_id(),lower);
  time_table.set_upper(txn->get_thd_id(),txn->get_txn_id(),upper);
  INC_STATS(txn->get_thd_id(),maat_validate_cnt,1);
  timespan = get_sys_clock() - start_time;
  INC_STATS(txn->get_thd_id(),maat_validate_time,timespan);
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;
  DEBUG("PSI Validate End %ld: %d [%lu,%lu]\n",txn->get_txn_id(),rc==RCOK,lower,upper);
  //  printf("PSI Validate End %ld: %d [%lu,%lu]\n",txn->get_txn_id(),rc==RCOK,lower,upper);
  // sem_post(&_semaphore);
  return rc;

}

RC psi::find_bound(TxnManager * txn) {
  RC rc = RCOK;
  uint64_t lower = time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
  uint64_t upper = time_table.get_upper(txn->get_thd_id(),txn->get_txn_id());
  if(lower >= upper) {
    time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),MAAT_VALIDATED);
    rc = Abort;
  } else {
    time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),MAAT_COMMITTED);
    // TODO: can commit_time be selected in a smarter way?
    txn->set_start_timestamp(lower);
  }
  uint64_t cts = time_table.get_cts(txn->get_thd_id(),txn->get_txn_id());
  txn->set_commit_timestamp(cts + 1);
  DEBUG("PSI Bound %ld: %d [%lu,%lu] %lu\n", txn->get_txn_id(), rc, lower, upper,
        txn->commit_timestamp);
  return rc;
}

RC psi::handle_conflict_txn_4b(TxnManager * txn) {
  uint64_t start_time = get_sys_clock();
  uint64_t timespan;
  // sem_wait(&_semaphore);

  timespan = get_sys_clock() - start_time;
  txn->txn_stats.cc_block_time += timespan;
  txn->txn_stats.cc_block_time_short += timespan;
  INC_STATS(txn->get_thd_id(),maat_cs_wait_time,timespan);
  start_time = get_sys_clock();
  RC rc = RCOK;

  
  for (auto it = txn->reads_before->begin(); it != txn->reads_before->end(); ++it) {
    uint64_t txn_id = *it;
    // 处理Ti->Tj
    if (IS_LOCAL(txn_id)) {
      sem_wait(&ad_table._semaphore);
      ad_table.reverse_table[txn_id].push_back(txn->get_txn_id());
      sem_post(&ad_table._semaphore);
      uint64_t upper = time_table.get_upper(txn->get_thd_id(),txn_id);
      if (upper >= txn->get_commit_timestamp()) {
        time_table.set_upper(txn->get_thd_id(),txn_id,txn->get_commit_timestamp()-1);
        DEBUG("PSI forward val set upper %ld: %lu\n",txn_id,txn->get_commit_timestamp()-1);
      }
    }
  }

  for (auto it = txn->writes_after->begin(); it != txn->writes_after->end(); ++it) {
    uint64_t txn_id = *it;
    // 处理Tj->Tk
    if (IS_LOCAL(txn_id)) {
      uint64_t cts = time_table.get_cts(txn->get_thd_id(),txn_id);
      if (cts <= txn->get_start_timestamp()) {
        time_table.set_cts(txn->get_thd_id(),txn_id,txn->get_start_timestamp()+1);
        DEBUG("PSI forward val set cts %ld: %lu\n",txn_id,txn->get_start_timestamp()+1);
      }
    }
  }
}

RC psi::finish(RC rc, TxnManager * txn) {
  uint64_t start_time = get_sys_clock();
  uint64_t timespan;
  // sem_wait(&_semaphore);
  for (int i = 0; i < txn->get_access_cnt(); i++) {
    Access * access = txn->get_access(i);
    if (rc == RCOK) {
      RC rc = access->orig_row->manager->commit(access->type, txn, access->data, access->pversion);
    } else {
      RC rc = access->orig_row->manager->abort(access->type, txn, access->pversion);
    }
  }
  return rc;
}
#endif