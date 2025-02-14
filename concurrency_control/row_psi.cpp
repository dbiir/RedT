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
#include "row_psi.h"
#include "mem_alloc.h"
#include "manager.h"
#include "helper.h"
#include "psi.h"
#include "maat.h"

#if CC_ALG == PSI
void Row_psi::init(row_t * row) {
	_row = row;
  versions = new std::vector<PSIVersion*>();
  PSIVersion *version = new PSIVersion(0, row, 0);
  versions->insert(versions->begin(), version);
  psi_avail = true;
}

RC Row_psi::access(access_t type, TxnManager * txn, PSIVersion * &version) {
  uint64_t starttime = get_sys_clock();
  RC rc = RCOK;
#if WORKLOAD == TPCC
  rc = read(txn,version);
  // prewrite(txn,version);
#else
  // if (type == RD) 
  rc = read(txn,version);
  // if (type == WR) prewrite(txn,version);
#endif
  uint64_t timespan = get_sys_clock() - starttime;
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;
  return rc;
}

RC Row_psi::read(TxnManager * txn, PSIVersion * &target_version) {
	assert (CC_ALG == PSI);
	RC rc = RCOK;

  uint64_t mtx_wait_starttime = get_sys_clock();
  while (!ATOM_CAS(psi_avail, true, false)) {
  }
  INC_STATS(txn->get_thd_id(),mtx[30],get_sys_clock() - mtx_wait_starttime);
  INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - mtx_wait_starttime);
  DEBUG_P("READ %ld -- %ld\n", txn->get_txn_id(), _row->get_primary_key());
  target_version = NULL;
  for (auto it = versions->begin(); it != versions->end(); it++) {
    // 检查数据项的提交时间戳是否满足当前事务的开始时间戳
    PSIVersion *version = *it;
    uint64_t upper = time_table.get_upper(txn->get_thd_id(), txn->get_txn_id());
    if (version->cid > upper) {
      DEBUG_P("row_psi.cpp:66, txn %ld Read %ld, version %ld / %ld not valid\n", txn->get_txn_id(), _row->get_primary_key(), version->cid, upper);
      continue;
    }
    else {
      // Found a version that is committed before this txn's start timestamp
      // Copy the value to the txn's read set
      target_version = version;
      break;
    }
  }
  if (target_version == NULL) {
    // No committed version found
    // Copy the latest version to the txn's read set
    DEBUG_P("Abort at row_psi.cpp:78, txn %ld Read %ld, no committed version found\n", txn->get_txn_id(), _row->get_primary_key());
    rc = Abort;
  } else {
    // Found a committed version
    // Copy the value to the txn's read set
    ts_t now_lb = time_table.get_lower(txn->get_thd_id(), txn->get_txn_id());
    ts_t newlb = now_lb > target_version->cid ? now_lb : target_version->cid;

    ts_t now_cts = time_table.get_cts(txn->get_thd_id(), txn->get_txn_id());
    ts_t newc = now_cts > target_version->cid ? now_cts : target_version->cid;
    newc = newc > target_version->sid ? newc : target_version->sid;
    time_table.set_lower(txn->get_thd_id(), txn->get_txn_id(), newlb);
    time_table.set_cts(txn->get_thd_id(), txn->get_txn_id(), newc);

    target_version->visitor_list->insert(txn->get_txn_id());
    
  }

  ATOM_CAS(psi_avail,false,true);
  assert(rc != RCOK || target_version != NULL);

	return rc;
}

RC Row_psi::prewrite(TxnManager * txn, PSIVersion * &target_version) {
	assert (CC_ALG == PSI);
	RC rc = RCOK;

  uint64_t mtx_wait_starttime = get_sys_clock();
  while (!ATOM_CAS(psi_avail, true, false)) {
  }
  INC_STATS(txn->get_thd_id(),mtx[31],get_sys_clock() - mtx_wait_starttime);
  INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - mtx_wait_starttime);
  DEBUG_P("PREWRITE %ld -- %ld\n", txn->get_txn_id(), _row->get_primary_key());
  target_version = NULL;
  // 先尝试加写锁
  if (wlock != 0) {
    // 写锁已经被占用
    rc = Abort;
    DEBUG_P("Abort at row_psi.cpp:117, txn %ld Write %ld, cannot get lock %ld\n", txn->get_txn_id(), _row->get_primary_key(), wlock);
    // goto PREWRITE_END;
  } else {
    // 写锁未被占用
    wlock = txn->get_txn_id();

    auto it = versions->begin();
    PSIVersion* version = (*it);
    uint64_t upper = time_table.get_upper(txn->get_thd_id(), txn->get_txn_id());
    if (version->cid > upper) {
      // 事务的开始时间戳小于数据项的提交时间戳，回滚
      rc = Abort;
      wlock = 0;
      DEBUG_P("Abort at row_psi.cpp:131, txn %ld Write %ld, version not valid\n", txn->get_txn_id(), _row->get_primary_key());
      // goto PREWRITE_END;
    } else {
      ts_t now_lb = time_table.get_lower(txn->get_thd_id(), txn->get_txn_id());
      ts_t newlb = now_lb > version->cid ? now_lb : version->cid;

      ts_t now_cts = time_table.get_cts(txn->get_thd_id(), txn->get_txn_id());
      ts_t newc = now_cts > version->cid ? now_cts : version->cid;
      newc = newc > version->sid ? newc : version->sid;
      time_table.set_lower(txn->get_thd_id(), txn->get_txn_id(), newlb);
      time_table.set_cts(txn->get_thd_id(), txn->get_txn_id(), newc);

      version->visitor_list->insert(txn->get_txn_id());
      target_version = version;
    }
  }
// PREWRITE_END:
  ATOM_CAS(psi_avail,false,true);
  assert(rc != RCOK || target_version != NULL);

	return rc;
}

RC Row_psi::commit(access_t type, TxnManager * txn, row_t * data, PSIVersion * orig_version) {
  uint64_t mtx_wait_starttime = get_sys_clock();
  while (!ATOM_CAS(psi_avail, true, false)) {
  }
  INC_STATS(txn->get_thd_id(),mtx[33],get_sys_clock() - mtx_wait_starttime);
  DEBUG_P("PSI Commit %ld: %d,%lu -- %ld\n", txn->get_txn_id(), type, txn->get_commit_timestamp(),
        _row->get_primary_key());

  if(type == WR) {
    // Apply write to DB
    PSIVersion* newversion = new PSIVersion(txn->get_commit_timestamp(), data, txn->get_txn_id());
    // versions->push_forward(newversion);
    versions->insert(versions->begin(), newversion);
  } 
  if (wlock == txn->get_txn_id()) {
    wlock = 0;
  }
  orig_version->visitor_list->erase(txn->get_txn_id());
  uint64_t lower = time_table.get_lower(txn->get_thd_id(), txn->get_txn_id());
  orig_version->sid = lower > orig_version->sid ? lower : orig_version->sid;


  ATOM_CAS(psi_avail,false,true);

  return RCOK;
}

RC Row_psi::abort(access_t type, TxnManager * txn, PSIVersion * orig_version) {
  uint64_t mtx_wait_starttime = get_sys_clock();
  while (!ATOM_CAS(psi_avail, true, false)) {
  }
  INC_STATS(txn->get_thd_id(),mtx[32],get_sys_clock() - mtx_wait_starttime);
  DEBUG_P("PSI Abort %ld: %d -- %ld\n",txn->get_txn_id(),type,_row->get_primary_key());
  orig_version->visitor_list->erase(txn->get_txn_id());
  if (wlock == txn->get_txn_id()) {
    wlock = 0;
  }
  ATOM_CAS(psi_avail,false,true);
  return Abort;
}


#endif