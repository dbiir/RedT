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

#include "row_null.h"

#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "txn.h"

void Row_null::init(row_t* row) {
  _row = row;
  blatch = false;
  latch = (pthread_mutex_t*)mem_allocator.alloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(latch, NULL);
}

RC Row_null::access(access_t type, TxnManager* txn) {
  uint64_t starttime = get_sys_clock();
  RC rc = RCOK;
  // rc = read_and_write(type, txn, row, version);

  uint64_t timespan = get_sys_clock() - starttime;
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;
  return rc;
}

RC Row_null::abort(access_t type, TxnManager* txn) {
  return Abort;
}


RC Row_null::commit(access_t type, TxnManager* txn) {
  return RCOK;
}
