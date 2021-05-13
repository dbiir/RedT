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

#include "txn.h"
#include "row.h"
#include "row_occ.h"
#include "mem_alloc.h"
#define MUTX
void Row_occ::init(row_t *row) {
	_row = row;
  _latch = (pthread_mutex_t *)mem_allocator.alloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(_latch, NULL);
  sem_init(&_semaphore, 0, 1);
	wts = 0;
	blatch = false;
}

RC Row_occ::access(TxnManager *txn, TsType type) {
	RC rc = RCOK;
	uint64_t starttime = get_sys_clock();
#ifdef MUTX
	pthread_mutex_lock( _latch );
#else
  sem_wait(&_semaphore);
#endif
	INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
	if (type == R_REQ) {
		if (txn->get_start_timestamp() < wts) {
      		INC_STATS(txn->get_thd_id(),occ_ts_abort_cnt,1);
			rc = Abort;
    } else {
			txn->cur_row->copy(_row);
			rc = RCOK;
		}
	} else
		assert(false);
#ifdef MUTX
	pthread_mutex_unlock( _latch );
#else
  sem_post(&_semaphore);
#endif
  uint64_t timespan = get_sys_clock() - starttime;
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;
	return rc;
}

void Row_occ::latch() {
#ifdef MUTX
	pthread_mutex_lock( _latch );
#else
  sem_wait(&_semaphore);
#endif
}

bool Row_occ::validate(uint64_t ts) {
  if (ts < wts)
    return false;
  else
    return true;
}

void Row_occ::write(row_t *data, uint64_t ts) {
	_row->copy(data);
	if (PER_ROW_VALID) {
		assert(ts > wts);
		wts = ts;
	}
}

void Row_occ::release() {
#ifdef MUTX
	pthread_mutex_unlock( _latch );
#else
  sem_post(&_semaphore);
#endif
}
