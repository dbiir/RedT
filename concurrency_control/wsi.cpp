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
#include "wsi.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_wsi.h"


wsi_set_ent::wsi_set_ent() {
	set_size = 0;
	txn = NULL;
	rows = NULL;
	next = NULL;
}

void wsi::init() {
  	sem_init(&_semaphore, 0, 1);
}

RC wsi::validate(TxnManager * txn) {
	RC rc;
	// uint64_t starttime = get_sys_clock();
	rc = central_validate(txn);
  	// INC_STATS(txn->get_thd_id(),wsi_validate_time,get_sys_clock() - starttime);
	return rc;
}

RC wsi::central_validate(TxnManager * txn) {
	RC rc;
	// uint64_t starttime = get_sys_clock();
	// uint64_t total_starttime = starttime;
#if CC_ALG == WSI
	uint64_t start_tn = txn->get_start_timestamp();
#endif
	bool valid = true;

	wsi_set_ent * wset;
	wsi_set_ent * rset;
	get_rw_set(txn, rset, wset);
	bool readonly = (wset->set_size == 0);

  	int stop __attribute__((unused));
	uint64_t checked = 0;
	//pthread_mutex_lock( &latch );
	sem_wait(&_semaphore);
	// INC_STATS(txn->get_thd_id(),wsi_cs_wait_time,get_sys_clock() - starttime);
	// starttime = get_sys_clock();

	if (!readonly) {
		for (UInt32 i = 0; i < rset->set_size; i++) {
			checked++;
#if CC_ALG == WSI
			if (rset->rows[i]->manager->get_last_commit() > start_tn)
				rc = Abort;
#endif
		}
	}
	// INC_STATS(txn->get_thd_id(),wsi_validate_time,get_sys_clock() - starttime);
	sem_post(&_semaphore);
	// starttime = get_sys_clock();
	/*
	if (valid)
		txn->cleanup(RCOK);
    */
	mem_allocator.free(rset->rows, sizeof(row_t *) * rset->set_size);
	mem_allocator.free(rset, sizeof(wsi_set_ent));
	//mem_allocator.free(finish_active, sizeof(wsi_set_ent*)* f_active_len);


	if (valid) {
		rc = RCOK;
    	// INC_STATS(txn->get_thd_id(),wsi_check_cnt,checked);
	} else {
		//txn->cleanup(Abort);
    	// INC_STATS(txn->get_thd_id(),wsi_abort_check_cnt,checked);
		rc = Abort;
	}
	DEBUG("End Validation %ld\n",txn->get_txn_id());
	// INC_STATS(txn->get_thd_id(),wsi_validate_time,get_sys_clock() - total_starttime);
	return rc;
}

void wsi::finish(RC rc, TxnManager * txn) {
	central_finish(rc,txn);
}

void wsi::central_finish(RC rc, TxnManager * txn) {
	wsi_set_ent * wset;
	wsi_set_ent * rset;
	get_rw_set(txn, rset, wset);
#if CC_ALG == WSI
	for (UInt32 i = 0; i < rset->set_size; i++) {
		rset->rows[i]->manager->update_last_commit(txn->get_commit_timestamp());
	}
#endif

}

void wsi::gene_finish_ts(TxnManager * txn) {
	txn->set_commit_timestamp(glob_manager.get_ts(txn->get_thd_id()));
}

RC wsi::get_rw_set(TxnManager * txn, wsi_set_ent * &rset, wsi_set_ent *& wset) {
	wset = (wsi_set_ent*) mem_allocator.alloc(sizeof(wsi_set_ent));
	rset = (wsi_set_ent*) mem_allocator.alloc(sizeof(wsi_set_ent));
	wset->set_size = txn->get_write_set_size();
	rset->set_size = txn->get_read_set_size();
	wset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * wset->set_size);
	rset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * rset->set_size);
	wset->txn = txn;
	rset->txn = txn;

	UInt32 n = 0, m = 0;
	for (uint64_t i = 0; i < wset->set_size + rset->set_size; i++) {
		if (txn->get_access_type(i) == WR)
			wset->rows[n ++] = txn->get_access_original_row(i);
		else
			rset->rows[m ++] = txn->get_access_original_row(i);
	}

	assert(n == wset->set_size);
	assert(m == rset->set_size);
	return RCOK;
}
