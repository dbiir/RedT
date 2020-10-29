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
#include "focc.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_occ.h"


f_set_ent::f_set_ent() {
	set_size = 0;
	txn = NULL;
	rows = NULL;
	next = NULL;
}

void Focc::init() {
  	sem_init(&_semaphore, 0, 1);
	// sem_init(&_active_semaphore, 0, 1);

	tnc = 0;
	active_len = 0;
	active = NULL;
	lock_all = false;
}

RC Focc::validate(TxnManager * txn) {
	RC rc;
  	uint64_t starttime = get_sys_clock();
	rc = central_validate(txn);
  	INC_STATS(txn->get_thd_id(),occ_validate_time,get_sys_clock() - starttime);
	return rc;
}

void Focc::finish(RC rc, TxnManager * txn) {
	central_finish(rc,txn);
}

void Focc::active_storage(access_t type, TxnManager * txn, Access * access) {
	f_set_ent * act = active;
	// f_set_ent * prev = NULL;
	f_set_ent * wset;
	f_set_ent * rset;
	get_rw_set(txn, rset, wset);
	if (rset->set_size == 0)
		return;
	sem_wait(&_semaphore);
	while (act != NULL && act->txn != txn) {
		// prev = act;
		act = act->next;
	}
	if (act == NULL) {
		active_len ++;
		STACK_PUSH(active, rset);
	} else {
		act = rset;
	}
	sem_post(&_semaphore);
	DEBUG("FOCC active_storage %ld: active size %lu\n",txn->get_txn_id(),active_len);
}

RC Focc::central_validate(TxnManager * txn) {
	RC rc;
	uint64_t starttime = get_sys_clock();
	uint64_t total_starttime = starttime;
	uint64_t start_tn = txn->get_start_timestamp();

	// uint64_t f_active_len;
	// int n = 0;
	bool valid = true;
	// OptCC is centralized. No need to do per partition malloc.
	f_set_ent * wset;
	f_set_ent * rset;
	get_rw_set(txn, rset, wset);
	bool readonly = (wset->set_size == 0);
	f_set_ent * ent;
	uint64_t checked = 0;
	uint64_t active_checked = 0;
  	int stop __attribute__((unused));

	//pthread_mutex_lock( &latch );
	DEBUG("Start Validation %ld: start_ts %lu, active size %lu\n",txn->get_txn_id(),start_tn,active_len);
	sem_wait(&_semaphore);
	INC_STATS(txn->get_thd_id(),occ_cs_wait_time,get_sys_clock() - starttime);
	starttime = get_sys_clock();

	ent = active;
	// f_active_len = active_len;
	// f_set_ent * finish_active[f_active_len];
	//finish_active = (f_set_ent**) mem_allocator.alloc(sizeof(f_set_ent *) * f_active_len);
	// while (ent != NULL) {
	// 	finish_active[n++] = ent;
	// 	ent = ent->next;
	// }
	//pthread_mutex_unlock( &latch );

	INC_STATS(txn->get_thd_id(),occ_hist_validate_time,get_sys_clock() - starttime);
	starttime = get_sys_clock();
	stop = 1;
	if ( !readonly ) {
		// for (UInt32 i = 0; i < f_active_len; i++) {
		for (ent = active; ent != NULL; ent = ent->next) {
			// f_set_ent * ract = finish_active[i];
			f_set_ent * ract = ent;
			++checked;
			++active_checked;
			valid = test_valid(ract, wset);
			if (valid) {
				++checked;
				++active_checked;
			}
			if (!valid) {
				INC_STATS(txn->get_thd_id(),occ_act_validate_fail_time,get_sys_clock() - starttime);
				goto final;
			}
		}
	}
	INC_STATS(txn->get_thd_id(),occ_act_validate_time,get_sys_clock() - starttime);
	starttime = get_sys_clock();
final:
  /*
	if (valid)
		txn->cleanup(RCOK);
    */
	mem_allocator.free(rset->rows, sizeof(row_t *) * rset->set_size);
	mem_allocator.free(rset, sizeof(f_set_ent));
	//mem_allocator.free(finish_active, sizeof(f_set_ent*)* f_active_len);


	if (valid) {
		rc = RCOK;
    	INC_STATS(txn->get_thd_id(),occ_check_cnt,checked);
	} else {
		//txn->cleanup(Abort);
    	INC_STATS(txn->get_thd_id(),occ_abort_check_cnt,checked);
		rc = Abort;
    	// Optimization: If this is aborting, remove from active set now
        f_set_ent * act = active;
        f_set_ent * prev = NULL;
        while (act != NULL && act->txn != txn) {
          prev = act;
          act = act->next;
        }
        if(act != NULL && act->txn == txn) {
          if (prev != NULL)
            prev->next = act->next;
          else
            active = act->next;
          active_len --;
        }
	}
		sem_post(&_semaphore);
	DEBUG("End Validation %ld: active# %ld\n",txn->get_txn_id(),active_checked);
	INC_STATS(txn->get_thd_id(),occ_validate_time,get_sys_clock() - total_starttime);
	return rc;
}

void Focc::central_finish(RC rc, TxnManager * txn) {
	f_set_ent * wset;
	f_set_ent * rset;
	get_rw_set(txn, rset, wset);

	// only update active & tnc for non-readonly transactions
	uint64_t starttime = get_sys_clock();
	//		pthread_mutex_lock( &latch );
	f_set_ent * act = active;
	f_set_ent * prev = NULL;
	sem_wait(&_semaphore);
	while (act != NULL && act->txn != txn) {
		prev = act;
		act = act->next;
	}
	if(act == NULL) {
		// assert(rc == Abort);
		//pthread_mutex_unlock( &latch );
		sem_post(&_semaphore);
		return;
	}
	assert(act->txn == txn);
	if (prev != NULL)
		prev->next = act->next;
	else
		active = act->next;
	active_len --;
	//	pthread_mutex_unlock( &latch );
	sem_post(&_semaphore);
	INC_STATS(txn->get_thd_id(),occ_finish_time,get_sys_clock() - starttime);

}

RC Focc::get_rw_set(TxnManager * txn, f_set_ent * &rset, f_set_ent *& wset) {
	wset = (f_set_ent*) mem_allocator.alloc(sizeof(f_set_ent));
	rset = (f_set_ent*) mem_allocator.alloc(sizeof(f_set_ent));
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

bool Focc::test_valid(f_set_ent * set1, f_set_ent * set2) {
	for (UInt32 i = 0; i < set1->set_size; i++)
		for (UInt32 j = 0; j < set2->set_size; j++) {
			if (set1->txn == set2->txn)
				continue;
			if (set1->rows[i] == set2->rows[j]) {
				return false;
			}
		}
	return true;
}
