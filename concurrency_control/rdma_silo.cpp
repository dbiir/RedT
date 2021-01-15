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
#include "config.h"
#if RMDA_SILO_OPEN

#include "helper.h"
#include "txn.h"
#include "row.h"
#include "rdma_silo.h"
#include "manager.h"
#include "mem_alloc.h"

RC RDMA_silo::get_and_sort_rw_set(Transaction * txn, int * &read_set, int *& write_set) {

	RC rc = RCOK;
	// lock write tuples in the primary key order.
	uint64_t wr_cnt = txn->->write_cnt;
	write_set = (int *) mem_allocator.alloc(sizeof(int) * wr_cnt);
	read_set = (int *) mem_allocator.alloc(sizeof(int) * wr_cnt);

	int cur_wr_idx = 0;

	int read_set[txn->row_cnt - txn->write_cnt];
	int cur_rd_idx = 0;
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			write_set[cur_wr_idx ++] = rid;
		else 
			read_set[cur_rd_idx ++] = rid;
	}

	// bubble sort the write_set, in primary key order 
	if (wr_cnt > 1)
	{
		for (uint64_t i = wr_cnt - 1; i >= 1; i--) {
			for (uint64_t j = 0; j < i; j++) {
				if (txn->accesses[ write_set[j] ]->orig_row->get_primary_key() > 
					txn->accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
				{
					int tmp = write_set[j];
					write_set[j] = write_set[j+1];
					write_set[j+1] = tmp;
				}
			}
		}
	}

	return RCOK;
}

RDMA_silo::validate(TxnManager * txn)
{
	RC rc = RCOK;
	silo_set_ent * wset;
	silo_set_ent * rset;
	// lock write tuples in the primary key order.
	uint64_t wr_cnt = txn->write_cnt;

	int cur_wr_idx = 0;
	int read_set[txn->row_cnt - txn->write_cnt];
	int cur_rd_idx = 0;
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			write_set[cur_wr_idx ++] = rid;
		else 
			read_set[cur_rd_idx ++] = rid;
	}

	// bubble sort the write_set, in primary key order 
	if (wr_cnt > 1)
	{
		for (uint64_t i = wr_cnt - 1; i >= 1; i--) {
			for (uint64_t j = 0; j < i; j++) {
				if (txn->accesses[ write_set[j] ]->orig_row->get_primary_key() > 
					txn->accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
				{
					int tmp = write_set[j];
					write_set[j] = write_set[j+1];
					write_set[j+1] = tmp;
				}
			}
		}
	}

	num_locks = 0;
	ts_t max_tid = 0;
	bool done = false;
	if (_pre_abort) {
		for (uint64_t i = 0; i < wr_cnt; i++) {
			row_t * row = txn->accesses[ write_set[i] ]->orig_row;
			if (row->manager->get_tid() != txn->accesses[write_set[i]]->tid) {
				rc = Abort;
				return rc;
			}	
		}	
		for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i ++) {
			Access * access = txn->accesses[ read_set[i] ];
			if (access->orig_row->manager->get_tid() != txn->accesses[read_set[i]]->tid) {
				rc = Abort;
				return rc;
			}
		}
	}

	// lock all rows in the write set.
	if (_validation_no_wait) {
		while (!done) {
			num_locks = 0;
			for (uint64_t i = 0; i < wr_cnt; i++) {
				row_t * row = txn->accesses[ write_set[i] ]->orig_row;
				if (!row->manager->try_lock())
				{
					// rc = Abort;
					// return rc;
					break;
				}
				DEBUG("silo %ld write lock row %ld \n", this->get_txn_id(), row->get_primary_key());
				row->manager->assert_lock();
				num_locks ++;
				if (row->manager->get_tid() != txn->accesses[write_set[i]]->tid)
				{
					rc = Abort;
					return rc;
				}
			}
			if (num_locks == wr_cnt) {
				DEBUG("TRY LOCK true %ld\n", get_txn_id());
				done = true;
			} else {
				rc = Abort;
				return rc;

				// for (uint64_t i = 0; i < num_locks; i++)
				// {
				// 	txn->accesses[ write_set[i] ]->orig_row->manager->release();
				// }
				// if (_pre_abort) {
				// 	num_locks = 0;
				// 	for (uint64_t i = 0; i < wr_cnt; i++) {
				// 		row_t * row = txn->accesses[ write_set[i] ]->orig_row;
				// 		if (row->manager->get_tid() != txn->accesses[write_set[i]]->tid) {
				// 			rc = Abort;
				// 			return rc;
				// 		}	
				// 	}	
				// 	for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i ++) {
				// 		Access * access = txn->accesses[ read_set[i] ];
				// 		if (access->orig_row->manager->get_tid() != txn->accesses[read_set[i]]->tid) {
				// 			rc = Abort;
				// 			return rc;
				// 		}
				// 	}
				// }
                // PAUSE_SILO
			}
		}
	} else {
		for (uint64_t i = 0; i < wr_cnt; i++) {
			row_t * row = txn->accesses[ write_set[i] ]->orig_row;
			row->manager->lock();
			DEBUG("silo %ld write lock row %ld \n", this->get_txn_id(), row->get_primary_key());
			num_locks++;
			if (row->manager->get_tid() != txn->accesses[write_set[i]]->tid) {
				rc = Abort;
				return rc;
			}
		}
	}

	COMPILER_BARRIER

	// validate rows in the read set
	// for repeatable_read, no need to validate the read set.
	for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i ++) {
		Access * access = txn->accesses[ read_set[i] ];
		bool success = access->orig_row->manager->validate(access->tid, false);
		if (!success) {
			rc = Abort;
			return rc;
		}
		if (access->tid > max_tid)
			max_tid = access->tid;
	}
	// validate rows in the write set
	for (uint64_t i = 0; i < wr_cnt; i++) {
		Access * access = txn->accesses[ write_set[i] ];
		bool success = access->orig_row->manager->validate(access->tid, true);
		if (!success) {
			rc = Abort;
			return rc;
		}
		if (access->tid > max_tid)
			max_tid = access->tid;
	}

	this->max_tid = max_tid;

	return rc;
}

RC
TxnManager::finish(RC rc)
{
	if (rc == Abort) {
		if (this->num_locks > get_access_cnt()) 
			return rc;
		// DEBUG("silo abort finish %ld", txn->get_txn_id());
		for (uint64_t i = 0; i < this->num_locks; i++) {
			txn->accesses[ write_set[i] ]->orig_row->manager->release();
			DEBUG("silo %ld abort release row %ld \n", this->get_txn_id(), txn->accesses[ write_set[i] ]->orig_row->get_primary_key());
		}
	} else {
		
		for (uint64_t i = 0; i < txn->write_cnt; i++) {
			Access * access = txn->accesses[ write_set[i] ];
			access->orig_row->manager->write( 
				access->data, this->commit_timestamp );
			txn->accesses[ write_set[i] ]->orig_row->manager->release();
			DEBUG("silo %ld commit release row %ld \n", this->get_txn_id(), txn->accesses[ write_set[i] ]->orig_row->get_primary_key());
		}
	}
	num_locks = 0;
	memset(write_set, 0, 100);
	// mem_allocator.free(write_set, sizeof(int) * txn->write_cnt);
	return rc;
}

RC
TxnManager::find_tid_silo(ts_t max_tid)
{
	if (max_tid > _cur_tid)
		_cur_tid = max_tid;
	return RCOK;
}

#endif