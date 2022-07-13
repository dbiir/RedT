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

//#if RMDA_SILO_OPEN
#include "manager.h"
#include "helper.h"
#include "txn.h"
#include "row.h"
#include "rdma_mocc.h"
#include "mem_alloc.h"
#include "lib.hh"
#include "qps/op.hh"
#include "transport/rdma.h"
#include "row_rdma_mocc.h"
#include "src/rdma/sop.hh"

#if CC_ALG == RDMA_MOCC

RC RDMA_mocc::validate_rdma_mocc(yield_func_t &yield, TxnManager * txnMng, uint64_t cor_id)
{   
	Transaction *txn = txnMng->txn;
	RC rc = RCOK;
	// lock write tuples in the primary key order.
	uint64_t wr_cnt = txn->write_cnt;

	int cur_wr_idx = 0;
  	//int read_set[10];
	int read_set[txn->row_cnt - txn->write_cnt];
	int cur_rd_idx = 0;
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[cur_wr_idx ++] = rid;
		else
			read_set[cur_rd_idx ++] = rid;
	}

	// bubble sort the write_set, in primary key order
	if (wr_cnt > 1)
	{
		for (uint64_t i = wr_cnt - 1; i >= 1; i--) {
			for (uint64_t j = 0; j < i; j++) {
				if (txn->accesses[ txnMng->write_set[j] ]->orig_row->get_primary_key() >
					txn->accesses[ txnMng->write_set[j + 1] ]->orig_row->get_primary_key())
				{
					int tmp = txnMng->write_set[j];
					txnMng->write_set[j] = txnMng->write_set[j+1];
					txnMng->write_set[j+1] = tmp;
				}
			}
		}
	}

	txnMng->num_locks = 0;
	ts_t max_tid = 0;
	bool done = false;

	if (wr_cnt != 0) {
		// uint64_t i = 0;
		for ( uint64_t i = 0; i < wr_cnt; i++) {
			int retry_count = 0;
			while (!done && !simulation->is_done()) {
				row_t * row = txn->accesses[ txnMng->write_set[i] ]->orig_row;
				// printf("write_set have lock:%ld\n", row->get_primary_key());
				set<uint64_t>::iterator iter;
				if((iter = txnMng->lock_set.find(row->get_primary_key())) != txnMng->lock_set.end())
				{
					// printf("write_set have lock:%ld\n", row->get_primary_key());
					// printf("write_set have lock\n");
					done = true;
					continue;
				} else {
					if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){//lock in local
						if (!row->manager->try_lock(yield, txnMng,txnMng->write_set[i], cor_id))
						{
							INC_STATS(txnMng->get_thd_id(), local_try_lock_fail_abort, 1); //17%
							INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
							done = false;
							retry_count++;
							if (retry_count < MOCC_MAX_RETRY_COUNT) continue;
							return Abort;
						}
						DEBUG("silo %ld write lock row %ld \n", txnMng->get_txn_id(), row->get_primary_key());
					}
					else{//lock remote
						// printf("silo %ld write lock row %ld \n", txnMng->get_txn_id(), row->get_primary_key());
						RC lock_result = remote_try_lock(yield, txnMng, txnMng->write_set[i], cor_id);
						if(lock_result == WAIT) {
							INC_STATS(txnMng->get_thd_id(), remote_lock_fail_abort, 1); //12%
							INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
							done = false;
							retry_count++;
							if (retry_count < MOCC_MAX_RETRY_COUNT) continue;
							return Abort;
						} else if (lock_result == Abort) {
							return Abort;
						}
					}
				}
				done = true;
			}
		}
	}

	// COMPILER_BARRIER

	// validate rows in the read set
	// for repeatable_read, no need to validate the read set.
	for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i ++) {
		bool success = false;
		Access * access = txn->accesses[ read_set[i] ];
		if(txn->accesses[read_set[i]]->location == g_node_id){//local
		    // success = access->orig_row->manager->validate(access->tid, false);
			success = access->orig_row->manager->validate(txnMng->get_txn_id(), access->timestamp,false);
			if(!success){
				 INC_STATS(txnMng->get_thd_id(), local_readset_validate_fail_abort, 1); //48%
				 INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
			}
       	}
		else{//remote
        	success = validate_rw_remote(yield, txnMng , read_set[i], cor_id);
			if(success)DEBUG("silo %ld validate read row %ld success \n",txnMng->get_txn_id(),access->key);
			if(!success){
				 INC_STATS(txnMng->get_thd_id(), remote_readset_validate_fail_abort, 1);//32%
				 INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
				 DEBUG("silo %ld validate read row %ld fail \n",txnMng->get_txn_id(),access->key);
			}

		}

		if (!success) {
			rc = Abort;
			return rc;
		}
		if (access->tid > max_tid)
			max_tid = access->tid;
	}
	// validate rows in the write set
	for (uint64_t i = 0; i < wr_cnt; i++) {
		bool success = false;
		Access * access = txn->accesses[ txnMng->write_set[i] ];
		if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){//local
			// success = access->orig_row->manager->validate(access->tid, true);
			success = access->orig_row->manager->validate(txnMng->get_txn_id(),access->timestamp, true);
			// success = true;
      		if (!success){
				  INC_STATS(txnMng->get_thd_id(), local_writeset_validate_fail_abort, 1);
				  INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
			  }
		}
		else{//remote
      		success = validate_rw_remote(yield, txnMng,txnMng->write_set[i], cor_id);
			// success = true;
			if(success)DEBUG("silo %ld validate write row %ld success \n",txnMng->get_txn_id(),access->key);
			if(!success){
				 INC_STATS(txnMng->get_thd_id(), remote_writeset_validate_fail_abort, 1);
				 INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
				 DEBUG("silo %ld validate write row %ld fail\n",txnMng->get_txn_id(),access->key);
			}

		}

		if (!success) {
			rc = Abort;
			return rc;
		}
		if (access->tid > max_tid)
			max_tid = access->tid;
	}

	txnMng->max_tid = max_tid;

	return rc;
}


RC RDMA_mocc::remote_try_lock(yield_func_t &yield,TxnManager * txnMng , uint64_t num, uint64_t cor_id) {
	RC result = RCOK;
	Transaction *txn = txnMng->txn;

  	uint64_t remote_offset = txn->accesses[num]->offset;
	uint64_t thd_id = txnMng->get_thd_id() + cor_id * g_thread_cnt;
	uint64_t lock = txnMng->get_txn_id();
	
	// printf("lock the key %ld in %ld\n", txn->accesses[num]->orig_row->get_primary_key(), remote_offset);
    uint64_t try_lock = -1;
	uint64_t loc = txn->accesses[num]->location;
	uint64_t lock_type = 0;
	bool conflict = false;
	// uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	// auto mr = client_rm_handler->get_reg_attr().value();
	uint64_t tts = txnMng->get_timestamp();
	try_lock = txnMng->cas_remote_content(yield,loc,remote_offset,0,lock,cor_id);
	// _row->_tid_word = lock; 
	if(try_lock != 0) {
		// printf("cas fault!\n");
		return WAIT;
	}
	row_t * test_row = txnMng->read_remote_row(yield,loc,remote_offset,cor_id);
	// printf("read remote lock lower:%ld, local key: %ld\n",test_row->lock_owner[0], txn->accesses[num]->orig_row->get_primary_key());
	// if(row_t::get_row_size(test_row->tuple_size) > ROW_DEFAULT_SIZE) {
	// 	test_row->_tid_word = 0;
	// 	assert(txnMng->write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), remote_offset,(char*)test_row, cor_id) == true);
	// 	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	// 	return Abort;
	// }
	lock_type = test_row->lock_type;
	if(lock_type == 0) {
		test_row->lock_owner[0] = txnMng->get_txn_id();
		test_row->ts[0] = tts;
		test_row->lock_type = 1;
		// printf("lock the key %ld in %ld\n", txn->accesses[num]->orig_row->get_primary_key(),  row_t::get_row_size(test_row->tuple_size));
		test_row->_tid_word = 0; 
		assert(txnMng->write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), remote_offset,(char*)test_row, cor_id) == true);
		mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
		txnMng->lock_set.insert(txn->accesses[num]->orig_row->get_primary_key());
		txnMng->num_locks ++;
		result = RCOK;
		return result;
	} else if(lock_type == 1) {
		conflict = true;
		if(conflict) {
			uint64_t i = 0;
			for(i = 0; i < LOCK_LENGTH; i++) {
				if((tts > test_row->ts[i] && test_row->ts[i] != 0)) {
					test_row->is_hot++;
					test_row->_tid_word = 0;
					assert(txnMng->write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), remote_offset,(char*)test_row, cor_id) == true);
					mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					return Abort;
				}
				// if(_row->ts[0] == 0) break;
			}
			// total_num_atomic_retry++;
			// txn->num_atomic_retry++;
			// if(txn->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txn->num_atomic_retry;
			//sleep(1);
			test_row->is_hot++;
			test_row->_tid_word = 0;
			assert(txnMng->write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), remote_offset,(char*)test_row, cor_id) == true);
			mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			return Abort;
		}
	} else {
		test_row->_tid_word = 0;
		test_row->is_hot++;
		assert(txnMng->write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), remote_offset,(char*)test_row, cor_id) == true);
		mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	}
  	
    
    // if(try_lock != 0)printf("lock = %ld , origin number = %ld\n",lock,try_lock);
	result = Abort;
    DEBUG("silo %ld try to acquire lock %ld row %ld \n", txnMng->get_txn_id(), test_row->_tid_word, test_row->get_primary_key());
	return result;
}

bool RDMA_mocc::validate_rw_remote(yield_func_t &yield, TxnManager * txn , uint64_t num, uint64_t cor_id){
	bool succ = true;

	row_t * row = txn->txn->accesses[num]->orig_row;
	// row_t * row = txn->txn->accesses[num]->test_row;
	uint64_t lock = txn->get_txn_id();

	// row_t *temp_row = read_remote_row(txn , num);
    uint64_t target_server = txn->txn->accesses[num]->location;
    uint64_t remote_offset = txn->txn->accesses[num]->offset;

    row_t *temp_row = txn->read_remote_row(yield,target_server,remote_offset,cor_id);
	//ADD FAA

	if(temp_row->timestamp != txn->txn->accesses[num]->timestamp){
		succ = false;
		// uint64_t res = txn->faa_remote_content(yield, target_server, remote_offset + sizeof(uint64_t), cor_id);
		uint64_t add_value = 1;
		uint64_t faa_result = 0;
		faa_result = txn->faa_remote_content(yield, target_server, remote_offset + sizeof(uint64_t) * 3, add_value,cor_id);//TODO - check faa result? 
	}

	mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));

	return succ;
}

bool RDMA_mocc::remote_commit_write(yield_func_t &yield, TxnManager * txnMng , uint64_t num, uint64_t cor_id, RC type){
	bool result = false;
	Transaction *txn = txnMng->txn;
	uint64_t remote_offset = txn->accesses[num]->offset;
	uint64_t thd_id = txnMng->get_thd_id() + cor_id * g_thread_cnt;
	uint64_t lock = txnMng->get_txn_id();
	uint64_t try_lock = -1;
	uint64_t loc = txn->accesses[num]->location;
mocc_retry_unlock:
	try_lock = txnMng->cas_remote_content(yield,loc,remote_offset,0,lock,cor_id);
	uint64_t tts = txnMng->get_timestamp();
	uint64_t lock_type;
	// _row->_tid_word = lock; 
	if(try_lock != 0) {
		if (!simulation->is_done()) goto mocc_retry_unlock;
		else {
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			// row_local = NULL;
			// txn->rc = Abort;
			// mem_allocator.free(m_item, sizeof(itemid_t));
			// mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			return Abort; //原子性被破坏，CAS失败
		}
	}
	row_t * test_row = txnMng->read_remote_row(yield,loc,remote_offset,cor_id);
	// printf("read remote key:%ld, local key: %ld\n",test_row->get_primary_key(), txn->accesses[num]->orig_row->get_primary_key());
    // test_row->copy(txn->accesses[num]->data);
	if (type != Abort) {
		test_row->timestamp = get_sys_clock();
	}
	uint64_t i = 0;
    uint64_t lock_num = 0;
    for(i = 0; i < LOCK_LENGTH; i++) {
        if(test_row->lock_owner[i] == txnMng->get_txn_id()) {
            test_row->ts[i] = 0;
            test_row->lock_owner[i] = 0;
			// printf("release lock %d on record %d\n", test_row->lock_type, test_row->get_primary_key());
            if(test_row->lock_type == 1) {
                test_row->lock_type = 0;
            }
        }
        if(test_row->ts[i] != 0) {
            lock_num += 1;
        }
    }
    if(lock_num == 0) {
        test_row->lock_type = 0;
    }
    test_row->_tid_word = 0;
	assert(txnMng->write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), remote_offset,(char*)test_row, cor_id) == true);
	txnMng->lock_set.erase(txn->accesses[num]->orig_row->get_primary_key());
	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	result = true;
	// 
	return result;
}

RC
RDMA_mocc::finish(yield_func_t &yield, RC rc , TxnManager * txnMng, uint64_t cor_id)
{
	Transaction *txn = txnMng->txn;
	//abort
	if (rc == Abort) {
		if (txnMng->num_locks > txnMng->get_access_cnt())
			return rc;

		for (uint64_t i = 0; i < txnMng->get_access_cnt(); i++) {

            // uint64_t num = txnMng->write_set[i];
			set<uint64_t>::iterator iter;
			if((iter = txnMng->lock_set.find(txn->accesses[i]->orig_row->get_primary_key())) == txnMng->lock_set.end())
			{
				continue;
			}
            uint64_t remote_offset = txn->accesses[i]->offset;
            uint64_t loc = txn->accesses[i]->location;
	        uint64_t lock = txnMng->get_txn_id();

			//local
			if(txn->accesses[i]->location == g_node_id){
				txn->accesses[i]->orig_row->manager->release(yield,txnMng,i,cor_id);
			} else{
			//remote(release lock)
                // uint64_t try_lock = txnMng->cas_remote_content(yield,loc,remote_offset,lock,0,cor_id);
				remote_commit_write(yield, txnMng, i, cor_id, Abort);
                // assert(try_lock == lock);
			}
		}
	} 
	else { //commit
		ts_t time = get_sys_clock();
		for (uint64_t i = 0; i < txnMng->get_access_cnt(); i++) {
			set<uint64_t>::iterator iter;
			if((iter = txnMng->lock_set.find(txn->accesses[i]->orig_row->get_primary_key())) == txnMng->lock_set.end())
			{
				continue;
			}
            uint64_t num = i;
            uint64_t remote_offset = txn->accesses[num]->offset;
            uint64_t loc = txn->accesses[num]->location;
	        uint64_t lock = txnMng->get_txn_id();

			//local
			if(txn->accesses[num]->location == g_node_id){
				Access * access = txn->accesses[ num ];
				access->orig_row->manager->write( access->data, txnMng->get_txn_id(),time);
				txn->accesses[ num ]->orig_row->manager->release(yield,txnMng,num,cor_id);
			}else{
			//remote
				remote_commit_write(yield, txnMng, num, cor_id, RCOK);
                // assert(try_lock == lock);
			}
		}
	}
  for (uint64_t i = 0; i < txn->row_cnt; i++) {
    //local
    if(txn->accesses[i]->location != g_node_id){
    //remote
      mem_allocator.free(txn->accesses[i]->data,0);
      mem_allocator.free(txn->accesses[i]->orig_row,0);
      // mem_allocator.free(txn->accesses[i]->test_row,0);
      txn->accesses[i]->data = NULL;
      txn->accesses[i]->orig_row = NULL;
      txn->accesses[i]->orig_data = NULL;
      txn->accesses[i]->version = 0;

      txn->accesses[i]->key = 0;
      txn->accesses[i]->tid = 0;
	  txn->accesses[i]->timestamp = 0;
      txn->accesses[i]->test_row = NULL;
      txn->accesses[i]->offset = 0;
    }
  }
	txnMng->num_locks = 0;
	memset(txnMng->write_set, 0, 100);
	txnMng->lock_set.clear();
	// mem_allocator.free(write_set, sizeof(int) * txn->write_cnt);
	return rc;
}

#endif
