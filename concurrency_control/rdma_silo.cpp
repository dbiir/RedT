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
#include "rdma_silo.h"
#include "manager.h"
#include "mem_alloc.h"
#include "lib.hh"
#include "qps/op.hh"
#include "transport/rdma.h"
#include "row_rdma_silo.h"
#include "src/rdma/sop.hh"

#if CC_ALG == RDMA_SILO
RC RDMA_silo::validate_key(TxnManager * txn , uint64_t num) {
  row_t * row = txn->txn->accesses[num]->orig_row;
  assert(row->get_primary_key() == txn->txn->accesses[num]->key);
}

row_t * RDMA_silo::read_remote_row(TxnManager * txn , uint64_t num){
	row_t * row = txn->txn->accesses[num]->orig_row;
	// row_t * row = txn->txn->accesses[num]->test_row;

	uint64_t off = txn->txn->accesses[num]->offset;
 	uint64_t loc = txn->txn->accesses[num]->location;
	uint64_t thd_id = txn->get_thd_id();

	uint64_t operate_size = sizeof(row_t);

	char *test_buf = Rdma::get_row_client_memory(thd_id);
    memset(test_buf, 0, operate_size);

	// r2::rdma::SROp op;
  //   op.set_payload(test_buf,operate_size).set_remote_addr(off).set_read();

	// bool runned = false;
  //   r2::SScheduler ssched;

	// ssched.spawn([&op, &runned,&loc,&thd_id](R2_ASYNC) {
	// 	auto ret = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED, R2_ASYNC_WAIT);
	// 	R2_YIELD;
	// 	ASSERT(ret == IOCode::Ok);
	// 	runned = true;
	// 	R2_STOP();
	// 	R2_RET;
	// });
	// ssched.run();
	// ASSERT(runned == true);
	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

	auto res_s = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = off,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
  	auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

	endtime = get_sys_clock();

	INC_STATS(txn->get_thd_id(), rdma_read_time, endtime-starttime);
	INC_STATS(txn->get_thd_id(), rdma_read_cnt, 1);

	row_t *temp_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
	memcpy(temp_row, test_buf, operate_size);
	assert(temp_row->get_primary_key() == row->get_primary_key());

	return temp_row;
}
#if 1
RC
RDMA_silo::validate_rdma_silo(TxnManager * txnMng)
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
	if (txnMng->_pre_abort) {
		for (uint64_t i = 0; i < wr_cnt; i++) {
			row_t * row = txn->accesses[ txnMng->write_set[i] ]->orig_row;
			if (row->manager->get_tid() != txn->accesses[txnMng->write_set[i]]->tid) {
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

	while (!done) {
		txnMng->num_locks = 0;
		for (uint64_t i = 0; i < wr_cnt; i++) {
			if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){//lock in local
				row_t * row = txn->accesses[ txnMng->write_set[i] ]->orig_row;
				if (!row->manager->try_lock(txnMng,txnMng->write_set[i]))
				{
					INC_STATS(txnMng->get_thd_id(), local_try_lock_fail_abort, 1); //17%
					INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
					break;
				}
				DEBUG("silo %ld write lock row %ld \n", txnMng->get_txn_id(), row->get_primary_key());
				if (!row->manager->assert_lock(txn->txn_id)) {
				   //  RDMA_LOG(4) << "txn " << txn->txn_id << " lock "<<row->get_primary_key() <<" failed and abort";
				    INC_STATS(txnMng->get_thd_id(), local_lock_fail_abort, 1);
					INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
					rc = Abort;
					return rc;
				} else
				txnMng->num_locks ++;
			}
			else{//lock remote
				row_t * row = txn->accesses[ txnMng->write_set[i] ]->orig_row;
				if(!remote_try_lock(txnMng,txnMng->write_set[i])){//remote lock fail
					INC_STATS(txnMng->get_thd_id(), remote_try_lock_fail_abort, 1);
					INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
					break;
				}
				DEBUG("silo %ld write lock row %ld \n", txnMng->get_txn_id(), row->get_primary_key());

				if(assert_remote_lock(txnMng,txnMng->write_set[i])){//check whether the row was locked by current txn
					txnMng->num_locks ++;
				}else{
					// RDMA_LOG(4) << "txn " << txn->txn_id << " lock "<<row->get_primary_key() <<" failed and abort";
					INC_STATS(txnMng->get_thd_id(), remote_lock_fail_abort, 1); //12%
					INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
					txnMng->num_locks ++;
					rc = Abort;
					return rc;
				}

			}
		}
		if (txnMng->num_locks == wr_cnt) {
			DEBUG("TRY LOCK true %ld\n", txnMng->get_txn_id());
			done = true;
		} else {
			INC_STATS(txnMng->get_thd_id(), cnt_unequal_abort, 1);
			INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
			rc = Abort;
			return rc;
		}
	}


	COMPILER_BARRIER

	// validate rows in the read set
	// for repeatable_read, no need to validate the read set.
	for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i ++) {
		bool success = false;
		Access * access = txn->accesses[ read_set[i] ];
		if(txn->accesses[read_set[i]]->location == g_node_id){//local
		    // success = access->orig_row->manager->validate(access->tid, false);
			success = access->orig_row->manager->validate(txnMng->get_txn_id(), false);
		    // success = true;
			if(!success){
				 INC_STATS(txnMng->get_thd_id(), local_readset_validate_fail_abort, 1); //48%
				 INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
			}
       	}else{//remote
        	success = validate_rw_remote(txnMng,read_set[i]);
			// success = true;
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
			success = access->orig_row->manager->validate(txnMng->get_txn_id(), true);
			// success = true;
      		if (!success){
				  INC_STATS(txnMng->get_thd_id(), local_writeset_validate_fail_abort, 1);
				  INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
			  }
		}else{//remote
      		success = validate_rw_remote(txnMng,txnMng->write_set[i]);
			// success = true;
			if(success)DEBUG("silo %ld validate write row %ld success \n",txnMng->get_txn_id(),access->key);
			if(!success){
				 INC_STATS(txnMng->get_thd_id(), remote_writeset_validate_fail_abort, 1);
				 INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
				 DEBUG("silo %ld validate write row %ld fail\n",txnMng->get_txn_id(),access->key);
			}

      		// if (!success) RDMA_LOG(4) << "txn " << txn->txn_id << " validate "<<access->orig_row->get_primary_key() <<" failed and abort";
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

#else
RC
RDMA_silo::validate_rdma_silo(TxnManager * txnMng)
{
	RC rc = RCOK;
	// validate rows in the read set
	// for repeatable_read, no need to validate the read set.
	for (uint64_t i = 0; i < txnMng->txn->row_cnt; i ++) {
		Access * access = txnMng->txn->accesses[i];
		if(access->location == g_node_id){//local
		}else{//remote
          validate_key(txnMng,i);
		}
  }
	return rc;
}

#endif
bool RDMA_silo::validate_rw_remote(TxnManager * txn , uint64_t num){
	bool succ = true;

	row_t * row = txn->txn->accesses[num]->orig_row;
	// row_t * row = txn->txn->accesses[num]->test_row;
	uint64_t lock = txn->get_txn_id();

//read the row again
	row_t *temp_row = read_remote_row(txn , num);

//check whether it was changed by other transaction
	if(temp_row->_tid_word != lock && temp_row->_tid_word != 0){
		succ = false;
		INC_STATS(txn->get_thd_id(), validate_lock_abort, 1);
    // RDMA_LOG(4) << "txn " << txn->txn->txn_id << " validate "<<row->get_primary_key() <<" failed and abort";
	}
	if(strcmp(row->data,temp_row->data)){
		succ = false;
    // RDMA_LOG(4) << "txn " << txn->txn->txn_id << " validate "<<row->get_primary_key() <<" failed and abort";
	}

	mem_allocator.free(temp_row,sizeof(row_t));

	return succ;
}

bool RDMA_silo::remote_try_lock(TxnManager * txnMng , uint64_t num){
    bool result = false;
	Transaction *txn = txnMng->txn;

    uint64_t off = txn->accesses[num]->offset;
    uint64_t loc = txn->accesses[num]->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id();

	uint64_t *test_loc = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto mr = client_rm_handler->get_reg_attr().value();

	rdmaio::qp::Op<> op;
    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + off), remote_mr_attr[loc].key).set_cas(0, lock);
  	assert(op.set_payload(test_loc, sizeof(uint64_t), mr.key) == true);
  	auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

	RDMA_ASSERT(res_s2 == IOCode::Ok);
	auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p2 == IOCode::Ok);

	result = true;
	return result;
}

bool RDMA_silo::assert_remote_lock(TxnManager * txn , uint64_t num){
	bool succ = true;

	uint64_t lock = txn->get_txn_id();

//read the row again
	row_t *temp_row = read_remote_row(txn , num);

//check whether it was locked by current txn
    if(temp_row->_tid_word != lock){
	//	RDMA_LOG(4) <<"check _tid_word = "<<temp_row->_tid_word;
		succ = false;
	}

	DEBUG("silo %ld asser lock row %ld \n",lock,txn->txn->accesses[num]->key);
	mem_allocator.free(temp_row,sizeof(row_t));
	return succ;

}

bool RDMA_silo::remote_commit_write(TxnManager * txnMng , uint64_t num , row_t * data, uint64_t tid){
	bool result = false;
	Transaction *txn = txnMng->txn;

    uint64_t off = txn->accesses[num]->offset;
    uint64_t loc = txn->accesses[num]->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id();
//RDMA_SILO:usage of tid?
	data->_tid_word = tid;
    uint64_t operate_size = sizeof(row_t);

    char *test_buf = Rdma::get_row_client_memory(thd_id);
    memcpy(test_buf, data, operate_size);

//async
	// r2::rdma::SROp op;
    // op.set_payload(test_buf,operate_size).set_remote_addr(off).set_write();

	// bool runned = false;
  //   r2::SScheduler ssched;

	// ssched.spawn([&op, &runned,&loc,&thd_id](R2_ASYNC) {
	// 	auto ret = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED, R2_ASYNC_WAIT);
	// 	R2_YIELD;
	// 	ASSERT(ret == IOCode::Ok);
	// 	runned = true;
	// 	R2_STOP();
	// 	R2_RET;
	// });
	// ssched.run();
	// ASSERT(runned == true);
//

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

	auto res_s = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = off,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
  	auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

	endtime = get_sys_clock();

	INC_STATS(txnMng->get_thd_id(), rdma_write_time, endtime-starttime);
	INC_STATS(txnMng->get_thd_id(), rdma_write_cnt, 1);

	result = true;
	return result;
}

void
RDMA_silo::release_remote_lock(TxnManager * txnMng , uint64_t num){

    Transaction *txn = txnMng->txn;

    uint64_t off = txn->accesses[num]->offset;
    uint64_t loc = txn->accesses[num]->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id();

	uint64_t *test_loc = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto mr = client_rm_handler->get_reg_attr().value();

	rdmaio::qp::Op<> op;
    op.set_atomic_rbuf((uint64_t*)remote_mr_attr[loc].buf, remote_mr_attr[loc].key).set_cas(lock, 0);//0-locked,1-unlock
  	assert(op.set_payload(test_loc, sizeof(uint64_t), mr.key) == true);
  	auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

	RDMA_ASSERT(res_s2 == IOCode::Ok);
	auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p2 == IOCode::Ok);

	uint64_t operate_size = sizeof(row_t);
	auto res_s = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_loc),
		.remote_addr = off,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
  	auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

	row_t *temp_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
	memcpy(temp_row, test_loc, operate_size);
	DEBUG("tx %ld release lock row %ld, reread lock as %ld \n",lock,txn->accesses[num]->key,temp_row->_tid_word);
	//assert(temp_row->_tid_word != loc);

}

RC
RDMA_silo::finish(RC rc , TxnManager * txnMng)
{
	Transaction *txn = txnMng->txn;
	//abort
	if (rc == Abort) {
		if (txnMng->num_locks > txnMng->get_access_cnt())
			return rc;
		for (uint64_t i = 0; i < txnMng->num_locks; i++) {
			//local
			if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){
				txn->accesses[ txnMng->write_set[i] ]->orig_row->manager->release(txnMng,txnMng->write_set[i]);
			} else{
			//remote
        		release_remote_lock(txnMng,txnMng->write_set[i] );
			}
			// DEBUG("silo %ld abort release row %ld \n", txnMng->get_txn_id(), txn->accesses[ txnMng->write_set[i] ]->orig_row->get_primary_key());
		}
	} else {
	//commit
		for (uint64_t i = 0; i < txn->write_cnt; i++) {
			//local
			if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){
				Access * access = txn->accesses[ txnMng->write_set[i] ];
				access->orig_row->manager->write( access->data, txnMng->commit_timestamp );
				txn->accesses[ txnMng->write_set[i] ]->orig_row->manager->release(txnMng,txnMng->write_set[i]);
			}else{
			//remote
				Access * access = txn->accesses[ txnMng->write_set[i] ];
				remote_commit_write(txnMng,txnMng->write_set[i],access->data,txnMng->commit_timestamp);
				release_remote_lock(txnMng,txnMng->write_set[i] );//unlock
			}
			// DEBUG("silo %ld abort release row %ld \n", txnMng->get_txn_id(), txn->accesses[ txnMng->write_set[i] ]->orig_row->get_primary_key());
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
      txn->accesses[i]->test_row = NULL;
      txn->accesses[i]->offset = 0;
    }
  }
	txnMng->num_locks = 0;
	memset(txnMng->write_set, 0, 100);
	// mem_allocator.free(write_set, sizeof(int) * txn->write_cnt);
	return rc;
}

#endif
