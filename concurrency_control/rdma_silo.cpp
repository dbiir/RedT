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
#include "mem_alloc.h"
#include "lib.hh"
#include "qps/op.hh"
#include "transport/rdma.h"
#include "row_rdma_silo.h"
#include "src/rdma/sop.hh"

#if CC_ALG == RDMA_SILO

RC RDMA_silo::validate_rdma_silo(yield_func_t &yield, TxnManager * txnMng, uint64_t cor_id)
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
			if (row->_tid_word != txn->accesses[txnMng->write_set[i]]->tid) {
				rc = Abort;
				return rc;
			}
		}
		for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i ++) {
			Access * access = txn->accesses[ read_set[i] ];
			if (access->orig_row->_tid_word != txn->accesses[read_set[i]]->tid) {
				rc = Abort;
				return rc;
			}
		}
	}
	txnMng->num_locks = 0;
	for (uint64_t i = 0; i < wr_cnt; i++) {
		while (!done) {
			if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){//lock in local
				row_t * row = txn->accesses[ txnMng->write_set[i] ]->orig_row;
				if (!row->manager->try_lock(yield, txnMng,txnMng->write_set[i], cor_id))
				{
					INC_STATS(txnMng->get_thd_id(), local_try_lock_fail_abort, 1); //17%
					INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
					continue;
				}
				DEBUG("silo %ld write lock row %ld \n", txnMng->get_txn_id(), row->get_primary_key());
				if (!row->manager->assert_lock(txnMng->get_txn_id())) {
                    // printf("[101]table_name = %s, _tid_word = %ld,lock = %ld\n",row->table_name,row->_tid_word,txnMng->get_txn_id());
				   //  RDMA_LOG(4) << "txn " << txn->txn_id << " lock "<<row->get_primary_key() <<" failed and abort";
				    INC_STATS(txnMng->get_thd_id(), local_lock_fail_abort, 1);
					INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
					rc = Abort;
					return rc;
				} else {
					done = true;
					txnMng->num_locks ++;
				}
			}
			else{//lock remote
				row_t * row = txn->accesses[ txnMng->write_set[i] ]->orig_row;
                uint64_t off = txn->accesses[ txnMng->write_set[i] ]->offset;
                uint64_t loc = txn->accesses[ txnMng->write_set[i] ]->location;
                uint64_t lock = txnMng->get_txn_id();
				if(txnMng->cas_remote_content(yield,loc,off,0,lock,cor_id) != 0){//remote lock fail
                    INC_STATS(txnMng->get_thd_id(), remote_lock_fail_abort, 1); //12%
					INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
					rc = Abort;
					return rc;
				}else{
                    txnMng->num_locks ++;
                }
				DEBUG("silo %ld write lock row %ld \n", txnMng->get_txn_id(), row->get_primary_key());
				// if(assert_remote_lock(txnMng,txnMng->write_set[i])){//check whether the row was locked by current txn
				// 	txnMng->num_locks ++;
				// }else{
				// 	INC_STATS(txnMng->get_thd_id(), remote_lock_fail_abort, 1); //12%
				// 	INC_STATS(txnMng->get_thd_id(), valid_abort_cnt, 1);
				// 	rc = Abort;
				// 	return rc;
				// }
				done = true;
			}
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


bool RDMA_silo::validate_rw_remote(yield_func_t &yield, TxnManager * txn , uint64_t num, uint64_t cor_id){
	bool succ = true;

	row_t * row = txn->txn->accesses[num]->orig_row;
	// row_t * row = txn->txn->accesses[num]->test_row;
	uint64_t lock = txn->get_txn_id();

//read the row again
	// row_t *temp_row = read_remote_row(txn , num);
    uint64_t target_server = txn->txn->accesses[num]->location;
    uint64_t remote_offset = txn->txn->accesses[num]->offset;

    row_t *temp_row = txn->read_remote_row(yield,target_server,remote_offset,cor_id);

//check whether it was changed by other transaction
	if(temp_row->_tid_word != lock && temp_row->_tid_word != 0){
		succ = false;
		INC_STATS(txn->get_thd_id(), validate_lock_abort, 1);
	}
//if the row has been re-write
	if(temp_row->timestamp != txn->txn->accesses[num]->timestamp){
		succ = false;
	}

	mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));

	return succ;
}

bool RDMA_silo::remote_commit_write(yield_func_t &yield, TxnManager * txnMng , uint64_t num , row_t * data , ts_t time, uint64_t cor_id){
	bool result = false;
	Transaction *txn = txnMng->txn;

    uint64_t off = txn->accesses[num]->offset + sizeof(data->_tid_word);
    uint64_t loc = txn->accesses[num]->location;
	uint64_t thd_id = txnMng->get_thd_id() + cor_id * g_thread_cnt;
	uint64_t lock = txnMng->get_txn_id();
	data->_tid_word = lock;
	data->timestamp = time;
    uint64_t operate_size = row_t::get_row_size(data->tuple_size) - sizeof(data->_tid_word);
    // printf("【rdma_silo.cpp:364】table_name = %s, loc = %ld , thd_id = %ld, off = %ld, lock = %ld,operate_size = %ld tuple_size = %ld , sizeof(row_t)=%d\n",data->table_name,loc,thd_id,off,lock,operate_size,data->tuple_size,sizeof(row_t));
    // char *test_buf = Rdma::get_row_client_memory(thd_id);
    // memcpy(test_buf, (char*)data + sizeof(data->_tid_word), operate_size);

    result = txnMng->write_remote_row(yield,loc,operate_size,off,(char*)data + sizeof(data->_tid_word),cor_id);

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

	return result;
}

RC
RDMA_silo::finish(yield_func_t &yield, RC rc , TxnManager * txnMng, uint64_t cor_id)
{
	Transaction *txn = txnMng->txn;
	//abort
	if (rc == Abort) {
		if (txnMng->num_locks > txnMng->get_access_cnt())
			return rc;

		vector<vector<uint64_t>> remote_access(g_node_cnt);
		for (uint64_t i = 0; i < txnMng->num_locks; i++) {

            uint64_t num = txnMng->write_set[i];
            uint64_t remote_offset = txn->accesses[num]->offset;
            uint64_t loc = txn->accesses[num]->location;
	        uint64_t lock = txnMng->get_txn_id();

			//local
			if(txn->accesses[num]->location == g_node_id){
				txn->accesses[ num ]->orig_row->manager->release(yield,txnMng,num,cor_id);
			} else{
			//remote(release lock)
				remote_access[loc].push_back(num);
#if USE_DBPAOR == false
                uint64_t try_lock = txnMng->cas_remote_content(yield,loc,remote_offset,lock,0,cor_id);
                // assert(try_lock == lock);
#endif
			}

		}
#if USE_DBPAOR == true
    uint64_t starttime = get_sys_clock(), endtime;
    for(int i=0;i<g_node_cnt;i++){ //for the same node, batch unlock remote
        if(remote_access[i].size() > 0){
            txnMng->batch_unlock_remote(yield, cor_id, i, Abort, txnMng, remote_access);
        }
    }
    for(int i=0;i<g_node_cnt;i++){ //poll result
        if(remote_access[i].size() > 0){
		    //to do: add coroutine
			INC_STATS(txnMng->get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
			uint64_t waitcomp_time;
			std::pair<int,ibv_wc> dbres1;
			INC_STATS(txnMng->get_thd_id(), worker_process_time, get_sys_clock() - txnMng->h_thd->cor_process_starttime[cor_id]);
			
			do {
				txnMng->h_thd->start_wait_time = get_sys_clock();
				txnMng->h_thd->last_yield_time = get_sys_clock();
				// printf("do\n");
				yield(txnMng->h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
				uint64_t yield_endtime = get_sys_clock();
				INC_STATS(txnMng->get_thd_id(), worker_yield_cnt, 1);
				INC_STATS(txnMng->get_thd_id(), worker_yield_time, yield_endtime - txnMng->h_thd->last_yield_time);
				INC_STATS(txnMng->get_thd_id(), worker_idle_time, yield_endtime - txnMng->h_thd->last_yield_time);
				dbres1 = rc_qp[i][txnMng->get_thd_id() + cor_id * g_thread_cnt]->poll_send_comp();
				waitcomp_time = get_sys_clock();
				
				INC_STATS(txnMng->get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
				INC_STATS(txnMng->get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
			} while (dbres1.first == 0);
			txnMng->h_thd->cor_process_starttime[cor_id] = get_sys_clock();
			// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
#else
            auto dbres1 = rc_qp[i][txnMng->get_thd_id() + cor_id * g_thread_cnt]->wait_one_comp();
            RDMA_ASSERT(dbres1 == IOCode::Ok);
			endtime = get_sys_clock();
			INC_STATS(txnMng->get_thd_id(), worker_waitcomp_time, endtime-starttime);
			INC_STATS(txnMng->get_thd_id(), worker_idle_time, endtime-starttime);
			DEL_STATS(txnMng->get_thd_id(), worker_process_time, endtime-starttime);
#endif 
        }
    }
#endif
	} 
	else { //commit
		ts_t time = get_sys_clock();
		
		vector<vector<uint64_t>> remote_access(g_node_cnt);
		for (uint64_t i = 0; i < txn->write_cnt; i++) {
            uint64_t num = txnMng->write_set[i];
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
				remote_access[loc].push_back(num);
#if USE_DBPAOR == false
				Access * access = txn->accesses[ num ];
				remote_commit_write(yield,txnMng,num,access->data,time,cor_id);
                uint64_t try_lock = txnMng->cas_remote_content(yield,loc,remote_offset,lock,0,cor_id);
                // assert(try_lock == lock);
#endif
			}
		}
#if USE_DBPAOR == true
    uint64_t starttime = get_sys_clock(), endtime;
    for(int i=0;i<g_node_cnt;i++){ //for the same node, batch unlock remote
        if(remote_access[i].size() > 0){
            txnMng->batch_unlock_remote(yield, cor_id, i, RCOK, txnMng, remote_access,time);
        }
    }
    for(int i=0;i<g_node_cnt;i++){ //poll result
        if(remote_access[i].size() > 0){
		    //to do: add coroutine
			INC_STATS(txnMng->get_thd_id(), worker_oneside_cnt, 1);
#if USE_COROUTINE
			uint64_t waitcomp_time;
			std::pair<int,ibv_wc> dbres1;
			INC_STATS(txnMng->get_thd_id(), worker_process_time, get_sys_clock() - txnMng->h_thd->cor_process_starttime[cor_id]);
			
			do {
				txnMng->h_thd->start_wait_time = get_sys_clock();
				txnMng->h_thd->last_yield_time = get_sys_clock();
				// printf("do\n");
				yield(txnMng->h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
				uint64_t yield_endtime = get_sys_clock();
				INC_STATS(txnMng->get_thd_id(), worker_yield_cnt, 1);
				INC_STATS(txnMng->get_thd_id(), worker_yield_time, yield_endtime - txnMng->h_thd->last_yield_time);
				INC_STATS(txnMng->get_thd_id(), worker_idle_time, yield_endtime - txnMng->h_thd->last_yield_time);
				dbres1 = rc_qp[i][txnMng->get_thd_id() + cor_id * g_thread_cnt]->poll_send_comp();
				waitcomp_time = get_sys_clock();
				
				INC_STATS(txnMng->get_thd_id(), worker_idle_time, waitcomp_time - yield_endtime);
				INC_STATS(txnMng->get_thd_id(), worker_waitcomp_time, waitcomp_time - yield_endtime);
			} while (dbres1.first == 0);
			txnMng->h_thd->cor_process_starttime[cor_id] = get_sys_clock();
			// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
#else
            auto dbres1 = rc_qp[i][txnMng->get_thd_id() + cor_id * g_thread_cnt]->wait_one_comp();
            RDMA_ASSERT(dbres1 == IOCode::Ok); 
			endtime = get_sys_clock();
			INC_STATS(txnMng->get_thd_id(), worker_waitcomp_time, endtime-starttime);
			INC_STATS(txnMng->get_thd_id(), worker_idle_time, endtime-starttime);
			DEL_STATS(txnMng->get_thd_id(), worker_process_time, endtime-starttime);
#endif      
        }
    }
#endif

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
	// mem_allocator.free(write_set, sizeof(int) * txn->write_cnt);
	return rc;
}

#endif
