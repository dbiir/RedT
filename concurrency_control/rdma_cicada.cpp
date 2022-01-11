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
#include "rdma_cicada.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_rdma_cicada.h"
#include "rdma.h"
//#include "src/allocator_master.hh"
#include "qps/op.hh"
#include "string.h"

#if CC_ALG == RDMA_CICADA

void RDMA_Cicada::init() { sem_init(&_semaphore, 0, 1); }

RC RDMA_Cicada::validate(yield_func_t &yield, TxnManager * txnMng, uint64_t cor_id) {
	uint64_t start_time = get_sys_clock();
	uint64_t timespan;
	Transaction *txn = txnMng->txn;
	// sem_wait(&_semaphore);
	int read_set[txn->row_cnt - txn->write_cnt];
	int cur_rd_idx = 0;
    int cur_wr_idx = 0;
	uint64_t wr_cnt = txn->write_cnt;
	//Sort the write set in WTS descending order
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[cur_wr_idx ++] = rid;
		else
			read_set[cur_rd_idx ++] = rid;
	}
	if (wr_cnt > 1)
	{
		for (uint64_t i = wr_cnt - 1; i >= 1; i--) {
			for (uint64_t j = 0; j < i; j++) {
				if (txn->accesses[ txnMng->write_set[j] ]->orig_row->cicada_version[txnMng->version_num[txnMng->write_set[j] ] % HIS_CHAIN_NUM].Wts <
					txn->accesses[ txnMng->write_set[j + 1] ]->orig_row->cicada_version[txnMng->version_num[txnMng->write_set[j] ] % HIS_CHAIN_NUM ].Wts)
				{
					int tmp = txnMng->write_set[j];
					txnMng->write_set[j] = txnMng->write_set[j+1];
					txnMng->write_set[j+1] = tmp;
				}
			}
		}
	}

	timespan = get_sys_clock() - start_time;
	txnMng->txn_stats.cc_block_time += timespan;
	txnMng->txn_stats.cc_block_time_short += timespan;
	start_time = get_sys_clock();
	RC rc = RCOK;
	Access * access;
	// 2.	预检查版本一致性，对写集中的每一项Xi
	for (uint64_t j = 0; j < txn->write_cnt; j++) {
        //local
		// printf("%d\n", txn->accesses[txnMng->write_set[i]]->location);
        if(txn->accesses[txnMng->write_set[j]]->location == g_node_id){
            access = txn->accesses[ txnMng->write_set[j] ];
			if (access->orig_row->manager->local_cas_lock(txnMng, 0, txnMng->get_txn_id()) == false) {
				INC_STATS(txnMng->get_thd_id(), cicada_case1_cnt, 1);
				return Abort;
			}
			for(int cnt = access->orig_row->version_cnt; cnt >= access->orig_row->version_cnt - HIS_CHAIN_NUM && cnt >= 0; cnt--) {
				int i = cnt % HIS_CHAIN_NUM;
				if(access->orig_row->cicada_version[i].state == Cicada_ABORTED) {
					continue;
				}
				if(access->orig_row->cicada_version[i].Wts > txnMng->get_timestamp() || access->orig_row->cicada_version[i].Rts > txnMng->get_timestamp()) {
					INC_STATS(txnMng->get_thd_id(), cicada_case1_cnt, 1);
					rc = Abort;
					break;
				}
				// if(access->orig_row->cicada_version[i].key != txnMng->version_num[txnMng->write_set[j]])
				// printf("row_key:%ld, version[i].key :%ld, version_num:%ld\n",access->orig_row->get_primary_key(), access->orig_row->cicada_version[i].key,txnMng->version_num[txnMng->write_set[j]]);
				if(access->orig_row->cicada_version[i].key == txnMng->version_num[txnMng->write_set[j]]) {
					
					rc = RCOK;
					access->orig_row->version_cnt ++;
					access->orig_row->cicada_version[(access->orig_row->version_cnt) % HIS_CHAIN_NUM].init(access->orig_row->version_cnt, txnMng->get_timestamp(), txnMng->get_timestamp());
					txnMng->uncommitted_set.insert(std::make_pair(txnMng->write_set[j], access->orig_row->version_cnt));
					// printf("add a num:%ld version:%ld in key:%ld\n", txnMng->write_set[j], access->orig_row->version_cnt, access->orig_row->get_primary_key());
					break;
				} else {
					INC_STATS(txnMng->get_thd_id(), cicada_case1_cnt, 1);
					// printf("Abort 2\n");
					rc = Abort;
					break;
				}
			}
			// access->orig_row->manager->local_cas_lock(txnMng, txnMng->get_txn_id(), 0);
			access->orig_row->_tid_word = 0;
        }else{
        //remote
            access = txn->accesses[ txnMng->write_set[j] ];
			rc = remote_read_or_write(yield, access, txnMng, txnMng->write_set[j], true, cor_id);
			if (rc == Abort) INC_STATS(txnMng->get_thd_id(), cicada_case2_cnt, 1);
        }
    }
	// 3.	对读集中的每一项
	if(rc == RCOK)
	for (uint64_t j = 0; j < txn->row_cnt - txn->write_cnt; j++) {
        //local
        if(txn->accesses[read_set[j]]->location == g_node_id){
            access = txn->accesses[ read_set[j] ];
			// access->orig_row->manager->local_cas_lock(txnMng, 0, txnMng->get_txn_id());
			if (access->orig_row->manager->local_cas_lock(txnMng, 0, txnMng->get_txn_id()) == false) {
				INC_STATS(txnMng->get_thd_id(), cicada_case3_cnt, 1);
				return Abort;
			}
            for(int cnt = access->orig_row->version_cnt ; cnt >= access->orig_row->version_cnt - HIS_CHAIN_NUM && cnt >= 0; cnt--) {
				int i = cnt % HIS_CHAIN_NUM;
				if(access->orig_row->cicada_version[i].state == Cicada_ABORTED) {
					continue;
				}
				if(access->orig_row->cicada_version[i].Wts > txnMng->get_timestamp()) {
					continue;
				}
				// printf("version:%d : num:%d: cor_id:%ld\n", access->orig_row->cicada_version[i].key, txnMng->version_num[read_set[i]], cor_id);
				if(access->orig_row->cicada_version[i].key == txnMng->version_num[read_set[j]]) {
					rc = RCOK;
					if(access->orig_row->cicada_version[i].Rts < txnMng->get_timestamp())
						access->orig_row->cicada_version[i].Rts = txnMng->get_timestamp();
					break;
				} else {
					rc = Abort;
					INC_STATS(txnMng->get_thd_id(), cicada_case3_cnt, 1);
					// printf("Abort 6\n");
					break;
				}
			}
			access->orig_row->_tid_word = 0;
			// access->orig_row->manager->local_cas_lock(txnMng, txnMng->get_txn_id(), 0);
        }else{
        //remote
            access = txn->accesses[ read_set[j] ];
            rc = remote_read_or_write(yield, access, txnMng, read_set[j], true, cor_id);
			if (rc == Abort) INC_STATS(txnMng->get_thd_id(), cicada_case4_cnt, 1);
        }
    } 
	timespan = get_sys_clock() - start_time;
	txnMng->txn_stats.cc_time += timespan;
	txnMng->txn_stats.cc_time_short += timespan;
	DEBUG("RDMA_CICADA Validate End %ld: %d\n",txnMng->get_txn_id(),rc==RCOK);
	// printf("RDMA_CICADA Validate End %ld: %d\n",txnMng->get_txn_id(),rc==RCOK);
	// sem_post(&_semaphore);
	return rc;


}

RC RDMA_Cicada::remote_read_or_write(yield_func_t &yield, Access * data, TxnManager * txnMng, uint64_t num, bool real_write, uint64_t cor_id) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	Transaction * txn = txnMng->txn;
	uint64_t off = data->offset;
	uint64_t loc = data->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id() + 1;
	uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);

    RC rc = RCOK;

	if(data->type == RD) {
		row_t *temp_row = txnMng->read_remote_row(yield,loc,off,cor_id);
		for(int cnt = temp_row->version_cnt; cnt >= temp_row->version_cnt - HIS_CHAIN_NUM && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if(temp_row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(temp_row->cicada_version[i].Wts > txnMng->get_timestamp()) {
				continue;
			}
			// printf("row %ld, type %d %d : %d\n", temp_row->get_primary_key(), temp_row->cicada_version[i].state, temp_row->cicada_version[i].key, txnMng->version_num[num]);
			if(temp_row->cicada_version[i].key == txnMng->version_num[num] ){
				rc = RCOK;
				// temp_row->cicada_version[i].Rts = txnMng->get_timestamp();
				break;
			} else {
				rc = Abort;
				// printf("row %ld, type %d %d : %d, wts %ld, txnts %ld \n", temp_row->get_primary_key(), temp_row->cicada_version[i].state, temp_row->cicada_version[i].key, txnMng->version_num[num], temp_row->cicada_version[i].Wts, txnMng->get_timestamp());
				// printf("rdma_cicada.cpp:236 abort due to read set validate failed, now %lu, need %lu,", temp_row->cicada_version[i].key, txnMng->version_num[num]);
				break;
			}
		}
		uint64_t Rts = temp_row->cicada_version[txnMng->version_num[num] % HIS_CHAIN_NUM].Rts;
		if (rc == RCOK && temp_row->cicada_version[txnMng->version_num[num] % HIS_CHAIN_NUM].Rts < txnMng->get_timestamp()) {
			off = off + sizeof(uint64_t) * 2 + sizeof(RdmaCicadaVersion) * (txnMng->version_num[num] % HIS_CHAIN_NUM);
		#if USE_DBPAOR
			uint64_t try_lock;
			row_t * temp_row2 = txnMng->cas_and_read_remote(yield, try_lock,loc,off,off,Rts,txnMng->get_timestamp(), cor_id);
			if (try_lock != Rts && try_lock < Rts) {
				mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				// printf("rdma_cicada.cpp:255 abort due to rts update %lu, need %lu, new data %lu," ,try_lock, Rts, txnMng->get_timestamp());
				mem_allocator.free(temp_row2, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort;
			}
		#else
			uint64_t try_lock = txnMng->cas_remote_content(yield,loc,off,Rts, txnMng->get_timestamp(),cor_id);
			if (try_lock != Rts && try_lock < Rts) {
				mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				// printf("rdma_cicada.cpp:255 abort due to rts update %lu, need %lu, new data %lu,", try_lock, Rts, txnMng->get_timestamp());
				return Abort;
			}
			row_t *temp_row2 = txnMng->read_remote_row(yield,loc,off,cor_id);
		#endif
			mem_allocator.free(temp_row2, row_t::get_row_size(ROW_DEFAULT_SIZE));
		}
		mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	}

	if(data->type == WR && real_write == true) {
		#if USE_DBPAOR
			uint64_t try_lock;
			row_t * temp_row = txnMng->cas_and_read_remote(yield, try_lock,loc,off,off,0,lock, cor_id);
			if (try_lock != 0) {
				// INC_STATS(txnMng->get_thd_id(), cicada_case1_cnt, 1);
				mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort;
			}
		#else
			uint64_t try_lock = txnMng->cas_remote_content(yield,loc,off,0,lock,cor_id);
			if (try_lock != 0) {
				// INC_STATS(txnMng->get_thd_id(), cicada_case1_cnt, 1);
				return Abort;
			}
			row_t * temp_row = txnMng->read_remote_row(yield,loc,off,cor_id);
		#endif
		assert(temp_row->get_primary_key() >= data->data->get_primary_key());
		for(int cnt = temp_row->version_cnt; cnt >= temp_row->version_cnt - HIS_CHAIN_NUM && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if (temp_row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(temp_row->cicada_version[i].Wts > txnMng->get_timestamp() || temp_row->cicada_version[i].Rts > txnMng->get_timestamp()) {
				rc = Abort;
				break;
			}
			if(temp_row->cicada_version[i].key == txnMng->version_num[num] ){
				rc = RCOK;
				temp_row->version_cnt ++;
				// printf("row %ld, type %d %d, wts %ld, txnts %ld \n", temp_row->get_primary_key(), temp_row->cicada_version[(temp_row->version_cnt) % HIS_CHAIN_NUM].state, temp_row->cicada_version[(temp_row->version_cnt) % HIS_CHAIN_NUM].key, txnMng->version_num[num], temp_row->cicada_version[(temp_row->version_cnt) % HIS_CHAIN_NUM].Wts);
				temp_row->cicada_version[(temp_row->version_cnt) % HIS_CHAIN_NUM].init(temp_row->version_cnt, txnMng->get_timestamp(), txnMng->get_timestamp());
				txnMng->uncommitted_set.insert(std::make_pair(num, temp_row->version_cnt));
				break;
			} else {
				INC_STATS(txnMng->get_thd_id(), cicada_case6_cnt, 1);
				rc = Abort;
				break;
			}
		}
		temp_row->_tid_word = 0;
		if (rc == Abort) {
			operate_size = sizeof(uint64_t);
		} else {
			operate_size = row_t::get_row_size(temp_row->tuple_size);
		}
		assert(txnMng->write_remote_row(yield,loc,operate_size,off, (char *)temp_row,cor_id) == true);
		mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	}
	return rc;
}

RC
RDMA_Cicada::finish(yield_func_t &yield, RC rc , TxnManager * txnMng, uint64_t cor_id)
{
	Transaction *txn = txnMng->txn;
	if (rc == Abort) {
		//if (txnMng->num_locks > txnMng->get_access_cnt())
		//	return rc;
		vector<vector<uint64_t>> remote_access(g_node_cnt);
		vector<vector<uint64_t>> remote_num(g_node_cnt);
		for (unordered_map<uint64_t, uint64_t>::iterator i=txnMng->uncommitted_set.begin(); i!=txnMng->uncommitted_set.end(); i++) {
			// printf("abort:%d:%d\n", i->first, i->second);
			if(txn->accesses[i->first]->location == g_node_id){
				rc = txn->accesses[i->first]->orig_row->manager->abort(i->second,txnMng);
			} else{
			//remote
        		//release_remote_lock(txnMng,txnMng->write_set[i] );
	            remote_access[txn->accesses[i->first]->location].push_back(i->first);
	            remote_num[txn->accesses[i->first]->location].push_back(i->second);
		#if USE_DBPAOR == false
				rc = remote_abort(yield, txnMng, txn->accesses[i->first], i->second, cor_id);
		#endif

			}
  		}
		#if USE_DBPAOR == true
			uint64_t starttime = get_sys_clock(), endtime;
			for(int i=0;i<g_node_cnt;i++){ //for the same node, batch write back remote
				if(remote_access[i].size() > 0){
					ts_t temp_time = 0;
					txnMng->batch_unlock_remote(yield, cor_id, i, Abort, txnMng, remote_access,temp_time,remote_num);
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
	} else {
	//commit
		ts_t time = get_sys_clock();
		vector<vector<uint64_t>> remote_access(g_node_cnt);
		vector<vector<uint64_t>> remote_num(g_node_cnt);
		for (unordered_map<uint64_t, uint64_t>::iterator i=txnMng->uncommitted_set.begin(); i!=txnMng->uncommitted_set.end(); i++) {
			// printf("commit%d:%d\n", i->first, i->second);
			if(txn->accesses[i->first]->location == g_node_id){
				// Access * access = txn->accesses[i->first];
				rc = txn->accesses[i->first]->orig_row->manager->commit(i->second, txnMng, txn->accesses[i->first]->data);
			} else{
			//remote
        		//release_remote_lock(txnMng,txnMng->write_set[i] );
				// Access * access = txn->accesses[i->first];
				//DEBUG("commit access txn %ld, off: %ld loc: %ld and key: %ld\n",txnMng->get_txn_id(), access->offset, access->location, access->data->get_primary_key());
	            remote_access[txn->accesses[i->first]->location].push_back(i->first);
	            remote_num[txn->accesses[i->first]->location].push_back(i->second);
			#if USE_DBPAOR == false
				rc =  remote_commit(yield, txnMng, txn->accesses[i->first], i->second, cor_id);
			#endif
			}
  		}
	#if USE_DBPAOR == true
		uint64_t starttime = get_sys_clock(), endtime;
		for(int i=0;i<g_node_cnt;i++){ //for the same node, batch write back remote
			if(remote_access[i].size() > 0){
				ts_t temp_time = 0;
				txnMng->batch_unlock_remote(yield, cor_id, i, RCOK, txnMng, remote_access,temp_time,remote_num);
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
		txn->accesses[i]->offset = 0;
		}
	}
	txnMng->uncommitted_set.clear();
	memset(txnMng->write_set, 0, 100);
	return rc;
}
RC RDMA_Cicada::remote_abort(yield_func_t &yield, TxnManager * txnMng, Access * data, uint64_t num, uint64_t cor_id) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	INC_STATS(txnMng->get_thd_id(),mtx[32],get_sys_clock() - mtx_wait_starttime);
	DEBUG("Cicada Abort %ld: %d -- %ld\n",txnMng->get_txn_id(), data->type, data->data->get_primary_key());
	// Transaction * txn = txnMng->txn;
	uint64_t off = data->offset;
	uint64_t loc = data->location;
	uint64_t lock = txnMng->get_txn_id() + 1;
	off = off + sizeof(uint64_t) * 4 + sizeof(RdmaCicadaVersion) * (num % HIS_CHAIN_NUM);
    CicadaState try_lock = Cicada_PENDING;
	uint64_t result = txnMng->cas_remote_content(yield,loc,off,(uint64_t)Cicada_PENDING, (uint64_t)Cicada_ABORTED,cor_id);
	try_lock = (CicadaState) result;

	return Abort;
}

RC RDMA_Cicada::remote_commit(yield_func_t &yield, TxnManager * txnMng, Access * data, uint64_t num, uint64_t cor_id) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	INC_STATS(txnMng->get_thd_id(),mtx[33],get_sys_clock() - mtx_wait_starttime);
	DEBUG("Cicada Commit %ld: %d,%lu -- %ld\n", txnMng->get_txn_id(), data->type, txnMng->get_commit_timestamp(),
			data->data->get_primary_key());
	// Transaction * txn = txnMng->txn;
	uint64_t off = data->offset;
	uint64_t loc = data->location;
	uint64_t lock = txnMng->get_txn_id() + 1;
	off = off + sizeof(uint64_t) * 4 + sizeof(RdmaCicadaVersion) * (num % HIS_CHAIN_NUM);
    CicadaState try_lock = Cicada_PENDING;

	uint64_t result = txnMng->cas_remote_content(yield,loc,off,(uint64_t)Cicada_PENDING, (uint64_t)Cicada_COMMITTED,cor_id);
	try_lock = (CicadaState) result;

	return RCOK;
}

#endif

