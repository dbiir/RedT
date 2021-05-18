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
	// for(uint64_t i = 0; i < txn->row_cnt - txn->write_cnt; i++) {
	// 	//if (txn->accesses[ txnMng->write_set[i] ]->orig_row->cicada_version[txnMng->version_num[txnMng->write_set[i] ]].Wts != 0)
	// 	// printf("read:%d:wts:%d\n ", read_set[i], txn->accesses[ read_set[i] ]->orig_row->cicada_version[txnMng->version_num[read_set[i]]].Wts);

	// 	//printf("%d\n", txnMng->get_timestamp());
	// }
	timespan = get_sys_clock() - start_time;
	txnMng->txn_stats.cc_block_time += timespan;
	txnMng->txn_stats.cc_block_time_short += timespan;
	start_time = get_sys_clock();
	RC rc = RCOK;

	// 2.	预检查版本一致性，对写集中的每一项Xi
	for (uint64_t j = 0; j < txn->write_cnt; j++) {
        //local
		// printf("%d\n", txn->accesses[txnMng->write_set[i]]->location);
        if(txn->accesses[txnMng->write_set[j]]->location == g_node_id){
            Access * access = txn->accesses[ txnMng->write_set[j] ];
			// access->orig_row->manager->local_cas_lock(txnMng, 0, txnMng->get_txn_id());
			for(int cnt = access->orig_row->version_cnt; cnt >= access->orig_row->version_cnt - 4 && cnt >= 0; cnt--) {
				int i = cnt % HIS_CHAIN_NUM;
				if(access->orig_row->cicada_version[i].state == Cicada_ABORTED) {
					continue;
				}
				if(access->orig_row->cicada_version[i].Wts > txnMng->get_timestamp() || access->orig_row->cicada_version[i].Rts > txnMng->get_timestamp()) {
					rc = Abort;
					// printf("Abort\n");
					break;
				}
				if(access->orig_row->cicada_version[i].key == txnMng->version_num[txnMng->write_set[j]]) {
					rc = RCOK;
					access->orig_row->version_cnt ++;
					access->orig_row->cicada_version[(access->orig_row->version_cnt) % HIS_CHAIN_NUM].init(access->orig_row->version_cnt, txnMng->get_timestamp(), txnMng->get_timestamp());
					txnMng->uncommitted_set.insert(std::make_pair(txnMng->write_set[j], access->orig_row->version_cnt));
					// printf("add a num:%ld version:%ld in key:%ld\n", txnMng->write_set[j], access->orig_row->version_cnt, access->orig_row->get_primary_key());
					break;
				} else {
					rc = Abort;
				}
			}
			// access->orig_row->manager->local_cas_lock(txnMng, txnMng->get_txn_id(), 0);
        }else{
        //remote
            Access * access = txn->accesses[ txnMng->write_set[j] ];
			rc = remote_read_or_write(yield, access, txnMng, txnMng->write_set[j], true, cor_id);
        }
    }
	// 3.	对读集中的每一项
	for (uint64_t j = 0; j < txn->row_cnt - txn->write_cnt; j++) {
        //local
        if(txn->accesses[read_set[j]]->location == g_node_id){
            Access * access = txn->accesses[ read_set[j] ];
			// access->orig_row->manager->local_cas_lock(txnMng, 0, txnMng->get_txn_id());
            for(int cnt = access->orig_row->version_cnt ; cnt >= access->orig_row->version_cnt - 4 && cnt >= 0; cnt--) {
				int i = cnt % HIS_CHAIN_NUM;
				if(access->orig_row->cicada_version[i].state == Cicada_ABORTED) {
					continue;
				}
				if(access->orig_row->cicada_version[i].Wts > txnMng->get_timestamp()) {
					rc = Abort;
					break;
				}
				// printf("version:%d : num:%d: cor_id:%ld\n", access->orig_row->cicada_version[i].key, txnMng->version_num[read_set[i]], cor_id);
				if(access->orig_row->cicada_version[i].key == txnMng->version_num[read_set[j]] ) {
					rc = RCOK;
					if(access->orig_row->cicada_version[i].Rts < txnMng->get_timestamp())
						access->orig_row->cicada_version[i].Rts = txnMng->get_timestamp();
					break;
				} else {
					rc = Abort;
				}
			}
			// access->orig_row->manager->local_cas_lock(txnMng, txnMng->get_txn_id(), 0);
        }else{
        //remote
            Access * access = txn->accesses[ read_set[j] ];
            rc = remote_read_or_write(yield, access, txnMng, read_set[j], true, cor_id);
        }
    } 
	for (uint64_t j = 0; j < txn->write_cnt; j++) {
        //local
        if(txn->accesses[txnMng->write_set[j]]->location == g_node_id){
            Access * access = txn->accesses[ txnMng->write_set[j] ];
			for(int cnt = access->orig_row->version_cnt ; cnt >= access->orig_row->version_cnt - 4 && cnt >= 0; cnt--) {
				int i = cnt % HIS_CHAIN_NUM;
				if(access->orig_row->cicada_version[i].Wts > txnMng->get_timestamp() || access->orig_row->cicada_version[i].Rts > txnMng->get_timestamp() || access->orig_row->cicada_version[i].state == Cicada_ABORTED) {
					rc = Abort;
					continue;
				}
				if(access->orig_row->cicada_version[i].key == txnMng->version_num[txnMng->write_set[j]]) {
					//access->orig_row->version_cnt ++;
					//access->orig_row->cicada_version[(access->orig_row->version_cnt) % HIS_CHAIN_NUM].init(access->orig_row->version_cnt);
					rc = RCOK;
					break;
				} else {
					rc = Abort;
				}
			}
        }else{
        //remote
            Access * access = txn->accesses[ txnMng->write_set[j] ];
			rc = remote_read_or_write(yield, access, txnMng, txnMng->write_set[j], false, cor_id);
        }
    }
	timespan = get_sys_clock() - start_time;
	txnMng->txn_stats.cc_time += timespan;
	txnMng->txn_stats.cc_time_short += timespan;
	DEBUG("RDMA_CICADA Validate End %ld: %d\n",txnMng->get_txn_id(),rc==RCOK);
	// printf("RDMA_CICADA Validate End %ld: %d\n",txnMng->get_txn_id(),rc==RCOK);
	//printf("MAAT Validate End %ld: %d [%lu,%lu]\n",txn->get_txn_id(),rc==RCOK,lower,upper);
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

    // assert(try_lock == 0);

    

	if(data->type == RD) {
		row_t *temp_row = txnMng->read_remote_row(loc,off);
		for(int cnt = temp_row->version_cnt; cnt >= temp_row->version_cnt - 4 && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if(temp_row->cicada_version[i].Wts > txnMng->get_timestamp() || temp_row->cicada_version[i].state == Cicada_ABORTED) {
				rc = Abort;
				continue;
			}
			// printf("%d : %d\n", temp_row->cicada_version[i].key, txnMng->version_num[num]);
			if(temp_row->cicada_version[i].key == txnMng->version_num[num]) {
				rc = RCOK;
				// temp_row->cicada_version[i].Rts = txnMng->get_timestamp();
				break;
			} else {
				rc = Abort;
			}
		}
		if (rc == RCOK && temp_row->cicada_version[txnMng->version_num[num] % HIS_CHAIN_NUM].Rts < txnMng->get_timestamp()) {
			off = off + sizeof(uint64_t) * 2 + sizeof(RdmaCicadaVersion) * (txnMng->version_num[num] % HIS_CHAIN_NUM);
			uint64_t try_lock = txnMng->cas_remote_content(loc,off,temp_row->cicada_version[txnMng->version_num[num] % HIS_CHAIN_NUM].Rts, txnMng->get_timestamp());
			temp_row = txnMng->read_remote_row(loc,off);
			for(int cnt = temp_row->version_cnt; cnt >= temp_row->version_cnt - 4 && cnt >= 0; cnt--) {
				int i = cnt % HIS_CHAIN_NUM;
				if(temp_row->cicada_version[i].Wts > txnMng->get_timestamp() || temp_row->cicada_version[i].state == Cicada_ABORTED) {
					rc = Abort;
					continue;
				}
				//printf("%d : %d\n", temp_row->cicada_version[i].key, txnMng->version_num[num]);
				if(temp_row->cicada_version[i].key == txnMng->version_num[num]) {
					rc = RCOK;
					// temp_row->cicada_version[i].Rts = txnMng->get_timestamp();
					break;
				} else {
					rc = Abort;
				}
			}
		}
		mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	}

	if(data->type == WR && real_write == true) {
	    uint64_t try_lock = txnMng->cas_remote_content(loc,off,0,lock);
		row_t *temp_row = txnMng->read_remote_row(loc,off);
		assert(temp_row->get_primary_key() == data->data->get_primary_key());
		for(int cnt = temp_row->version_cnt; cnt >= temp_row->version_cnt - 4 && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if (temp_row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(temp_row->cicada_version[i].Wts > txnMng->get_timestamp() || temp_row->cicada_version[i].Rts > txnMng->get_timestamp()) {
				rc = Abort;
				// printf("Abort\n");
				break;
			}
			if(temp_row->cicada_version[i].key == txnMng->version_num[num] && real_write == true) {
				rc = RCOK;
				temp_row->version_cnt ++;
				temp_row->cicada_version[(temp_row->version_cnt) % HIS_CHAIN_NUM].init(temp_row->version_cnt, txnMng->get_timestamp(), txnMng->get_timestamp());
				txnMng->uncommitted_set.insert(std::make_pair(num, temp_row->version_cnt));
				// printf("add a access:%ld version:%ld in key:%ld\n", num, temp_row->version_cnt, temp_row->get_primary_key());
			} else {
				rc = Abort;
			}
		}
		temp_row->_tid_word = 0;

        operate_size = row_t::get_row_size(temp_row->tuple_size);
        // char *tmp_buf2 = Rdma::get_row_client_memory(thd_id);
		// memcpy(tmp_buf2, (char *)temp_row, operate_size);
        assert(txnMng->write_remote_row(loc,operate_size,off, (char *)temp_row) == true);
		//uncommitted_writes->erase(txn->get_txn_id());
		mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	}
	if(data->type == WR && real_write == false) {
		row_t *temp_row = txnMng->read_remote_row(loc,off);
		assert(temp_row->get_primary_key() == data->data->get_primary_key());
		for(int cnt = temp_row->version_cnt; cnt >= temp_row->version_cnt - 4 && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if (temp_row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(temp_row->cicada_version[i].Wts > txnMng->get_timestamp() || temp_row->cicada_version[i].Rts > txnMng->get_timestamp()) {
				rc = Abort;
				// printf("Abort\n");
				break;
			}
			if(temp_row->cicada_version[i].key == txnMng->version_num[num] && real_write == true) {
				rc = RCOK;
				// temp_row->version_cnt ++;
				// printf("add a access:%ld version:%ld in key:%ld\n", num, temp_row->version_cnt, temp_row->get_primary_key());
			} else {
				rc = Abort;
			}
		}
		//uncommitted_writes->erase(txn->get_txn_id());
		mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	}
	//mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	return rc;
}

RC
RDMA_Cicada::finish(yield_func_t &yield, RC rc , TxnManager * txnMng, uint64_t cor_id)
{
	Transaction *txn = txnMng->txn;
	if (rc == Abort) {
		//if (txnMng->num_locks > txnMng->get_access_cnt())
		//	return rc;
		for (map<uint64_t, uint64_t>::iterator i=txnMng->uncommitted_set.begin(); i!=txnMng->uncommitted_set.end(); i++) {
			// printf("abort:%d:%d\n", i->first, i->second);
			if(txn->accesses[i->first]->location == g_node_id){
				rc = txn->accesses[i->first]->orig_row->manager->abort(i->second,txnMng);
			} else{
			//remote
        		//release_remote_lock(txnMng,txnMng->write_set[i] );
				
				rc = remote_abort(yield, txnMng, txn->accesses[i->first], i->second, cor_id);

			}
  		}
	} else {
	//commit
		ts_t time = get_sys_clock();
		for (map<uint64_t, uint64_t>::iterator i=txnMng->uncommitted_set.begin(); i!=txnMng->uncommitted_set.end(); i++) {
			// printf("commit%d:%d\n", i->first, i->second);
			if(txn->accesses[i->first]->location == g_node_id){
				Access * access = txn->accesses[i->first];
				rc = txn->accesses[i->first]->orig_row->manager->commit(i->second, txnMng, access->data);
			} else{
			//remote
        		//release_remote_lock(txnMng,txnMng->write_set[i] );
				Access * access = txn->accesses[i->first];
				//DEBUG("commit access txn %ld, off: %ld loc: %ld and key: %ld\n",txnMng->get_txn_id(), access->offset, access->location, access->data->get_primary_key());
				rc =  remote_commit(yield, txnMng, txn->accesses[i->first], i->second, cor_id);
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
		txn->accesses[i]->offset = 0;
		}
	}
	//memset(txnMng->write_set, 0, 100);
	return rc;
}
RC RDMA_Cicada::remote_abort(yield_func_t &yield, TxnManager * txnMng, Access * data, uint64_t num, uint64_t cor_id) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	INC_STATS(txnMng->get_thd_id(),mtx[32],get_sys_clock() - mtx_wait_starttime);
	DEBUG("Cicada Abort %ld: %d -- %ld\n",txnMng->get_txn_id(), data->type, data->data->get_primary_key());
	Transaction * txn = txnMng->txn;
	uint64_t off = data->offset;
	uint64_t loc = data->location;

	off = off + sizeof(uint64_t) * 4 + sizeof(RdmaCicadaVersion) * (num % HIS_CHAIN_NUM);
    uint64_t try_lock = txnMng->cas_remote_content(yield,loc,off,Cicada_PENDING, Cicada_ABORTED,cor_id);
	return Abort;
}

RC RDMA_Cicada::remote_commit(yield_func_t &yield, TxnManager * txnMng, Access * data, uint64_t num, uint64_t cor_id) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	INC_STATS(txnMng->get_thd_id(),mtx[33],get_sys_clock() - mtx_wait_starttime);
	DEBUG("Cicada Commit %ld: %d,%lu -- %ld\n", txnMng->get_txn_id(), data->type, txnMng->get_commit_timestamp(),
			data->data->get_primary_key());
	Transaction * txn = txnMng->txn;
	uint64_t off = data->offset;
	uint64_t loc = data->location;
	off = off + sizeof(uint64_t) * 4 + sizeof(RdmaCicadaVersion) * (num % HIS_CHAIN_NUM);
    uint64_t try_lock = txnMng->cas_remote_content(yield,loc,off,Cicada_PENDING, Cicada_COMMITTED,cor_id);
    
	return Abort;
}

#endif

