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

RC RDMA_Cicada::validate(TxnManager * txnMng) {
	uint64_t start_time = get_sys_clock();
	uint64_t timespan;
	Transaction *txn = txnMng->txn;
	sem_wait(&_semaphore);
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
				if (txn->accesses[ txnMng->write_set[j] ]->orig_row->cicada_version[txnMng->version_num[txnMng->write_set[j] ]].Wts <
					txn->accesses[ txnMng->write_set[j + 1] ]->orig_row->cicada_version[txnMng->version_num[txnMng->write_set[j] ]].Wts)
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

	//2.Precheck version consistency for each entry in the write set
	for (uint64_t i = 0; i < txn->write_cnt; i++) {
        //local
        if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ txnMng->write_set[i] ];
			for(int cnt = access->orig_row->version_cnt - 1; cnt >= access->orig_row->version_cnt - 5 && cnt >= 0; cnt--) {
				int i = cnt % HIS_CHAIN_NUM;
				if(access->orig_row->cicada_version[i].Wts > txnMng->start_ts || access->orig_row->cicada_version[i].Wts > txnMng->start_ts || access->orig_row->cicada_version[i].state == Cicada_ABORTED) {
					continue;
				}
				if(access->orig_row->cicada_version[i].key == txnMng->version_num[txnMng->write_set[i]]) {
					access->orig_row->version_cnt ++;
					access->orig_row->cicada_version[(access->orig_row->version_cnt) % HIS_CHAIN_NUM].init(access->orig_row->version_cnt);
					txnMng->uncommitted_set.insert(std::make_pair(txnMng->write_set[i], access->orig_row->version_cnt));
				} else {
					rc = Abort;
				}
			}
        }else{
        //remote
            Access * access = txn->accesses[ txnMng->write_set[i] ];
			rc = remote_read_or_write(access, txnMng, txnMng->write_set[i], true);
        }
    }
	//3. for each entry in the read set
	for (uint64_t i = 0; i < txn->row_cnt-txn->write_cnt; i++) {
        //local
        if(txn->accesses[read_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ read_set[i] ];
            for(int cnt = access->orig_row->version_cnt - 1; cnt >= access->orig_row->version_cnt - 5 && cnt >= 0; cnt--) {
				int i = cnt % HIS_CHAIN_NUM;
				if(access->orig_row->cicada_version[i].Wts > txnMng->start_ts || access->orig_row->cicada_version[i].state == Cicada_ABORTED) {
					continue;
				}
				if(access->orig_row->cicada_version[i].key == txnMng->version_num[read_set[i]]) {
					access->orig_row->cicada_version[i].Rts = get_sys_clock();
				} else {
					rc = Abort;
				}
			}
        }else{
        //remote
            Access * access = txn->accesses[ read_set[i] ];
            rc = remote_read_or_write(access, txnMng, read_set[i], true);
        }
    }
	for (uint64_t i = 0; i < txn->write_cnt; i++) {
        //local
        if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ txnMng->write_set[i] ];
			for(int cnt = access->orig_row->version_cnt - 1; cnt >= access->orig_row->version_cnt - 5 && cnt >= 0; cnt--) {
				int i = cnt % HIS_CHAIN_NUM;
				if(access->orig_row->cicada_version[i].Wts > txnMng->start_ts || access->orig_row->cicada_version[i].Wts > txnMng->start_ts || access->orig_row->cicada_version[i].state == Cicada_ABORTED) {
					continue;
				}
				if(access->orig_row->cicada_version[i].key == txnMng->version_num[txnMng->write_set[i]]) {
					//access->orig_row->version_cnt ++;
					//access->orig_row->cicada_version[(access->orig_row->version_cnt) % HIS_CHAIN_NUM].init(access->orig_row->version_cnt);
				} else {
					rc = Abort;
				}
			}
        }else{
        //remote
            Access * access = txn->accesses[ txnMng->write_set[i] ];
			rc = remote_read_or_write(access, txnMng, txnMng->write_set[i], false);
        }
    }
	timespan = get_sys_clock() - start_time;
	txnMng->txn_stats.cc_time += timespan;
	txnMng->txn_stats.cc_time_short += timespan;
	DEBUG("RDMA_CICADA Validate End %ld: %d\n",txnMng->get_txn_id(),rc==RCOK);
	//printf("RDMA_CICADA Validate End %ld: %d\n",txnMng->get_txn_id(),rc==RCOK);
	//printf("MAAT Validate End %ld: %d [%lu,%lu]\n",txn->get_txn_id(),rc==RCOK,lower,upper);
	sem_post(&_semaphore);
	return rc;

}

RC RDMA_Cicada::remote_read_or_write(Access * data, TxnManager * txnMng, uint64_t num, bool real_write) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	Transaction * txn = txnMng->txn;
	uint64_t off = data->offset;
	uint64_t loc = data->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id() + 1;
	uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);

    RC rc = RCOK;

    uint64_t try_lock = txnMng->cas_remote_content(loc,off,0,lock);
    // assert(try_lock == 0);

    row_t *temp_row = txnMng->read_remote_row(loc,off);
	assert(temp_row->get_primary_key() == data->data->get_primary_key());

	if(data->type == RD) {
		for(int cnt = temp_row->version_cnt - 1; cnt >= temp_row->version_cnt - 5 && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if(temp_row->cicada_version[i].Wts > txnMng->start_ts || temp_row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(temp_row->cicada_version[i].key == txnMng->version_num[num]) {
				temp_row->cicada_version[i].Rts = get_sys_clock();
			} else {
				rc = Abort;
			}
		}
		temp_row->_tid_word = 0;

        operate_size = row_t::get_row_size(temp_row->tuple_size);
        // char *tmp_buf2 = Rdma::get_row_client_memory(thd_id);
		// memcpy(tmp_buf2, (char *)temp_row, operate_size);

        assert(txnMng->write_remote_row(loc,operate_size,off,(char *)temp_row) == true);
	}

	if(data->type == WR) {
		for(int cnt = temp_row->version_cnt - 1; cnt >= temp_row->version_cnt - 5 && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if(temp_row->cicada_version[i].Wts > txnMng->start_ts || temp_row->cicada_version[i].Wts > txnMng->start_ts || temp_row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(temp_row->cicada_version[i].key == txnMng->version_num[num] && real_write == true) {
				temp_row->version_cnt ++;
				temp_row->cicada_version[(temp_row->version_cnt) % HIS_CHAIN_NUM].init(temp_row->version_cnt);
				txnMng->uncommitted_set.insert(std::make_pair(num, temp_row->version_cnt));
			} else if (temp_row->cicada_version[i].key == txnMng->version_num[num] && real_write == false) {

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
	}
	mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	return rc;
}

RC
RDMA_Cicada::finish(RC rc , TxnManager * txnMng)
{
	Transaction *txn = txnMng->txn;
	
	if (rc == Abort) {
		//if (txnMng->num_locks > txnMng->get_access_cnt())
		//	return rc;
		for (map<uint64_t, uint64_t>::iterator i=txnMng->uncommitted_set.begin(); i!=txnMng->uncommitted_set.end(); i++) {
			if(txn->accesses[i->first]->location == g_node_id){
				rc = txn->accesses[i->first]->orig_row->manager->abort(i->second,txnMng);
			} else{
			//remote
        		//release_remote_lock(txnMng,txnMng->write_set[i] );
				
				rc = remote_abort(txnMng, txn->accesses[i->first], i->second);

			}
  		}
	} else {
	//commit
		ts_t time = get_sys_clock();
		for (map<uint64_t, uint64_t>::iterator i=txnMng->uncommitted_set.begin(); i!=txnMng->uncommitted_set.end(); i++) {
			if(txn->accesses[i->first]->location == g_node_id){
				Access * access = txn->accesses[i->first];
				rc = txn->accesses[i->first]->orig_row->manager->commit(i->second, txnMng, access->data);
			} else{
			//remote
        		//release_remote_lock(txnMng,txnMng->write_set[i] );
				Access * access = txn->accesses[i->first];
				//DEBUG("commit access txn %ld, off: %ld loc: %ld and key: %ld\n",txnMng->get_txn_id(), access->offset, access->location, access->data->get_primary_key());
				rc =  remote_commit(txnMng, txn->accesses[i->first], i->second);
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
RC RDMA_Cicada::remote_abort(TxnManager * txnMng, Access * data, uint64_t num) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	INC_STATS(txnMng->get_thd_id(),mtx[32],get_sys_clock() - mtx_wait_starttime);
	DEBUG("Cicada Abort %ld: %d -- %ld\n",txnMng->get_txn_id(), data->type, data->data->get_primary_key());
	Transaction * txn = txnMng->txn;
	uint64_t off = data->offset;
	uint64_t loc = data->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id() + 1;
	uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);

    uint64_t try_lock = txnMng->cas_remote_content(loc,off,0,lock);
    // assert(try_lock == 0);

    row_t *temp_row = txnMng->read_remote_row(loc,off);
	assert(temp_row->get_primary_key() == data->data->get_primary_key());

	temp_row->cicada_version[num % HIS_CHAIN_NUM].state = Cicada_ABORTED;
	temp_row->_tid_word = 0;

    operate_size = row_t::get_row_size(temp_row->tuple_size);
    // char *tmp_buf2 = Rdma::get_row_client_memory(thd_id);
	// std::memcpy(tmp_buf2, (char *)temp_row, operate_size);
    assert(txnMng->write_remote_row(loc,operate_size,off,(char *)temp_row) == true);
    
		//uncommitted_writes->erase(txn->get_txn_id());
	mem_allocator.free(temp_row, sizeof(row_t::get_row_size(ROW_DEFAULT_SIZE)));
	return Abort;
}

RC RDMA_Cicada::remote_commit(TxnManager * txnMng, Access * data, uint64_t num) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	INC_STATS(txnMng->get_thd_id(),mtx[33],get_sys_clock() - mtx_wait_starttime);
	DEBUG("Cicada Commit %ld: %d,%lu -- %ld\n", txnMng->get_txn_id(), data->type, txnMng->get_commit_timestamp(),
			data->data->get_primary_key());
	Transaction * txn = txnMng->txn;
	uint64_t off = data->offset;
	uint64_t loc = data->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id() + 1;
	uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
	
    uint64_t try_lock = txnMng->cas_remote_content(loc,off,0,lock);
    // assert(try_lock == 0);

    row_t *temp_row = txnMng->read_remote_row(loc,off);
	assert(temp_row->get_primary_key() == data->data->get_primary_key());
	
	temp_row->cicada_version[num % HIS_CHAIN_NUM].state = Cicada_COMMITTED;
	temp_row->_tid_word = 0;

    operate_size = row_t::get_row_size(temp_row->tuple_size);
    // char *tmp_buf2 = Rdma::get_row_client_memory(thd_id);
	// std::memcpy(tmp_buf2, (char *)temp_row, operate_size);
    assert(txnMng->write_remote_row(loc,operate_size,off,(char *)temp_row) == true);
	
	mem_allocator.free(temp_row, sizeof(row_t::get_row_size(ROW_DEFAULT_SIZE)));
	return Abort;
}

#endif

