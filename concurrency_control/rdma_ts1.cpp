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
#include "row.h"
#include "rdma_ts1.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_rdma_ts1.h"
#include "rdma.h"
#include "qps/op.hh"
#include "src/rdma/sop.hh"
#include "src/sshed.hh"
#include "transport.h"
#include "qps/op.hh"
#include "rdma_maat.h"

#if CC_ALG == RDMA_TS1


void  RDMA_ts1::write_remote(yield_func_t &yield, RC rc, TxnManager * txn, Access * access, uint64_t cor_id) {
	uint64_t off = access->offset;
    uint64_t loc = access->location;
	uint64_t thd_id = txn->get_thd_id();

	uint64_t operate_size = row_t::get_row_size(access->data->tuple_size);
    if(rc == Abort) 
		operate_size = sizeof(uint64_t);

#if DEBUG_PRINTF
	// printf("[远程写的传入数据]rc:%d,事务号：%d，主键：%d，锁：%lu，tid：%lu,rts：%lu,wts：%lu\n",rc,txn->get_txn_id(),access->data->get_primary_key(),access->data->mutx,access->data->tid,access->data->rts,access->data->wts);
#endif

    txn->write_remote_row(yield,loc,operate_size,off,(char*)access->data,cor_id);
    INC_STATS(txn->get_thd_id(), rdma_write_cnt, 1);
    // char *local_loc = Rdma::get_row_client_memory(thd_id);
    // memcpy(local_loc, (char*)access->data, operate_size);

#if DEBUG_PRINTF
// if(access->type == WR){
	
//     row_t *temp_row = txn->read_remote_row(loc,off);
// 	printf("[远程写入后再次读]事务号：%d，主键：%d，锁：%lu，tid：%lu,rts：%lu,wts：%lu\n",txn->get_txn_id(),temp_row->get_primary_key(),temp_row->mutx,temp_row->tid,temp_row->rts,temp_row->wts);
// 	mem_allocator.free(temp_row,sizeof(ROW_DEFAULT_SIZE));
// }
#endif
}

RC RDMA_ts1::validate(yield_func_t &yield, TxnManager * txnMng, uint64_t cor_id) {
	Transaction *txn = txnMng->txn;
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
	for (uint64_t i = 0; i < wr_cnt; i++) {
		for (uint64_t j = 0; j < CASCADING_LENGTH; j++) {
			uint64_t wid = txn->accesses[txnMng->write_set[i]]->wid[j];
			if (wid == 0) continue;
			TSState state;
		retry:
			if(wid % g_node_cnt == g_node_id) {
				state = rdma_txn_table.local_get_state(txnMng->get_thd_id(), wid);
			} else {
				state = rdma_txn_table.remote_get_state(yield, txnMng, wid, cor_id);
			}
			if (state == TS_RUNNING) goto retry;
			if (state == TS_ABORTING) {
				// printf("[级联回滚]事务号:%ld, 级联事务: %ld\n",txnMng->get_txn_id(),wid);
				return Abort;
			} 
		}
	}
	return RCOK;
}
 
void  RDMA_ts1::commit_write(yield_func_t &yield, TxnManager * txn , uint64_t num , access_t type, uint64_t cor_id){
	Access * access = txn->txn->accesses[num];
	uint64_t loc = access->location;
    uint64_t offset = access->offset;
    uint64_t lock_num = txn->get_txn_id() + 1;
	assert(loc != g_node_id);

	row_t * row = access->data;
	assert(row != NULL);
    // row_t *remote_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
    // row_t *remote_row = Rdma::get_row_client_memory(txn->get_thd_id());
	ts_t ts = txn->get_timestamp();
	
#if USE_DBPAOR == true
	if(type == XP){
		uint64_t try_lock;
		row_t* remote_row = txn->cas_and_read_remote(yield,try_lock,loc,offset,offset,0,lock_num,cor_id);
		while(try_lock!=0){ //lock fail
			mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			remote_row = txn->cas_and_read_remote(yield,try_lock,loc,offset,offset,0,lock_num,cor_id);
		}
		ts_t ts = txn->get_timestamp();
		// remote_row->tid = access->wid;
		bool clean = false;
		for (int i = 0; i < CASCADING_LENGTH; i++) {
			if (remote_row->tid[i] == txn->get_txn_id()) clean = true;
			if (clean && remote_row->tid[i]!=0) {
				remote_row->tid[i] = 0;
			}
		}
		remote_row->mutx = 0;
		memcpy(row, remote_row, row_t::get_row_size(remote_row->tuple_size));
		write_remote(yield,RCOK,txn,access,cor_id);
		mem_allocator.free(row, row_t::get_row_size(access->data->tuple_size));
		mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
	}
	else if(type == WR){
		uint64_t try_lock;
		row_t* remote_row = txn->cas_and_read_remote(yield,try_lock,loc,offset,offset,0,lock_num,cor_id);
		while(try_lock!=0){ //lock fail
			mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			remote_row = txn->cas_and_read_remote(yield,try_lock,loc,offset,offset,0,lock_num,cor_id);
		}
		//update wts, tid and unlock
		if(remote_row->wts < ts) remote_row->wts = ts;
		int i = 0;
		for (i = 0; i < CASCADING_LENGTH; i++) {
			if (remote_row->tid[i] == txn->get_txn_id()) break;
		}
		for (; i < CASCADING_LENGTH; i ++) {
			if (i + 1 < CASCADING_LENGTH && remote_row->tid[i + 1] != 0) {
				remote_row->tid[i] = remote_row->tid[i+1];
			}
			if (i + 1 == CASCADING_LENGTH) {
				remote_row->tid[i] = 0;
			}
		}
		remote_row->mutx = 0;

		memcpy(row, remote_row, row_t::get_row_size(remote_row->tuple_size));
		write_remote(yield,RCOK,txn,access,cor_id);
		// assert(remote_row->mutx != 0);

		row->free_row();
		mem_allocator.free(row, row_t::get_row_size(access->data->tuple_size));
		mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
	}
#else
    assert(txn->loop_cas_remote(yield,loc,offset,0,lock_num,cor_id) == true);//lock in loop
	row_t *remote_row = txn->read_remote_row(yield,loc,offset,cor_id);

	if (type == XP) {
		bool clean = false;
		for (int i = 0; i < CASCADING_LENGTH; i++) {
			if (remote_row->tid[i] == txn->get_txn_id()) clean = true;
			if (clean && remote_row->tid[i]!=0) {
				remote_row->tid[i] = 0;
			}
		}
		ts_t ts = txn->get_timestamp();
		// _row->tid = access->wid;
		remote_row->mutx = 0;
	}
	else if (type == WR) {
#if DEBUG_PRINTF
		// printf("[远程写提交]事务号：%d，主键：%d，锁：%d,tid:%lu,rts:%lu,wts:%lu\n",txn->get_txn_id(),remote_row->get_primary_key(),remote_row->mutx,remote_row->tid,remote_row->rts,remote_row->wts);
#endif
		if(remote_row->wts < ts) remote_row->wts = ts;
		int i = 0;
		for (i = 0; i < CASCADING_LENGTH; i++) {
			if (remote_row->tid[i] == txn->get_txn_id()) break;
		}
		for (; i < CASCADING_LENGTH; i ++) {
			if (i + 1 < CASCADING_LENGTH && remote_row->tid[i + 1] != 0) {
				remote_row->tid[i] = remote_row->tid[i+1];
			}
			if (i + 1 == CASCADING_LENGTH) {
				remote_row->tid[i] = 0;
			}
		}
	}else {
		assert(false);
	}
	remote_row->mutx = 0;
	memcpy(row, remote_row, row_t::get_row_size(remote_row->tuple_size));
	write_remote(yield,RCOK,txn,access,cor_id);
	// assert(remote_row->mutx != 0);

	row->free_row();
	mem_allocator.free(row, row_t::get_row_size(access->data->tuple_size));
	mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
	// mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
#endif
}

void RDMA_ts1::finish(RC rc, TxnManager * txnMng){
    Transaction *txn = txnMng->txn;
    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        if(txn->accesses[i]->location != g_node_id){
        //remote
			// mem_allocator.free(txn->accesses[i]->data,0);
			mem_allocator.free(txn->accesses[i]->orig_row,0);
			// txn->accesses[i]->data = NULL;
			txn->accesses[i]->orig_row = NULL;
        }
    }
}

#endif

