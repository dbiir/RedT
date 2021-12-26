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
#include "rdma_ts.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_rdma_ts.h"
#include "rdma.h"
#include "qps/op.hh"
#include "src/rdma/sop.hh"
#include "src/sshed.hh"
#include "transport.h"
#include "qps/op.hh"

#if CC_ALG == RDMA_TS

void RDMA_ts::write_remote(yield_func_t &yield, RC rc, TxnManager * txn, Access * access, uint64_t cor_id) {
	uint64_t off = access->offset;
    uint64_t loc = access->location;
	uint64_t thd_id = txn->get_thd_id();

	uint64_t operate_size = row_t::get_row_size(access->data->tuple_size);
    if(rc == Abort) 
		operate_size = sizeof(uint64_t);

    txn->write_remote_row(yield,loc,operate_size,off,(char*)access->data,cor_id);
    INC_STATS(txn->get_thd_id(), rdma_write_cnt, 1);
}
 
void RDMA_ts::commit_write(yield_func_t &yield, TxnManager * txn , uint64_t num , access_t type, uint64_t cor_id){
	Access * access = txn->txn->accesses[num];
	uint64_t loc = access->location;
    uint64_t offset = access->offset;
    uint64_t lock_num = txn->get_txn_id() + 1;
	assert(loc != g_node_id);

	row_t * row = access->data;
	assert(row != NULL);
	ts_t ts = txn->get_timestamp();
	//validate lock suc
	#if USE_DBPAOR == true
		uint64_t try_lock;
		row_t* remote_row = txn->cas_and_read_remote(yield,try_lock,loc,offset,offset,0,lock_num,cor_id);
		while(try_lock!=0){ //lock fail
			mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			remote_row = txn->cas_and_read_remote(yield,try_lock,loc,offset,offset,0,lock_num,cor_id);
		}
	#else
		assert(txn->loop_cas_remote(yield,loc,offset,0,lock_num,cor_id) == true);//lock in loop
		row_t *remote_row = txn->read_remote_row(yield,loc,offset,cor_id);
	#endif
	row_t* _row = remote_row;
	if(type == XP) {
		Row_rdma_ts::abort_modify_row(yield, txn, _row, cor_id);
	}
	if(type == WR){
		Row_rdma_ts::commit_modify_row(yield, txn, _row, cor_id);
	}
	_row->mutx = 0;
	// memcpy(row, remote_row, row_t::get_row_size(remote_row->tuple_size));

	uint64_t operate_size = row_t::get_row_size(remote_row->tuple_size);
    txn->write_remote_row(yield,loc,operate_size,offset,(char*)_row,cor_id);
	// assert(remote_row->mutx != 0);
	row->free_row();
	mem_allocator.free(row, row_t::get_row_size(access->data->tuple_size));
	mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
}

void RDMA_ts::finish(RC rc, TxnManager * txnMng){
    Transaction *txn = txnMng->txn;
    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        if(txn->accesses[i]->location != g_node_id && txn->accesses[i]->orig_row != NULL){
			mem_allocator.free(txn->accesses[i]->orig_row,0);
			txn->accesses[i]->orig_row = NULL;
        }
    }
}

#endif

