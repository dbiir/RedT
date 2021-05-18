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

#if CC_ALG == RDMA_TS1

bool  RDMA_ts1::remote_try_lock(TxnManager * txn, uint64_t loc, uint64_t off){
    bool result = false;
	uint64_t thd_id = txn->get_thd_id();
	uint64_t lock = txn->get_txn_id()+1;

	uint64_t *local_loc = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto mr = client_rm_handler->get_reg_attr().value();

	rdmaio::qp::Op<> op;
    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + off), remote_mr_attr[loc].key).set_cas(0, lock);
  	assert(op.set_payload(local_loc, sizeof(uint64_t), mr.key) == true);
  	auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

	RDMA_ASSERT(res_s2 == IOCode::Ok);
	auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p2 == IOCode::Ok);
	if (*local_loc == 0)
		result = true;

	return result;
}

bool  RDMA_ts1::remote_try_lock(TxnManager * txn, row_t * temp_row, uint64_t loc, uint64_t off){
    bool result = false;
	uint64_t thd_id = txn->get_thd_id();
	uint64_t lock = txn->get_txn_id()+1;

	uint64_t *local_loc = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto mr = client_rm_handler->get_reg_attr().value();

	rdmaio::qp::Op<> op;
    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + off), remote_mr_attr[loc].key).set_cas(0, lock);
  	assert(op.set_payload(local_loc, sizeof(uint64_t), mr.key) == true);
  	auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

	RDMA_ASSERT(res_s2 == IOCode::Ok);
	auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p2 == IOCode::Ok);
	if (*local_loc == 0)
		result = true;
	
	//check the lock 
	uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
	char *test_buf = Rdma::get_row_client_memory(thd_id);
	memset(test_buf, 0, operate_size);
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
	memset(temp_row, 0, operate_size);
	memcpy(temp_row, test_buf, operate_size);
	if (temp_row->mutx == lock )
		result = true;
#if DEBUG_PRINTF
	printf("[RDMA加锁后]事务号：%d，主键：%d，锁：%lu tid:%lu\n",txn->get_txn_id(),temp_row->get_primary_key(),temp_row->mutx, temp_row->tid);
#endif
	return result;
}

void RDMA_ts1::read_remote_row(TxnManager * txn, row_t * remote_row, uint64_t loc, uint64_t off){
	uint64_t thd_id = txn->get_thd_id();
	uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
	char *test_buf = Rdma::get_row_client_memory(thd_id);
    memset(test_buf, 0, operate_size);

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
	memset(remote_row, 0, operate_size);
	memcpy(remote_row, test_buf, operate_size);
}

void  RDMA_ts1::write_remote(yield_func_t &yield, RC rc, TxnManager * txn, Access * access, uint64_t cor_id) {
	uint64_t off = access->offset;
    uint64_t loc = access->location;
	uint64_t thd_id = txn->get_thd_id();

	uint64_t operate_size = row_t::get_row_size(access->data->tuple_size);
    if(rc == Abort) 
		operate_size = sizeof(uint64_t);

#if DEBUG_PRINTF
	printf("[远程写的传入数据]rc:%d,事务号：%d，主键：%d，锁：%lu，tid：%lu,rts：%lu,wts：%lu\n",rc,txn->get_txn_id(),access->data->get_primary_key(),access->data->mutx,access->data->tid,access->data->rts,access->data->wts);
#endif

    txn->write_remote_row(yield,loc,operate_size,off,(char*)access->data,cor_id);
    INC_STATS(txn->get_thd_id(), rdma_write_cnt, 1);
    // char *local_loc = Rdma::get_row_client_memory(thd_id);
    // memcpy(local_loc, (char*)access->data, operate_size);

#if DEBUG_PRINTF
if(access->type == WR){
	
    row_t *temp_row = txn->read_remote_row(loc,off);
	printf("[远程写入后再次读]事务号：%d，主键：%d，锁：%lu，tid：%lu,rts：%lu,wts：%lu\n",txn->get_txn_id(),temp_row->get_primary_key(),temp_row->mutx,temp_row->tid,temp_row->rts,temp_row->wts);
	mem_allocator.free(temp_row,sizeof(ROW_DEFAULT_SIZE));
}
#endif

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
	//validate lock suc
	row_t *temp_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));

// 	bool suc = false;
// 	//lock row
// 	suc = remote_try_lock(txn, temp_row, loc, offset);

// 	while(suc == false){
// 		INC_STATS(txn->get_thd_id(),lock_retry_cnt,1);
// #if DEBUG_PRINTF
// 		printf("远程提交 lock retry！事务号：%ld,主键：%ld\n",txn->get_txn_id(),row->get_primary_key());
// #endif
// 		suc = remote_try_lock(txn, temp_row, loc, offset);
// 	}

// read_remote_row(txn, remote_row, loc, offset);

    assert(txn->loop_cas_remote(yield,loc,offset,0,lock_num,cor_id) == true);//lock in loop
	row_t *remote_row = txn->read_remote_row(yield,loc,offset,cor_id);

	if (type == XP) {
		remote_row->tid = 0;
	}
	else if (type == WR) {
#if DEBUG_PRINTF
		printf("[远程写提交]事务号：%d，主键：%d，锁：%d,tid:%lu,rts:%lu,wts:%lu\n",txn->get_txn_id(),remote_row->get_primary_key(),remote_row->mutx,remote_row->tid,remote_row->rts,remote_row->wts);
#endif
		if(remote_row->wts < ts) remote_row->wts = ts;
		remote_row->tid = 0;
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
	mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
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

