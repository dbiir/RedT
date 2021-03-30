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
	
	//判断是否成功加锁
	uint64_t operate_size = sizeof(row_t);
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
	uint64_t operate_size = sizeof(row_t);
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

void  RDMA_ts1::write_remote(RC rc, TxnManager * txn, Access * access) {
	uint64_t off = access->offset;
    uint64_t loc = access->location;
	uint64_t thd_id = txn->get_thd_id();

	uint64_t operate_size = sizeof(row_t);
    if(rc == Abort) 
		operate_size = sizeof(uint64_t);

#if DEBUG_PRINTF
	printf("[远程写的传入数据]rc:%d,事务号：%d，主键：%d，锁：%lu，tid：%lu,rts：%lu,wts：%lu\n",rc,txn->get_txn_id(),access->data->get_primary_key(),access->data->mutx,access->data->tid,access->data->rts,access->data->wts);
#endif

    char *local_loc = Rdma::get_row_client_memory(thd_id);
    memcpy(local_loc, (char*)access->data, operate_size);

    uint64_t starttime = get_sys_clock();
	uint64_t endtime;

	auto res_s = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_loc),
		.remote_addr = off,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
  	auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

	endtime = get_sys_clock();
	INC_STATS(txn->get_thd_id(), rdma_write_time, endtime-starttime);
	INC_STATS(txn->get_thd_id(), rdma_write_cnt, 1);

#if DEBUG_PRINTF
if(access->type == WR){
	operate_size = sizeof(row_t);
	char *test_buf = Rdma::get_row_client_memory(thd_id);
    memset(test_buf, 0, operate_size);
	res_s = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = off,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
  	res_p = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	row_t *temp_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
	memcpy(temp_row, test_buf, operate_size);
	printf("[远程写入后再次读]事务号：%d，主键：%d，锁：%lu，tid：%lu,rts：%lu,wts：%lu\n",txn->get_txn_id(),temp_row->get_primary_key(),temp_row->mutx,temp_row->tid,temp_row->rts,temp_row->wts);
	mem_allocator.free(temp_row,sizeof(row_t));
}
#endif

}
 
void  RDMA_ts1::commit_write(TxnManager * txn , uint64_t num , access_t type){
	Access * access = txn->txn->accesses[num];
	uint64_t loc = access->location;
	assert(loc != g_node_id);
	uint64_t offset = access->offset;
	row_t * row = access->data;
	assert(row != NULL);
	row_t *remote_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
	ts_t ts = txn->get_timestamp();
	//validate lock suc
	row_t *temp_row = (row_t *)mem_allocator.alloc(sizeof(row_t));

	bool suc = false;
	//lock row
	suc = remote_try_lock(txn, temp_row, loc, offset);
	while(suc == false){
		INC_STATS(txn->get_thd_id(),lock_retry_cnt,1);
#if DEBUG_PRINTF
		printf("远程提交 lock retry！事务号：%ld,主键：%ld\n",txn->get_txn_id(),row->get_primary_key());
#endif
		suc = remote_try_lock(txn, temp_row, loc, offset);
	}
	
	read_remote_row(txn, remote_row, loc, offset);

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
	memcpy(row, remote_row, sizeof(row_t));
	write_remote(RCOK,txn,access);
	// assert(remote_row->mutx != 0);
/*
#if DEBUG_PRINTF
		printf("[远程写入前]事务号：%d，主键：%d，锁：%d,tid:%lu,rts:%lu,wts:%lu\n",txn->get_txn_id(),row->get_primary_key(),row->mutx,row->tid,row->rts,row->wts);
#endif
	// remote_write_and_unlock(RCOK, txn, access);
	uint64_t thd_id = txn->get_thd_id();
	uint64_t operate_size = sizeof(row_t);
	char *local_loc = Rdma::get_row_client_memory(thd_id);
	row->mutx = 0;
	memcpy(local_loc, (char*)row, operate_size);
	uint64_t starttime = get_sys_clock();
	uint64_t endtime;

	auto res_s = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_loc),
		.remote_addr = offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
  	auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

	endtime = get_sys_clock();
	INC_STATS(txn->get_thd_id(), rdma_write_time, endtime-starttime);
	INC_STATS(txn->get_thd_id(), rdma_write_cnt, 1);

#if DEBUG_PRINTF
if(access->type == WR){
	operate_size = sizeof(row_t);
	char *test_buf = Rdma::get_row_client_memory(thd_id);
    memset(test_buf, 0, operate_size);
	res_s = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
  	res_p = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	row_t *test_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
	memcpy(test_row, test_buf, operate_size);
	printf("[远程写入后再次读]事务号：%d，主键：%d，锁：%lu，tid：%lu,rts：%lu,wts：%lu\n",txn->get_txn_id(),test_row->get_primary_key(),test_row->mutx,test_row->tid,test_row->rts,test_row->wts);
	assert(test_row->tid == 0);
	mem_allocator.free(test_row,sizeof(row_t));
}
#endif
*/

	row->free_row();
	mem_allocator.free(row, sizeof(row_t));
	mem_allocator.free(remote_row,sizeof(row_t));
	mem_allocator.free(temp_row,sizeof(row_t));
}

#endif

