#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "rdma_nowait.h"
#include "row_rdma_nowait.h"

#if CC_ALG == RDMA_NO_WAIT
void RDMA_nowait::write_and_unlock(row_t * row, row_t * data, TxnManager * txnMng) {
	//row->copy(data);  //复制access->data到access->orig_row
    //上一步不需要：如果是本地：data和orig_row相同
    uint64_t lock_info = row->_lock_info;
    row->_lock_info = 0;
    //printf("---线程号：%lu, 本地解写锁成功，锁位置: %u; %p, 事务号: %lu, 原lock_info: %lu, new_lock_info: 0\n", txnMng->get_thd_id(), g_node_id, &row->_lock_info, txnMng->get_txn_id(), lock_info);
}

void RDMA_nowait::remote_write_and_unlock(RC rc, TxnManager * txnMng , uint64_t num){
    Access *access = txnMng->txn->accesses[num];
    row_t *data = access->data;
    data->_lock_info = 0; //直接把unlock的结果一起写回

    uint64_t off = access->offset;
    uint64_t loc = access->location;
	uint64_t thd_id = txnMng->get_thd_id();

    //Abort的时候，只用解锁，不写回数据
    uint64_t operate_size = 0;
    if(rc != Abort) operate_size = sizeof(row_t);
    else operate_size = sizeof(uint64_t);

    char *test_buf = Rdma::get_row_client_memory(thd_id);
    memcpy(test_buf, (char*)data, operate_size);

    uint64_t starttime = get_sys_clock();
	uint64_t endtime;

    /*uint64_t *sec_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto res_s0 = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = sizeof(uint64_t),
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(sec_buf),
		.remote_addr = off,
		.imm_data = 0});
	RDMA_ASSERT(res_s0 == rdmaio::IOCode::Ok);
  	auto res_p0 = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p0 == rdmaio::IOCode::Ok);
    uint64_t orig_lock_info = *sec_buf;
    assert(orig_lock_info == 2);*/

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
    //printf("---线程号：%lu, 远程解写锁成功，锁位置: %lu;..., 事务号: %lu, 原lock_info: %lu, new_lock_info: 0\n", txnMng->get_thd_id(), loc, txnMng->get_txn_id(), orig_lock_info);
    //printf("---线程号：%lu, 远程解写锁成功，锁位置: %lu;..., 事务号: %lu, new_lock_info: 0\n", txnMng->get_thd_id(), loc, txnMng->get_txn_id());
}

void RDMA_nowait::unlock(row_t * row , TxnManager * txnMng){
retry_unlock: 
    int lock_type;
    int lock_num;
    uint64_t lock_info = row->_lock_info;
    uint64_t new_lock_info;
    Row_rdma_nowait::info_decode(lock_info,lock_type,lock_num);
    if(lock_type!=0) printf("---lock_info:%lu\n",lock_info);
    assert(lock_type == 0);  //一定是读锁
    Row_rdma_nowait::info_encode(new_lock_info,lock_type,lock_num-1);
    //本地CAS，成功返回1，失败返回0
    if(!__sync_bool_compare_and_swap(&row->_lock_info, lock_info, new_lock_info)){
    //原子性被破坏
        goto retry_unlock;
    }
    //printf("---线程号：%lu, 本地解读锁成功，锁位置: %u; %p, 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), g_node_id, &row->_lock_info, txnMng->get_txn_id(), lock_info, new_lock_info);
}

void RDMA_nowait::remote_unlock(TxnManager * txnMng , uint64_t num){
remote_retry_unlock:    
    Access *access = txnMng->txn->accesses[num];

    uint64_t off = access->offset;
    uint64_t loc = access->location;
	uint64_t thd_id = txnMng->get_thd_id();
    uint64_t operate_size = sizeof(uint64_t);

    char *test_buf = Rdma::get_row_client_memory(thd_id);

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
    
    uint64_t *lock_info = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
	memcpy(lock_info, test_buf, operate_size);
    uint64_t new_lock_info;
    int lock_type;
    int lock_num;
    Row_rdma_nowait::info_decode(*lock_info,lock_type,lock_num);
    if(lock_type!=0) printf("---lock_info:%lu\n",*lock_info);
    assert(lock_type == 0);  //一定是读锁
    Row_rdma_nowait::info_encode(new_lock_info,lock_type,lock_num-1);

    //远程CAS解锁
    uint64_t *tmp_loc = (uint64_t *)Rdma::get_row_client_memory(thd_id);
    auto mr = client_rm_handler->get_reg_attr().value();

    rdmaio::qp::Op<> op;
    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + off), remote_mr_attr[loc].key).set_cas(*lock_info,new_lock_info);
    assert(op.set_payload(tmp_loc, sizeof(uint64_t), mr.key) == true);
    auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

    RDMA_ASSERT(res_s2 == IOCode::Ok);
    auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
    RDMA_ASSERT(res_p2 == IOCode::Ok);
    if(*tmp_loc != *lock_info){ //原子性被破坏，CAS失败
        goto remote_retry_unlock;
    }
    //printf("---线程号：%lu, 远程解读锁成功，锁位置: %lu;..., 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), loc, txnMng->get_txn_id(), *lock_info, new_lock_info);
}


//实现write back and unlock
void RDMA_nowait::finish(RC rc, TxnManager * txnMng){
	Transaction *txn = txnMng->txn;
    uint64_t starttime = get_sys_clock();
	//得到读集和写集
    //NO_WAIT不会有死锁问题，故不需要bubble sort the write_set in primary key order
	int read_set[txn->row_cnt - txn->write_cnt];
	int cur_rd_idx = 0;
    int cur_wr_idx = 0;
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[cur_wr_idx ++] = rid;
		else
			read_set[cur_rd_idx ++] = rid;
	}

    //对读集元素，解锁
    for (uint64_t i = 0; i < txn->row_cnt-txn->write_cnt; i++) {
        //local
        if(txn->accesses[read_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ read_set[i] ];
            unlock(access->orig_row, txnMng);
        }else{
        //remote
            Access * access = txn->accesses[ read_set[i] ];
            remote_unlock(txnMng, read_set[i]);
        }
    }
    //对写集元素，写回并解锁
    for (uint64_t i = 0; i < txn->write_cnt; i++) {
        //local
        if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ txnMng->write_set[i] ];
            write_and_unlock(access->orig_row, access->data, txnMng); 
        }else{
        //remote
            Access * access = txn->accesses[ txnMng->write_set[i] ];
            remote_write_and_unlock(rc, txnMng, txnMng->write_set[i]);
        }
    }

    uint64_t timespan = get_sys_clock() - starttime;
    txnMng->txn_stats.cc_time += timespan;
    txnMng->txn_stats.cc_time_short += timespan;
    INC_STATS(txnMng->get_thd_id(),twopl_release_time,timespan);
    INC_STATS(txnMng->get_thd_id(),twopl_release_cnt,1);
    
/*
    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        if(txn->accesses[i]->location != g_node_id){
        //remote
        mem_allocator.free(txn->accesses[i]->data,0);
        mem_allocator.free(txn->accesses[i]->orig_row,0);
        // mem_allocator.free(txn->accesses[i]->test_row,0);
        txn->accesses[i]->data = NULL;
        txn->accesses[i]->orig_row = NULL;
        txn->accesses[i]->orig_data = NULL;
        txn->accesses[i]->version = 0;

        //txn->accesses[i]->test_row = NULL;
        txn->accesses[i]->offset = 0;
        }
    }
	memset(txnMng->write_set, 0, 100);
*/
}
#endif
