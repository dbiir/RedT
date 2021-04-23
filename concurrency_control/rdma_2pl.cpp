#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "rdma_2pl.h"
#include "row_rdma_2pl.h"

#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2
void RDMA_2pl::write_and_unlock(row_t * row, row_t * data, TxnManager * txnMng) {
	//row->copy(data);  //copy access->data to access->orig_row
    //no need for last step:data = orig_row in local situation
    uint64_t lock_info = row->_lock_info;
    row->_lock_info = 0;
#if DEBUG_PRINTF
    printf("---thd：%lu, local lock succ,lock location: %u; %p, txn: %lu, old lock_info: %lu, new_lock_info: 0\n", txnMng->get_thd_id(), g_node_id, &row->_lock_info, txnMng->get_txn_id(), lock_info);
#endif
}

void RDMA_2pl::remote_write_and_unlock(RC rc, TxnManager * txnMng , uint64_t num){
    Access *access = txnMng->txn->accesses[num];
    row_t *data = access->data;
    data->_lock_info = 0; //write data and unlock

    uint64_t off = access->offset;
    uint64_t loc = access->location;
	uint64_t thd_id = txnMng->get_thd_id();

    //just unlock, not write back when ABORT 
    uint64_t operate_size = 0;
    if(rc != Abort) operate_size = row_t::get_row_size(data->tuple_size);
    else operate_size = sizeof(uint64_t);

    char *test_buf = Rdma::get_row_client_memory(thd_id);
    memcpy(test_buf, (char*)data, operate_size);

    uint64_t starttime = get_sys_clock();
	uint64_t endtime;

#if DEBUG_PRINTF
    uint64_t *sec_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
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
    if(CC_ALG == RDMA_NO_WAIT) assert(orig_lock_info == 3);
    else if(CC_ALG == RDMA_NO_WAIT2) assert(orig_lock_info == 1);
#endif

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
#if DEBUG_PRINTF 
    printf("---线程号：%lu, 远程解写锁成功，锁位置: %lu; %p, 事务号: %lu, 原lock_info: %lu, new_lock_info: 0\n", txnMng->get_thd_id(), loc, remote_mr_attr[loc].buf + off, txnMng->get_txn_id(), orig_lock_info);
#endif
}

void RDMA_2pl::unlock(row_t * row , TxnManager * txnMng){
#if CC_ALG == RDMA_NO_WAIT
retry_unlock: 
    uint64_t lock_type;
    uint64_t lock_num;
    uint64_t lock_info = row->_lock_info;
    uint64_t new_lock_num;
    uint64_t new_lock_info;
    Row_rdma_2pl::info_decode(lock_info,lock_type,lock_num);
    new_lock_num = lock_num-1;
    Row_rdma_2pl::info_encode(new_lock_info,lock_type,new_lock_num);
    
    if(lock_type!=0 || lock_num <= 0) {
        printf("---线程号：%lu, 本地解读锁失败!!!!!!锁位置: %u; %p, 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), g_node_id, &row->_lock_info, txnMng->get_txn_id(), lock_info, new_lock_info);
    }
    assert(lock_type == 0);  //一定是读锁
    assert(lock_num > 0); //一定有锁

    //RDMA CAS，不用本地CAS
        uint64_t loc = g_node_id;
        uint64_t thd_id = txnMng->get_thd_id();
		uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
		auto mr = client_rm_handler->get_reg_attr().value();

		rdmaio::qp::Op<> op;
		op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + (char*)row - rdma_global_buffer), remote_mr_attr[loc].key).set_cas(lock_info,new_lock_info);
		assert(op.set_payload(tmp_buf2, sizeof(uint64_t), mr.key) == true);
		auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

		RDMA_ASSERT(res_s2 == IOCode::Ok);
		auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
		RDMA_ASSERT(res_p2 == IOCode::Ok);

		if(*tmp_buf2 != lock_info){
        //原子性被破坏
        txnMng->num_atomic_retry++;
        total_num_atomic_retry++;
        if(txnMng->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txnMng->num_atomic_retry;

#if DEBUG_PRINTF
        printf("---retry_unlock读集元素\n");
#endif
        goto retry_unlock;
        }
#if DEBUG_PRINTF
    printf("---线程号：%lu, 本地解读锁成功，锁位置: %u; %p, 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), g_node_id, &row->_lock_info, txnMng->get_txn_id(), lock_info, new_lock_info);
#endif     

#elif CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2
    assert(row->_lock_info != 0);
    row->_lock_info = 0;
#if DEBUG_PRINTF
    printf("---线程号：%lu, 本地解读锁成功，锁位置: %u; %p, 事务号: %lu, 原lock_info: 1, new_lock_info: 0\n", txnMng->get_thd_id(), g_node_id, &row->_lock_info, txnMng->get_txn_id());
#endif
#endif
}

void RDMA_2pl::remote_unlock(TxnManager * txnMng , uint64_t num){
#if CC_ALG == RDMA_NO_WAIT
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
    uint64_t lock_type;
    uint64_t lock_num;
    uint64_t new_lock_num;
remote_retry_unlock:    
    Row_rdma_2pl::info_decode(*lock_info,lock_type,lock_num);
    new_lock_num = lock_num-1;
    Row_rdma_2pl::info_encode(new_lock_info,lock_type,new_lock_num);

    assert(lock_type == 0);  //一定是读锁
    assert(lock_num > 0); //一定有锁

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
        txnMng->num_atomic_retry++;
        total_num_atomic_retry++;
        if(txnMng->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txnMng->num_atomic_retry;
#if DEBUG_PRINTF
        printf("---remote_retry_unlock读集元素\n");
#endif
        *lock_info = *tmp_loc;
        goto remote_retry_unlock;
    }
#if DEBUG_PRINTF
    printf("---线程号：%lu, 远程解读锁成功，锁位置: %lu; %p, 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), loc, remote_mr_attr[loc].buf + off, txnMng->get_txn_id(), *lock_info, new_lock_info);
#endif
	mem_allocator.free(lock_info, sizeof(uint64_t));
#elif CC_ALG == RDMA_NO_WAIT2 ||  CC_ALG == RDMA_WAIT_DIE2
    Access *access = txnMng->txn->accesses[num];
    uint64_t off = access->offset;
    uint64_t loc = access->location;
	uint64_t thd_id = txnMng->get_thd_id();
    uint64_t operate_size = sizeof(uint64_t);

    uint64_t *test_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
    *test_buf = 0;

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
#if DEBUG_PRINTF
    printf("---线程号：%lu, 远程解读锁成功，锁位置: %lu; %p, 事务号: %lu, 原lock_info: 1, new_lock_info: 0\n", txnMng->get_thd_id(), loc, remote_mr_attr[loc].buf + off, txnMng->get_txn_id());
#endif
#endif
}


//实现write back and unlock
void RDMA_2pl::finish(RC rc, TxnManager * txnMng){
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

}
#endif
