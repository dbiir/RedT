#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "rdma_2pl.h"
#include "row_rdma_2pl.h"

#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT
void RDMA_2pl::write_and_unlock(yield_func_t &yield,row_t * row, row_t * data, TxnManager * txnMng,uint64_t cor_id) {
	//row->copy(data);  //copy access->data to access->orig_row
    //no need for last step:data = orig_row in local situation
    uint64_t lock_info = row->_tid_word;
    row->_tid_word = 0;
#if DEBUG_PRINTF
    printf("---thd：%lu, local lock succ,lock location: %u; %p, txn: %lu, old lock_info: %lu, new_lock_info: 0\n", txnMng->get_thd_id(), g_node_id, &row->_tid_word, txnMng->get_txn_id(), lock_info);
#endif
}

void RDMA_2pl::remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id){
    Access *access = txnMng->txn->accesses[num];
    row_t *data = access->data;
    data->_tid_word = 0; //write data and unlock

    uint64_t off = access->offset;
    uint64_t loc = access->location;
	uint64_t thd_id = txnMng->get_thd_id();

    //just unlock, not write back when ABORT 
    uint64_t operate_size = 0;
    if(rc != Abort) operate_size = row_t::get_row_size(data->tuple_size);
    else operate_size = sizeof(uint64_t);

    // char *test_buf = Rdma::get_row_client_memory(thd_id);
    // memcpy(test_buf, (char*)data, operate_size);

#if DEBUG_PRINTF
    row_t * remote_row = txnMng->read_remote_row(yield,loc,off,cor_id);
    uint64_t orig_lock_info = remote_row->_tid_word;
	mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));

    if(CC_ALG == RDMA_NO_WAIT) assert(orig_lock_info == 3);
    else if(CC_ALG == RDMA_NO_WAIT2) assert(orig_lock_info == 1);
#endif

    assert(txnMng->write_remote_row(yield,loc,operate_size,off,(char*)data,cor_id) == true);

#if DEBUG_PRINTF 
    printf("---线程号：%lu, 远程解写锁成功，锁位置: %lu; %p, 事务号: %lu, 原lock_info: %lu, new_lock_info: 0\n", txnMng->get_thd_id(), loc, remote_mr_attr[loc].buf + off, txnMng->get_txn_id(), orig_lock_info);
#endif
}

void RDMA_2pl::unlock(yield_func_t &yield,row_t * row , TxnManager * txnMng,uint64_t cor_id){
#if CC_ALG == RDMA_NO_WAIT
retry_unlock: 
    uint64_t lock_type;
    uint64_t lock_num;
    uint64_t lock_info = row->_tid_word;
    uint64_t new_lock_num;
    uint64_t new_lock_info;
    Row_rdma_2pl::info_decode(lock_info,lock_type,lock_num);
    new_lock_num = lock_num-1;
    Row_rdma_2pl::info_encode(new_lock_info,lock_type,new_lock_num);
    
    if(lock_type!=0 || lock_num <= 0) {
        printf("---thd：%lu, lock release read lock fail!!!!!!lock location: %u; %p, txn_id: %lu, old_lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), g_node_id, &row->_tid_word, txnMng->get_txn_id(), lock_info, new_lock_info);
    }
    assert(lock_type == 0);  //already write locked
    assert(lock_num > 0); //already locked

    //RDMA CAS，not use local CAS
        uint64_t loc = g_node_id;
        uint64_t thd_id = txnMng->get_thd_id();
        uint64_t off = (char*)row - rdma_global_buffer;

        uint64_t try_lock = txnMng->cas_remote_content(yield,loc,off,lock_info,new_lock_info,cor_id);

		if(try_lock != lock_info){
        //atomicity is destroyed
        txnMng->num_atomic_retry++;
        total_num_atomic_retry++;
        if(txnMng->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txnMng->num_atomic_retry;

#if DEBUG_PRINTF
        printf("---retry_unlock read set element\n");
#endif
        goto retry_unlock;
        }
#if DEBUG_PRINTF
    printf("---线程号：%lu, 本地解读锁成功，锁位置: %u; %p, 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), g_node_id, &row->_tid_word, txnMng->get_txn_id(), lock_info, new_lock_info);
#endif     

#elif CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT
    assert(row->_tid_word != 0);
    row->_tid_word = 0;
#if DEBUG_PRINTF
    printf("---线程号：%lu, 本地解读锁成功，锁位置: %u; %p, 事务号: %lu, 原lock_info: 1, new_lock_info: 0\n", txnMng->get_thd_id(), g_node_id, &row->_tid_word, txnMng->get_txn_id());
#endif
#endif
}

void RDMA_2pl::remote_unlock(yield_func_t &yield,TxnManager * txnMng , uint64_t num,uint64_t cor_id){
#if CC_ALG == RDMA_NO_WAIT
    Access *access = txnMng->txn->accesses[num];

    uint64_t off = access->offset;
    uint64_t loc = access->location;

    row_t * remote_row = txnMng->read_remote_row(yield,loc,off,cor_id);
    uint64_t *lock_info = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
    *lock_info = remote_row->_tid_word;
	mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));

    uint64_t new_lock_info;
    uint64_t lock_type;
    uint64_t lock_num;
    uint64_t new_lock_num;
remote_retry_unlock:    
    Row_rdma_2pl::info_decode(*lock_info,lock_type,lock_num);
    new_lock_num = lock_num-1;
    Row_rdma_2pl::info_encode(new_lock_info,lock_type,new_lock_num);

    assert(lock_type == 0);  //already write locked
    assert(lock_num > 0); //already locked

    //remote CAS unlock
    uint64_t try_lock = txnMng->cas_remote_content(yield,loc,off,*lock_info,new_lock_info,cor_id);
    if(try_lock != *lock_info){ //atomicity is destroyed，CAS fail
        txnMng->num_atomic_retry++;
        total_num_atomic_retry++;
        if(txnMng->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txnMng->num_atomic_retry;
#if DEBUG_PRINTF
        printf("---remote_retry_unlock read set element\n");
#endif
        *lock_info = try_lock;
        goto remote_retry_unlock;
    }
#if DEBUG_PRINTF
    printf("---thd：%lu, remote release lock succ,lock location: %lu; %p, 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), loc, remote_mr_attr[loc].buf + off, txnMng->get_txn_id(), *lock_info, new_lock_info);
#endif
	mem_allocator.free(lock_info, sizeof(uint64_t));
#elif CC_ALG == RDMA_NO_WAIT2 ||  CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT
    Access *access = txnMng->txn->accesses[num];
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    uint64_t thd_id = txnMng->get_thd_id();
    uint64_t operate_size = sizeof(uint64_t);

    // uint64_t *test_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
    // *test_buf = 0;
    row_t *unlock_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
    unlock_row->_tid_word = 0;
    assert(txnMng->write_remote_row(yield,loc,operate_size,off,(char *)unlock_row,cor_id) == true);
    mem_allocator.free(unlock_row, row_t::get_row_size(ROW_DEFAULT_SIZE));

#if DEBUG_PRINTF
    printf("---thd：%lu,remote release read lock succ,lock location : %lu; %p, txn_id: %lu, old lock_info: 1, new_lock_info: 0\n", txnMng->get_thd_id(), loc, remote_mr_attr[loc].buf + off, txnMng->get_txn_id());
#endif
#endif
}


//write back and unlock
void RDMA_2pl::finish(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id){
	Transaction *txn = txnMng->txn;
    uint64_t starttime = get_sys_clock();
    //NO_WAIT has no problem of deadlock,so doesnot need to bubble sort the write_set in primary key order
	int read_set[txn->row_cnt - txn->write_cnt];
	int cur_rd_idx = 0;
    int cur_wr_idx = 0;
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[cur_wr_idx ++] = rid;
		else
			read_set[cur_rd_idx ++] = rid;
	}
    
    vector<vector<uint64_t>> remote_access(g_node_cnt);
    //for read set element, release lock
    for (uint64_t i = 0; i < txn->row_cnt-txn->write_cnt; i++) {
        //local
        if(txn->accesses[read_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ read_set[i] ];
            unlock(yield,access->orig_row, txnMng,cor_id);
        }else{
        //remote
            remote_access[txn->accesses[read_set[i]]->location].push_back(read_set[i]);
#if USE_DBPA == false
            Access * access = txn->accesses[ read_set[i] ];
            remote_unlock(yield,txnMng, read_set[i],cor_id);
#endif
        }
    }
    //for write set element,write back and release lock
    for (uint64_t i = 0; i < txn->write_cnt; i++) {
        //local
        if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ txnMng->write_set[i] ];
            write_and_unlock(yield,access->orig_row, access->data, txnMng,cor_id); 
        }else{
        //remote
            remote_access[txn->accesses[txnMng->write_set[i]]->location].push_back(txnMng->write_set[i]);
#if USE_DBPA == false
            Access * access = txn->accesses[ txnMng->write_set[i] ];
            remote_write_and_unlock(yield,rc, txnMng, txnMng->write_set[i],cor_id);
#endif
        }
    }

#if USE_DBPA == true
    for(int i=0;i<g_node_cnt;i++){ //for the same node, batch unlock remote
        if(remote_access[i].size() > 0){
            txnMng->batch_unlock_remote(yield, cor_id, i, rc, txnMng, remote_access);
        }
    }
#if USE_OR == true && CC_ALG != RDMA_NO_WAIT
    for(int i=0;i<g_node_cnt;i++){ //poll result
        if(remote_access[i].size() > 0){
        	//to do: add coroutine
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
            INC_STATS(txnMng->get_thd_id(), worker_idle_time, get_sys_clock()-starttime);
            DEL_STATS(txnMng->get_thd_id(), worker_process_time, get_sys_clock()-starttime);
#endif      
        }
    }
#endif 
#endif

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
