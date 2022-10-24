#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "rdma_2pl.h"
#include "row_rdma_2pl.h"
#include "log_rdma.h"

#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3
void RDMA_2pl::write_and_unlock(yield_func_t &yield,row_t * row, row_t * data, TxnManager * txnMng,uint64_t cor_id) {
	//row->copy(data);  //copy access->data to access->orig_row
    //no need for last step:data = orig_row in local situation
#if CC_ALG == RDMA_NO_WAIT3
    uint64_t lock_type;
    uint64_t loc = g_node_id;
    uint64_t try_lock = -1;
    uint64_t off = (char*)row - rdma_global_buffer;
retry_unlock:
    try_lock = txnMng->cas_remote_content(yield,loc,off,0,txnMng->get_txn_id(),cor_id);
    if(try_lock != 0 && !simulation->is_done()) {
        goto retry_unlock;
    }
    uint64_t lock_index = txnMng->get_txn_id() % LOCK_LENGTH;
    uint64_t try_time = 0;
    while(try_time <= LOCK_LENGTH) {
        if(row->lock_owner[lock_index] == txnMng->get_txn_id()) {
            row->lock_owner[lock_index] = 0;
            row->lock_type = 0;
            break;
        }
        lock_index = (lock_index + 1) % LOCK_LENGTH;
		try_time ++;
    }
    row->_tid_word = 0;
    // printf("txn %d release local lock on item %d, lock_type: %d, try_time:%d \n", txnMng->get_txn_id(), row->get_primary_key(), row->lock_type, try_time);
#else
    uint64_t lock_info = row->_tid_word;
    row->_tid_word = 0;
#endif
#if DEBUG_PRINTF
    printf("---thd %lu, local unlock write succ, lock location: %u; %lu, txn: %lu, old lock_info: %lu\n", txnMng->get_thd_id(), g_node_id, row->get_primary_key(), txnMng->get_txn_id(), lock_info);
#endif
}

void RDMA_2pl::remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id){
#if CC_ALG == RDMA_NO_WAIT3
    Access *access = txnMng->txn->accesses[num];
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    uint64_t thd_id = txnMng->get_thd_id();
    uint64_t operate_size = sizeof(uint64_t);
retry_remote_unlock:
    uint64_t try_lock = -1;
	uint64_t lock_type = 0;
    try_lock = txnMng->cas_remote_content(yield,loc,off,0,txnMng->get_txn_id(),cor_id);
    if(try_lock != 0) {
        // printf("cas retry\n");
        if (!simulation->is_done()) goto retry_remote_unlock;
    }
    row_t * test_row = txnMng->read_remote_row(yield,loc,off,cor_id);
    uint64_t i = 0;
    uint64_t lock_num = 0;
    uint64_t lock_index = txnMng->get_txn_id() % LOCK_LENGTH;
    uint64_t try_time = 0;
    while(try_time <= LOCK_LENGTH) {
        if(test_row->lock_owner[lock_index] == txnMng->get_txn_id()) {
            test_row->lock_owner[lock_index] = 0;
            test_row->lock_type = 0;
            break;
        }
        lock_index = (lock_index + 1) % LOCK_LENGTH;
		try_time ++;
    }
    test_row->_tid_word = 0;
    assert(txnMng->write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), off,(char*)test_row, cor_id) == true);
	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
#else
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
    // if(CC_ALG == RDMA_NO_WAIT) assert(orig_lock_info == 3);
    // else if(CC_ALG == RDMA_NO_WAIT2) assert(orig_lock_info == 1);
#endif

    assert(txnMng->write_remote_row(yield,loc,operate_size,off,(char*)data,cor_id) == true);

#if DEBUG_PRINTF 

    printf("---thread id:%lu, local unlock shared lock, nodeid-key: %u; %lu, txnid: %lu, origin lock_info: %lu\n", txnMng->get_thd_id(), g_node_id, remote_row->get_primary_key(), txnMng->get_txn_id(), orig_lock_info);

    // printf("---thd: %lu, remote unlock write succ,lock location:%lu; %p, txn: %lu, old lock_info: %lu\n", txnMng->get_thd_id(), loc, remote_mr_attr[loc].buf + off, txnMng->get_txn_id(), orig_lock_info);
#endif
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
        printf("---thd:%lu, lock unlock read lock fail!!!!!! lock location: %u; %p, txn_id: %lu, old_lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), g_node_id, &row->_tid_word, txnMng->get_txn_id(), lock_info, new_lock_info);
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
        unlock_atomic_failed_count++;
        total_num_atomic_retry++;
        if(txnMng->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txnMng->num_atomic_retry;

    #if DEBUG_PRINTF
        printf("---retry_unlock read set element\n");
    #endif
        if (!simulation->is_done()) goto retry_unlock;
    }
#if DEBUG_PRINTF
    printf("---thread id:%lu, local unlock shared lock, nodeid-key: %u; %lu, txnid: %lu, origin lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), g_node_id, row->get_primary_key(), txnMng->get_txn_id(), lock_info, new_lock_info);
#endif
#elif CC_ALG == RDMA_NO_WAIT3 

retry_unlock: 
    uint64_t lock_type;
    uint64_t loc = g_node_id;
    uint64_t try_lock = -1;
    uint64_t thd_id = txnMng->get_thd_id();
    uint64_t off = (char*)row - rdma_global_buffer;
    try_lock = txnMng->cas_remote_content(yield,loc,off,0,txnMng->get_txn_id(),cor_id);
    if(try_lock != 0) {
        // printf("cas retry\n");
        if (!simulation->is_done()) goto retry_unlock;
    }
    lock_type = row->lock_type;
    if(lock_type == 0 || lock_type == 1) {
        //printf("---thd:%lu, lock unlock read lock fail!!!!!! lock location: %u; %p, txn_id: %lu, old_lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), g_node_id, &row->_tid_word, txnMng->get_txn_id(), lock_info, new_lock_info);
    }
    uint64_t lock_index = txnMng->get_txn_id() % LOCK_LENGTH;
    uint64_t try_time = 0;
    while(try_time <= LOCK_LENGTH) {
        if(row->lock_owner[lock_index] == txnMng->get_txn_id()) {
            row->lock_owner[lock_index] = 0;
            row->lock_type = row->lock_type - 1;
            row->_tid_word = 0;
            break;
        }
        lock_index = (lock_index + 1) % LOCK_LENGTH;
        try_time ++;
    }
    if(row->lock_type == 1) {
        row->lock_type = 0;
    }
    // printf("txn %d release local lock on item %d, lock_type: %d, try_time: %d\n", txnMng->get_txn_id(), row->get_primary_key(), row->lock_type, try_time);
    row->_tid_word = 0;
#if DEBUG_PRINTF
    printf("---thread id:%lu, local unlock shared lock, nodeid-key: %u; %lu, txnid: %lu, origin lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), g_node_id, row->get_primary_key(), txnMng->get_txn_id(), lock_info, new_lock_info);
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
        unlock_atomic_failed_count++;
        if(txnMng->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txnMng->num_atomic_retry;
#if DEBUG_PRINTF
        printf("---remote_retry_unlock read set element\n");
#endif
        *lock_info = try_lock;
        if (!simulation->is_done()) goto remote_retry_unlock;
    }
#if DEBUG_PRINTF
    printf("---thd：%lu, remote unlock lock succ,lock location: %lu; %p, 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txnMng->get_thd_id(), loc, remote_mr_attr[loc].buf + off, txnMng->get_txn_id(), *lock_info, new_lock_info);
#endif
	mem_allocator.free(lock_info, sizeof(uint64_t));
#elif CC_ALG == RDMA_NO_WAIT3
    Access *access = txnMng->txn->accesses[num];
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    uint64_t thd_id = txnMng->get_thd_id();
    uint64_t operate_size = sizeof(uint64_t);
retry_remote_unlock:
    uint64_t try_lock = -1;
	uint64_t lock_type = 0;
    try_lock = txnMng->cas_remote_content(yield,loc,off,0,txnMng->get_txn_id(),cor_id);
    if(try_lock != 0) {
        // printf("cas retry\n");
        if (!simulation->is_done()) goto retry_remote_unlock;
    }
    row_t * test_row = txnMng->read_remote_row(yield,loc,off,cor_id);
    uint64_t lock_index = txnMng->get_txn_id() % LOCK_LENGTH;
    uint64_t try_time = 0;
    while(try_time <= LOCK_LENGTH) {
        if(test_row->lock_owner[lock_index] == txnMng->get_txn_id()) {
            test_row->lock_owner[lock_index] = 0;
            test_row->lock_type = test_row->lock_type - 1;
            test_row->_tid_word = 0;
            break;
        }
        lock_index = (lock_index + 1) % LOCK_LENGTH;
        try_time ++;
    }
    if(test_row->lock_type == 1 || test_row->lock_type < 0) {
        test_row->lock_type = 0;
    }
    test_row->_tid_word = 0;
    assert(txnMng->write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), off,(char*)test_row, cor_id) == true);
	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
#endif
}


//write back and unlock
void RDMA_2pl::finish(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id){
    Transaction *txn = txnMng->txn;
#if USE_REPLICA
    LogEntry * le = (LogEntry *)mem_allocator.alloc(sizeof(LogEntry));
    if(rc == Abort){
        le->state = LE_ABORTED;
    }else{
        le->state = LE_COMMITTED;
    }
    le->c_ts = txnMng->get_commit_timestamp();
    // printf("c_ts:%lu\n",le->c_ts);
    uint64_t operate_size = sizeof(le->state) + sizeof(le->c_ts);

    int count = 0;
	for(int i=0;i<g_node_cnt;i++){
        if(txnMng->log_idx[i] != redo_log_buf.get_size()){
            assert(le->c_ts != UINT64_MAX && le->c_ts != 0);
			uint64_t start_idx = txnMng->log_idx[i];
			if(i==g_node_id){ //local 
                char* start_addr = (char *)redo_log_buf.get_entry(start_idx);
				memcpy(start_addr, (char *)le, operate_size);
			}else{ //remote 
                uint64_t start_offset = redo_log_buf.get_entry_offset(start_idx);
#if RDMA_DBPAOR
				txnMng->write_remote_log(yield, i, operate_size, start_offset, (char *)le, cor_id, count+1, true);
#else 
				txnMng->write_remote_log(yield, i, operate_size, start_offset, (char *)le, cor_id);
#endif 
				++count;
			}
		}
	}
	mem_allocator.free(le, sizeof(LogEntry));
#if RDMA_DBPAOR
    //poll write result
	for(int i=0;i<g_node_cnt;i++){
        if(txnMng->log_idx[i] != redo_log_buf.get_size()){
            if(i!=g_node_id){ //remote 
				uint64_t starttime = get_sys_clock();
				INC_STATS(txnMng->get_thd_id(), worker_oneside_cnt, 1);
				#if USE_COROUTINE
				assert(false); //not support yet
				#else
				auto res_p = rc_qp[i][txnMng->get_thd_id()]->wait_one_comp();
				RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
				uint64_t endtime = get_sys_clock();
				INC_STATS(txnMng->get_thd_id(), rdma_read_time, endtime-starttime);
				INC_STATS(txnMng->get_thd_id(), rdma_read_cnt, 1);
				INC_STATS(txnMng->get_thd_id(), worker_idle_time, endtime-starttime);
				INC_STATS(txnMng->get_thd_id(), worker_waitcomp_time, endtime-starttime);
				DEL_STATS(txnMng->get_thd_id(), worker_process_time, endtime-starttime);
				#endif
            }
            txnMng->log_idx[i] = redo_log_buf.get_size();
        }
    }
#endif

#endif
// #else
    uint64_t starttime = get_sys_clock();
    //NO_WAIT has no problem of deadlock,so doesnot need to bubble sort the write_set in primary key order
	int read_set[txn->row_cnt - txn->write_cnt];
	int cur_rd_idx = 0;
    int cur_wr_idx = 0;
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
        assert(GET_CENTER_ID(txn->accesses[rid]->location) == g_center_id);
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[cur_wr_idx ++] = rid;
		else
			read_set[cur_rd_idx ++] = rid;
	}

    vector<vector<uint64_t>> remote_access(g_node_cnt);
    for (uint64_t i = 0; i < txn->row_cnt-txn->write_cnt; i++) {
        //local
        if(txn->accesses[read_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ read_set[i] ];
            unlock(yield,access->orig_row, txnMng,cor_id);
        }else{
        //remote
            remote_access[txn->accesses[read_set[i]]->location].push_back(read_set[i]);
            Access * access = txn->accesses[ read_set[i] ];
            remote_unlock(yield,txnMng, read_set[i],cor_id);
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
            Access * access = txn->accesses[ txnMng->write_set[i] ];
            remote_write_and_unlock(yield,rc, txnMng, txnMng->write_set[i],cor_id);
        }
    }


    uint64_t timespan = get_sys_clock() - starttime;
    txnMng->txn_stats.cc_time += timespan;
    txnMng->txn_stats.cc_time_short += timespan;
    INC_STATS(txnMng->get_thd_id(),twopl_release_time,timespan);
    INC_STATS(txnMng->get_thd_id(),twopl_release_cnt,1);
// #endif
    

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
