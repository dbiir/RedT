#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "rdma_redt.h"
#include "row_rdma_redt.h"
#include "log_rdma.h"

#if CC_ALG == RDMA_RED_T
RC RDMA_redt::write_and_unlock(yield_func_t &yield,row_t * row, row_t * data, TxnManager * txnMng,uint64_t cor_id) {
	//row->copy(data);  //copy access->data to access->orig_row
    //no need for last step:data = orig_row in local situation
    uint64_t lock_type;
    uint64_t loc = g_node_id;
    uint64_t try_lock = -1;
    uint64_t off = (char*)row - rdma_global_buffer;
retry_unlock:
    RC rc = txnMng->cas_remote_content(yield,loc,off,0,txnMng->get_txn_id(),&try_lock,cor_id);
    // todo: how to continue the commit operation.
    // In this case, this node must crashed. Thus, we cannot do anything
    if (rc == NODE_FAILED) {
        node_status.set_node_status(loc, NS::Failure, txnMng->get_thd_id());
        // txnMng->insert_failed_partition();
        return RCOK;
    }

    if(try_lock != 0 && !simulation->is_done()) {
        goto retry_unlock;
    }
    // 在lock_owner中释放锁
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
    // 调整版本链
    uint64_t index = ++row->newest_index;
    row->commit_ts[index%HIS_CHAIN_NUM] = txnMng->get_commit_timestamp();
    // memcpy(row->datas[index%HIS_CHAIN_NUM], data->data, ROW_DEFAULT_SIZE);
    // 调整时间戳
    set_watermark(row->get_part_id(),txnMng->get_commit_timestamp());
    // for (int node_id = 0; node_id < NODE_CNT; node_id++) {
    //     if (GET_CENTER_ID(node_id) == GET_CENTER_ID(g_node_id)) {
    //         set_remote_watermark(yield, row->get_part_id(), node_id, txnMng->get_commit_timestamp(), txnMng->get_thd_id(), cor_id);
    //     }
    // }

    row->_tid_word = 0;
    // printf("txn %d release local lock on item %d, lock_type: %d, try_time:%d \n", txnMng->get_txn_id(), row->get_primary_key(), row->lock_type, try_time);
#if DEBUG_PRINTF
    printf("---thd %lu, local unlock write succ, lock location: %u; %lu, txn: %lu\n", txnMng->get_thd_id(), g_node_id, row->get_primary_key(), txnMng->get_txn_id());
#endif
}

RC RDMA_redt::remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id){
    Access *access = txnMng->txn->accesses[num];
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    uint64_t thd_id = txnMng->get_thd_id();
    uint64_t operate_size = sizeof(uint64_t);
retry_remote_unlock:
    uint64_t try_lock = -1;
	uint64_t lock_type = 0;
    RC arc = txnMng->cas_remote_content(yield,loc,off,0,txnMng->get_txn_id(),&try_lock, cor_id);
    // todo: how to continue the commit operation.
    // In this case, executor find another node crashed.
    if (arc == NODE_FAILED) {
        node_status.set_node_status(loc, NS::Failure, txnMng->get_thd_id());
        txnMng->insert_failed_partition(access->partition_id);
        return RCOK;
    }
    
    if(try_lock != 0) {
        // printf("cas retry\n");
        if (!simulation->is_done()) goto retry_remote_unlock;
    }
    row_t * test_row = nullptr;
    arc = txnMng->read_remote_row(yield,loc,off,test_row,cor_id);
    // todo: how to continue the commit operation.
    if (arc == NODE_FAILED) {
        node_status.set_node_status(loc, NS::Failure, txnMng->get_thd_id());
        txnMng->insert_failed_partition(access->partition_id);
        return RCOK;
    }
    char *local_buf = Rdma::get_row_client_memory(thd_id);
    assert(test_row->get_primary_key() == access->key);
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
    // 调整版本链
    uint64_t index = ++test_row->newest_index;
    test_row->commit_ts[index%HIS_CHAIN_NUM] = txnMng->get_commit_timestamp();
    // memcpy(test_row->datas[index%HIS_CHAIN_NUM], data->data, ROW_DEFAULT_SIZE);
    // 调整远程的时间戳
    // set_watermark(test_row->get_part_id(),txnMng->get_commit_timestamp());
    set_remote_watermark(yield,test_row->get_part_id(),loc,txnMng->get_commit_timestamp(),txnMng->get_thd_id(),cor_id);
    // !--------

    test_row->_tid_word = 0;
    rc = txnMng->write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), off,(char*)test_row, cor_id);
    // todo: how to continue the commit operation.


	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
}

RC RDMA_redt::unlock(yield_func_t &yield,row_t * row , TxnManager * txnMng,uint64_t cor_id){
retry_unlock: 
    uint64_t lock_type;
    uint64_t loc = g_node_id;
    uint64_t try_lock = -1;
    uint64_t thd_id = txnMng->get_thd_id();
    uint64_t off = (char*)row - rdma_global_buffer;
    RC rc = txnMng->cas_remote_content(yield,loc,off,0,txnMng->get_txn_id(),&try_lock,cor_id);
    // todo: how to continue the commit operation.
    if (rc == NODE_FAILED) {
        node_status.set_node_status(loc, NS::Failure, txnMng->get_thd_id());
        // txnMng->insert_failed_partition();
        return RCOK;
    }

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
    printf("---thread id:%lu, local unlock shared lock, nodeid-key: %u; %lu, txnid: %lu\n", txnMng->get_thd_id(), g_node_id, row->get_primary_key(), txnMng->get_txn_id());
#endif
}

RC RDMA_redt::remote_unlock(yield_func_t &yield,TxnManager * txnMng , uint64_t num,uint64_t cor_id){
    Access *access = txnMng->txn->accesses[num];
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    uint64_t thd_id = txnMng->get_thd_id();
    uint64_t operate_size = sizeof(uint64_t);
retry_remote_unlock:
    uint64_t try_lock = -1;
	uint64_t lock_type = 0;
    RC arc = txnMng->cas_remote_content(yield,loc,off,0,txnMng->get_txn_id(),&try_lock,cor_id);
    // todo: report the failed primary replica to coordinator
    // For read operation, we do not need to redo this operation.
    if (arc == NODE_FAILED) {
        node_status.set_node_status(loc, NS::Failure, txnMng->get_thd_id());
        // txnMng->insert_failed_partition();
        return RCOK;
    }

    if(try_lock != 0) {
        // printf("cas retry\n");
        if (!simulation->is_done()) goto retry_remote_unlock;
    }
    row_t * test_row = nullptr;
    arc = txnMng->read_remote_row(yield,loc,off,test_row,cor_id);
    if (arc == NODE_FAILED) {
        node_status.set_node_status(loc, NS::Failure, txnMng->get_thd_id());
        // txnMng->insert_failed_partition();
        return RCOK;
    }
    assert(test_row->get_primary_key() == access->key);

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
    arc = txnMng->write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), off,(char*)test_row, cor_id);
    // todo: report the failed primary replica to coordinator
    if (arc == NODE_FAILED) {
        node_status.set_node_status(loc, NS::Failure, txnMng->get_thd_id());
        // txnMng->insert_failed_partition();
        return RCOK;
    }
	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
}

RC RDMA_redt::commit_log(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id) {
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
            if (rc != Abort) assert(le->c_ts != UINT64_MAX && le->c_ts != 0);
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
				auto res_p = rc_qp[i][txnMng->get_thd_id()]->wait_one_comp(RDMA_CALLS_TIMEOUT);
				// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
				uint64_t endtime = get_sys_clock();
				INC_STATS(txnMng->get_thd_id(), rdma_read_time, endtime-starttime);
				INC_STATS(txnMng->get_thd_id(), rdma_read_cnt, 1);
				INC_STATS(txnMng->get_thd_id(), worker_idle_time, endtime-starttime);
				INC_STATS(txnMng->get_thd_id(), worker_waitcomp_time, endtime-starttime);
				DEL_STATS(txnMng->get_thd_id(), worker_process_time, endtime-starttime);
                if (res_p != rdmaio::IOCode::Ok) {
                    node_status.set_node_status(i, NS::Failure, txnMng->get_thd_id());
                    DEBUG_T("Thd %ld send RDMA one-sided failed.\n", txnMng->get_thd_id());
                    return NODE_FAILED;
                }
				#endif
            }
            txnMng->log_idx[i] = redo_log_buf.get_size();
        }
    }
#endif
#endif
}

RC RDMA_redt::commit_recover_log(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id) {
#if USE_REPLICA
    return txnMng->redo_commit_log(yield, rc, cor_id);
#endif
}

//write back and unlock
RC RDMA_redt::finish(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id){
    Transaction *txn = txnMng->txn;

    RC arc = RCOK;
    if (txnMng->enable_read_only_optimization) return arc;
    if (!txnMng->is_recover && txnMng->is_logged) arc = commit_log(yield, rc, txnMng, cor_id);
    else if (rc == RCOK) arc = commit_recover_log(yield, rc, txnMng, cor_id);
    if (arc == NODE_FAILED) {
        //todo: handle write failed.
    }
    DEBUG_T("txn %ld enters the finish phase.\n", txnMng->get_txn_id());
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
            if (!txnMng->is_recover) unlock(yield,access->orig_row, txnMng,cor_id);
        }else{
        //remote
            remote_access[txn->accesses[read_set[i]]->location].push_back(read_set[i]);
            Access * access = txn->accesses[ read_set[i] ];
            if (!txnMng->is_recover)remote_unlock(yield,txnMng, read_set[i],cor_id);
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