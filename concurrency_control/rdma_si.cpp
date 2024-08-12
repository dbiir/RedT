#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "rdma_si.h"
#include "row_rdma_si.h"
#include "log_rdma.h"

#if CC_ALG == RDMA_SI
RC RDMA_si::write_and_unlock(yield_func_t &yield,row_t * row, row_t * data, TxnManager * txnMng,uint64_t cor_id) {
	//row->copy(data);  //copy access->data to access->orig_row
    //no need for last step:data = orig_row in local situation
    uint64_t lock_type;
    uint64_t loc = g_node_id;
    uint64_t try_lock = -1;
    uint64_t off = (char*)row - rdma_global_buffer;

    // 调整版本链
    uint64_t index = ++row->newest_index;
    row->commit_ts[index%HIS_CHAIN_NUM] = txnMng->get_commit_timestamp();
    row->wts = txnMng->get_commit_timestamp();
    // memcpy(row->datas[index%HIS_CHAIN_NUM], data->data, ROW_DEFAULT_SIZE);
    // 调整时间戳
    #if READ_OPTIMIZATION && WORKLOAD != TPCC
    set_watermark(row->get_part_id(),txnMng->get_commit_timestamp());
    #endif
    row->_tid_word = 0;
#if DEBUG_PRINTF
    printf("---thd %lu, local unlock write succ, lock location: %u; %lu, txn: %lu\n", txnMng->get_thd_id(), g_node_id, row->get_primary_key(), txnMng->get_txn_id());
#endif
}

RC RDMA_si::remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id){
    Access *access = txnMng->txn->accesses[num];
    uint64_t off = access->offset;
    uint64_t loc = access->location;
    uint64_t thd_id = txnMng->get_thd_id();
    uint64_t operate_size = sizeof(uint64_t);

    row_t * test_row = nullptr;
    RC arc = txnMng->read_remote_row(yield,loc,off,test_row,cor_id);
    // todo: how to continue the commit operation.
    if (arc == NODE_FAILED) {
        node_status.set_node_status(loc, NS::Failure, txnMng->get_thd_id());
        txnMng->insert_failed_partition(access->partition_id);
        return RCOK;
    }
    char *local_buf = Rdma::get_row_client_memory(thd_id);
    assert(test_row->get_primary_key() == access->key);

    // 调整版本链
    uint64_t index = ++test_row->newest_index;
    test_row->commit_ts[index%HIS_CHAIN_NUM] = txnMng->get_commit_timestamp();
    test_row->wts = txnMng->get_commit_timestamp();
    // memcpy(test_row->datas[index%HIS_CHAIN_NUM], data->data, ROW_DEFAULT_SIZE);
    // 调整远程的时间戳
    // set_watermark(test_row->get_part_id(),txnMng->get_commit_timestamp());
    #if READ_OPTIMIZATION && WORKLOAD != TPCC
    set_remote_watermark(yield,test_row->get_part_id(),loc,txnMng->get_commit_timestamp(),txnMng->get_thd_id(),cor_id);
    #endif

    test_row->_tid_word = 0;
    rc = txnMng->write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), off,(char*)test_row, cor_id);
    // todo: how to continue the commit operation.

	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
}

RC RDMA_si::commit_log(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id) {
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

RC RDMA_si::commit_recover_log(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id) {
#if USE_REPLICA
    return txnMng->redo_commit_log(yield, rc, cor_id);
#endif
}

//write back and unlock
RC RDMA_si::finish(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id){
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

    //for write set element,write back and release lock
    for (uint64_t i = 0; i < txn->write_cnt; i++) {
        //local
        if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ txnMng->write_set[i] ];
            write_and_unlock(yield,access->orig_row, access->data, txnMng,cor_id); 
        }else{
        //remote
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