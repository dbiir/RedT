#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "row_rdma_si.h"
#include "global.h"

#if CC_ALG == RDMA_SI

void Row_rdma_si::init(row_t * row){
	_row = row;
}

RC Row_rdma_si::write(yield_func_t &yield, access_t type, TxnManager * txn, row_t * row,uint64_t cor_id) {  //本地加锁
    RC rc = RCOK;
    uint64_t retry_time = 0;
    uint64_t loc = g_node_id;
local_retry_lock:
    uint64_t try_lock = -1;
    uint64_t wts = row->wts;
    
    rc = txn->cas_remote_content(yield,loc,(char*)row - rdma_global_buffer,0,txn->get_txn_id(),&try_lock, cor_id);
    if (rc != RCOK) {
        rc = rc == NODE_FAILED ? Abort : rc;
        return rc;
    }
    if(try_lock != 0 && !simulation->is_done()) {
        #if DEBUG_PRINTF
        printf("txn %d add local mutx lock on item %d failed !!!!!\n", txn->get_txn_id(), row->get_primary_key());
        #endif
        return Abort;
    }

    if (wts > txn->get_start_timestamp()) {
        row->_tid_word = 0;
        #if DEBUG_PRINTF
        printf("txn %d write on item %d failed !!!!! because wts %ld, txn snapshot %ld\n", txn->get_txn_id(), row->get_primary_key(),wts, txn->get_start_timestamp());
        #endif
        rc = Abort;
        return rc;
    }
    #if DEBUG_PRINTF
        printf("txn %d write on item %d, because wts %ld, txn snapshot %ld\n", txn->get_txn_id(), row->get_primary_key(), wts, txn->get_start_timestamp());
    #endif
	return rc;
}

RC Row_rdma_si::read(yield_func_t &yield, access_t type, TxnManager * txn, row_t * row,uint64_t cor_id) {  //本地加锁
    RC rc = Abort;
    uint64_t retry_time = 0;
    uint64_t loc = g_node_id;

    uint64_t wts = row->wts;
    uint64_t idx = 0;
    for (int i = row->newest_index; i > row->newest_index - HIS_CHAIN_NUM; i--) {
        int index = i % HIS_CHAIN_NUM;
        if (row->commit_ts[index] <= txn->get_start_timestamp()) {
            idx = index;
            #if DEBUG_PRINTF
            printf("row_rdma_si.cpp:67 txn %ld get version %ld\n", txn->get_txn_id(),idx);
            #endif
            return RCOK;
        } else {
            #if DEBUG_PRINTF
            // printf("row_rdma_si.cpp:67 txn %ld search version %ld commit_ts %ld\n", txn->get_txn_id(),idx,row->commit_ts[index]);
            #endif
        }
    }
    #if DEBUG_PRINTF
        printf("row_rdma_si.cpp:77 txn %ld get version failed\n", txn->get_txn_id());
    #endif
	return Abort;
}

RC Row_rdma_si::access(yield_func_t &yield, access_t type, TxnManager * txn, row_t * row,uint64_t cor_id) {  //本地加锁
    if (type == RD) return read(yield, type, txn, row, cor_id);
    else return write(yield, type, txn, row, cor_id);
}
#endif