#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "row_rdma_2pl.h"
#include "global.h"

#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3

void Row_rdma_2pl::init(row_t * row){
	_row = row;
}

void Row_rdma_2pl::info_decode(uint64_t lock_info,uint64_t& lock_type,uint64_t& lock_num){
    lock_type = (lock_info)&1;  //第1位为锁类型信息 0：shared_lock 1: mutex_lock
    lock_num = (lock_info>>1); //第2位及以后为锁数量信息
}
void Row_rdma_2pl::info_encode(uint64_t& lock_info,uint64_t lock_type,uint64_t lock_num){
    lock_info = (lock_num<<1)+lock_type;
}

bool Row_rdma_2pl::conflict_lock(uint64_t lock_info, lock_t l2, uint64_t& new_lock_info) {
    uint64_t lock_type; 
    uint64_t lock_num;
    info_decode(lock_info,lock_type,lock_num);
    
    if(lock_num == 0) {//无锁
        if(l2 == DLOCK_EX) info_encode(new_lock_info, 1, 1);
        else info_encode(new_lock_info, 0, 1);
        return false;
    }
    if(l2 == DLOCK_EX || lock_type == 1)  return true;
    else{ //l2==DLOCK_SH&&lock_type == 0
        uint64_t new_lock_num = lock_num + 1;
        //if(new_lock_num != 1) printf("---new_lock_num:%lu\n",new_lock_num);
        info_encode(new_lock_info,lock_type,new_lock_num);
		if(new_lock_info == 0) {
        printf("---lock_info:%lu, lock_type:%lu, lock_num:%lu\n",lock_info,lock_type,lock_num);
        printf("---new_lock_info:%lu, lock_type:%lu, new_lock_num:%lu\n",new_lock_info, lock_type, new_lock_num);
        }
        return false;
    }
}

RC Row_rdma_2pl::lock_get(yield_func_t &yield,lock_t type, TxnManager * txn, row_t * row,uint64_t cor_id) {  //本地加锁
	assert(CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3);
    RC rc;
#if CC_ALG == RDMA_NO_WAIT
    atomic_retry_lock:
    uint64_t lock_info = row->_tid_word;
    uint64_t new_lock_info = 0;

    //检测是否有冲突，在conflict=false的情况下得到new_lock_info
    bool conflict = conflict_lock(lock_info, type, new_lock_info);
    if (conflict) {
        #if DEBUG_PRINTF
        printf("---thread id:%lu, lock failed!!!!!!  nodeid-key : %u; %lu , txnid: %lu, origin lock_info: %lu, new_lock_info: %lu\n", txn->get_thd_id(), g_node_id, row->get_primary_key(), txn->get_txn_id(), lock_info, new_lock_info);  
        #endif
        rc = Abort;
	    return rc;
    }
    assert(new_lock_info != 0);
    //RDMA CAS，不用本地CAS
    uint64_t loc = g_node_id;
    uint64_t thd_id = txn->get_thd_id();

    uint64_t try_lock = -1;
    try_lock = txn->cas_remote_content(yield,loc,(char*)row - rdma_global_buffer,lock_info,new_lock_info,cor_id);

    if(try_lock != lock_info){ //如果CAS失败，原子性被破坏	
        #if DEBUG_PRINTF
            printf("---atomic_retry_lock\n");
        #endif 
        total_num_atomic_retry++;
        lock_atomic_failed_count++;
        txn->num_atomic_retry++;
        if(txn->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txn->num_atomic_retry;
        if (!simulation->is_done()) goto atomic_retry_lock;
        else {
            DEBUG_M("TxnManager::get_row(abort) access free\n");
            
            return Abort; //原子性被破坏，CAS失败	
        }
    }         
    else{   //加锁成功
    #if DEBUG_PRINTF
        printf("---thread id:%lu, lock success, nodeid-key : %u; %lu , txnid: %lu, origin lock_info: %lu, new_lock_info: %lu\n", txn->get_thd_id(), g_node_id, row->get_primary_key(), txn->get_txn_id(), lock_info, new_lock_info);
    #endif
        rc = RCOK;
    } 
#endif
#if CC_ALG == RDMA_NO_WAIT3
    uint64_t retry_time = 0;
    uint64_t loc = g_node_id;
local_retry_lock:

    uint64_t try_lock = -1;
    uint64_t lock_type = 0;
    retry_time ++;
    try_lock = txn->cas_remote_content(yield,loc,(char*)row - rdma_global_buffer,0,txn->get_txn_id(),cor_id);
    if(try_lock != 0 && !simulation->is_done()) {
        // rc = Abort;
        // return rc;
        goto local_retry_lock;
    }
    lock_type = _row->lock_type;
    if(lock_type == 0) {
        uint64_t lock_index = txn->get_txn_id() % LOCK_LENGTH;
        _row->lock_owner[lock_index] = txn->get_txn_id();
        _row->lock_type = type == DLOCK_EX? 1:2;
        // _row->_tid_word = 0;
        rc = RCOK;
    } else if(lock_type == 1 || type == DLOCK_EX) {
        // printf("row_rdma_2pl:119\n");
        _row->_tid_word = 0;
        rc = Abort;
        return rc;
    } else {
        uint64_t lock_index = txn->get_txn_id() % LOCK_LENGTH;
        uint64_t try_time = 0;
        while(try_time <= LOCK_LENGTH) {
            // printf("row_rdma_2pl:125 try_time: %d\n", try_time);
            // printf("txn %d add local lock on item %d, lock_type: %d\n", txn->get_txn_id(), _row->get_primary_key(), _row->lock_type);
            if(_row->lock_owner[lock_index] == 0) {
                _row->lock_owner[lock_index] = txn->get_txn_id();
                _row->lock_type = _row->lock_type + 1;
                // _row->_tid_word = 0;
                rc = RCOK;
                break;
            }
            lock_index = (lock_index + 1) % LOCK_LENGTH;
            try_time ++;
        }
        if(try_time > LOCK_LENGTH) {
            // printf("row_rdma_2pl:138\n");
            _row->_tid_word = 0;
            rc = Abort;
            return rc;
        }      
    }
    _row->_tid_word = 0;
    // printf("txn %d add local lock on item %d, lock_type: %d\n", txn->get_txn_id(), _row->get_primary_key(), _row->lock_type);
#endif
	return rc;
}
#endif