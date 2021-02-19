#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "row_rdma_nowait.h"

#if CC_ALG == RDMA_NO_WAIT

void Row_rdma_nowait::init(row_t * row){
	_row = row;
}

void Row_rdma_nowait::info_decode(uint64_t lock_info,int& lock_type,int& lock_num){
    lock_type = (lock_info)&1;  //第1位为锁类型信息 0：shared_lock 1: mutex_lock
    lock_num = (lock_info>>1); //第2位及以后为锁数量信息
}
void Row_rdma_nowait::info_encode(uint64_t& lock_info,int lock_type,int lock_num){
    lock_info = (lock_num<<1)+lock_type;
}

bool Row_rdma_nowait::conflict_lock(uint64_t lock_info, lock_t l2, uint64_t& new_lock_info) {
    int lock_type; 
    int lock_num;
    info_decode(lock_info,lock_type,lock_num);
    
    if(lock_num == 0) {//无锁
        if(l2 == DLOCK_EX) info_encode(new_lock_info, 1, 1);
        else info_encode(new_lock_info, 0, 1);
        return false;
    }
    if(l2 == DLOCK_EX || lock_type == 1)  return true;
    else{ //l2==DLOCK_SH&&lock_type == 0
        info_encode(new_lock_info,lock_type,lock_num+1);
        return false;
    }
}

RC Row_rdma_nowait::lock_get(lock_t type, TxnManager * txn, row_t * row) {  //本地加锁
	assert(CC_ALG == RDMA_NO_WAIT);
    RC rc;

    uint64_t lock_info = row->_lock_info;
    uint64_t new_lock_info = 0;

    //检测是否有冲突，在conflict=false的情况下得到new_lock_info
    bool conflict = conflict_lock(lock_info, type, new_lock_info);
    if (conflict) {
        rc = Abort;
	    return rc;
    }
    assert(new_lock_info != 0);
    //本地CAS，成功返回1，失败返回0
    if(__sync_bool_compare_and_swap(&row->_lock_info, lock_info, new_lock_info)){
        //printf("---线程号：%lu, 本地加锁成功，锁位置: %u; %p , 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txn->get_thd_id(), g_node_id, &row->_lock_info, txn->get_txn_id(), lock_info, new_lock_info);
        rc = RCOK;
    }
    else    rc = Abort;  //原子性被破坏
	return rc;
}
#endif