#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "row_rdma_2pl.h"
#include "global.h"

#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT

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
	assert(CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT);
    RC rc;
#if CC_ALG == RDMA_NO_WAIT
atomic_retry_lock:
    uint64_t lock_info = row->_tid_word;
    uint64_t new_lock_info = 0;

    //检测是否有冲突，在conflict=false的情况下得到new_lock_info
    bool conflict = conflict_lock(lock_info, type, new_lock_info);
    if (conflict) {
        rc = Abort;
	    return rc;
    }
    if(new_lock_info == 0){
        printf("---线程号：%lu, 本地加锁失败!!!!!!锁位置: %u; %p , 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txn->get_thd_id(), g_node_id, &row->_tid_word, txn->get_txn_id(), lock_info, new_lock_info);    
    }
    assert(new_lock_info != 0);
    //RDMA CAS，不用本地CAS
        uint64_t loc = g_node_id;
        uint64_t thd_id = txn->get_thd_id();

        uint64_t try_lock = -1;
        try_lock = txn->cas_remote_content(yield,loc,(char*)row - rdma_global_buffer,lock_info,new_lock_info,cor_id);

		// uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
		// auto mr = client_rm_handler->get_reg_attr().value();

		// rdmaio::qp::Op<> op;
		// op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + (char*)row - rdma_global_buffer), remote_mr_attr[loc].key).set_cas(lock_info,new_lock_info);
		// assert(op.set_payload(tmp_buf2, sizeof(uint64_t), mr.key) == true);
		// auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

		// RDMA_ASSERT(res_s2 == IOCode::Ok);
		// auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
		// RDMA_ASSERT(res_p2 == IOCode::Ok);

		//if(*tmp_buf2 != lock_info){ //如果CAS失败，原子性被破坏	
		if(try_lock != lock_info){ //如果CAS失败，原子性被破坏	
#if DEBUG_PRINTF
            printf("---atomic_retry_lock\n");
#endif 
            total_num_atomic_retry++;
            txn->num_atomic_retry++;
            if(txn->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txn->num_atomic_retry;
            goto atomic_retry_lock;
            //rc = Abort; 
        }         
        else{   //加锁成功
#if DEBUG_PRINTF
        printf("---线程号：%lu, 本地加锁成功，锁位置: %u; %p , 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txn->get_thd_id(), g_node_id, &row->_tid_word, txn->get_txn_id(), lock_info, new_lock_info);
#endif
        rc = RCOK;
        } 
/*
    //本地CAS，成功返回1，失败返回0
    auto res = __sync_bool_compare_and_swap(&row->_tid_word, lock_info, new_lock_info);
    //if(res != 0 && res != 1) printf(res);
    assert(res == 0 || res == 1);
    if(res){
#if DEBUG_PRINTF
        printf("---线程号：%lu, 本地加锁成功，锁位置: %u; %p , 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txn->get_thd_id(), g_node_id, &row->_tid_word, txn->get_txn_id(), lock_info, new_lock_info);
#endif
        rc = RCOK;
    }
    else    rc = Abort;  //原子性被破坏
*/
#endif
#if CC_ALG == RDMA_NO_WAIT2
    //RDMA CAS，不用本地CAS
        uint64_t loc = g_node_id;
        uint64_t try_lock = -1;
        if(txn->get_txn_id()==0)
            try_lock = txn->cas_remote_content(yield,loc,(char*)row - rdma_global_buffer,0,1,cor_id);
        else
            try_lock = txn->cas_remote_content(yield,loc,(char*)row - rdma_global_buffer,0,txn->get_txn_id(),cor_id);
        // uint64_t thd_id = txn->get_thd_id();
		// uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
		// auto mr = client_rm_handler->get_reg_attr().value();

		// rdmaio::qp::Op<> op;
		// op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + (char*)row - rdma_global_buffer), remote_mr_attr[loc].key).set_cas(0,1);
		// assert(op.set_payload(tmp_buf2, sizeof(uint64_t), mr.key) == true);
		// auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

		// RDMA_ASSERT(res_s2 == IOCode::Ok);
		// auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
		// RDMA_ASSERT(res_p2 == IOCode::Ok);

		// if(*tmp_buf2 != 0) rc = Abort; //加锁冲突，Abort	
		if(try_lock != 0) rc = Abort; //加锁冲突，Abort	
        else{   //加锁成功
        rc = RCOK;
        } 
#if DEBUG_PRINTF
        if(rc == RCOK) printf("---thd %lu, local lock suc, lock location: %u; %p, txn: %lu\n", txn->get_thd_id(), g_node_id, &row->_tid_word, txn->get_txn_id());
        else if(rc == Abort) printf("---thd %lu, local lock fail, lock location: %u; %p, txn: %lu, current lock:%lu\n", txn->get_thd_id(), g_node_id, &row->_tid_word, txn->get_txn_id(), try_lock);
#endif        
/*
    //本地CAS，成功返回1，失败返回0
    if(__sync_bool_compare_and_swap(&row->_tid_word, 0, 1)){
        //printf("---线程号：%lu, 本地加锁成功，锁位置: %u; %p , 事务号: %lu, 原lock_info: %lu, new_lock_info: %lu\n", txn->get_thd_id(), g_node_id, &row->_tid_word, txn->get_txn_id(), lock_info, new_lock_info);
        rc = RCOK;
    }
    else    rc = Abort;  //加锁冲突
*/
#endif
#if CC_ALG == RDMA_WAIT_DIE
    		//直接RDMA CAS加锁
local_retry_lock:
        uint64_t loc = g_node_id;
        uint64_t try_lock = -1;
        uint64_t lock_type = 0;
		bool conflict = false;
        uint64_t thd_id = txn->get_thd_id();
		uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
		auto mr = client_rm_handler->get_reg_attr().value();
        uint64_t tts = txn->get_timestamp();
        try_lock = txn->cas_remote_content(yield,loc,(char*)row - rdma_global_buffer,0,txn->get_txn_id(),cor_id);
        if(try_lock != 0) {
            // printf("cas retry\n");
            rc = Abort;
            return rc;
            goto local_retry_lock;
        }
        lock_type = _row->lock_type;
        if(lock_type == 0) {
            _row->lock_owner[0] = txn->get_txn_id();
            _row->ts[0] = tts;
            _row->lock_type = type == DLOCK_EX? 1:2;
            _row->_tid_word = 0; 
            rc = RCOK;
        } else if(lock_type == 1 || type == DLOCK_EX) {
            conflict = true;
            if(conflict) {
                uint64_t i = 0;
                for(i = 0; i < LOCK_LENGTH; i++) {
                    if((tts > _row->ts[i] && _row->ts[i] != 0)) {
                        _row->_tid_word = 0;
                        rc = Abort;
                        return rc;
                    }
                    // if(_row->ts[0] == 0) break;
                }
                total_num_atomic_retry++;
                txn->num_atomic_retry++;
                if(txn->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txn->num_atomic_retry;
                //sleep(1);
                _row->_tid_word = 0;
                goto local_retry_lock;	
            }
        } else {
            uint64_t i = 0; 
			for(i = 0; i < LOCK_LENGTH; i++) {
				if(_row->ts[i] == 0) {
					_row->ts[i] = tts;
					_row->lock_owner[i] == txn->get_txn_id();
					_row->lock_type = type == DLOCK_EX? 1:2;
					_row->_tid_word = 0;
                    rc = RCOK;
                    break;
				}
			}
            if(i == LOCK_LENGTH) {
				_row->_tid_word = 0;
                rc = Abort;
                return rc;
			}
        }
#endif
#if CC_ALG == RDMA_WAIT_DIE2
    		//直接RDMA CAS加锁
local_retry_lock:
        uint64_t loc = g_node_id;
        uint64_t try_lock = -1;
        

        uint64_t thd_id = txn->get_thd_id();
		uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
		auto mr = client_rm_handler->get_reg_attr().value();
        uint64_t tts = txn->get_timestamp();
        try_lock = txn->cas_remote_content(yield,loc,(char*)row - rdma_global_buffer,0,tts,cor_id);
        if(try_lock == 0) {
            _row->lock_owner = txn->get_txn_id();
            rc = RCOK;
        }
		if(try_lock != 0){ //如果CAS失败
			if(tts <= *tmp_buf2){  //wait
#if DEBUG_PRINTF
            printf("---local_retry_lock\n");
#endif 
                total_num_atomic_retry++;
                txn->num_atomic_retry++;
                if(txn->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txn->num_atomic_retry;
				//sleep(1);
				goto local_retry_lock;			
			}	
			else{ //abort
                // printf("local WAIT_DIE DIE:%ld\n", txn->get_txn_id());
				rc = Abort;
			}
		}
        else{ //加锁成功
            rc = RCOK;
        }
#endif
#if CC_ALG == RDMA_WOUND_WAIT
        uint64_t tts = txn->get_timestamp();
        int retry_time = 0;
        bool is_wound = false;
        uint64_t loc = g_node_id;
        RdmaTxnTableNode * value = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
local_retry_lock:
        uint64_t try_lock = -1;
		uint64_t lock_type = 0;
		bool canwait = true;
		bool canwound = true;
        try_lock = txn->cas_remote_content(yield,loc,(char*)row - rdma_global_buffer,0,txn->get_txn_id(),cor_id);
        if(try_lock != 0) {
            // printf("cas retry\n");
            mem_allocator.free(value, sizeof(RdmaTxnTableNode));
            rc = Abort;
            return rc;
            goto local_retry_lock;
        }
        lock_type = _row->lock_type;
		if(lock_type == 0) {
			_row->lock_owner[0] = txn->get_txn_id();
			_row->ts[0] = tts;
			_row->lock_type = type == DLOCK_EX? 1:2;
			_row->_tid_word = 0;
            rc = RCOK;
		} else if(lock_type == 1 || type == DLOCK_EX) {
            uint64_t i = 0; 
			for(i = 0; i < LOCK_LENGTH; i++) {
				if((tts > _row->ts[i] && _row->ts[i] != 0)) {
					canwound = false;
				}
				if((tts < _row->ts[i] && _row->ts[i] != 0)) {
					canwait = false;
				}
			}
            if(canwound == true) {
				WOUNDState state;
				char * local_buf;
				for(int i = 0; i < LOCK_LENGTH; i++) {
					if(_row->lock_owner[i] != 0) {
						if(_row->lock_owner[i] % g_node_cnt == g_node_id) {
							state = rdma_txn_table.local_get_state(txn->get_thd_id(), _row->lock_owner[i]);
						} else {
							local_buf = rdma_txn_table.remote_get_state(yield, txn, _row->lock_owner[i], cor_id);
							memcpy(value, local_buf, sizeof(RdmaTxnTableNode));
							state = value->state;
						}
						if(state != WOUND_COMMITTING && state != WOUND_ABORTING){  //wound
							
							if(_row->lock_owner[i] % g_node_cnt == g_node_id) {
								rdma_txn_table.local_set_state(txn->get_thd_id(), _row->lock_owner[i], WOUND_ABORTING);

							} else {
								value->state = WOUND_ABORTING;
								rdma_txn_table.remote_set_state(yield, txn, _row->lock_owner[i], value, cor_id);
								// mem_allocator.free(value, sizeof(RdmaTxnTableNode));
							}
						}
					}
					// mem_allocator.free(value, sizeof(RdmaTxnTableNode));
					_row->_tid_word = 0;
					// mem_allocator.free(m_item, sizeof(itemid_t));
					// rc = Abort;
					// return rc;
					goto local_retry_lock;
					// num_atomic_retry++;
					// total_num_atomic_retry++;
					// if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;	
					// //sleep(1);
					// goto retry_lock;			
				}	
			}
            if(canwait == true) {
				txn->num_atomic_retry++;
				total_num_atomic_retry++;
				if(txn->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txn->num_atomic_retry;	
				// mem_allocator.free(value, sizeof(RdmaTxnTableNode));
				_row->_tid_word = 0;
				// if(test_row->lock_owner % g_node_cnt != g_node_id)
				// mem_allocator.free(value, sizeof(RdmaTxnTableNode));
				//sleep(1);
					goto local_retry_lock;
			}
            if(canwait == false && canwound == false) {
				_row->_tid_word = 0;
				mem_allocator.free(value, sizeof(RdmaTxnTableNode));
				rc = Abort;
				return rc;
			}
        } else {
            uint64_t i = 0; 
			for(i = 0; i < LOCK_LENGTH; i++) {
				if(_row->ts[i] == 0) {
					_row->ts[i] = tts;
					_row->lock_owner[i] == txn->get_txn_id();
					_row->lock_type =  type == DLOCK_EX? 1:2;
					_row->_tid_word = 0;
					rc = RCOK;
					break;
				}
			}
            if(i == LOCK_LENGTH) {
				_row->_tid_word = 0;
                rc = Abort;
                return rc;
			}
        }
        if(value) mem_allocator.free(value, sizeof(RdmaTxnTableNode));
#endif
#if CC_ALG == RDMA_WOUND_WAIT2
    		//直接RDMA CAS加锁 
        int retry_time = 0;
        bool is_wound = false;
        RdmaTxnTableNode * value = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
local_retry_lock:
        uint64_t loc = g_node_id;
        uint64_t try_lock = -1;
        retry_time += 1;

        uint64_t thd_id = txn->get_thd_id();
		uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
		auto mr = client_rm_handler->get_reg_attr().value();
        uint64_t tts = txn->get_timestamp();
        try_lock = txn->cas_remote_content(yield,loc,(char*)row - rdma_global_buffer,0,tts,cor_id);
        if(try_lock == 0) {
            _row->lock_owner = txn->get_txn_id();
            rc = RCOK;
        }
		if(try_lock != 0 && retry_time <= MAX_RETRY_TIME){ //如果CAS失败
            WOUNDState state;
            char * local_buf;
			if(_row->lock_owner % g_node_cnt == g_node_id) {
				state = rdma_txn_table.local_get_state(txn->get_thd_id(), _row->lock_owner);
			} else {
		    	local_buf = rdma_txn_table.remote_get_state(yield, txn, _row->lock_owner, cor_id);
                memcpy(value, local_buf, sizeof(RdmaTxnTableNode));
                state = value->state;
			}
			if(tts <= *tmp_buf2 && state != WOUND_COMMITTING && state != WOUND_ABORTING && is_wound == false){  //wound   
                if(_row->lock_owner % g_node_cnt == g_node_id) {
					rdma_txn_table.local_set_state(txn->get_thd_id(), _row->lock_owner, WOUND_ABORTING);
				} else {
                    value->state = WOUND_ABORTING;
					rdma_txn_table.remote_set_state(yield, txn, _row->lock_owner, value, cor_id);
                    // mem_allocator.free(value, sizeof(RdmaTxnTableNode));
				}
                    // printf("set WOUND_ABORTING:%ld\n", _row->lock_owner);
                    is_wound = true;
					goto local_retry_lock;	
                
			}	
			else{ //wait
				// rc = Abort;
#if DEBUG_PRINTF
            printf("---local_retry_lock\n");
#endif 
                total_num_atomic_retry++;
                txn->num_atomic_retry++;
                if(txn->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txn->num_atomic_retry;
				//sleep(1);
					goto local_retry_lock;	
			}
		} else if(try_lock != 0 && retry_time > MAX_RETRY_TIME) {
            rc = Abort;
        }
    
    mem_allocator.free(value, sizeof(RdmaTxnTableNode));
#endif
	return rc;
}
#endif