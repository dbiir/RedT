/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

//#include "mvcc.h"
#include "txn.h"
#include "row.h"
#include "manager.h"
#include "helper.h"
#include "rdma.h"
#include "rdma_mvcc.h"
#include "row_rdma_mvcc.h"
#include "mem_alloc.h"
#include "lib.hh"
#include "qps/op.hh"
#if CC_ALG == RDMA_MVCC
void rdma_mvcc::init(row_t * row) {

}

row_t * rdma_mvcc::clear_history(TsType type, ts_t ts) {

}

MVReqEntry * rdma_mvcc::get_req_entry() {
	return (MVReqEntry *) mem_allocator.alloc(sizeof(MVReqEntry));
}

void rdma_mvcc::return_req_entry(MVReqEntry * entry) {
	mem_allocator.free(entry, sizeof(MVReqEntry));
}

MVHisEntry * rdma_mvcc::get_his_entry() {
	return (MVHisEntry *) mem_allocator.alloc(sizeof(MVHisEntry));
}

void rdma_mvcc::return_his_entry(MVHisEntry * entry) {
	if (entry->row != NULL) {
		entry->row->free_row();
		mem_allocator.free(entry->row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	}
	mem_allocator.free(entry, sizeof(MVHisEntry));
}

void rdma_mvcc::buffer_req(TsType type, TxnManager *txn) {

}

MVReqEntry * rdma_mvcc::debuffer_req( TsType type, TxnManager * txn) {
	
}

void rdma_mvcc::insert_history(ts_t ts, row_t *row) {
	
}

bool rdma_mvcc::conflict(TsType type, ts_t ts) {
	// find the unique prewrite-read couple (prewrite before read)
	// if no such couple found, no conflict.
	// else
	// 	 if exists writehis between them, NO conflict!!!!
	// 	 else, CONFLICT!!!
	
	return true;
}

RC rdma_mvcc::access(TxnManager * txn, TsType type, row_t * row) {
	RC rc = RCOK;
    ts_t ts = txn->get_timestamp();
	uint64_t starttime = get_sys_clock();

	if (g_central_man)
		glob_manager.lock_row(_row);
	else
		pthread_mutex_lock( latch );
	INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
  uint64_t acesstime = get_sys_clock();
  if (type == R_REQ) {
		// figure out if ts is in interval(prewrite(x))
		bool conf = conflict(type, ts);
		if ( conf && rreq_len < g_max_read_req) {
			rc = WAIT;
      //txn->wait_starttime = get_sys_clock();
        DEBUG("buf R_REQ %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
			buffer_req(R_REQ, txn);
			txn->ts_ready = false;
		} else if (conf) {
			rc = Abort;
			printf("\nshould never happen. rreq_len=%ld", rreq_len);
		} else {
			// return results immediately.
			rc = RCOK;
			MVHisEntry * whis = writehis;
			while (whis != NULL && whis->ts > ts) whis = whis->next;
			row_t *ret = (whis == NULL) ? _row : whis->row;
			txn->cur_row = ret;
			insert_history(ts, NULL);
			assert(strstr(_row->get_table_name(), ret->get_table_name()));
		}
	} else if (type == P_REQ) {
		if ( conflict(type, ts) ) {
			rc = Abort;
		} else if (preq_len < g_max_pre_req){
        DEBUG("buf P_REQ %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
			buffer_req(P_REQ, txn);
			rc = RCOK;
		} else  {
			rc = Abort;
		}
	} else if (type == W_REQ) {
		rc = RCOK;
		// the corresponding prewrite request is debuffered.
		insert_history(ts, row);
        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
		MVReqEntry * req = debuffer_req(P_REQ, txn);
		assert(req != NULL);
		return_req_entry(req);
		update_buffer(txn);
	} else if (type == XP_REQ) {
        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
		MVReqEntry * req = debuffer_req(P_REQ, txn);
		assert (req != NULL);
		return_req_entry(req);
		update_buffer(txn);
	} else
		assert(false);
  INC_STATS(txn->get_thd_id(), trans_mvcc_access, get_sys_clock() - acesstime);
	if (rc == RCOK) {
		uint64_t clear_his_starttime = get_sys_clock();
		if (whis_len > g_his_recycle_len || rhis_len > g_his_recycle_len) {
			ts_t t_th = glob_manager.get_min_ts(txn->get_thd_id());
		if (readhistail && readhistail->ts < t_th) clear_history(R_REQ, t_th);
				// Here is a tricky bug. The oldest transaction might be
				// reading an even older version whose timestamp < t_th.
				// But we cannot recycle that version because it is still being used.
				// So the HACK here is to make sure that the first version older than
				// t_th not be recycled.
		if (whis_len > 1 && writehistail->prev->ts < t_th) {
				row_t * latest_row = clear_history(W_REQ, t_th);
				if (latest_row != NULL) {
					assert(_row != latest_row);
					_row->copy(latest_row);
				}
			}
		}
		uint64_t clear_his_timespan = get_sys_clock() - clear_his_starttime;
		INC_STATS(txn->get_thd_id(), trans_mvcc_clear_history, clear_his_timespan);
	}

	uint64_t timespan = get_sys_clock() - starttime;
	txn->txn_stats.cc_time += timespan;
	txn->txn_stats.cc_time_short += timespan;

	if (g_central_man)
		glob_manager.release_row(_row);
	else
		pthread_mutex_unlock( latch );
	return rc;
}

row_t * rdma_mvcc::read_remote_row(TxnManager * txn , uint64_t num){
	row_t * row = txn->txn->accesses[num]->orig_row;
	// row_t * row = txn->txn->accesses[num]->test_row;

	uint64_t off = txn->txn->accesses[num]->offset;
 	uint64_t loc = txn->txn->accesses[num]->location;
	uint64_t thd_id = txn->get_thd_id();

	uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);

	char *test_buf = Rdma::get_row_client_memory(thd_id);
    memset(test_buf, 0, operate_size);

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

	row_t *temp_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
	memcpy(temp_row, test_buf, operate_size);
	assert(temp_row->get_primary_key() == row->get_primary_key());

	return temp_row;
}

bool rdma_mvcc::lock_write_set(TxnManager * txnMng , uint64_t num){
   // INC_STATS(txnMng->get_thd_id(), cas_cnt, 1);
    bool res = false;
	Transaction *txn = txnMng->txn;

    uint64_t off = txn->accesses[num]->offset;
    uint64_t loc = txn->accesses[num]->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id() + 1;

	uint64_t *test_loc = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto mr = client_rm_handler->get_reg_attr().value();

	rdmaio::qp::Op<> op;
    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + off), remote_mr_attr[loc].key).set_cas(0, lock);
  	assert(op.set_payload(test_loc, sizeof(uint64_t), mr.key) == true);
  	auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

	RDMA_ASSERT(res_s2 == IOCode::Ok);
	auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p2 == IOCode::Ok);

    // if(result == lock)res=true;
    if(*test_loc == 0)res = true;
    return res;
  // return 0;
}

void * rdma_mvcc::remote_release_lock(TxnManager * txnMng , uint64_t num){
    
    Transaction *txn = txnMng->txn;

    uint64_t off = txn->accesses[num]->offset;
    uint64_t loc = txn->accesses[num]->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id() + 1;

	uint64_t *test_loc = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto mr = client_rm_handler->get_reg_attr().value();

	rdmaio::qp::Op<> op;
    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + off), remote_mr_attr[loc].key).set_cas(lock, 0);
  	assert(op.set_payload(test_loc, sizeof(uint64_t), mr.key) == true);
  	auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

	RDMA_ASSERT(res_s2 == IOCode::Ok);
	auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p2 == IOCode::Ok);

   // return *test_loc;
}

void * rdma_mvcc::local_write_back(TxnManager * txnMng , uint64_t num){
    row_t *temp_row = txnMng->txn->accesses[num]->orig_row;

     // 2.	获取数据项所有版本，找出最旧的版本，将其替换为要写入的新数据，
    int version_change;
    int last_ver = (temp_row->version_num) % HIS_CHAIN_NUM;//最新版本
    if(last_ver == HIS_CHAIN_NUM -1 )version_change = 0;
    else version_change = last_ver + 1 ;//要替换的版本

    //要通过txn_id解锁的版本
    int release_ver = txnMng->txn->accesses[num]->old_version_num % HIS_CHAIN_NUM;
    
    // 3.	单边写写入新版本、RTS、start_ts、end_ts、txn-id等信息，并将对上一个版本的txn-id设置为0，并解锁
    temp_row->version_num = temp_row->version_num +1;

    temp_row->txn_id[version_change] = 0;// txnMng->get_txn_id();
    temp_row->rts[version_change] = txnMng->txn->timestamp;
    temp_row->start_ts[version_change] = txnMng->txn->timestamp;
    temp_row->end_ts[version_change] = UINT64_MAX;

    temp_row->txn_id[release_ver] = 0;
    temp_row->end_ts[release_ver] = txnMng->txn->timestamp;
}

void * rdma_mvcc::remote_write_back(TxnManager * txnMng , uint64_t num){
    row_t *temp_row = read_remote_row(txnMng , num);

    // 2.	单边读获取数据项所有版本，找出最旧的版本，将其替换为要写入的新数据，
    int version_change;
    int last_ver = (temp_row->version_num) % HIS_CHAIN_NUM;//最新版本
    if(last_ver == HIS_CHAIN_NUM -1 )version_change = 0;
    else version_change = last_ver + 1 ;//要替换的版本

    //要通过txn_id解锁的版本
    int release_ver = txnMng->txn->accesses[num]->old_version_num % HIS_CHAIN_NUM;
    
    // 3.	单边写写入新版本、RTS、start_ts、end_ts、txn-id等信息，并将对上一个版本的txn-id设置为0，并解锁
    temp_row->version_num = temp_row->version_num +1;

    temp_row->txn_id[version_change] = 0;// txnMng->get_txn_id();
    temp_row->rts[version_change] = txnMng->txn->timestamp;
    temp_row->start_ts[version_change] = txnMng->txn->timestamp;
    temp_row->end_ts[version_change] = UINT64_MAX;

    temp_row->txn_id[release_ver] = 0;
    temp_row->end_ts[release_ver] = txnMng->txn->timestamp;

    //temp_row->_tid_word = 0;//解锁
//write back
    Transaction *txn = txnMng->txn;
    uint64_t off = txn->accesses[num]->offset;
    uint64_t loc = txn->accesses[num]->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id() + 1;
    uint64_t operate_size = row_t::get_row_size(temp_row->tuple_size);

    char *test_buf = Rdma::get_row_client_memory(thd_id);
    memcpy(test_buf, (char*)temp_row , operate_size);
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

}

bool rdma_mvcc::get_version(row_t * temp_row,uint64_t * change_num,Transaction *txn){
    bool result = false;
    uint64_t check_num = temp_row->version_num < HIS_CHAIN_NUM ?  temp_row->version_num : HIS_CHAIN_NUM;
               
    uint64_t k = 0;
    if(temp_row->version_num < HIS_CHAIN_NUM){//查找符合版本的事务
        for(k = 0;k <= check_num ; k++){
            if((temp_row->start_ts[k] < txn->timestamp || temp_row->start_ts[k] == 0)&& (temp_row->end_ts[k] > txn->timestamp || temp_row->end_ts[k] == UINT64_MAX)){
                result = true;
                *change_num = k;
                break;
            }
        }
    }else{
        for( k = 0 ; k < HIS_CHAIN_NUM ; k++){
            uint64_t j = 0;
            j = (temp_row->version_num + k)%HIS_CHAIN_NUM;
            if((temp_row->start_ts[j] < txn->timestamp || temp_row->start_ts[j] == 0) && (temp_row->end_ts[j] > txn->timestamp || temp_row->end_ts[k] == UINT64_MAX)){
                result = true;
                *change_num = j;
                break;
            }
        }
    }

    return result;
}

RC rdma_mvcc::validate_local(TxnManager * txnMng){
    RC rc = RCOK;
    //对读集和写集排序
    Transaction *txn = txnMng->txn;
    uint64_t wr_cnt = txn->write_cnt;

	int wr = 0; 
  	//int read_set[10];
	int read_set[txn->row_cnt - txn->write_cnt];
	int rd = 0;
	// for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
	// 	if (txn->accesses[rid]->type == WR)
	// 		txnMng->write_set[wr ++] = rid;
	// 	else
	// 		read_set[rd ++] = rid;
	// }

    int  i = 0;
    for(i = 0;i < txn->row_cnt;i ++ ){
        if(txn->accesses[i]->location == g_node_id){//local data
            //加锁
            //while(!lock_write_set(txnMng, i));
            if(!lock_write_set(txnMng, i)){
                rc = Abort;
            }

            if(txn->accesses[i]->type == RD){
                 //若版本的txn-id不为0且不等于当前事务，则当前事务回滚
                row_t *temp_row = txn->accesses[i]->orig_row;

                uint64_t change_num = 0;
                bool result = false;
                result = get_version(temp_row,&change_num,txn);
                
                if(result == false){//无合适版本
                    INC_STATS(txnMng->get_thd_id(), result_false, 1);
                    // printf("rdam_mvcc:377 version_num = %ld , txn_id = %ld %ld %ld %ld \n",temp_row->version_num, temp_row->txn_id[0],temp_row->txn_id[1],temp_row->txn_id[2],temp_row->txn_id[3]);
                    rc = Abort;
                }
                else{//check txn_id
                    if(temp_row->txn_id[change_num] != 0 && temp_row->txn_id[change_num] != txnMng->get_txn_id() + 1){
                        INC_STATS(txnMng->get_thd_id(), result_false, 1);
                        // printf("【rdma_mvcc:356】version_num = %ld , txn_id = %ld %ld %ld %ld \n",temp_row->version_num, temp_row->txn_id[0],temp_row->txn_id[1],temp_row->txn_id[2],temp_row->txn_id[3]);
                        rc = Abort;//版本不符合条件
                    }
                    else{
                        //其他情况单边写修改版本的RTS并解锁
                        //uint64_t version = (temp_row->version_num)%HIS_CHAIN_NUM;
                        uint64_t version = change_num;
                        temp_row->rts[version] = txn->timestamp;
                        //temp_row->_tid_word = 0;
                        //将读到数据存入读集
                    }
                }
            }
            else if(txn->accesses[i]->type == WR){
                row_t * temp_row = txn->accesses[i]->orig_row;  
                uint64_t version = (temp_row->version_num)%HIS_CHAIN_NUM;
                if(temp_row->txn_id[version] != 0 && temp_row->txn_id[version] != txnMng->get_txn_id() + 1){
                    INC_STATS(txnMng->get_thd_id(), ts_error, 1);
                    // printf("【rdma_mvcc:322】version_num = %ld , txn_id = %ld %ld %ld %ld \n",temp_row->version_num, temp_row->txn_id[0],temp_row->txn_id[1],temp_row->txn_id[2],temp_row->txn_id[3]);
                    rc = Abort;
                }
                else if(txn->timestamp <= temp_row->rts[version]){
                    INC_STATS(txnMng->get_thd_id(), ts_error, 1);
                    // printf("【rdma_mvcc:327】txn->timestamp = %ld temp_row->rts[%ld] = %ld \n",txn->timestamp,version,temp_row->rts[version]);
                    rc = Abort;
                }
                else{
                    //其他情况通过单边写修改版本的txn-id为当前事务id，RTS并解锁
                   temp_row->txn_id[version] = txnMng->get_txn_id() + 1;
                   temp_row->rts[version] = txn->timestamp;
                  //temp_row->version_num = temp_row->version_num + 1;
                   //temp_row->_tid_word = 0;//解锁
                }
           }
            remote_release_lock(txnMng,i);//解锁
        }else{
           // while(!lock_write_set(txnMng, i));//lock in loop
             if(!lock_write_set(txnMng, i)){
                rc = Abort;
            }

            bool write_back = false;

            uint64_t thd_id = txnMng->get_thd_id();
            
            uint64_t loc = txn->accesses[i]->location;
            uint64_t off = txn->accesses[i]->offset;

            row_t *temp_row = txn->accesses[i]->orig_row;
            uint64_t operate_size = row_t::get_row_size(temp_row->tuple_size);
            if(txn->accesses[i]->type == RD){
                //get target version
              
                uint64_t change_num = 0;
                bool result = false;
                result = get_version(temp_row,&change_num,txn);

                if(result == false){//no proper version
                    INC_STATS(txnMng->get_thd_id(), result_false, 1);
                    rc = Abort;
                }
                else{//check txn_id
                    if(temp_row->txn_id[change_num] != 0 && temp_row->txn_id[change_num] != txnMng->get_txn_id() + 1){
                        INC_STATS(txnMng->get_thd_id(), result_false, 1);
                        rc = Abort;//version dont match condition
                    }
                    else{
                        //uint64_t version = (temp_row->version_num)%HIS_CHAIN_NUM;
                        uint64_t version = change_num;
                        temp_row->rts[version] = txn->timestamp;
                        temp_row->_tid_word = 0;
                        write_back = true;

                        char *test_buf = Rdma::get_row_client_memory(thd_id);
                        memcpy(test_buf, (char*)temp_row, operate_size);

                        assert(txnMng->write_remote_content(loc,thd_id,operate_size,off,test_buf)==true);
    
/*
                        auto res_s4 = rc_qp[loc][thd_id]->send_normal(
                            {.op = IBV_WR_RDMA_WRITE,
                            .flags = IBV_SEND_SIGNALED,
                            .len = operate_size,
                            .wr_id = 0},
                            {.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
                            .remote_addr = off,
                            .imm_data = 0});                             
                        RDMA_ASSERT(res_s4 == rdmaio::IOCode::Ok);
                        auto res_p4 = rc_qp[loc][thd_id]->wait_one_comp();
                        RDMA_ASSERT(res_p4 == rdmaio::IOCode::Ok);
  */                      
                        //将读到数据存入读集
                    }
                }
                
            }else if(txn->accesses[i]->type == WR){
                uint64_t version = (temp_row->version_num)%HIS_CHAIN_NUM;
                if(temp_row->txn_id[version] != 0 && temp_row->txn_id[version] != txnMng->get_txn_id() + 1){
                    INC_STATS(txnMng->get_thd_id(), ts_error, 1);
                    rc = Abort;
                }else if(txn->timestamp <= temp_row->rts[version]){
                    INC_STATS(txnMng->get_thd_id(), ts_error, 1);
                    rc = Abort;
                }else{
                        //其他情况通过单边写修改版本的txn-id为当前事务id，RTS并解锁
                    temp_row->txn_id[version] = txnMng->get_txn_id() + 1;
                    temp_row->rts[version] = txn->timestamp;
                    //temp_row->version_num = temp_row->version_num + 1;
                    temp_row->_tid_word = 0;//解锁

                    char *test_buf = Rdma::get_row_client_memory(thd_id);
                    memcpy(test_buf, (char*)temp_row, operate_size);

                    assert(txnMng->write_remote_content(loc,thd_id,operate_size,off,test_buf)==true);
                    write_back = true;
                    /*
                    auto res_s4 = rc_qp[loc][thd_id]->send_normal(
                        {.op = IBV_WR_RDMA_WRITE,
                        .flags = IBV_SEND_SIGNALED,
                        .len = operate_size,
                        .wr_id = 0},
                        {.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
                        .remote_addr = off,
                        .imm_data = 0});
                    RDMA_ASSERT(res_s4 == rdmaio::IOCode::Ok);
                    auto res_p4 = rc_qp[loc][thd_id]->wait_one_comp();
                    RDMA_ASSERT(res_p4 == rdmaio::IOCode::Ok);
                    */
                }
            }

             //rlease lock
            if(write_back == false){
                rdmaio::qp::Op<> op;
                auto mr = client_rm_handler->get_reg_attr().value();
                uint64_t *test_loc = (uint64_t *)Rdma::get_row_client_memory(thd_id);
                op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + off), remote_mr_attr[loc].key).set_cas(txnMng->get_txn_id() + 1,0);
                assert(op.set_payload(test_loc, sizeof(uint64_t), mr.key) == true);
                auto res_s5 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

                RDMA_ASSERT(res_s5 == IOCode::Ok);
                auto res_p5 = rc_qp[loc][thd_id]->wait_one_comp();
                RDMA_ASSERT(res_p5 == IOCode::Ok);
            }

        }

    if(rc == Abort)break;
   }

    return rc;
}

RC rdma_mvcc::lock_row(TxnManager * txnMng){
    RC rc = RCOK;

  // 1.	单边CAS对写集数据项加锁
    Transaction *txn = txnMng->txn;
    uint64_t wr_cnt = txn->write_cnt;
    int cur_wr_idx = 0;
    for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[cur_wr_idx ++] = rid;
	    }
        uint64_t lock = txnMng->get_txn_id() + 1;
        for (uint64_t i = 0; i < wr_cnt; i++) {
            row_t * row = txn->accesses[ txnMng->write_set[i] ]->orig_row;
            if(!lock_write_set(txnMng,txnMng->write_set[i]) ){//lock fail
                rc = Abort;
                INC_STATS(txnMng->get_thd_id(), lock_row_fail, 1);
                return rc;
            }
            txnMng->num_locks ++;
        }
       if (txnMng->num_locks != wr_cnt) {
           rc = Abort;
           INC_STATS(txnMng->get_thd_id(), lock_num_unequal, 1);
           return rc;
       }

    return rc;
}

void * rdma_mvcc::abort_release_local_lock(TxnManager * txnMng , uint64_t num){
        Transaction *txn = txnMng->txn;
        row_t * temp_row = txn->accesses[num]->orig_row;
        int version = txn->accesses[num]->old_version_num % HIS_CHAIN_NUM ;//加锁的版本
        //int version = temp_row->version_num % HIS_CHAIN_NUM;
        temp_row->txn_id[version] =0;
}

void * rdma_mvcc::abort_release_remote_lock(TxnManager * txnMng , uint64_t num){
        Transaction *txn = txnMng->txn;
        row_t * temp_row = txn->accesses[num]->orig_row;
        int version = txn->accesses[num]->old_version_num % HIS_CHAIN_NUM ;//加锁的版本
        //int version = temp_row->version_num % HIS_CHAIN_NUM;
        temp_row->txn_id[version] = 0;

        uint64_t off = txn->accesses[num]->offset;
        uint64_t loc = txn->accesses[num]->location;
        uint64_t thd_id = txnMng->get_thd_id();
        uint64_t lock = txnMng->get_txn_id() + 1;
        uint64_t operate_size = row_t::get_row_size(temp_row->tuple_size);

        char *test_buf = Rdma::get_row_client_memory(thd_id);
        memcpy(test_buf, (char*)temp_row , operate_size);
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
}

row_t * rdma_mvcc::read_again(TxnManager * txnMng,uint64_t num){
        Transaction *txn = txnMng->txn;

        uint64_t off = txn->accesses[num]->offset;
        uint64_t loc = txn->accesses[num]->location;
        uint64_t thd_id = txnMng->get_thd_id();
        uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);

        char *test_buf = Rdma::get_row_client_memory(thd_id);
       // memcpy(test_buf, (char*)temp_row , operate_size);
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

        row_t *temp_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
	    memcpy(temp_row, test_buf, operate_size);

        //assert(temp_row->_tid_word != 0);

        return temp_row;
}

RC rdma_mvcc::finish(RC rc,TxnManager * txnMng){
    Transaction *txn = txnMng->txn;

    int wr = 0; 
  	//int read_set[10];
	int read_set[txn->row_cnt - txn->write_cnt];
	int rd = 0;
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[wr ++] = rid;
		else
			read_set[rd ++] = rid;
	}

    uint64_t wr_cnt = txn->write_cnt;
    
     if(rc == Abort){
       
        for (uint64_t i = 0; i < wr_cnt; i++) {
           //循环加锁
           bool lock = false;
           
           do {
               // if(lock!=-1)printf("[rdma_mvcc:499]lock = %ld\n",lock);
                lock = lock_write_set(txnMng,txnMng->write_set[i]);
                // INC_STATS(txnMng->get_thd_id(), cas_cnt, 1);
           }while(lock == false);

           //解锁
           if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){
               abort_release_local_lock(txnMng,txnMng->write_set[i]);
           }else{
               abort_release_remote_lock(txnMng,txnMng->write_set[i]);
           }

           remote_release_lock(txnMng,txnMng->write_set[i]);//解锁
         }
         
     }
     else{

      //  printf("【529\n】");
       for (uint64_t i = 0; i < wr_cnt; i++) {
           //循环加锁
           bool lock = false;
           do { 
                lock = lock_write_set(txnMng,txnMng->write_set[i]);
                INC_STATS(txnMng->get_thd_id(), cas_cnt, 1);
           }while(lock == false);

           if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){//local
                local_write_back(txnMng , txnMng->write_set[i]);
           }
           else{
                remote_write_back(txnMng , txnMng->write_set[i]);
           }  
            remote_release_lock(txnMng,txnMng->write_set[i]);//解锁
       }
       
    }
    
    return rc;
}

void rdma_mvcc::update_buffer(TxnManager * txn) {
	
}

#endif
