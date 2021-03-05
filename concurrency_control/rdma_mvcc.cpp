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
#include "rdma.h"
#include "rdma_mvcc.h"
#include "row_rdma_mvcc.h"
#include "mem_alloc.h"
#include "qps/op.hh"

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
		mem_allocator.free(entry->row, sizeof(row_t));
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

	uint64_t operate_size = sizeof(row_t);

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

	row_t *temp_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
	memcpy(temp_row, test_buf, operate_size);
	assert(temp_row->get_primary_key() == row->get_primary_key());

	return temp_row;
}

uint64_t rdma_mvcc::lock_write_set(TxnManager * txnMng , uint64_t num){
	Transaction *txn = txnMng->txn;

    uint64_t off = txn->accesses[num]->offset;
    uint64_t loc = txn->accesses[num]->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id();

	uint64_t *test_loc = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto mr = client_rm_handler->get_reg_attr().value();

	rdmaio::qp::Op<> op;
    op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + off), remote_mr_attr[loc].key).set_cas(0, lock);
  	assert(op.set_payload(test_loc, sizeof(uint64_t), mr.key) == true);
  	auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

	RDMA_ASSERT(res_s2 == IOCode::Ok);
	auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p2 == IOCode::Ok);

    return *test_loc;
}

void * rdma_mvcc::remote_release_lock(TxnManager * txnMng , uint64_t num){
    Transaction *txn = txnMng->txn;

    uint64_t off = txn->accesses[num]->offset;
    uint64_t loc = txn->accesses[num]->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id();

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

void * rdma_mvcc::remote_write_back(TxnManager * txnMng , uint64_t num){
    row_t *temp_row = read_remote_row(txnMng , num);
    int version_change;
    // 2.	单边读获取数据项所有版本，找出最旧的版本，将其替换为要写入的新数据，
    if(temp_row->version_num < HIS_CHAIN_NUM -1 ){
        version_change = temp_row->version_num ;//=temp_row->version_num+1-1
    }
    else{
        version_change =(temp_row->version_num) % HIS_CHAIN_NUM;
    }
    
    // 3.	单边写写入新版本、RTS、start_ts、end_ts、txn-id等信息，并将对上一个版本的txn-id设置为0，并解锁
    temp_row->txn_id[version_change] = txnMng->get_txn_id();
    temp_row->rts[version_change] = txnMng->txn->timestamp;
    int last_ver;
    if(version_change == 0){
        last_ver = HIS_CHAIN_NUM - 1;
    }
    else{
        last_ver = version_change - 1;
    } 
    temp_row->txn_id[last_ver] = 0;
//write back
    Transaction *txn = txnMng->txn;
    uint64_t off = txn->accesses[num]->offset;
    uint64_t loc = txn->accesses[num]->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id();
    uint64_t operate_size = sizeof(row_t);

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
        uint64_t lock = txnMng->get_txn_id();
        for (uint64_t i = 0; i < wr_cnt; i++) {
            row_t * row = txn->accesses[ txnMng->write_set[i] ]->orig_row;
            if(lock_write_set(txnMng,txnMng->write_set[i]) != 0 ){//lock fail
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

RC rdma_mvcc::finish(RC rc,TxnManager * txnMng){
    Transaction *txn = txnMng->txn;
    uint64_t wr_cnt = txn->write_cnt;
    if(rc == Commit){
       for (uint64_t i = 0; i < wr_cnt; i++) {
           if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){//local
                row_t * row = txn->accesses[ txnMng->write_set[i] ]->orig_row;
                row->manager->local_write_back(txnMng , txnMng->write_set[i]);
                row->manager->local_release_lock(txnMng , txnMng->write_set[i]);
           }
           else{
                row_t *row = txn->accesses[ txnMng->write_set[i] ]->orig_row;
                remote_write_back(txnMng , txnMng->write_set[i]);
                remote_release_lock(txnMng , txnMng->write_set[i]);
           }  
       }
    }
    else{//abort
         for (uint64_t i = 0; i < wr_cnt; i++) {
            if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){//local
                row_t * row = txn->accesses[ txnMng->write_set[i] ]->orig_row;
                row->manager->local_release_lock(txnMng , txnMng->write_set[i]);
            }
           else{
                row_t *row = txn->accesses[ txnMng->write_set[i] ]->orig_row;
                remote_release_lock(txnMng , txnMng->write_set[i]);
           }
         }
    }
}

void rdma_mvcc::update_buffer(TxnManager * txn) {
	
}