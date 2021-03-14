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
#include "row_rdma_mvcc.h"
#include "mem_alloc.h"
#include "qps/op.hh"
#if CC_ALG == RDMA_MVCC
void Row_rdma_mvcc::init(row_t * row) {

}

row_t * Row_rdma_mvcc::clear_history(TsType type, ts_t ts) {

}




void Row_rdma_mvcc::buffer_req(TsType type, TxnManager *txn) {

}


void Row_rdma_mvcc::insert_history(ts_t ts, row_t *row) {
	
}

bool Row_rdma_mvcc::conflict(TsType type, ts_t ts) {
	// find the unique prewrite-read couple (prewrite before read)
	// if no such couple found, no conflict.
	// else
	// 	 if exists writehis between them, NO conflict!!!!
	// 	 else, CONFLICT!!!
	
	return true;
}

RC Row_rdma_mvcc::access(TxnManager * txn, access_t type, row_t * row) {
	RC rc = RCOK;
    if(type == RD){

    }
    else if(type == WR){
        
    }
	return rc;
}

void Row_rdma_mvcc::local_write_back(TxnManager * txnMng , int num){
    Transaction *txn = txnMng->txn;
    int i,version_change;
    row_t * row = txn->accesses[ txnMng->write_set[num] ]->orig_row;
    if(row->version_num < HIS_CHAIN_NUM -1 ){
        version_change = row->version_num ;//=temp_row->version_num+1-1
    }
    else{
        version_change =(row->version_num) % HIS_CHAIN_NUM;
    }

    row->txn_id[version_change] = txnMng->get_txn_id();
    row->rts[version_change] = txnMng->txn->timestamp;
    int last_ver;
    if(version_change == 0){
        last_ver = HIS_CHAIN_NUM - 1;
    }
    else{
        last_ver = version_change - 1;
    } 
    row->txn_id[last_ver] = 0;
}

void Row_rdma_mvcc::local_release_lock(TxnManager * txnMng , int num){
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
}

RC Row_rdma_mvcc::finish(RC rc,TxnManager * txnMng){
    
}

void Row_rdma_mvcc::update_buffer(TxnManager * txn) {
	
}
#endif