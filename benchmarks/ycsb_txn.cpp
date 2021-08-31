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

#include "global.h"
#include "config.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_rdma.h"
#include "transport/rdma.h"
#include "catalog.h"
#include "manager.h"
#include "row.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "row_rdma_mvcc.h"
#include "row_rdma_2pl.h"
#include "row_rdma_ts1.h"
#include "row_rdma_cicada.h"
#include "rdma_mvcc.h"
#include "rdma_ts1.h"
#include "rdma_null.h"
#include "mem_alloc.h"
#include "query.h"
#include "msg_queue.h"
#include "message.h"
#include "src/rdma/sop.hh"
#include "qps/op.hh"
#include "src/sshed.hh"
#include "transport.h"
#include "qps/op.hh"
#include "stats.h"

void YCSBTxnManager::init(uint64_t thd_id, Workload * h_wl) {
	TxnManager::init(thd_id, h_wl);
	_wl = (YCSBWorkload *) h_wl;
  reset();
}

void YCSBTxnManager::reset() {
  state = YCSB_0;
  next_record_id = 0;
  TxnManager::reset();
}

RC YCSBTxnManager::acquire_locks() {
  uint64_t starttime = get_sys_clock();
	assert(CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN);
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  locking_done = false;
  RC rc = RCOK;
  incr_lr();
  assert(ycsb_query->requests.size() == g_req_per_query);
  assert(phase == CALVIN_RW_ANALYSIS);
	for (uint32_t rid = 0; rid < ycsb_query->requests.size(); rid ++) {
		ycsb_request * req = ycsb_query->requests[rid];
		uint64_t part_id = _wl->key_to_part( req->key );
	DEBUG("LK Acquire (%ld,%ld) %d,%ld -> %ld\n", get_txn_id(), get_batch_id(), req->acctype,
		  req->key, GET_NODE_ID(part_id));
	if (GET_NODE_ID(part_id) != g_node_id) continue;
		INDEX * index = _wl->the_index;
		itemid_t * item;
		item = index_read(index, req->key, part_id);
		row_t * row = ((row_t *)item->location);
		RC rc2 = get_lock(row,req->acctype);
	if(rc2 != RCOK) {
	  rc = rc2;
	}
	}
  if(decr_lr() == 0) {
	if (ATOM_CAS(lock_ready, false, true)) rc = RCOK;
  }
  txn_stats.wait_starttime = get_sys_clock();
  /*
  if(rc == WAIT && lock_ready_cnt == 0) {
	if(ATOM_CAS(lock_ready,false,true))
	//lock_ready = true;
	  rc = RCOK;
  }
  */
  INC_STATS(get_thd_id(),calvin_sched_time,get_sys_clock() - starttime);
  locking_done = true;
  return rc;
}

RC YCSBTxnManager::run_txn(yield_func_t &yield, uint64_t cor_id) {

	RC rc = RCOK;
	assert(CC_ALG != CALVIN && CC_ALG != RDMA_CALVIN);

	if(IS_LOCAL(txn->txn_id) && state == YCSB_0 && next_record_id == 0) {
		DEBUG("Running txn %ld\n",txn->txn_id);
#if DEBUG_PRINTF
		printf("[txn start]txn：%d，ts：%lu\n",txn->txn_id,get_timestamp());
#endif
		//query->print();
		query->partitions_touched.add_unique(GET_PART_ID(0,g_node_id));
	}
	
	uint64_t starttime = get_sys_clock();

#if USE_DBPA == true && CC_ALG == RDMA_SILO
	//batch read all index for remote access
	ycsb_batch_read(yield,R_INDEX,cor_id);
	//batch read all row for remote access
	ycsb_batch_read(yield,R_ROW,cor_id);
#endif
	while(rc == RCOK && !is_done()) {
		rc = run_txn_state(yield, cor_id);
#if CC_ALG == WOUND_WAIT
		if (txn_state == WOUNDED) {
			rc = Abort;
			break;
		}  
#endif
	}
#if USE_DBPA == true && CC_ALG == RDMA_SILO
	reqId_index.erase(reqId_index.begin(),reqId_index.end());
	reqId_row.erase(reqId_row.begin(),reqId_row.end());
#endif

	uint64_t curr_time = get_sys_clock();
	txn_stats.process_time += curr_time - starttime;
	txn_stats.process_time_short += curr_time - starttime;
	txn_stats.wait_starttime = get_sys_clock();
//RDMA_SILO:logic?
	if(IS_LOCAL(get_txn_id())) {  //for one-side rdma, must be local
		if(is_done() && rc == RCOK) {
			// printf("a txn is done\n");
#if CC_ALG == WOUND_WAIT
      		txn_state = STARTCOMMIT;
#endif
			rc = start_commit(yield, cor_id);
		}
		else if(rc == Abort)
		rc = start_abort(yield, cor_id);
	} else if(rc == Abort){
		rc = abort(yield, cor_id);
	}
	
  return rc;

}

RC YCSBTxnManager::run_txn_post_wait() {
	uint64_t starttime = get_sys_clock();
	get_row_post_wait(row);
	uint64_t curr_time = get_sys_clock();
	txn_stats.process_time += curr_time - starttime;
	txn_stats.process_time_short += curr_time - starttime;
	next_ycsb_state();
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - curr_time);
	return RCOK;
}

bool YCSBTxnManager::is_done() { 
	return next_record_id >= ((YCSBQuery*)query)->requests.size();
}

void YCSBTxnManager::next_ycsb_state() {
  switch(state) {
	case YCSB_0:
	  state = YCSB_1;
	  break;
	case YCSB_1:
	  next_record_id++;
	  if(send_RQRY_RSP || !IS_LOCAL(txn->txn_id) || !is_done()) {
		state = YCSB_0;
	  } else {
		state = YCSB_FIN;
	  }
	  break;
	case YCSB_FIN:
	  break;
	default:
	  assert(false);
  }
}

bool YCSBTxnManager::is_local_request(uint64_t idx) {
  return GET_NODE_ID(_wl->key_to_part(((YCSBQuery*)query)->requests[idx]->key)) == g_node_id;
}

#if USE_DBPA == true && CC_ALG == RDMA_SILO
void YCSBTxnManager::ycsb_batch_read(yield_func_t &yield,BatchReadType rtype, uint64_t cor_id){
  	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	vector<vector<uint64_t>> remote_index(g_node_cnt);

	for(int i=0;i<ycsb_query->requests.size();i++){
		ycsb_request * req = ycsb_query->requests[i];
		uint64_t part_id = _wl->key_to_part( req->key );
		uint64_t loc = GET_NODE_ID(part_id);
		if(loc != g_node_id){  //remote
			remote_index[loc].push_back(i);
		}
	}
	for(int i=0;i<g_node_cnt;i++){
		if(remote_index[i].size()>0){
			batch_read(yield, rtype, i, remote_index, cor_id);
		}
	}
#if USE_OR == true
	for(int i=0;i<g_node_cnt;i++){
		if(remote_index[i].size()>0){
			get_batch_read(yield, rtype,i, remote_index, cor_id);
		}
	}
#endif
 }
#endif

itemid_t* YCSBTxnManager::ycsb_read_remote_index(yield_func_t &yield, ycsb_request * req, uint64_t cor_id) {
	uint64_t part_id = _wl->key_to_part( req->key );
  	uint64_t loc = GET_NODE_ID(part_id);
	// printf("loc:%d and g_node_id:%d\n", loc, g_node_id);
	assert(loc != g_node_id);
	uint64_t thd_id = get_thd_id();

	//get corresponding index
  // uint64_t index_key = 0;
	uint64_t index_key = req->key / g_node_cnt;
	uint64_t index_addr = (index_key) * sizeof(IndexInfo);
	uint64_t index_size = sizeof(IndexInfo);

    itemid_t* item = read_remote_index(yield, loc, index_addr,req->key, cor_id);
	return item;
}

RC YCSBTxnManager::send_remote_one_side_request(yield_func_t &yield, ycsb_request * req, row_t *& row_local, uint64_t cor_id) {
	// get the index of row to be operated
	
	itemid_t * m_item;
#if USE_DBPA == true && CC_ALG == RDMA_SILO
	m_item = reqId_index.find(next_record_id)->second;
#else
    m_item = ycsb_read_remote_index(yield, req, cor_id);
#endif
	uint64_t part_id = _wl->key_to_part( req->key );
    uint64_t loc = GET_NODE_ID(part_id);
	assert(loc != g_node_id);
    
    RC rc = RCOK;
    uint64_t version = 0;
    uint64_t lock = get_txn_id() + 1;

#if CC_ALG == RDMA_CNULL
    row_t * test_row = read_remote_row(yield,loc,m_item->offset,,cor_id);
    assert(test_row->get_primary_key() == req->key);
	mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
    mem_allocator.free(m_item, sizeof(itemid_t));
    return RCOK;
#endif

#if CC_ALG == RDMA_SILO
    if(req->acctype == RD || req->acctype == WR){
		uint64_t tts = get_timestamp();

		//read remote data
#if USE_DBPA == true
		row_t * test_row = reqId_row.find(next_record_id)->second;
#else
        row_t * test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
#endif
		assert(test_row->get_primary_key() == req->key);

        //preserve the txn->access
        access_t type = req->acctype;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);

        return rc;	   		
	}
	rc = RCOK;
	return rc;
#endif

#if CC_ALG == RDMA_MVCC
	row_t * test_row;
	if(req->acctype == RD){
		//read remote data
		test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
		assert(test_row->get_primary_key() == req->key);

		uint64_t change_num = 0;
		bool result = false;
		result = get_version(test_row,&change_num,txn);
		if(result == false){//no proper version: Abort
			INC_STATS(get_thd_id(), result_false, 1);
			rc = Abort;
			return rc;
		}
		//check txn_id
		if(test_row->txn_id[change_num] != 0 && test_row->txn_id[change_num] != get_txn_id() + 1){
			INC_STATS(get_thd_id(), result_false, 1);
			rc = Abort;
			return rc;
		}
		//CAS(old_rts,new_rts)
        uint64_t version = change_num;
		uint64_t old_rts = test_row->rts[version];
		uint64_t new_rts = get_timestamp();
		uint64_t rts_offset = m_item->offset + 2*sizeof(uint64_t) + HIS_CHAIN_NUM*sizeof(uint64_t) + version*sizeof(uint64_t);
		uint64_t cas_result = cas_remote_content(yield,loc,rts_offset,old_rts,new_rts,cor_id);//lock
		if(cas_result!=old_rts){ //CAS fail, atomicity violated
			INC_STATS(get_thd_id(), result_false, 1);
			rc = Abort;
			return rc;			
		}
		//read success
		test_row->rts[version] = get_timestamp();
	}
	else if(req->acctype == WR){
		uint64_t lock = get_txn_id()+1;
		uint64_t try_lock = -1;
#if USE_DBPA
		test_row = cas_and_read_remote(yield, try_lock,loc,m_item->offset,m_item->offset,0,lock,cor_id);
		if(try_lock != 0){
			INC_STATS(get_thd_id(), lock_fail, 1);
			mem_allocator.free(test_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			rc = Abort;
			return rc;			
		}
#else
		try_lock = cas_remote_content(yield,loc,m_item->offset,0,lock,cor_id);//lock
		if(try_lock != 0){
			INC_STATS(get_thd_id(), lock_fail, 1);
			rc = Abort;
			return rc;
		}
		//read remote data
		test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
#endif
		assert(test_row->get_primary_key() == req->key);

		uint64_t version = (test_row->version_num)%HIS_CHAIN_NUM;
		if((test_row->txn_id[version] != 0 && test_row->txn_id[version] != get_txn_id() + 1)||(get_timestamp() <= test_row->rts[version])){
			INC_STATS(get_thd_id(), ts_error, 1);
			//unlock and Abort
			uint64_t* temp__tid_word = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
			*temp__tid_word = 0;
			assert(write_remote_row(loc, sizeof(uint64_t),m_item->offset,(char*)(temp__tid_word))==true);
			mem_allocator.free(temp__tid_word,sizeof(uint64_t));
			mem_allocator.free(test_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			rc = Abort;
			return rc;
		}

		test_row->txn_id[version] = get_txn_id() + 1;
		test_row->rts[version] = get_timestamp();
		//temp_row->version_num = temp_row->version_num + 1;
		test_row->_tid_word = 0;//release lock
		//write back row
        uint64_t operate_size = row_t::get_row_size(test_row->tuple_size);
		assert(write_remote_row(loc,operate_size,m_item->offset,(char*)test_row)==true);
	}
    //preserve the txn->access
    access_t type = req->acctype;
	rc =  RCOK;
    rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
	return rc;
#endif

#if CC_ALG == RDMA_NO_WAIT
if(req->acctype == RD || req->acctype == WR){
		uint64_t tts = get_timestamp();

		uint64_t new_lock_info;
		uint64_t lock_info;
		row_t * test_row = NULL;

		if(req->acctype == RD){ //读集元素
			//第一次rdma read，得到数据项的锁信息
            row_t * lock_read = read_remote_row(yield,loc,m_item->offset,cor_id);

			new_lock_info = 0;
			lock_info = lock_read->_tid_word;
			mem_allocator.free(lock_read, row_t::get_row_size(ROW_DEFAULT_SIZE));
remote_atomic_retry_lock:
			bool conflict = Row_rdma_2pl::conflict_lock(lock_info, DLOCK_SH, new_lock_info);

			if(conflict){
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				return Abort;
			}
			assert(new_lock_info!=0);
#if USE_DBPA == true
			uint64_t try_lock = -1;
			test_row = cas_and_read_remote(yield, try_lock,loc,m_item->offset,m_item->offset,lock_info,new_lock_info,cor_id);
			if(try_lock != lock_info){ //CAS fail: Atomicity violated
				num_atomic_retry++;
				total_num_atomic_retry++;
				if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				lock_info = try_lock;
				goto remote_atomic_retry_lock;
			}
			assert(test_row->get_primary_key() == req->key);
#else
            uint64_t try_lock = -1;
            try_lock = cas_remote_content(yield,loc,m_item->offset,lock_info,new_lock_info,cor_id);
            if(try_lock != lock_info){
                num_atomic_retry++;
                total_num_atomic_retry++;
                if(num_atomic_retry > max_num_atomic_retry)
                    max_num_atomic_retry = num_atomic_retry;
                    lock_info = try_lock;
                    goto remote_atomic_retry_lock;
            }
			//read remote data
        	test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
        	assert(test_row->get_primary_key() == req->key);
#endif
		}
		else{ //写集元素直接CAS即可，不需要RDMA READ
			lock_info = 0; //只有lock_info==0时才可以CAS，否则加写锁失败，Abort
			new_lock_info = 3; //二进制11，即1个写锁
#if USE_DBPA == true
			uint64_t try_lock;
			test_row = cas_and_read_remote(yield, try_lock,loc,m_item->offset,m_item->offset,lock_info,new_lock_info, cor_id);
			if(try_lock != lock_info){ //CAS fail: lock conflict. Ignore read content 
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort; //原子性被破坏，CAS失败			
			}	
			//CAS success, now get read content
#else
			//RDMA CAS加锁
			uint64_t try_lock = -1;
			try_lock = cas_remote_content(yield,loc,m_item->offset,lock_info,new_lock_info,cor_id);
			if(try_lock != lock_info){ //如果CAS失败:对写集元素来说,即已经有锁;
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				return Abort; //原子性被破坏，CAS失败			
			}	
			//read remote data
			test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
			assert(test_row->get_primary_key() == req->key);   
#endif
		}
        //preserve the txn->access
        access_t type = req->acctype;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);

        return rc;
		   		
	}
	rc = RCOK;
	return rc;
#endif

#if CC_ALG == RDMA_NO_WAIT2
if(req->acctype == RD || req->acctype == WR){
		uint64_t tts = get_timestamp();
#if USE_DBPA == true
		//cas result
		uint64_t try_lock;
		//read result
		row_t * test_row = cas_and_read_remote(yield,try_lock,loc,m_item->offset,m_item->offset,0,1,cor_id);
		
		if(try_lock != 0){ //if CAS failed, ignore read content
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			row_local = NULL;
			txn->rc = Abort;
			mem_allocator.free(m_item, sizeof(itemid_t));
			mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			return Abort;
		}
		//CAS success, now get read content
		assert(test_row->_tid_word == 1);		
#if DEBUG_PRINTF
		printf("---thread id：%lu, remote lock success，lock location: %lu; %p, txn id: %lu,original lock_info: 0, new_lock_info: 1\n", get_thd_id(), loc, remote_mr_attr[loc].buf + m_item->offset, get_txn_id());
#endif
#endif

#if USE_DBPA == false
        uint64_t try_lock = -1;
        try_lock = cas_remote_content(yield,loc,m_item->offset,0,1,cor_id);

		if(try_lock != 0){ //CAS fail
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			row_local = NULL;
			txn->rc = Abort;
			mem_allocator.free(m_item, sizeof(itemid_t));

			return Abort;
		}
        row_t * test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
        assert(test_row->get_primary_key() == req->key);
#endif
        access_t type = req->acctype;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
        return rc;   		
	}
	rc = RCOK;
	return rc;
#endif

#if CC_ALG == RDMA_WAIT_DIE2
if(req->acctype == RD || req->acctype == WR){
		uint64_t tts = get_timestamp();

#if USE_DBPA == true
retry_lock:
		uint64_t try_lock;
		row_t* test_row = cas_and_read_remote(yield,try_lock,loc,m_item->offset,m_item->offset,0,tts, cor_id);
		if(try_lock != 0){// CAS fail
			if(tts <= try_lock){ //wait
				num_atomic_retry++;
				total_num_atomic_retry++;
				if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;	
				//sleep(1);
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				goto retry_lock;			
			}
			else{ //abort
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				return Abort;			
			}
		}
		assert(test_row->get_primary_key() == req->key);
#endif

#if USE_DBPA == false 
retry_lock:
        uint64_t try_lock = -1;
        try_lock = cas_remote_content(yield,loc,m_item->offset,0,tts,cor_id);

		if(try_lock != 0){ // cas fail
			if(tts <= try_lock){  //wait

				num_atomic_retry++;
				total_num_atomic_retry++;
				if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;	
				//sleep(1);
				goto retry_lock;			
			}	
			else{ //abort
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));

				return Abort;
			}
		}
        row_t * test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
        assert(test_row->get_primary_key() == req->key);
#endif
        access_t type = req->acctype;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
        return rc;	
	}
	rc = RCOK;
	return rc;
#endif

#if CC_ALG == RDMA_WOUND_WAIT
if(req->acctype == RD || req->acctype == WR){
		uint64_t tts = get_timestamp();
        int retry_time = 0;
		bool is_wound = false;
		RdmaTxnTableNode * value = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
retry_lock:
        uint64_t try_lock = -1;
		row_t * test_row;
		
#if USE_DBPA
		test_row = cas_and_read_remote(yield,try_lock,loc,m_item->offset,m_item->offset,0,tts, cor_id);
#else
        try_lock = cas_remote_content(yield,loc,m_item->offset,0,tts,cor_id);
		test_row = read_remote_row(yield,loc,m_item->offset,cor_id);
#endif
		assert(test_row->get_primary_key() == req->key);
		retry_time += 1;
		if(try_lock == 0) {
			test_row->lock_owner = txn->txn_id;			
			assert(write_remote_row(yield, loc, row_t::get_row_size(test_row->tuple_size), m_item->offset,(char*)test_row, cor_id) == true);
		}
		if(try_lock != 0 && retry_time <= MAX_RETRY_TIME){ // cas fail
			// printf("try_lock: %ld\n", retry_time);
			WOUNDState state;
			char * local_buf;
			if(test_row->lock_owner % g_node_cnt == g_node_id) {
				state = rdma_txn_table.local_get_state(get_thd_id(), test_row->lock_owner);
			} else {
		    	local_buf = rdma_txn_table.remote_get_state(yield, this, test_row->lock_owner, cor_id);
				memcpy(value, local_buf, sizeof(RdmaTxnTableNode));
				state = value->state;
			}
			// printf("read remote state:%ld", state);
			if(tts <= try_lock && state != WOUND_COMMITTING && state != WOUND_ABORTING && is_wound == false){  //wound
			    
				if(test_row->lock_owner % g_node_cnt == g_node_id) {
					rdma_txn_table.local_set_state(get_thd_id(), test_row->lock_owner, WOUND_ABORTING);

				} else {
					value->state = WOUND_ABORTING;
					rdma_txn_table.remote_set_state(yield, this, test_row->lock_owner, value, cor_id);
					// mem_allocator.free(value, sizeof(RdmaTxnTableNode));
				}
				// mem_allocator.free(value, sizeof(RdmaTxnTableNode));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				is_wound = true;
					goto retry_lock;
				// num_atomic_retry++;
				// total_num_atomic_retry++;
				// if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;	
				// //sleep(1);
				// goto retry_lock;			
			}	
			else{ //wait
				// DEBUG_M("TxnManager::get_row(abort) access free\n");
				// row_local = NULL;
				// txn->rc = Abort;
				// mem_allocator.free(m_item, sizeof(itemid_t));

				// return Abort;
				num_atomic_retry++;
				total_num_atomic_retry++;
				if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;	
				// mem_allocator.free(value, sizeof(RdmaTxnTableNode));
				mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
				// if(test_row->lock_owner % g_node_cnt != g_node_id)
				// mem_allocator.free(value, sizeof(RdmaTxnTableNode));
				//sleep(1);
					goto retry_lock;			
			}
			
		} else if(try_lock != 0 && retry_time > MAX_RETRY_TIME) {
			mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			mem_allocator.free(m_item, sizeof(itemid_t));
			mem_allocator.free(value, sizeof(RdmaTxnTableNode));
			rc = Abort;
			return rc;
		}       
		if(value) mem_allocator.free(value, sizeof(RdmaTxnTableNode));
        access_t type = req->acctype;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
        return rc;	
	}
	rc = RCOK;
	return rc;
#endif



#if CC_ALG == RDMA_TS1
    
    ts_t ts = get_timestamp();
#if USE_DBPA == true
    row_t * remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
	assert(remote_row->get_primary_key() == req->key);
	if(req->acctype == RD) {
		if(ts < remote_row->wts || (ts > remote_row->tid && remote_row->tid != 0)){
			rc = Abort;
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
	        mem_allocator.free(m_item, sizeof(itemid_t));
			return rc;
		} 
		else {//CAS(old_rts, new_rts) and read again
			rc = RCOK;
			if (remote_row->rts < ts){ //if ">=", no need to CAS
				ts_t old_rts = remote_row->rts;
				ts_t new_rts = ts;
				ts_t cas_result;
				uint64_t rts_offset = m_item->offset + sizeof(uint64_t)+sizeof(uint64_t)+sizeof(ts_t);
				row_t * second_row = cas_and_read_remote(yield,cas_result,loc,rts_offset,m_item->offset,old_rts,new_rts, cor_id);
				if(cas_result!=old_rts){ //cas fail, atomicity violated
					rc = Abort;
					DEBUG_M("TxnManager::get_row(abort) access free\n");
					mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
					mem_allocator.free(m_item, sizeof(itemid_t));
					mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
					return rc;
				}
				//cas success, now do double read check
				remote_row->rts = ts;
				if(second_row->wts!=remote_row->wts || second_row->tid!=remote_row->tid){ //atomicity violated
					rc = Abort;
					DEBUG_M("TxnManager::get_row(abort) access free\n");
					mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
					mem_allocator.free(m_item, sizeof(itemid_t));
					mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
					return rc;					
				}
				//read success
				mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			}
		}
	}
	else if(req->acctype == WR) {
		if (remote_row->tid != 0 || ts < remote_row->rts || ts < remote_row->wts){
			rc = Abort;
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
	        mem_allocator.free(m_item, sizeof(itemid_t));
			return rc;
		} 
		else {//CAS(old_tid, new_tid) and read again
			uint64_t old_tid = 0;
			uint64_t new_tid = ts;
			ts_t cas_result;
			uint64_t tid_offset = m_item->offset + sizeof(uint64_t);
			row_t * second_row = cas_and_read_remote(yield,cas_result,loc,tid_offset,m_item->offset,old_tid,new_tid, cor_id);
			if(cas_result!=old_tid){ //cas fail, atomicity violated
				rc = Abort;
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
				return rc;
			}
			//cas success, now do double read check
			if(ts < second_row->rts || ts < second_row->wts){
				rc = Abort;
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				//RDMA WRITE, SET tid = 0
				uint64_t* temp_tid = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t));
				*temp_tid = 0;
				assert(write_remote_row(loc, sizeof(uint64_t),m_item->offset+sizeof(uint64_t),(char*)(temp_tid))==true);
				mem_allocator.free(temp_tid,sizeof(uint64_t));
				mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
				mem_allocator.free(m_item, sizeof(itemid_t));
				mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
				return rc;					
			}
			//read success
			remote_row->tid = ts;
			mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
		}
	} else {
		assert(false);
	}
#else
    uint64_t try_lock = -1;
    try_lock = cas_remote_content(yield,loc,m_item->offset,0,lock,cor_id);
	if(try_lock != 0) {
		rc = Abort;
        return rc;
	}

    row_t * remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
	assert(remote_row->get_primary_key() == req->key);

    if(req->acctype == RD) {
		if(ts < remote_row->wts || (ts > remote_row->tid && remote_row->tid != 0)) rc = Abort;
		else {
			if (remote_row->rts < ts) remote_row->rts = ts;
			rc = RCOK;
		}
	}else if(req->acctype == WR) {
		if (remote_row->tid != 0 || ts < remote_row->rts || ts < remote_row->wts) rc = Abort;
		else {
			remote_row->tid = ts;
			rc = RCOK;	
		}
	} else {
		assert(false);
	}
    remote_row->mutx = 0;

    if (rc == Abort){
		DEBUG_M("TxnManager::get_row(abort) access free\n");

        try_lock = cas_remote_content(yield, loc,m_item->offset,lock , 0, cor_id);

		//mem_allocator.free(m_item,sizeof(itemid_t));
		mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
		return rc;
	}

    uint64_t operate_size = row_t::get_row_size(remote_row->tuple_size);
    // char *write_buf = Rdma::get_row_client_memory(get_thd_id());
    // memcpy(write_buf, (char*)remote_row, operate_size);

    assert(write_remote_row(loc,operate_size,m_item->offset,(char*)remote_row) == true);

    if(rc == Abort) {
        mem_allocator.free(m_item, sizeof(itemid_t));
		return rc;
	}
#endif
	rc = RCOK;
    rc = preserve_access(row_local,m_item,remote_row,req->acctype,remote_row->get_primary_key(),loc);
	
	return rc;
#endif

#if CC_ALG == RDMA_MAAT

    ts_t ts = get_timestamp();

    uint64_t try_lock = -1;
#if USE_DBPA == true
	row_t * remote_row = cas_and_read_remote(yield, try_lock,loc,m_item->offset,m_item->offset,0,lock,cor_id);
	if(try_lock != 0) {
		rc = Abort;
		//row_local = NULL;
		//mem_allocator.free(m_item, sizeof(itemid_t));
		mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
        return rc;
	}	
#else
    try_lock = cas_remote_content(yield,loc,m_item->offset,0,lock,cor_id);
	if(try_lock != 0) {
		rc = Abort;
        return rc;
	}
    row_t * remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
#endif
	assert(remote_row->get_primary_key() == req->key);

    if(req->acctype == RD) {
		if(remote_row->ucreads_len >= row_set_length - 1 ) {
            try_lock = cas_remote_content(yield,loc,m_item->offset,lock,0,cor_id);
            mem_allocator.free(m_item, sizeof(itemid_t));
			INC_STATS(get_thd_id(), maat_case6_cnt, 1);
            return Abort;
        }
		for(uint64_t i = 0, j = 0; i < row_set_length && j < remote_row->ucwrites_len; i++) {
			if(remote_row->uncommitted_writes[i] == 0) {
				continue;
			} else {
				uncommitted_writes.insert(remote_row->uncommitted_writes[i]);
				j++;
			}	
		}
		if(greatest_write_timestamp < remote_row->timestamp_last_write) {
			greatest_write_timestamp = remote_row->timestamp_last_write;
		}
		for(uint64_t i = 0; i < row_set_length; i++) {
			if(remote_row->uncommitted_reads[i] == get_txn_id() || unread_set.find(remote_row->get_primary_key()) != unread_set.end()) {
				break;
			}
			if(remote_row->uncommitted_reads[i] == 0) {
				remote_row->ucreads_len += 1;
				unread_set.insert(remote_row->get_primary_key());
				remote_row->uncommitted_reads[i] = get_txn_id();
				break;
			}
		}
	}else if(req->acctype == WR) {
		if(remote_row->ucwrites_len >= row_set_length - 1 ) {
            try_lock = cas_remote_content(yield,loc,m_item->offset,lock,0,cor_id);
            mem_allocator.free(m_item, sizeof(itemid_t));
			INC_STATS(get_thd_id(), maat_case6_cnt, 1);
            return Abort;
        }
		for(uint64_t i = 0, j = 0; i < row_set_length && j < remote_row->ucreads_len; i++) {
			if(remote_row->uncommitted_reads[i] == 0) {
				continue;
			} else {
				uncommitted_reads.insert(remote_row->uncommitted_reads[i]);
				j++;
			}
		}
		if(greatest_write_timestamp < remote_row->timestamp_last_write) {
			greatest_write_timestamp = remote_row->timestamp_last_write;
		}
		if(greatest_read_timestamp < remote_row->timestamp_last_read) {
			greatest_read_timestamp = remote_row->timestamp_last_read;
		}
		bool in_set = false;
		for(uint64_t i = 0, j = 0; i < row_set_length && j < remote_row->ucwrites_len; i++) {
			if(remote_row->uncommitted_writes[i] == get_txn_id() || unwrite_set.find(remote_row->get_primary_key()) != unwrite_set.end()) {
				in_set = true;
				continue;
			}
			if(remote_row->uncommitted_writes[i] == 0 && in_set == false) {
				remote_row->ucwrites_len += 1;
				unwrite_set.insert(remote_row->get_primary_key());
				remote_row->uncommitted_writes[i] = get_txn_id();
				in_set = true;
				j++;
				continue;
			}
			if(remote_row->uncommitted_writes[i] != 0) {
				uncommitted_writes_y.insert(remote_row->uncommitted_writes[i]);
				j++;
			}
		}
	}
    remote_row->_tid_word = 0;

    uint64_t operate_size = row_t::get_row_size(remote_row->tuple_size);
    // char *write_buf = Rdma::get_row_client_memory(get_thd_id());
    // memcpy(write_buf, (char*)remote_row, operate_size);

    assert(write_remote_row(yield,loc,operate_size,m_item->offset,(char*)remote_row,cor_id) == true);

    if(rc == Abort) {
        mem_allocator.free(m_item, sizeof(itemid_t));
		return rc;
	}

    rc = preserve_access(row_local,m_item,remote_row,req->acctype,remote_row->get_primary_key(),loc);
	
	return rc;
#endif

#if CC_ALG == RDMA_CICADA

 
    ts_t ts = get_timestamp();
	uint64_t retry_time = 0;
    row_t * remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
	assert(remote_row->get_primary_key() == req->key);

    if(req->acctype == RD) {
		assert(remote_row->version_cnt >= 0);
		for(int cnt = remote_row->version_cnt; cnt >= remote_row->version_cnt - HIS_CHAIN_NUM && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if(remote_row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(remote_row->cicada_version[i].Wts > this->get_timestamp()) {
				rc = Abort;
				break;
			}
			if(remote_row->cicada_version[i].state == Cicada_PENDING) {
				rc = WAIT;
				while(rc == WAIT && !simulation->is_done()) {
					retry_time += 1;
					mem_allocator.free(remote_row, sizeof(row_t));
                    remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
					assert(remote_row->get_primary_key() == req->key);

					if(remote_row->cicada_version[i].state == Cicada_PENDING) {
						rc = WAIT;
					} else {
						rc = RCOK;
						version = remote_row->cicada_version[i].key;
						// printf("_row->version_cnt: %ld, cnt : %ld, the version is %ld\n",remote_row->version_cnt, cnt, version);
					}
					if(retry_time > 1) {
						rc = Abort;
					}
					// rc =Abort;	
				}				
			} else {
				if(remote_row->cicada_version[i].Wts > this->get_timestamp()) {
					rc = Abort;
					
				}
				rc = RCOK;
				version = remote_row->cicada_version[i].key;
				// printf("_row->version_cnt: %ld, cnt : %ld, the version is %ld\n",remote_row->version_cnt, cnt, version);
			}	
		}
	}
    	if(req->acctype == WR) {
		assert(remote_row->version_cnt >= 0);
		for(int cnt = remote_row->version_cnt; cnt >= remote_row->version_cnt - HIS_CHAIN_NUM && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if(remote_row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(remote_row->cicada_version[i].Wts > this->get_timestamp() || remote_row->cicada_version[i].Rts > this->get_timestamp()) {
				rc = Abort;
				break;
			}
			if(remote_row->cicada_version[i].state == Cicada_PENDING) {
				// --todo !---pendind need wait //
				rc = WAIT;
				while(rc == WAIT && !simulation->is_done()) {
					retry_time += 1;
					mem_allocator.free(remote_row, sizeof(row_t));
				    remote_row = read_remote_row(yield,loc,m_item->offset,cor_id);
					assert(remote_row->get_primary_key() == req->key);

					if(remote_row->cicada_version[i].state == Cicada_PENDING) {
						rc = WAIT;
					} else {
						version = remote_row->cicada_version[i].key;
						// printf("_row->version_cnt: %ld, cnt : %ld, the version is %ld\n",remote_row->version_cnt, cnt, version);
						rc = RCOK;
					}
					if(retry_time > 1) {
						rc = Abort;
					}
					// rc = Abort;
				}
			} else {
				if(remote_row->cicada_version[i].Wts > this->get_timestamp() || remote_row->cicada_version[i].Rts > this->get_timestamp()) {
					rc = Abort;
				} else {
					
					rc = RCOK;
					version = remote_row->cicada_version[i].key;
					// printf("_row->version_cnt: %ld, cnt : %ld, the version is %ld\n",remote_row->version_cnt, cnt, version);
				}
			}
			
		}
	}
    // try_lock = cas_remote_content(yield,loc,m_item->offset,lock ,0,cor_id);

    // if(rc != Abort)this->version_num.push_back(version);
    if(rc == Abort) {
        mem_allocator.free(m_item, sizeof(itemid_t));
		return rc;
	}
	
    this->version_num.push_back(version);
    rc = preserve_access(row_local,m_item,remote_row,req->acctype,remote_row->get_primary_key(),loc);
	
	return rc;

#endif

}


RC YCSBTxnManager::send_remote_request() {
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
	ycsb_query->partitions_touched.add_unique(GET_PART_ID(0,dest_node_id));
#if USE_RDMA == CHANGE_MSG_QUEUE
	tport_man.rdma_thd_send_msg(get_thd_id(), dest_node_id, Message::create_message(this,RQRY));
#else
    // DEBUG("ycsb send remote request %ld, %ld\n",txn->txn_id,txn->batch_id);
    msg_queue.enqueue(get_thd_id(),Message::create_message(this,RQRY),dest_node_id);
#endif

	return WAIT_REM;
}

void YCSBTxnManager::copy_remote_requests(YCSBQueryMessage * msg) {
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	//msg->requests.init(ycsb_query->requests.size());
	uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
	#if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
	while (next_record_id < ycsb_query->requests.size() && GET_NODE_ID(ycsb_query->requests[next_record_id]->key) == dest_node_id) {
	#else
	while (next_record_id < ycsb_query->requests.size() && !is_local_request(next_record_id) &&
			GET_NODE_ID(ycsb_query->requests[next_record_id]->key) == dest_node_id) {
	#endif
		YCSBQuery::copy_request_to_msg(ycsb_query,msg,next_record_id++);
	}
}


RC YCSBTxnManager::run_txn_state(yield_func_t &yield, uint64_t cor_id) {
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	ycsb_request * req = ycsb_query->requests[next_record_id];
	uint64_t part_id = _wl->key_to_part( req->key );
  	bool loc = GET_NODE_ID(part_id) == g_node_id;
	
	RC rc = RCOK;
	switch (state) {
	case YCSB_0 :
#if CC_ALG == RDMA_WOUND_WAIT
        // printf("read local WOUNDState:%ld\n", rdma_txn_table.local_get_state(get_thd_id(),txn->txn_id));
		if(rdma_txn_table.local_get_state(get_thd_id(),txn->txn_id) == WOUND_ABORTING) {
			rc = Abort;
		} else {
#endif
		if(loc) {
			rc = run_ycsb_0(yield,req,row,cor_id);
		} else if (rdma_one_side()) {
			// printf("%ld:%ld:%ld:%ld in run txn    %ld:%ld\n",cor_id,get_txn_id(), req->key, next_record_id, GET_NODE_ID(part_id), g_node_id);
			rc = send_remote_one_side_request(yield, req, row, cor_id);
		} else {
			rc = send_remote_request();
		}
#if CC_ALG == RDMA_WOUND_WAIT
		}
#endif
	  break;
	case YCSB_1 :
		//read local row,for message queue by TCP/IP,write set has actually been written in this point,
		//but for rdma, it was written in local, the remote data will actually be written when COMMIT
		rc = run_ycsb_1(req->acctype,row);  
		break;
	case YCSB_FIN :
		state = YCSB_FIN;
		break;
	default:
		assert(false);
  }

  if (rc == RCOK) next_ycsb_state();

  return rc;
}

RC YCSBTxnManager::run_ycsb_0(yield_func_t &yield,ycsb_request * req,row_t *& row_local,uint64_t cor_id) {
  uint64_t starttime = get_sys_clock();
  RC rc = RCOK;
  int part_id = _wl->key_to_part( req->key );
  access_t type = req->acctype;
  itemid_t * m_item;
  INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
  m_item = index_read(_wl->the_index, req->key, part_id);
  starttime = get_sys_clock();
  row_t * row = ((row_t *)m_item->location);
  if (INDEX_STRUCT == IDX_RDMA) {
    mem_allocator.free(m_item, sizeof(itemid_t));
  }
  INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
  rc = get_row(yield,row, type,row_local,cor_id);
  return rc;
}

RC YCSBTxnManager::run_ycsb_1(access_t acctype, row_t * row_local) {
  uint64_t starttime = get_sys_clock();
  if (acctype == RD || acctype == SCAN) {
	int fid = 0;
	char * data = row_local->get_data();
	uint64_t fval __attribute__ ((unused));
	fval = *(uint64_t *)(&data[fid * 100]); //read fata and store to fval
#if ISOLATION_LEVEL == READ_COMMITTED || ISOLATION_LEVEL == READ_UNCOMMITTED
	// Release lock after read
	release_last_row_lock();
#endif

  } 
  else {
	assert(acctype == WR);
		int fid = 0;
	  char * data = row_local->get_data();
	  *(uint64_t *)(&data[fid * 100]) = 0; //write data, set data[0]=0
#if YCSB_ABORT_MODE
	if (data[0] == 'a') return RCOK;
#endif

#if ISOLATION_LEVEL == READ_UNCOMMITTED
	// Release lock after write
	release_last_row_lock();
#endif
  }
  INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
  return RCOK;
}

RC YCSBTxnManager::run_calvin_txn(yield_func_t &yield,uint64_t cor_id) {
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  DEBUG("(%ld,%ld) Run calvin txn\n",txn->txn_id,txn->batch_id);
  while(!calvin_exec_phase_done() && rc == RCOK) {
	DEBUG("(%ld,%ld) phase %d\n",txn->txn_id,txn->batch_id,this->phase);
	switch(this->phase) {
	  case CALVIN_RW_ANALYSIS:

		// Phase 1: Read/write set analysis
		calvin_expected_rsp_cnt = ycsb_query->get_participants(_wl);
#if YCSB_ABORT_MODE
		if(query->participant_nodes[g_node_id] == 1) {
		  calvin_expected_rsp_cnt--;
		}
#else
		calvin_expected_rsp_cnt = 0;
#endif
		DEBUG("(%ld,%ld) expects %d responses;\n", txn->txn_id, txn->batch_id,
			  calvin_expected_rsp_cnt);

		this->phase = CALVIN_LOC_RD;
		break;
	  case CALVIN_LOC_RD:
		// Phase 2: Perform local reads
		DEBUG("(%ld,%ld) local reads\n",txn->txn_id,txn->batch_id);
		rc = run_ycsb(yield,cor_id);
		//release_read_locks(query);

		this->phase = CALVIN_SERVE_RD;
		break;
	  case CALVIN_SERVE_RD:
		// Phase 3: Serve remote reads
		// If there is any abort logic, relevant reads need to be sent to all active nodes...
		if(query->participant_nodes[g_node_id] == 1) {
		  rc = send_remote_reads();
		}
		if(query->active_nodes[g_node_id] == 1) {
		  this->phase = CALVIN_COLLECT_RD;
		  if(calvin_collect_phase_done()) {
			rc = RCOK;
		  } else {
			DEBUG("(%ld,%ld) wait in collect phase; %d / %d rfwds received\n", txn->txn_id,
				  txn->batch_id, rsp_cnt, calvin_expected_rsp_cnt);
			rc = WAIT;
		  }
		} else { // Done
		  rc = RCOK;
		  this->phase = CALVIN_DONE;
		}

		break;
	  case CALVIN_COLLECT_RD:
		// Phase 4: Collect remote reads
		this->phase = CALVIN_EXEC_WR;
		break;
	  case CALVIN_EXEC_WR:
		// Phase 5: Execute transaction / perform local writes
		DEBUG("(%ld,%ld) execute writes\n",txn->txn_id,txn->batch_id);
		rc = run_ycsb(yield,cor_id);
		this->phase = CALVIN_DONE;
		break;
	  default:
		assert(false);
	}

  }
  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;
  txn_stats.wait_starttime = get_sys_clock();
  return rc;
}

RC YCSBTxnManager::run_ycsb(yield_func_t &yield,uint64_t cor_id) {
  RC rc = RCOK;
  assert(CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN);
  YCSBQuery* ycsb_query = (YCSBQuery*) query;

  for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
	  ycsb_request * req = ycsb_query->requests[i];
	if (this->phase == CALVIN_LOC_RD && req->acctype == WR) continue;
	if (this->phase == CALVIN_EXEC_WR && req->acctype == RD) continue;

		uint64_t part_id = _wl->key_to_part( req->key );
	bool loc = GET_NODE_ID(part_id) == g_node_id;

	if (!loc) continue;

	rc = run_ycsb_0(yield,req,row,cor_id);
	assert(rc == RCOK);

	rc = run_ycsb_1(req->acctype,row);
	assert(rc == RCOK);
  }
  return rc;

}

