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
#include "mem_alloc.h"
#include "query.h"
#include "msg_queue.h"
#include "message.h"
#include "src/rdma/sop.hh"
#include "qps/op.hh"
#include "src/sshed.hh"
#include "transport.h"
#include "qps/op.hh"


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
  assert(CC_ALG == CALVIN);
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


RC YCSBTxnManager::run_txn() {

	RC rc = RCOK;
	assert(CC_ALG != CALVIN);

	if(IS_LOCAL(txn->txn_id) && state == YCSB_0 && next_record_id == 0) {
		DEBUG("Running txn %ld\n",txn->txn_id);
#if DEBUG_PRINTF
		printf("[txn start]txn：%d，ts：%lu\n",txn->txn_id,get_timestamp());
#endif
		//query->print();
		query->partitions_touched.add_unique(GET_PART_ID(0,g_node_id));
	}

	uint64_t starttime = get_sys_clock();

	while(rc == RCOK && !is_done()) {
		rc = run_txn_state();
	}

	uint64_t curr_time = get_sys_clock();
	txn_stats.process_time += curr_time - starttime;
	txn_stats.process_time_short += curr_time - starttime;
	txn_stats.wait_starttime = get_sys_clock();
//RDMA_SILO:logic?
	if(IS_LOCAL(get_txn_id())) {  //for one-side rdma, must be local
		if(is_done() && rc == RCOK)
		rc = start_commit();
		else if(rc == Abort)
		rc = start_abort();
	} else if(rc == Abort){
		rc = abort();
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

bool YCSBTxnManager::is_done() { return next_record_id >= ((YCSBQuery*)query)->requests.size(); }

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

itemid_t* YCSBTxnManager::ycsb_read_remote_index(ycsb_request * req) {
	uint64_t part_id = _wl->key_to_part( req->key );
  	uint64_t loc = GET_NODE_ID(part_id);
	assert(loc != g_node_id);
	uint64_t thd_id = get_thd_id();

	//get corresponding index
  // uint64_t index_key = 0;
	uint64_t index_key = req->key / g_node_cnt;
	uint64_t index_addr = (index_key) * sizeof(IndexInfo);
	uint64_t index_size = sizeof(IndexInfo);

    itemid_t* item = read_remote_index(loc, index_addr,req->key);
	return item;
}

RC YCSBTxnManager::send_remote_one_side_request(ycsb_request * req,row_t *& row_local) {
	// get the index of row to be operated
	itemid_t * m_item;
	m_item = ycsb_read_remote_index(req);
	
	uint64_t part_id = _wl->key_to_part( req->key );
    uint64_t loc = GET_NODE_ID(part_id);
	assert(loc != g_node_id);
    RC rc = RCOK;
    uint64_t version = 0;
    uint64_t lock = get_txn_id() + 1;

#if CC_ALG == RDMA_SILO || CC_ALG == RDMA_MVCC || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2
	if(req->acctype == RD || req->acctype == WR){
		uint64_t tts = get_timestamp();

#if CC_ALG == RDMA_MVCC
    uint64_t lock = get_txn_id()+1;
    uint64_t test_loc = -1;
    test_loc = cas_remote_content(loc,m_item->offset,0,lock);
    if(test_loc != 0){
       INC_STATS(get_thd_id(), lock_fail, 1);
       rc = Abort;
       return rc;
    }
#endif

#if CC_ALG == RDMA_WAIT_DIE2
		//lock by rdma cas
retry_lock:
        uint64_t try_lock = -1;
        try_lock = cas_remote_content(loc,m_item->offset,0,tts);

		if(try_lock != 0){ //cas fail
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
#endif
#if CC_ALG == RDMA_NO_WAIT2
		//lock by rdma cas
        uint64_t try_lock = -1;
        try_lock = cas_remote_content(loc,m_item->offset,0,1);

		if(try_lock != 0){ //cas fail
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			row_local = NULL;
			txn->rc = Abort;
			mem_allocator.free(m_item, sizeof(itemid_t));

			return Abort;
		}

#endif
#if CC_ALG == RDMA_NO_WAIT 
		uint64_t new_lock_info;
		uint64_t lock_info;
remote_atomic_retry_lock:
		if(req->acctype == RD){ //read set
			//first rdma read,get lock info of data 
            row_t * test_row = read_remote_content(loc,m_item->offset);

			new_lock_info = 0;
			lock_info = test_row->_lock_info;

			bool conflict = Row_rdma_2pl::conflict_lock(lock_info, DLOCK_SH, new_lock_info);

			if(conflict){
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				return Abort;
			}
			if(new_lock_info == 0){
				printf("--thd：%lu,remote lock fail!!!!!!lock location: %lu; %p, txn: %lu,old lock_info: %lu, new_lock_info: %lu\n", get_thd_id(), loc, remote_mr_attr[loc].buf + m_item->offset, get_txn_id(), lock_info, new_lock_info);
			} 
			assert(new_lock_info!=0);
		}
		else{ //read set data directly to CAS , no need to RDMA READ
			lock_info = 0; //if lock_info!=0, CAS fail , Abort
			new_lock_info = 3; //binary 11, aka 1 read lock
		}
		//lock by RDMA CAS
        uint64_t try_lock = -1;
        try_lock = cas_remote_content(loc,m_item->offset,lock_info,new_lock_info);

		if(try_lock != lock_info && req->acctype == RD){ //cas fail,for write set elements means The atomicity is destroyed

			num_atomic_retry++;
			total_num_atomic_retry++;
			if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;
			goto remote_atomic_retry_lock;

		}	
		if(try_lock != lock_info && req->acctype == WR){ //cas fail,for read set elements means already locked
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			row_local = NULL;
			txn->rc = Abort;
			mem_allocator.free(m_item, sizeof(itemid_t));

			return Abort; //CAS fail		
		}	

#endif
        //read remote data
        row_t * test_row = read_remote_content(loc,m_item->offset);

		assert(test_row->get_primary_key() == req->key);

#if CC_ALG == RDMA_MVCC
        test_loc = cas_remote_content(loc,m_item->offset,lock,0);
#endif
        //preserve the txn->access
        access_t type = req->acctype;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
       // mem_allocator.free(m_item, sizeof(itemid_t));

        return rc;
		   		
	}
    rc = RCOK;
	return rc;

#endif
#if CC_ALG == RDMA_TS1 || CC_ALG == RDMA_MAAT || CC_ALG == RDMA_CICADA

    ts_t ts = get_timestamp();

    uint64_t try_lock = -1;
    try_lock = cas_remote_content(loc,m_item->offset,0,lock);
	if(try_lock != 0) {
		rc = Abort;
        return rc;
	}

    row_t * remote_row = read_remote_content(loc,m_item->offset);
	assert(remote_row->get_primary_key() == req->key);

#if CC_ALG == RDMA_TS1
    if(req->acctype == RD) {

		if(ts < remote_row->wts || (ts > remote_row->tid && remote_row->tid != 0))rc = Abort;
		else {
			if (remote_row->rts < ts) remote_row->rts = ts;
			rc = RCOK;
		}
	} 
	else if(req->acctype == WR) {
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

        try_lock = cas_remote_content(loc,m_item->offset,lock , 0);

		//mem_allocator.free(m_item,sizeof(itemid_t));
		mem_allocator.free(remote_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
		return rc;
	}

    uint64_t operate_size = row_t::get_row_size(remote_row->tuple_size);
    char *write_buf = Rdma::get_row_client_memory(get_thd_id());
    memcpy(write_buf, (char*)remote_row, operate_size);

    assert(write_remote_content(loc,operate_size,m_item->offset,write_buf) == true);

#elif CC_ALG == RDMA_MAAT
    if(req->acctype == RD) {
		for(uint64_t i = 0; i < row_set_length; i++) {
			assert(i < row_set_length - 1);
			if(remote_row->uncommitted_writes[i] == 0 || remote_row->uncommitted_writes[i] > RDMA_TIMETABLE_MAX) {
				break;
			}
			uncommitted_writes.insert(remote_row->uncommitted_writes[i]);
		}
		if(greatest_write_timestamp < remote_row->timestamp_last_write) {
			greatest_write_timestamp = remote_row->timestamp_last_write;
		}
		for(uint64_t i = 0; i < row_set_length; i++) {
			assert(i < row_set_length - 1);
			if(remote_row->uncommitted_reads[i] == get_txn_id()) {
				break;
			}
			if(remote_row->uncommitted_reads[i] == 0 || remote_row->uncommitted_reads[i] > RDMA_TIMETABLE_MAX) {
				remote_row->uncommitted_reads[i] = get_txn_id();
				break;
			}
		}
	}else if(req->acctype == WR) {
		for(uint64_t i = 0; i < row_set_length; i++) {

			assert(i < row_set_length - 1);
			if(remote_row->uncommitted_reads[i] == 0 || remote_row->uncommitted_reads[i] > RDMA_TIMETABLE_MAX) {
				break;
			}
			uncommitted_reads.insert(remote_row->uncommitted_reads[i]);
		}
		if(greatest_write_timestamp < remote_row->timestamp_last_write) {
			greatest_write_timestamp = remote_row->timestamp_last_write;
		}
		if(greatest_read_timestamp < remote_row->timestamp_last_read) {
			greatest_read_timestamp = remote_row->timestamp_last_read;
		}
		bool in_set = false;
		for(uint64_t i = 0; i < row_set_length; i++) {
			assert(i < row_set_length - 1);
			if(remote_row->uncommitted_writes[i] == get_txn_id()) in_set = true;
			if(remote_row->uncommitted_writes[i] == 0 || remote_row->uncommitted_writes[i] > RDMA_TIMETABLE_MAX) {
				if(in_set == false)
				remote_row->uncommitted_writes[i] = get_txn_id();
				break;
			}
			uncommitted_writes_y.insert(remote_row->uncommitted_writes[i]);
		}
	}
    remote_row->_tid_word = 0;

    uint64_t operate_size = row_t::get_row_size(remote_row->tuple_size);
    char *write_buf = Rdma::get_row_client_memory(get_thd_id());
    memcpy(write_buf, (char*)remote_row, operate_size);

    assert(write_remote_content(loc,operate_size,m_item->offset,write_buf) == true);
#elif CC_ALG == RDMA_CICADA
    if(req->acctype == RD) {
		assert(remote_row->version_cnt >= 0);
		for(int cnt = remote_row->version_cnt - 1; cnt >= remote_row->version_cnt - 5 && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if(remote_row->cicada_version[i].Wts > this->start_ts || remote_row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(remote_row->cicada_version[i].state == Cicada_PENDING) {
				rc = WAIT;
				while(rc == WAIT) {
                    //release lock
                    try_lock = cas_remote_content(loc,m_item->offset,lock,0);
                    //add lock
                    try_lock = cas_remote_content(loc,m_item->offset,0,lock);
				
					if (try_lock != 0) {
						row_local = NULL;
						txn->rc = Abort;
						mem_allocator.free(m_item, sizeof(itemid_t));
						printf("RDMA_MAAT cas lock fault\n");
						return Abort; //CAS fail
					}

                    remote_row = read_remote_content(loc,m_item->offset);
					assert(remote_row->get_primary_key() == req->key);

					if(remote_row->cicada_version[i].state == Cicada_PENDING) {
						rc = WAIT;
					} else {
						rc = RCOK;
						version = remote_row->cicada_version[i].key;
					}
				}				
			} else {
				rc = RCOK;
				version = remote_row->cicada_version[i].key;
			}	
		}
	}
    	if(req->acctype == WR) {
		assert(remote_row->version_cnt >= 0);
		for(int cnt = remote_row->version_cnt - 1; cnt >= remote_row->version_cnt - 5 && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if(remote_row->cicada_version[i].Wts > this->start_ts || remote_row->cicada_version[i].Rts > this->start_ts || remote_row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(remote_row->cicada_version[i].state == Cicada_PENDING) {
				// --todo !---pendind need wait //
				rc = WAIT;
				while(rc == WAIT) {
					//release lock
                    try_lock = cas_remote_content(loc,m_item->offset,lock,0);
                    //add lock
                    try_lock = cas_remote_content(loc,m_item->offset,0,lock);

					if (try_lock != 0) {
						row_local = NULL;
						txn->rc = Abort;
						mem_allocator.free(m_item, sizeof(itemid_t));
						printf("RDMA_MAAT cas lock fault\n");

						return Abort; //CAS fail
					}
				    remote_row = read_remote_content(loc,m_item->offset);
					assert(remote_row->get_primary_key() == req->key);

					if(remote_row->cicada_version[i].state == Cicada_PENDING) {
						rc = WAIT;
					} else {
						version = remote_row->cicada_version[i].key;
						rc = RCOK;
					}
				}
			} else {
				if(remote_row->cicada_version[i].Wts > this->start_ts || remote_row->cicada_version[i].Rts > this->start_ts) {
					rc = Abort;
				} else {
					rc = RCOK;
					version = remote_row->cicada_version[i].key;
				}	
			}
			
		}
	}
    try_lock = cas_remote_content(loc,m_item->offset,lock ,0);

    if(rc != Abort)this->version_num.push_back(version);
#endif

    if(rc == Abort) {
		return rc;
	}

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


RC YCSBTxnManager::run_txn_state() {
  	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	ycsb_request * req = ycsb_query->requests[next_record_id];
	uint64_t part_id = _wl->key_to_part( req->key );
  	bool loc = GET_NODE_ID(part_id) == g_node_id;

	RC rc = RCOK;
	switch (state) {
	case YCSB_0 :
		if(loc) {
			rc = run_ycsb_0(req,row);
		} else if (rdma_one_side()) {
			rc = send_remote_one_side_request(req,row);
		} else {
			rc = send_remote_request();
		}
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

RC YCSBTxnManager::run_ycsb_0(ycsb_request * req,row_t *& row_local) {
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
  rc = get_row(row, type,row_local);
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

RC YCSBTxnManager::run_calvin_txn() {
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
		rc = run_ycsb();
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
		rc = run_ycsb();
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

RC YCSBTxnManager::run_ycsb() {
  RC rc = RCOK;
  assert(CC_ALG == CALVIN);
  YCSBQuery* ycsb_query = (YCSBQuery*) query;

  for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
	  ycsb_request * req = ycsb_query->requests[i];
	if (this->phase == CALVIN_LOC_RD && req->acctype == WR) continue;
	if (this->phase == CALVIN_EXEC_WR && req->acctype == RD) continue;

		uint64_t part_id = _wl->key_to_part( req->key );
	bool loc = GET_NODE_ID(part_id) == g_node_id;

	if (!loc) continue;

	rc = run_ycsb_0(req,row);
	assert(rc == RCOK);

	rc = run_ycsb_1(req->acctype,row);
	assert(rc == RCOK);
  }
  return rc;

}

