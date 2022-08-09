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
#include "row_rdma_2pl.h"
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
#include "log_rdma.h"

void YCSBTxnManager::init(uint64_t thd_id, Workload * h_wl) {
	TxnManager::init(thd_id, h_wl);
	_wl = (YCSBWorkload *) h_wl;
  reset();
}

void YCSBTxnManager::reset() {
  state = YCSB_0;
  next_record_id = 0;
  remote_next_center_id = 0;
  for(int i = 0; i < g_center_cnt; i++) {
	remote_center[i].clear();
  }
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

RC YCSBTxnManager::run_txn(yield_func_t &yield, uint64_t cor_id) {

	RC rc = RCOK;
	assert(CC_ALG != CALVIN);
#if DEBUG_PRINTF
		printf("[txn start]txn %d on node %u, is_local?%d\n",txn->txn_id,g_node_id,IS_LOCAL(txn->txn_id));
#endif

	if(IS_LOCAL(txn->txn_id) && state == YCSB_0 && next_record_id == 0) {
		DEBUG("Running txn %ld\n",txn->txn_id);
		//query->print();
		query->partitions_touched.add_unique(GET_PART_ID(0,g_node_id));
		query->centers_touched.add_unique(g_center_id);
#if PARAL_SUBTXN
		rc = send_remote_subtxn();
#endif
	}
	
	uint64_t starttime = get_sys_clock();

#if BATCH_INDEX_AND_READ
	//batch read all index for remote access
	ycsb_batch_read(yield,R_INDEX,cor_id);
	//batch read all row for remote access
	ycsb_batch_read(yield,R_ROW,cor_id);
#endif
	num_locks = 0;
	while(rc == RCOK && !is_done()) {
#if CC_ALG == WOUND_WAIT
		if (txn_state == WOUNDED) {
			rc = Abort;
			break;
		}  
#endif
		rc = run_txn_state(yield, cor_id);
	}
#if CC_ALG == WOUND_WAIT
	if (txn_state == WOUNDED) 
		rc = Abort;
#endif
#if BATCH_INDEX_AND_READ
	reqId_index.erase(reqId_index.begin(),reqId_index.end());
	reqId_row.erase(reqId_row.begin(),reqId_row.end());
#endif
    // if(rc == Abort) total_num_atomic_retry++;
	uint64_t curr_time = get_sys_clock();
	txn_stats.process_time += curr_time - starttime;
	txn_stats.process_time_short += curr_time - starttime;
	txn_stats.wait_starttime = get_sys_clock();
	if(IS_LOCAL(get_txn_id())) {  
		txn_stats.wait_for_rsp_time = curr_time;
		if(is_done() && rc == RCOK) {
			// printf("a txn is done\n");
#if CC_ALG == WOUND_WAIT
      		txn_state = STARTCOMMIT;
#endif
			rc = start_commit(yield, cor_id);
		}
		else if(rc == Abort) { 
			rc = start_abort(yield, cor_id);
		}
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
#if PARAL_SUBTXN == true
	return (next_record_id >= ((YCSBQuery*)query)->requests.size() && !waiting_for_response());
#else
	return next_record_id >= ((YCSBQuery*)query)->requests.size();
#endif
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

#if BATCH_INDEX_AND_READ
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
	for(int i=0;i<g_node_cnt;i++){
		if(remote_index[i].size()>0){
			get_batch_read(yield, rtype,i, remote_index, cor_id);
		}
	}
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
#if USE_REPLICA
	uint64_t index_key = req->key;
#else
	uint64_t index_key = req->key / g_node_cnt;
#endif
	uint64_t index_addr = (index_key) * sizeof(IndexInfo);
	uint64_t index_size = sizeof(IndexInfo);

    itemid_t* item = read_remote_index(yield, loc, index_addr,req->key, cor_id);
	return item;
}

RC YCSBTxnManager::send_remote_one_side_request(yield_func_t &yield, ycsb_request * req, row_t *& row_local, uint64_t cor_id) {
	// get the index of row to be operated
	
	itemid_t * m_item;
#if BATCH_INDEX_AND_READ
	m_item = reqId_index.find(next_record_id)->second;
#else
    m_item = ycsb_read_remote_index(yield, req, cor_id);
#endif
	uint64_t part_id = _wl->key_to_part( req->key );
    uint64_t loc = GET_NODE_ID(part_id);
	assert(loc != g_node_id);
    
    RC rc = RCOK;
    uint64_t version = 0;

	rc = get_remote_row(yield, req->acctype, loc, m_item, row_local, cor_id);
	// mem_allocator.free(m_item, sizeof(itemid_t));
	return rc;
}

RC YCSBTxnManager::send_remote_subtxn() {
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	RC rc = RCOK;
	
	for(int i = 0; i < ycsb_query->requests.size(); i++) {
		ycsb_request * req = ycsb_query->requests[i];
		uint64_t part_id = _wl->key_to_part(req->key);
		vector<uint64_t> node_id;
#if USE_REPLICA
		if(req->acctype == WR){
			node_id.push_back(GET_NODE_ID(part_id));
			node_id.push_back(GET_FOLLOWER1_NODE(part_id));
			node_id.push_back(GET_FOLLOWER2_NODE(part_id));
		}else if(req->acctype == RD){
			node_id.push_back(GET_NODE_ID(part_id));
		}else assert(false);
#else
		node_id.push_back(GET_NODE_ID(part_id));		
#endif
		for(int j=0;j<node_id.size();j++){
			uint64_t center_id = GET_CENTER_ID(node_id[j]);
			remote_center[center_id].push_back(i);
			ycsb_query->centers_touched.add_unique(center_id);
			ycsb_query->partitions_touched.add_unique(GET_PART_ID(0,node_id[j]));
			//center_master is set as the first toughed primary, if not exist, use the first toughed backup.
			auto ret = center_master.insert(pair<uint64_t, uint64_t>(center_id, node_id[j]));
			if(ret.second == false){
				if(!is_primary[center_id] && j == 0){
					center_master[center_id] = node_id[j]; //change center_master
					is_primary[center_id] = true;
				}
			}else{
				is_primary[center_id] = (j == 0 ? true : false); 
			}
		}
	}
	// printf("txn %lu, is_primary: %d %d %d %d\n", get_txn_id(),is_primary[0],is_primary[1],is_primary[2],is_primary[3]);
	//get rsp_cnt
	rsp_cnt = 0;
	for(int i=0;i<query->centers_touched.size();i++){
		if(is_primary[query->centers_touched[i]]) ++rsp_cnt;
	}
	--rsp_cnt; //exclude this center
	// rsp_cnt = query->centers_touched.size() - 1;
	// printf("p_rsp_cnt: %d\n", rsp_cnt);


	//get extra_wait and req_need_wait
	for(int i = 0; i < ycsb_query->requests.size(); i++) {
		ycsb_request * req = ycsb_query->requests[i];
		uint64_t part_id = _wl->key_to_part(req->key);
		if(req->acctype == WR){
			uint64_t center_id1 = GET_CENTER_ID(GET_FOLLOWER1_NODE(part_id));
			uint64_t center_id2 = GET_CENTER_ID(GET_FOLLOWER2_NODE(part_id));
			if(is_primary[center_id1] || is_primary[center_id2]){
				//no extra wait for req i
				extra_wait[i][0] = -1;
				extra_wait[i][1] = -1;			
			}else{
				req_need_wait[i] = true;
				extra_wait[i][0] = center_id1;
				extra_wait[i][1] = center_id2;							
			}
		}else if(req->acctype == RD){
			//no extra wait for req i
			extra_wait[i][0] = -1;
			extra_wait[i][1] = -1;			
		}
	}
	
	assert(num_msgs_rw_prep==0);
	for(int i = 0; i < g_center_cnt; i++) {
		if(remote_center[i].size() > 0 && i != g_center_id) {//send message to all masters
			remote_next_center_id = i;
			num_msgs_rw_prep++;
			msg_queue.enqueue(get_thd_id(),Message::create_message(this,RQRY),center_master[i]);
		}
	}
	// txn_stats.wait_for_rsp_time = get_sys_clock();
	return rc;
}

RC YCSBTxnManager::send_remote_request() {
	YCSBQuery* ycsb_query = (YCSBQuery*) query;

	uint64_t dest_part_id = _wl->key_to_part(ycsb_query->requests[next_record_id]->key);
	uint64_t dest_node_id = GET_NODE_ID(dest_part_id);
	uint64_t dest_center_id = GET_CENTER_ID(dest_node_id);
	ycsb_query->centers_touched.add_unique(dest_center_id);
	// if(ycsb_query->centers_touched.get)
	ycsb_query->partitions_touched.add_unique(GET_PART_ID(0,dest_node_id));
	if(this->center_master.find(dest_center_id) == this->center_master.end()) {
		this->center_master.insert(pair<uint64_t, uint64_t>(dest_center_id, dest_node_id));
	}
#if CENTER_MASTER == true
#if USE_RDMA == CHANGE_MSG_QUEUE
	tport_man.rdma_thd_send_msg(get_thd_id(), this->center_master[dest_center_id], Message::create_message(this,RQRY));
#else
    // DEBUG("ycsb send remote request %ld, %ld\n",txn->txn_id,txn->batch_id);
    msg_queue.enqueue(get_thd_id(),Message::create_message(this,RQRY),this->center_master[dest_center_id]);
#endif
#else
#if USE_RDMA == CHANGE_MSG_QUEUE
	tport_man.rdma_thd_send_msg(get_thd_id(), dest_node_id, Message::create_message(this,RQRY));
#else
    // DEBUG("ycsb send remote request %ld, %ld\n",txn->txn_id,txn->batch_id);
    msg_queue.enqueue(get_thd_id(),Message::create_message(this,RQRY),dest_node_id);
#endif
#endif
	return WAIT_REM;
}

void YCSBTxnManager::copy_remote_requests(YCSBQueryMessage * msg) {
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	//msg->requests.init(ycsb_query->requests.size());
    
#if PARAL_SUBTXN == true && CENTER_MASTER == true
#if USE_REPLICA
	//copy all query
    for(int i = 0; i < ycsb_query->requests.size();i++) {
		YCSBQuery::copy_request_to_msg(ycsb_query,msg,i);
	}
#else
	uint64_t remote_center_id = remote_next_center_id;
	uint64_t record_id = remote_center[remote_center_id][0];
	uint64_t index = 0;
	while(index < remote_center[remote_center_id].size()) {
		YCSBQuery::copy_request_to_msg(ycsb_query,msg,record_id);
		index++;
		record_id = remote_center[remote_center_id][index];
	}
#endif
#else
	uint64_t dest_part_id = _wl->key_to_part(ycsb_query->requests[next_record_id]->key);
	uint64_t dest_node_id = GET_NODE_ID(dest_part_id);
	uint64_t dest_center_id = GET_CENTER_ID(dest_node_id);
#if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
	while (next_record_id < ycsb_query->requests.size() && GET_NODE_ID(_wl->key_to_part(ycsb_query->requests[next_record_id]->key)) == dest_node_id) {
#else
#if CENTER_MASTER == true
    while (next_record_id < ycsb_query->requests.size() && !is_local_request(next_record_id) &&
			GET_CENTER_ID(GET_NODE_ID(_wl->key_to_part(ycsb_query->requests[next_record_id]->key))) == dest_center_id) {
#else
	while (next_record_id < ycsb_query->requests.size() && !is_local_request(next_record_id) &&
			GET_NODE_ID(_wl->key_to_part(ycsb_query->requests[next_record_id]->key)) == dest_node_id) {
#endif
#endif
		YCSBQuery::copy_request_to_msg(ycsb_query,msg,next_record_id++);
	}
#endif
}


RC YCSBTxnManager::run_txn_state(yield_func_t &yield, uint64_t cor_id) {
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	ycsb_request * req = ycsb_query->requests[next_record_id];
	uint64_t part_id = _wl->key_to_part( req->key );
	uint64_t node_id = GET_NODE_ID(part_id);
	uint64_t f1_id = GET_FOLLOWER1_NODE(part_id);
	uint64_t f2_id = GET_FOLLOWER2_NODE(part_id);
	uint32_t center_id = GET_CENTER_ID(node_id);
	uint32_t f1_center = GET_CENTER_ID(f1_id);
	uint32_t f2_center = GET_CENTER_ID(f2_id);
  	bool is_local = node_id == g_node_id;
	RC rc = RCOK;
	switch (state) {
	case YCSB_0 :
		if(is_local) {
			rc = run_ycsb_0(yield,req,row,cor_id);
		} else if (rdma_one_side() && center_id == g_center_id) {
			// printf("LOCAL CENTER: %ld:%ld:%ld:%ld in run txn    %ld:%ld\n",cor_id,get_txn_id(), req->key, next_record_id, GET_NODE_ID(part_id), g_node_id);
			rc = send_remote_one_side_request(yield, req, row, cor_id);
		} else {
			// printf("REMOTE CENTER: %ld:%ld:%ld:%ld in run txn    %ld:%ld\n",cor_id,get_txn_id(), req->key, next_record_id, GET_NODE_ID(part_id), g_node_id);
#if PARAL_SUBTXN == true && CENTER_MASTER == true
			if (waiting_for_response() ) {
				rc = WAIT_REM;
			} else {
				rc = RCOK;
			}
			
#else
			rc = send_remote_request();
#endif
		}
	  	break;
	case YCSB_1 :
		//read local row,for message queue by TCP/IP,write set has actually been written in this point,
		//but for rdma, it was written in local, the remote data will actually be written when COMMIT
		if(center_id == g_center_id){
			rc = run_ycsb_1(req->acctype,row);  
		}else{
			rc = RCOK;
		}
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
  assert(CC_ALG == CALVIN);
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


#if USE_REPLICA
RC YCSBTxnManager::redo_log(yield_func_t &yield,RC status, uint64_t cor_id) {
	if(CC_ALG == RDMA_NO_WAIT){
		assert(status != Abort);
		status = RCOK;		
	}
	
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	RC rc = RCOK;

	//for every node
	int change_cnt[g_node_cnt];
	for(int i=0;i<g_node_cnt;i++){
		change_cnt[i] = 0;
	}

	vector<vector<ChangeInfo>> change(g_node_cnt);
	

	//construct log 
	for(int i=0;i<((YCSBQuery*)query)->requests.size();i++){
		ycsb_request * req = ycsb_query->requests[i];
		if(req->acctype!=WR) continue; //log is only for write operation
		// key_to_part should be used here! but its only for YCSB workload, so manually set part_id
		// this part should be considered and changed for other workload!
		// uint64_t part_id = _wl->key_to_part( req->key );
		uint64_t part_id = req->key % g_part_cnt;
		vector<uint64_t> node_id;
		if(status == RCOK){ //validate success, log all replicas
			node_id.push_back(GET_FOLLOWER1_NODE(part_id));
			node_id.push_back(GET_FOLLOWER2_NODE(part_id));
			node_id.push_back(GET_NODE_ID(part_id));
		}
		else if(status == Abort){ //validate fail, only log the primary replicas that have been locked
#if CC_ALG == RDMA_NO_WAIT
			// int sum = 0;
			// for(int i=0;i<g_node_cnt;i++) sum += change_cnt[i];
			// if(sum>=num_locks) break;
			// node_id.push_back(GET_NODE_ID(part_id));				
#endif
		}else{
			assert(false);
		}

		for(int i=0;i<node_id.size();i++){
			uint32_t center_id = GET_CENTER_ID(node_id[i]);
			if(center_id != g_center_id) continue; //log is only for row in the same center
			++change_cnt[node_id[i]];
			ChangeInfo newChange;
			
			//fill in ChangeInfo here
			row_t* temp_row = (row_t *) mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
			//for simulation purpose, only write back metadata here
			//in actual application, data in req should also be written back
			temp_row->_tid_word = 0;
#if CC_ALG == RDMA_NO_WAIT
			uint64_t op_size = sizeof(temp_row->_tid_word);
			bool is_primary = (node_id[i] == GET_NODE_ID(part_id));
			newChange.set_change_info(req->key,op_size,(char *)temp_row,is_primary); //local 
#endif
			mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));

			change[node_id[i]].push_back(newChange);
		}
	}
#if RDMA_DBPAOR
	// int nnum = 0;
	// for(int i=0;i<g_node_cnt&&rc==RCOK;i++){
	// 	if(change_cnt[i]>0) ++nnum;
	// }
	// DBrequests dbreq(nnum);
	// dbreq.init();
	// //get remote tail
	// int count = 0;
	// for(int i=0;i<g_node_cnt&&rc==RCOK;i++){
	// 	if(change_cnt[i]>0){
	// 		int num_of_entry = 1;
    //         uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(get_thd_id(),count+1);            
	// 		uint64_t remote_off= (uint64_t)(remote_mr_attr[i].buf + rdma_buffer_size-rdma_log_size+sizeof(uint64_t));
	// 		dbreq.set_faa_meta(count,num_of_entry,local_buf,remote_off);
	// 		++count;
	// 	}
	// }
	// auto dbres = dbreq.post_reqs(rc_qp[i][thd_id]);

	//batch faa
    uint64_t starttime = get_sys_clock();
	uint64_t count = 0;
	int num_of_entry = 1; //suppose one log per node is enough
	for(int i=0;i<g_node_cnt&&rc==RCOK;i++){
		if(change_cnt[i]>0){
			//use RDMA_FAA for local and remote
			faa_remote_content(yield, i, rdma_buffer_size-rdma_log_size+sizeof(uint64_t), num_of_entry, cor_id, count+1, true);
			++count;
		}
	}
	//poll faa results
	uint64_t start_idx[g_node_cnt];
	count = 0;
	for(int i=0;i<g_node_cnt&&rc==RCOK;i++){
		if(change_cnt[i]>0){
			INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
			#if USE_COROUTINE
			assert(false); //not support yet
			#else
			auto res_p = rc_qp[i][get_thd_id()]->wait_one_comp();
			RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
			uint64_t endtime = get_sys_clock();
			INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
			DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
			INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
			#endif
    		uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(get_thd_id(),count+1);
			start_idx[i] = *local_buf;
			++count;
		}
	}
	//wait for AsyncRedoThread to clean buffer
	for(int i=0;i<g_node_cnt&&rc==RCOK;i++){
		if(change_cnt[i]>0){
			if(i == g_node_id){ //local log
				while((start_idx[i] + num_of_entry - *(redo_log_buf.get_head()) >= redo_log_buf.get_size()) && !simulation->is_done()){
				}
				if(simulation->is_done()) break;
			}else{ //remote log
				pthread_mutex_lock( LOG_HEAD_LATCH[i] );
				while(start_idx[i] + num_of_entry - log_head[i] >= redo_log_buf.get_size() && !simulation->is_done()){
					log_head[i] = read_remote_log_head(yield, i, cor_id);
				}
				pthread_mutex_unlock( LOG_HEAD_LATCH[i] );
				if(simulation->is_done()) break;
			}
		}
	}
	//write log
	count = 0;
	for(int i=0;i<g_node_cnt&&rc==RCOK;i++){
		if(change_cnt[i]>0){
			LogEntry* newEntry = (LogEntry*)mem_allocator.alloc(sizeof(LogEntry));
			newEntry->set_entry(change_cnt[i],change[i],get_start_timestamp());
			if(i == g_node_id){ //local log
				start_idx[i] = start_idx[i] % redo_log_buf.get_size();
				char* start_addr = (char*)redo_log_buf.get_entry(start_idx[i]);
				assert(((LogEntry *)start_addr)->state == EMPTY);					
				assert(((LogEntry *)start_addr)->change_cnt == 0);					
				memcpy(start_addr, (char *)newEntry, sizeof(LogEntry));
				assert(((LogEntry *)start_addr)->state == LOGGED);						
			}else{ //remote log
				start_idx[i] = start_idx[i] % redo_log_buf.get_size();
				uint64_t start_offset = redo_log_buf.get_entry_offset(start_idx[i]);
				// //for debug purpose
				// LogEntry* le= (LogEntry*)read_remote_log(yield,i,start_offset,cor_id);
				// assert(le->state == EMPTY);
				// assert(le->change_cnt == 0);
				write_remote_log(yield, i, sizeof(LogEntry), start_offset, (char *)newEntry, cor_id, count+1,true);
				//for debug purpose
				// LogEntry* le= (LogEntry*)read_remote_log(yield,i,start_offset,cor_id);
				// assert(le->state == LOGGED);
				++count;
			}
			log_idx[i] = start_idx[i];
			mem_allocator.free(newEntry, sizeof(LogEntry));
		}
	}
	//poll write results
	for(int i=0;i<g_node_cnt&&rc==RCOK;i++){
		if(change_cnt[i]>0){
			if(i != g_node_id){ //remote 
				starttime = get_sys_clock();
				INC_STATS(get_thd_id(), worker_oneside_cnt, 1);
				#if USE_COROUTINE
				assert(false); //not support yet
				#else
				auto res_p = rc_qp[i][get_thd_id()]->wait_one_comp();
				RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
				uint64_t endtime = get_sys_clock();
				INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
				INC_STATS(get_thd_id(), rdma_read_cnt, 1);
				INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
				INC_STATS(get_thd_id(), worker_waitcomp_time, endtime-starttime);
				DEL_STATS(get_thd_id(), worker_process_time, endtime-starttime);
				#endif
			}
		}
	}
#else
	//write log
	for(int i=0;i<g_node_cnt&&rc==RCOK;i++){
		if(change_cnt[i]>0){
			int num_of_entry = 1;
			//use RDMA_FAA for local and remote
			uint64_t start_idx = faa_remote_content(yield, i, rdma_buffer_size-rdma_log_size+sizeof(uint64_t), num_of_entry, cor_id);

			LogEntry* newEntry = (LogEntry*)mem_allocator.alloc(sizeof(LogEntry));

			newEntry->set_entry(change_cnt[i],change[i],get_start_timestamp());


			if(i == g_node_id){ //local log
				//consider possible overwritten:if no space, wait until cleaned 
				//for head: there is local read and write, and remote RDMA read, conflicts are ignored here, which might be problematic
				// while(start_idx + num_of_entry < *(redo_log_buf.get_head())){
				// 	//wait for head to reset
				// 	assert(false);
				// }
				while((start_idx + num_of_entry - *(redo_log_buf.get_head()) >= redo_log_buf.get_size()) && !simulation->is_done()){
					//wait for AsyncRedoThread to clean buffer
				}
				if(simulation->is_done()) break;
				
				start_idx = start_idx % redo_log_buf.get_size();

				char* start_addr = (char*)redo_log_buf.get_entry(start_idx);

				assert(((LogEntry *)start_addr)->state == EMPTY);					
				assert(((LogEntry *)start_addr)->change_cnt == 0);					

				memcpy(start_addr, (char *)newEntry, sizeof(LogEntry));
				assert(((LogEntry *)start_addr)->state == LOGGED);						
			}else{ //remote log
				//consider possible overwritten: if no space, wait until cleaned 
				//first prevent concurrent read and write among threads
				pthread_mutex_lock( LOG_HEAD_LATCH[i] );
				// while(start_idx + num_of_entry < log_head[i]){
				// 	//wait for head to reset
				// 	assert(false);
			// 	log_head[i] = read_remote_log_head(yield, i, cor_id);					
				// }
				while(start_idx + num_of_entry - log_head[i] >= redo_log_buf.get_size() && !simulation->is_done()){
					//wait for AsyncRedoThread to clean buffer
					log_head[i] = read_remote_log_head(yield, i, cor_id);
				}
				pthread_mutex_unlock( LOG_HEAD_LATCH[i] );
				if(simulation->is_done()) break;
				
				start_idx = start_idx % redo_log_buf.get_size();

				uint64_t start_offset = redo_log_buf.get_entry_offset(start_idx);
				
				// //for debug purpose
				// LogEntry* le= (LogEntry*)read_remote_log(yield,i,start_offset,cor_id);
				// assert(le->state == EMPTY);
				// assert(le->change_cnt == 0);

				write_remote_log(yield, i, sizeof(LogEntry), start_offset, (char *)newEntry, cor_id);
				//for debug purpose
				// LogEntry* le= (LogEntry*)read_remote_log(yield,i,start_offset,cor_id);
				// assert(le->state == LOGGED);

			}
			log_idx[i] = start_idx;
			mem_allocator.free(newEntry, sizeof(LogEntry));
		}
	}
#endif
	return rc;
}
#endif