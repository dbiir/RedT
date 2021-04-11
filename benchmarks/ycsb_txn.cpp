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
	if(IS_LOCAL(get_txn_id())) {  //对rdma单边，一定是local
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
	  if(!IS_LOCAL(txn->txn_id) || !is_done()) {
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

itemid_t* YCSBTxnManager::read_remote_index(ycsb_request * req) {
	uint64_t part_id = _wl->key_to_part( req->key );
  	uint64_t loc = GET_NODE_ID(part_id);
	assert(loc != g_node_id);
	uint64_t thd_id = get_thd_id();

	// todo: 这里需要获取对应的索引
  // uint64_t index_key = 0;
	uint64_t index_key = req->key / g_node_cnt;
	uint64_t index_addr = (index_key) * sizeof(IndexInfo);
	uint64_t index_size = sizeof(IndexInfo);
  // 每个线程只用自己的那一块客户端内存地址
	char *test_buf = Rdma::get_index_client_memory(thd_id);
    memset(test_buf, 0, index_size);

//async rdma read
	// ::r2::rdma::SROp op;
  //   op.set_payload(test_buf,index_size).set_remote_addr(index_addr).set_read();

	// bool runned = false;
  //   r2::SScheduler ssched;

	// ssched.spawn([test_buf, &op, &runned,&loc,&thd_id](R2_ASYNC) {
	// 	op.set_read();
	// 	auto ret = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED, R2_ASYNC_WAIT);
	// 	R2_YIELD;
	// 	ASSERT(ret == IOCode::Ok);
	// 	runned = true;
	// 	R2_STOP();
	// 	R2_RET;
  //   });
	// ssched.run();
	// ASSERT(runned == true);

	// ssched.exit();
//end asyn
    uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
	auto res_s = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = index_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = index_addr,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();

	INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
    INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	itemid_t* item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
	assert(((IndexInfo*)test_buf)->key == req->key);
	item->location = ((IndexInfo*)test_buf)->address;
	item->type = ((IndexInfo*)test_buf)->type;
	item->valid = ((IndexInfo*)test_buf)->valid;
	item->offset = ((IndexInfo*)test_buf)->offset;
  	item->table_offset = ((IndexInfo*)test_buf)->table_offset;
	return item;
}

RC YCSBTxnManager::send_remote_one_side_request(ycsb_request * req,row_t *& row_local) {
	// get the index of row to be operated
	itemid_t * m_item;
	m_item = read_remote_index(req);
	
	uint64_t part_id = _wl->key_to_part( req->key );
    uint64_t loc = GET_NODE_ID(part_id);
	assert(loc != g_node_id);
	uint64_t thd_id = get_thd_id();

	if(req->acctype == RD || req->acctype == WR){
		uint64_t starttime;
		uint64_t endtime;
		starttime = get_sys_clock();
		uint64_t tts = get_timestamp();

#if CC_ALG == RDMA_WAIT_DIE2
		//直接RDMA CAS加锁
retry_lock:
		uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
		auto mr = client_rm_handler->get_reg_attr().value();

		rdmaio::qp::Op<> op;
		op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + m_item->offset), remote_mr_attr[loc].key).set_cas(0,tts);
		assert(op.set_payload(tmp_buf2, sizeof(uint64_t), mr.key) == true);
		auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

		RDMA_ASSERT(res_s2 == IOCode::Ok);
		auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
		RDMA_ASSERT(res_p2 == IOCode::Ok);

		if(*tmp_buf2 != 0){ //如果CAS失败
			if(tts <= *tmp_buf2){  //wait
#if DEBUG_PRINTF
				printf("---retry_lock\n");
#endif
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

				uint64_t timespan = get_sys_clock() - starttime;
				INC_STATS(get_thd_id(), trans_store_access_count, 1);
				INC_STATS(get_thd_id(), txn_manager_time, timespan);
				INC_STATS(get_thd_id(), txn_conflict_cnt, 1);
				return Abort;
			}
		}
#endif
#if CC_ALG == RDMA_NO_WAIT2
		//直接RDMA CAS加锁
		uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
		auto mr = client_rm_handler->get_reg_attr().value();

		rdmaio::qp::Op<> op;
		op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + m_item->offset), remote_mr_attr[loc].key).set_cas(0,1);
		assert(op.set_payload(tmp_buf2, sizeof(uint64_t), mr.key) == true);
		auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

		RDMA_ASSERT(res_s2 == IOCode::Ok);
		auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
		RDMA_ASSERT(res_p2 == IOCode::Ok);

		if(*tmp_buf2 != 0){ //如果CAS失败
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			row_local = NULL;
			txn->rc = Abort;
			mem_allocator.free(m_item, sizeof(itemid_t));

			uint64_t timespan = get_sys_clock() - starttime;
			INC_STATS(get_thd_id(), trans_store_access_count, 1);
			INC_STATS(get_thd_id(), txn_manager_time, timespan);
			INC_STATS(get_thd_id(), txn_conflict_cnt, 1);
			return Abort;
		}
#if DEBUG_PRINTF
		printf("---线程号：%lu, 远程加锁成功，锁位置: %lu; %p, 事务号: %lu,原lock_info: 0, new_lock_info: 1\n", get_thd_id(), loc, remote_mr_attr[loc].buf + m_item->offset, get_txn_id());
#endif
#endif
#if CC_ALG == RDMA_NO_WAIT 
		uint64_t new_lock_info;
		uint64_t lock_info;
remote_atomic_retry_lock:
		if(req->acctype == RD){ //读集元素
			//第一次rdma read，得到数据项的锁信息
			uint64_t *tmp_buf1 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
			auto res_s1 = rc_qp[loc][thd_id]->send_normal(
				{.op = IBV_WR_RDMA_READ,
				.flags = IBV_SEND_SIGNALED,
				.len = sizeof(uint64_t),
				.wr_id = 0},
			{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(tmp_buf1),
				.remote_addr = m_item->offset,
				.imm_data = 0});
			RDMA_ASSERT(res_s1 == rdmaio::IOCode::Ok);
			auto res_p1 = rc_qp[loc][thd_id]->wait_one_comp();
			RDMA_ASSERT(res_p1 == rdmaio::IOCode::Ok);

			new_lock_info = 0;
			lock_info = *tmp_buf1;

			bool conflict = Row_rdma_2pl::conflict_lock(lock_info, DLOCK_SH, new_lock_info);

			if(conflict){
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));

				uint64_t timespan = get_sys_clock() - starttime;
				INC_STATS(get_thd_id(), trans_store_access_count, 1);
				INC_STATS(get_thd_id(), txn_manager_time, timespan);
				INC_STATS(get_thd_id(), txn_conflict_cnt, 1);
				return Abort;
			}
			if(new_lock_info == 0){
				printf("---线程号：%lu, 远程加锁失败!!!!!!锁位置: %lu; %p, 事务号: %lu,原lock_info: %lu, new_lock_info: %lu\n", get_thd_id(), loc, remote_mr_attr[loc].buf + m_item->offset, get_txn_id(), lock_info, new_lock_info);
			} 
			assert(new_lock_info!=0);
		}
		else{ //写集元素直接CAS即可，不需要RDMA READ
			lock_info = 0; //只有lock_info==0时才可以CAS，否则加写锁失败，Abort
			new_lock_info = 3; //二进制11，即1个写锁
		}
		//RDMA CAS加锁
		uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
		auto mr = client_rm_handler->get_reg_attr().value();

		rdmaio::qp::Op<> op;
		op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + m_item->offset), remote_mr_attr[loc].key).set_cas(lock_info,new_lock_info);
		assert(op.set_payload(tmp_buf2, sizeof(uint64_t), mr.key) == true);
		auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

		RDMA_ASSERT(res_s2 == IOCode::Ok);
		auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
		RDMA_ASSERT(res_p2 == IOCode::Ok);

		if(*tmp_buf2 != lock_info && req->acctype == RD){ //如果CAS失败:对读集元素来说，即原子性被破坏
#if DEBUG_PRINTF
			printf("---remote_atomic_retry_lock\n");
#endif
			num_atomic_retry++;
			total_num_atomic_retry++;
			if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;
			goto remote_atomic_retry_lock;
/*
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			row_local = NULL;
			txn->rc = Abort;
			mem_allocator.free(m_item, sizeof(itemid_t));

			uint64_t timespan = get_sys_clock() - starttime;
			INC_STATS(get_thd_id(), trans_store_access_count, 1);
			INC_STATS(get_thd_id(), txn_manager_time, timespan);
			INC_STATS(get_thd_id(), txn_conflict_cnt, 1);
			return Abort; //原子性被破坏，CAS失败			
*/
		}	
		if(*tmp_buf2 != lock_info && req->acctype == WR){ //如果CAS失败:对写集元素来说,即已经有锁;
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			row_local = NULL;
			txn->rc = Abort;
			mem_allocator.free(m_item, sizeof(itemid_t));

			uint64_t timespan = get_sys_clock() - starttime;
			INC_STATS(get_thd_id(), trans_store_access_count, 1);
			INC_STATS(get_thd_id(), txn_manager_time, timespan);
			INC_STATS(get_thd_id(), txn_conflict_cnt, 1);
			return Abort; //原子性被破坏，CAS失败			
		}	
#if DEBUG_PRINTF
		printf("---线程号：%lu, 远程加锁成功，锁位置: %lu; %p, 事务号: %lu,原lock_info: %lu, new_lock_info: %lu\n", get_thd_id(), loc, remote_mr_attr[loc].buf + m_item->offset, get_txn_id(), lock_info, new_lock_info);
#endif
#endif
		
		//读数据
		row_t *tmp_buf = (row_t *)Rdma::get_row_client_memory(thd_id);
		uint64_t operate_size = sizeof(row_t);
		auto res_s = rc_qp[loc][thd_id]->send_normal(
			{.op = IBV_WR_RDMA_READ,
			.flags = IBV_SEND_SIGNALED,
			.len = operate_size,
			.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(tmp_buf),
			.remote_addr = m_item->offset,
			.imm_data = 0});
		RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
		auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
		RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

		endtime = get_sys_clock();
		INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
		INC_STATS(get_thd_id(), rdma_read_cnt, 1);

		row_t *test_row = (row_t *)mem_allocator.alloc(sizeof(row_t));			
		memcpy(test_row, tmp_buf, operate_size);

		//preserve the txn->access
		Access * access = NULL;
		access_pool.get(get_thd_id(),access);
		
		RC rc = RCOK;
		rc = row->remote_get_row(test_row, this, access);
    	assert(test_row->get_primary_key() == access->data->get_primary_key());
		access->type = req->acctype;
		//注意：不同于本地, 远程写的情况也不用存orig_data, Abort时写回的时候不动数据即可
#if CC_ALG == RDMA_SILO
		//与之后再读一遍的结果比较，要求相同
  		access->orig_row = test_row;

		access->key = req->key;
		access->tid = last_tid;
		access->timestamp = test_row->timestamp;
		access->location = loc;
		access->offset = m_item->offset;
#endif
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2
		//要求orig_row和data相同，故abort时return_row中把orig_data返回orig_row等价于返回data
		//实际上，远程时不return_row,故此orig_row实际上没用上
  		access->orig_row = access->data;
		
		access->location = loc;
		access->offset = m_item->offset;
#endif


    	row_local = access->data;
		this->last_row = test_row;
		this->last_type = req->acctype;

		++txn->row_cnt;
		if (req->acctype == WR) ++txn->write_cnt;
		txn->accesses.add(access);

		mem_allocator.free(test_row, sizeof(row_t));
 	    mem_allocator.free(m_item, sizeof(itemid_t));		
	}
	RC rc = RCOK;
	return rc;
}

RC YCSBTxnManager::send_remote_request() {
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
	ycsb_query->partitions_touched.add_unique(GET_PART_ID(0,dest_node_id));
#if USE_RDMA == CHANGE_MSG_QUEUE
	tport_man.rdma_thd_send_msg(get_thd_id(), dest_node_id, Message::create_message(this,RQRY));
#else
	msg_queue.enqueue(get_thd_id(),Message::create_message(this,RQRY),dest_node_id);
#endif

	return WAIT_REM;
}

void YCSBTxnManager::copy_remote_requests(YCSBQueryMessage * msg) {
	YCSBQuery* ycsb_query = (YCSBQuery*) query;
	//msg->requests.init(ycsb_query->requests.size());
	uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
	while (next_record_id < ycsb_query->requests.size() && !is_local_request(next_record_id) &&
			GET_NODE_ID(ycsb_query->requests[next_record_id]->key) == dest_node_id) {
		YCSBQuery::copy_request_to_msg(ycsb_query,msg,next_record_id++);
	}
}

bool YCSBTxnManager::rdma_one_side() {
  if (CC_ALG == RDMA_SILO || CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2) return true;
  else return false;
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
			rc = send_remote_one_side_request(req,row); //读回来的数据放在row里面
		}
		else {
			rc = send_remote_request();
		}
	  break;
	case YCSB_1 :
		//读写本地数据row，对于TCP/IP消息队列方式来说，在这个时候写集已经真实写入
		//但是对于rdma，是在本地写入，对于远程数据commit时才真实写回
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

