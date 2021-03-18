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
#include "rdma_mvcc.h"
#include "row_rdma_mvcc.h"
#include "mem_alloc.h"
#include "query.h"
#include "msg_queue.h"
#include "message.h"
#include "src/rdma/sop.hh"
#include "qps/op.hh"
#include "src/sshed.hh"
#include "transport.h"

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
	if(IS_LOCAL(get_txn_id())) {
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
  // table_t * table = read_remote_table(m_item);

	uint64_t part_id = _wl->key_to_part( req->key );
    uint64_t loc = GET_NODE_ID(part_id);
	assert(loc != g_node_id);
	uint64_t thd_id = get_thd_id();
	uint64_t operate_size = sizeof(row_t);
	char *tmp_buf = Rdma::get_row_client_memory(thd_id);
	//read request
	if(req->acctype == RD || req->acctype == WR){
		//async get remote row
		// ::r2::rdma::SROp op;
        // op.set_payload(tmp_buf,operate_size).set_remote_addr(m_item->offset).set_read();

		// bool runned = false;
    	// r2::SScheduler ssched;

		// ssched.spawn([&op, &runned,&loc,&thd_id](R2_ASYNC) {
		// 	auto ret = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED, R2_ASYNC_WAIT);
		// 	R2_YIELD;
		// 	ASSERT(ret == IOCode::Ok);
		// 	runned = true;
		// 	R2_STOP();
		// 	R2_RET;
		// });
		// ssched.run();
		// ASSERT(runned == true);

	//	get remote row
	    uint64_t starttime;
		uint64_t endtime;
		starttime = get_sys_clock();
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

		row_t *temp_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
		memcpy(temp_row, tmp_buf, operate_size);
		assert(temp_row->get_primary_key() == req->key);


    	row_t *test_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
    	memcpy(test_row, tmp_buf, operate_size);

    	//preserve the txn->access
		Access * access = NULL;
		access_pool.get(get_thd_id(),access);

		this->last_row = test_row;
		this->last_type = req->acctype;


    	RC rc = RCOK;
		rc = row->remote_get_row(test_row, this, access);
    	assert(test_row->get_primary_key() == access->data->get_primary_key());

		if (rc == Abort || rc == WAIT) {
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			access_pool.put(get_thd_id(),access);
			return rc;
    	}

		access->type = req->acctype;
		// access->orig_row = temp_row;

		access->orig_row = test_row;
#if CC_ALG == RDMA_SILO
		access->key = req->key;
		access->tid = last_tid;
		access->timestamp = test_row->timestamp;

		access->location = loc;
		access->offset = m_item->offset;
#endif
    	row_local = access->data;
		++txn->row_cnt;

    	mem_allocator.free(m_item,0);

		if (req->acctype == WR) ++txn->write_cnt;
		txn->accesses.add(access);
	}
	RC rc = RCOK;
	return rc;
}
//#endif

#if CC_ALG == RDMA_MVCC
RC YCSBTxnManager::mvcc_remote_one_side_request(ycsb_request * req,row_t *& row_local){
    RC rc = RCOK;
    bool write_back = false;
//get location in memory
    itemid_t * m_item;
	m_item = read_remote_index(req);
//lock
    uint64_t off = m_item->offset;
    uint64_t part_id = _wl->key_to_part( req->key );
    uint64_t loc = GET_NODE_ID(part_id);
	assert(loc != g_node_id);
	uint64_t thd_id = get_thd_id();
	uint64_t lock = get_txn_id();
    uint64_t operate_size = sizeof(row_t);

	uint64_t *test_loc = (uint64_t *)Rdma::get_row_client_memory(thd_id);
    *test_loc = -1;
	auto mr = client_rm_handler->get_reg_attr().value();

    rdmaio::qp::Op<> op;
    int num = 0;

   do{
       // if(*(test_loc)!=-1)printf("row lock = %ld\n",*test_loc);
        op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + off), remote_mr_attr[loc].key).set_cas(0, lock);
        assert(op.set_payload(test_loc, sizeof(uint64_t), mr.key) == true);
        auto res_s2 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

        RDMA_ASSERT(res_s2 == IOCode::Ok);
        auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
        RDMA_ASSERT(res_p2 == IOCode::Ok);
        // INC_STATS(get_thd_id(), cas_cnt, 1);
     }while((*test_loc) != 0);

//lock fail
    if((*test_loc) != 0){
       INC_STATS(get_thd_id(), lock_fail, 1);
       //printf("row lock = %ld\n",*test_loc);
       rc = Abort;
       return rc;
    }

//get row

    char *tmp_buf = Rdma::get_row_client_memory(thd_id);
    auto res_s = rc_qp[loc][thd_id]->send_normal(
			{.op = IBV_WR_RDMA_READ,
			.flags = IBV_SEND_SIGNALED,
			.len = operate_size,
			.wr_id = 0},
		   {.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(tmp_buf),
			.remote_addr = off,
			.imm_data = 0});
    RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
    auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
    RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

 //store in temp_row
 
    row_t *temp_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
	memcpy(temp_row, tmp_buf, operate_size);
	assert(temp_row->get_primary_key() == req->key);

//rdma_mvcc.cpp_access()
    //rc = get_row();
    if(req->acctype == WR){
    
       //检查最新版本的txn-id不为0且不等于当前事务或时间戳小于rts，当前事务回滚。
       uint64_t version = (temp_row->version_num)%HIS_CHAIN_NUM;
    //    if((temp_row->txn_id[version] != 0 && temp_row->txn_id[version] != get_txn_id())
    //         || txn->txn->timestamp > temp_row->rts[version] ){
    //         INC_STATS(get_thd_id(), ts_error, 1);
    //         printf("【ycsb_txn:401】version_num = %ld , txn_id = %ld %ld %ld %ld \n",temp_row->version_num, temp_row->txn_id[0],temp_row->txn_id[1],temp_row->txn_id[2],temp_row->txn_id[3]);
    //         rc = Abort;
    //    }
       if(temp_row->txn_id[version] != 0 && temp_row->txn_id[version] != get_txn_id() ){
            INC_STATS(get_thd_id(), ts_error, 1);
            // printf("【ycsb_txn:408】version_num = %ld , txn_id = %ld %ld %ld %ld \n",temp_row->version_num, temp_row->txn_id[0],temp_row->txn_id[1],temp_row->txn_id[2],temp_row->txn_id[3]);
            rc = Abort;
       }
       else 
       if(txn->timestamp <= temp_row->rts[version]){
           INC_STATS(get_thd_id(), ts_error, 1);
          // printf("【ycsb_txn:412】txn->timestamp = %ld temp_row->rts[%ld] = %ld \n",txn->timestamp,version,temp_row->rts[version]);
           rc = Abort;
       }
       else{
            //其他情况通过单边写修改版本的txn-id为当前事务id，RTS并解锁
           temp_row->txn_id[version] = get_txn_id();
           temp_row->rts[version] = txn->timestamp;
           //temp_row->version_num = temp_row->version_num + 1;
           temp_row->_tid_word = 0;//解锁

           write_back = true;

           char *test_buf = Rdma::get_row_client_memory(thd_id);
           memcpy(test_buf, (char*)temp_row, operate_size);

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


            //将要写数据存入写集
       }
       
    }else if(req->acctype == RD){

        //查找合适版本逻辑不正确

        //若版本的txn-id不为0且不等于当前事务，则当前事务回滚
        uint64_t i;
        uint64_t check_num = temp_row->version_num < HIS_CHAIN_NUM ?  temp_row->version_num : HIS_CHAIN_NUM;
        uint64_t change_num = 0;
        bool result = false;
        if(temp_row->version_num < HIS_CHAIN_NUM){
            for(i = 0;i <= check_num ; i++){
                if((temp_row->start_ts[i] < txn->timestamp || temp_row->start_ts[i] == 0 ) && (temp_row->end_ts[i] > txn->timestamp || temp_row->end_ts[i] == UINT64_MAX)){
                    result = true;
                    change_num = i;
                    break;
                }
            }
        }else{
            for( i = 0 ; i < HIS_CHAIN_NUM ; i++){
                uint64_t j = 0;
                j = (temp_row->version_num + i)%HIS_CHAIN_NUM;
                if((temp_row->start_ts[j] < txn->timestamp || temp_row->start_ts[j] == 0) && (temp_row->end_ts[j] > txn->timestamp || temp_row->end_ts[i] == UINT64_MAX)){
                    result = true;
                    change_num = j;
                    break;
                }
            }
        }
 
        if(result == false){//无合适版本
           INC_STATS(get_thd_id(), result_false, 1);
          // printf("【ycsb_txn:482】version_num = %ld , txn_id = %ld %ld %ld %ld \n",temp_row->version_num, temp_row->txn_id[0],temp_row->txn_id[1],temp_row->txn_id[2],temp_row->txn_id[3]);
           rc = Abort;
        }
        else{//check txn_id
            if(temp_row->txn_id[change_num] != 0 && temp_row->txn_id[change_num] != get_txn_id()){
                 INC_STATS(get_thd_id(), result_false, 1);
               //  printf("【ycsb_txn:485】change_num = %ld , txn_id = %ld %ld %ld %ld \n",change_num, temp_row->txn_id[0],temp_row->txn_id[1],temp_row->txn_id[2],temp_row->txn_id[3]);
                 rc = Abort;//版本不符合条件
            }
            else{
                //其他情况单边写修改版本的RTS并解锁
                //uint64_t version = (temp_row->version_num)%HIS_CHAIN_NUM;
                uint64_t version = change_num;
                temp_row->rts[version] = txn->timestamp;
                temp_row->_tid_word = 0;
                write_back = true;

                char *test_buf = Rdma::get_row_client_memory(thd_id);
                memcpy(test_buf, (char*)temp_row, operate_size);

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
                
                //将读到数据存入读集
            }
        }

    }
    

    //rlease lock
    if(write_back == false){
        op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + off), remote_mr_attr[loc].key).set_cas(lock,0);
        assert(op.set_payload(test_loc, sizeof(uint64_t), mr.key) == true);
        auto res_s5 = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);

        RDMA_ASSERT(res_s5 == IOCode::Ok);
        auto res_p5 = rc_qp[loc][thd_id]->wait_one_comp();
        RDMA_ASSERT(res_p5 == IOCode::Ok);
    }

//preserve the txn->access

	Access * access = NULL;
	access_pool.get(get_thd_id(),access);

	this->last_row = temp_row;
	this->last_type = req->acctype;

    RC rc2 = RCOK;
	rc2 = row->remote_get_row(temp_row, this, access);
    assert(temp_row->get_primary_key() == access->data->get_primary_key());

    if (rc == Abort || rc == WAIT) {
        DEBUG_M("TxnManager::get_row(abort) access free\n");
        access_pool.put(get_thd_id(),access);
        return rc;
    }

	access->type = req->acctype;
	access->orig_row = temp_row;
    access->key = req->key;
    //access->tid = last_tid;
    //access->timestamp = temp_row->timestamp;
    access->location = loc;
    access->offset = m_item->offset;
    access->old_version_num = temp_row->version_num;//记录写的时候通过txn_id加锁的版本
    row_local = access->data;
    ++txn->row_cnt;

    mem_allocator.free(m_item,0);

	if (req->acctype == WR) ++txn->write_cnt;
	txn->accesses.add(access);

    return rc;
}

#endif
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

bool YCSBTxnManager::rdma_one_side() {
  if (CC_ALG == RDMA_SILO) return true;
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
			rc = send_remote_one_side_request(req,row);
		}else if(CC_ALG == RDMA_MVCC){
			rc = mvcc_remote_one_side_request(req,row);
		}else {
			rc = send_remote_request();
		}

	  break;
	case YCSB_1 :
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
	fval = *(uint64_t *)(&data[fid * 100]);
#if ISOLATION_LEVEL == READ_COMMITTED || ISOLATION_LEVEL == READ_UNCOMMITTED
	// Release lock after read
	release_last_row_lock();
#endif

  } else {
	assert(acctype == WR);
		int fid = 0;
	  char * data = row_local->get_data();
	  *(uint64_t *)(&data[fid * 100]) = 0;
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

