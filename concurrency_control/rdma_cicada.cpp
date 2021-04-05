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
#include "helper.h"
#include "txn.h"
#include "rdma_cicada.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_rdma_cicada.h"
#include "rdma.h"
//#include "src/allocator_master.hh"
#include "qps/op.hh"
#include "string.h"

#if CC_ALG == RDMA_CICADA

void RDMA_Cicada::init() { sem_init(&_semaphore, 0, 1); }

RC RDMA_Cicada::validate(TxnManager * txnMng) {
	uint64_t start_time = get_sys_clock();
	uint64_t timespan;
	Transaction *txn = txnMng->txn;
	sem_wait(&_semaphore);
	int read_set[txn->row_cnt - txn->write_cnt];
	int cur_rd_idx = 0;
    int cur_wr_idx = 0;
	uint64_t wr_cnt = txn->write_cnt;
	//对写集按照wts降序排序
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[cur_wr_idx ++] = rid;
		else
			read_set[cur_rd_idx ++] = rid;
	}
	if (wr_cnt > 1)
	{
		for (uint64_t i = wr_cnt - 1; i >= 1; i--) {
			for (uint64_t j = 0; j < i; j++) {
				if (txn->accesses[ txnMng->write_set[j] ]->orig_row->cicada_version[txnMng->version_num[txnMng->write_set[j] ]].Wts <
					txn->accesses[ txnMng->write_set[j + 1] ]->orig_row->cicada_version[txnMng->version_num[txnMng->write_set[j] ]].Wts)
				{
					int tmp = txnMng->write_set[j];
					txnMng->write_set[j] = txnMng->write_set[j+1];
					txnMng->write_set[j+1] = tmp;
				}
			}
		}
	}

	timespan = get_sys_clock() - start_time;
	txnMng->txn_stats.cc_block_time += timespan;
	txnMng->txn_stats.cc_block_time_short += timespan;
	start_time = get_sys_clock();
	RC rc = RCOK;

	//2.	预检查版本一致性，对写集中的每一项Xi
	for (uint64_t i = 0; i < txn->write_cnt; i++) {
        //local
		//row_t *temp_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
        if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ txnMng->write_set[i] ];
			if (access->orig_row->)
        }else{
        //remote
            Access * access = txn->accesses[ txnMng->write_set[i] ];
            remote_write_and_unlock(rc, txnMng, txnMng->write_set[i]);
        }
    }

	timespan = get_sys_clock() - start_time;
	txnMng->txn_stats.cc_time += timespan;
	txnMng->txn_stats.cc_time_short += timespan;
	DEBUG("RDMA_CICADA Validate End %ld: %d\n",txnMng->get_txn_id(),rc==RCOK);
	//printf("MAAT Validate End %ld: %d [%lu,%lu]\n",txn->get_txn_id(),rc==RCOK,lower,upper);
	sem_post(&_semaphore);
	return rc;

}


// RC
// RDMA_Cicada::finish(RC rc , TxnManager * txnMng)
// {
// 	Transaction *txn = txnMng->txn;
// 	//abort
// 	// int read_set[txn->row_cnt - txn->write_cnt];
// 	// int cur_rd_idx = 0;
//     // int cur_wr_idx = 0;
// 	// for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
// 	// 	if (txn->accesses[rid]->type == WR)
// 	// 		txnMng->write_set[cur_wr_idx ++] = rid;
// 	// 	else
// 	// 		read_set[cur_rd_idx ++] = rid;
// 	// }
// 	if (rc == Abort) {
// 		//if (txnMng->num_locks > txnMng->get_access_cnt())
// 		//	return rc;
// 		for (uint64_t i = 0; i < txnMng->get_access_cnt(); i++) {
// 			//local
// 			if(txn->accesses[i]->location == g_node_id){
// 				rc = txn->accesses[i]->orig_row->manager->abort(txn->accesses[i]->type,txnMng);
// 			} else{
// 			//remote
//         		//release_remote_lock(txnMng,txnMng->write_set[i] );
				
// 				rc = remote_abort(txnMng, txn->accesses[i]);

// 			}
// 			// DEBUG("silo %ld abort release row %ld \n", txnMng->get_txn_id(), txn->accesses[ txnMng->write_set[i] ]->orig_row->get_primary_key());
// 		}
// 	} else {
// 	//commit
// 		ts_t time = get_sys_clock();
// 		for (uint64_t i = 0; i < txnMng->get_access_cnt(); i++) {
// 			//local
// 			if(txn->accesses[i]->location == g_node_id){
// 				Access * access = txn->accesses[i];
				
// 				//access->orig_row->manager->write( access->data);
// 				rc = txn->accesses[i]->orig_row->manager->commit(txn->accesses[i]->type, txnMng, access->data);
// 			}else{
// 			//remote
// 				Access * access = txn->accesses[i];
// 				//DEBUG("commit access txn %ld, off: %ld loc: %ld and key: %ld\n",txnMng->get_txn_id(), access->offset, access->location, access->data->get_primary_key());
// 				rc =  remote_commit(txnMng, txn->accesses[i]);
// 				//release_remote_lock(txnMng,txnMng->write_set[i] );//unlock
// 			}
// 			// DEBUG("silo %ld abort release row %ld \n", txnMng->get_txn_id(), txn->accesses[ txnMng->write_set[i] ]->orig_row->get_primary_key());
// 		}
// 	}
// 	for (uint64_t i = 0; i < txn->row_cnt; i++) {
// 		//local
// 		if(txn->accesses[i]->location != g_node_id){
// 		//remote
// 		mem_allocator.free(txn->accesses[i]->data,0);
// 		mem_allocator.free(txn->accesses[i]->orig_row,0);
// 		// mem_allocator.free(txn->accesses[i]->test_row,0);
// 		txn->accesses[i]->data = NULL;
// 		txn->accesses[i]->orig_row = NULL;
// 		txn->accesses[i]->orig_data = NULL;
// 		txn->accesses[i]->version = 0;
// 		txn->accesses[i]->offset = 0;
// 		}
// 	}
// 	//memset(txnMng->write_set, 0, 100);
// 	return rc;
// }
// RC RDMA_Cicada::remote_abort(TxnManager * txnMng, Access * data) {
// 	uint64_t mtx_wait_starttime = get_sys_clock();
// 	INC_STATS(txnMng->get_thd_id(),mtx[32],get_sys_clock() - mtx_wait_starttime);
// 	DEBUG("Maat Abort %ld: %d -- %ld\n",txnMng->get_txn_id(), data->type, data->data->get_primary_key());
// 	Transaction * txn = txnMng->txn;
// 	uint64_t off = data->offset;
// 	uint64_t loc = data->location;
// 	uint64_t thd_id = txnMng->get_thd_id();
// 	uint64_t lock = txnMng->get_txn_id();
// 	uint64_t operate_size = sizeof(row_t);
// 	uint64_t starttime = get_sys_clock();
// 	uint64_t endtime;
// 	uint64_t *tmp_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
// 	auto mr = client_rm_handler->get_reg_attr().value();

// 	rdmaio::qp::Op<> op;
// 	op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + off), remote_mr_attr[loc].key).set_cas(0, lock);
// 	assert(op.set_payload(tmp_buf, sizeof(uint64_t), mr.key) == true);
// 	auto res_s = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);
// 	RDMA_ASSERT(res_s == IOCode::Ok);
// 	auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
// 	RDMA_ASSERT(res_p == IOCode::Ok);
// 	char *tmp_buf2 = Rdma::get_row_client_memory(thd_id);
// 	auto res_s2 = rc_qp[loc][thd_id]->send_normal(
// 		{.op = IBV_WR_RDMA_READ,
// 		.flags = IBV_SEND_SIGNALED,
// 		.len = operate_size,
// 		.wr_id = 0},
// 		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(tmp_buf2),
// 		.remote_addr = off,
// 		.imm_data = 0});
// 	RDMA_ASSERT(res_s2 == rdmaio::IOCode::Ok);
//     auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
//     RDMA_ASSERT(res_p2 == rdmaio::IOCode::Ok);
// 	row_t *temp_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
// 	memcpy(temp_row, tmp_buf2, operate_size);
// 	assert(temp_row->get_primary_key() == data->data->get_primary_key());
// 	if(data->type == RD) {
// 		//uncommitted_reads->erase(txn->get_txn_id());
// 		//ucread_erase(txn->get_txn_id());
// 		for(uint64_t i = 0; i < row_set_length; i++) {
// 			uint64_t last = temp_row->uncommitted_reads[i];
// 			assert(i < row_set_length - 1);
// 			if(last == 0 || txnMng->get_txn_id() == 0) break;
// 			if(last == txnMng->get_txn_id()) {
// 				if(temp_row->uncommitted_reads[i+1] == 0) {
// 					temp_row->uncommitted_reads[i] = 0;
// 					break;
// 				}
// 				for(uint64_t j = i; j < row_set_length; j++) {
// 					if(temp_row->uncommitted_reads[j+1] == 0) break;
// 					temp_row->uncommitted_reads[j] = temp_row->uncommitted_reads[j+1];
// 				}
// 			}
// 		}
// 		temp_row->_tid_word = 0;
// 		memcpy(tmp_buf2, (char *)temp_row, operate_size);
// 		auto res_s3 = rc_qp[loc][thd_id]->send_normal(
// 			{.op = IBV_WR_RDMA_WRITE,
// 			.flags = IBV_SEND_SIGNALED,
// 			.len = operate_size,
// 			.wr_id = 0},
// 			{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(tmp_buf2),
// 			.remote_addr = off,
// 			.imm_data = 0});
// 		RDMA_ASSERT(res_s3 == rdmaio::IOCode::Ok);
// 		auto res_p3 = rc_qp[loc][thd_id]->wait_one_comp();
// 		RDMA_ASSERT(res_p3 == rdmaio::IOCode::Ok);
// 	}

// 	if(data->type == WR) {
// 		for(uint64_t i = 0; i < row_set_length; i++) {
// 			uint64_t last = temp_row->uncommitted_writes[i];
// 			assert(i < row_set_length - 1);
// 			if(last == 0) break;
// 			if(last == txnMng->get_txn_id()) {
// 				if(temp_row->uncommitted_writes[i+1] == 0) {
// 					temp_row->uncommitted_writes[i] = 0;
// 					break;
// 				}
// 				for(uint64_t j = i; j < row_set_length; j++) {
// 					temp_row->uncommitted_writes[j] = temp_row->uncommitted_writes[j+1];
// 					if(temp_row->uncommitted_writes[j+1] == 0) break;
// 				}
// 			}
// 		}
// 		temp_row->_tid_word = 0;
// 		memcpy(tmp_buf2, (char *)temp_row, operate_size);
// 		auto res_s3 = rc_qp[loc][thd_id]->send_normal(
// 			{.op = IBV_WR_RDMA_WRITE,
// 			.flags = IBV_SEND_SIGNALED,
// 			.len = operate_size,
// 			.wr_id = 0},
// 			{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(tmp_buf2),
// 			.remote_addr = off,
// 			.imm_data = 0});
// 		RDMA_ASSERT(res_s3 == rdmaio::IOCode::Ok);
// 		auto res_p3 = rc_qp[loc][thd_id]->wait_one_comp();
// 		RDMA_ASSERT(res_p3 == rdmaio::IOCode::Ok);
// 		//uncommitted_writes->erase(txn->get_txn_id());
// 	}
// 	mem_allocator.free(temp_row, sizeof(row_t));
// 	return Abort;
// }
// RC RDMA_Cicada::remote_commit(TxnManager * txnMng, Access * data) {
// 	uint64_t mtx_wait_starttime = get_sys_clock();
// 	INC_STATS(txnMng->get_thd_id(),mtx[33],get_sys_clock() - mtx_wait_starttime);
// 	DEBUG("Maat Commit %ld: %d,%lu -- %ld\n", txnMng->get_txn_id(), data->type, txnMng->get_commit_timestamp(),
// 			data->data->get_primary_key());
// 	Transaction * txn = txnMng->txn;
// 	uint64_t off = data->offset;
// 	uint64_t loc = data->location;
// 	uint64_t thd_id = txnMng->get_thd_id();
// 	uint64_t lock = txnMng->get_txn_id();
// 	uint64_t operate_size = sizeof(row_t);
// 	uint64_t starttime = get_sys_clock();
// 	uint64_t endtime;
// 	uint64_t *tmp_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
// 	auto mr = client_rm_handler->get_reg_attr().value();

// 	rdmaio::qp::Op<> op;
// 	op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[loc].buf + off), remote_mr_attr[loc].key).set_cas(0, lock);
// 	assert(op.set_payload(tmp_buf, sizeof(uint64_t), mr.key) == true);
// 	auto res_s = op.execute(rc_qp[loc][thd_id], IBV_SEND_SIGNALED);
// 	RDMA_ASSERT(res_s == IOCode::Ok);
// 	auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
// 	RDMA_ASSERT(res_p == IOCode::Ok);
// 	char *tmp_buf2 = Rdma::get_row_client_memory(thd_id);
// 	auto res_s2 = rc_qp[loc][thd_id]->send_normal(
// 		{.op = IBV_WR_RDMA_READ,
// 		.flags = IBV_SEND_SIGNALED,
// 		.len = operate_size,
// 		.wr_id = 0},
// 		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(tmp_buf2),
// 		.remote_addr = off,
// 		.imm_data = 0});
// 	RDMA_ASSERT(res_s2 == rdmaio::IOCode::Ok);
//     auto res_p2 = rc_qp[loc][thd_id]->wait_one_comp();
//     RDMA_ASSERT(res_p2 == rdmaio::IOCode::Ok);
// 	row_t *temp_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
// 	memcpy(temp_row, tmp_buf2, operate_size);
// 	assert(temp_row->get_primary_key() == data->data->get_primary_key());
// 	uint64_t txn_commit_ts = txnMng->get_commit_timestamp();
// 	if(data->type == RD) {
// 		if (txn_commit_ts > temp_row->timestamp_last_read) temp_row->timestamp_last_read = txn_commit_ts;
// 		//ucread_erase(txn->get_txn_id());
// 		for(uint64_t i = 0; i < row_set_length; i++) {
// 			uint64_t last = temp_row->uncommitted_reads[i];
// 			assert(i < row_set_length - 1);
// 			if(last == 0 || txnMng->get_txn_id() == 0) break;
// 			if(last == txnMng->get_txn_id()) {
// 				if(temp_row->uncommitted_reads[i+1] == 0) {
// 					temp_row->uncommitted_reads[i] = 0;
// 					break;
// 				}
// 				for(uint64_t j = i; j < row_set_length; j++) {
// 					if(temp_row->uncommitted_reads[j+1] == 0) break;
// 					temp_row->uncommitted_reads[j] = temp_row->uncommitted_reads[j+1];
// 				}
// 			}
// 		}
// 		for(uint64_t i = 0; i < row_set_length; i++) {
// 			if (temp_row->uncommitted_writes[i] == 0) {
// 				break;
// 			}
// 			//printf("row->uncommitted_writes has txn: %u\n", _row->uncommitted_writes[i]);
// 			//exit(0);
// 			if(txnMng->uncommitted_writes.count(temp_row->uncommitted_writes[i]) == 0) {
// 				if(temp_row->uncommitted_writes[i] % g_node_cnt == g_node_id) {
// 					uint64_t it_lower = rdma_time_table.local_get_lower(txnMng->get_thd_id(),temp_row->uncommitted_writes[i]);
// 					if(it_lower <= txn_commit_ts) {
// 						rdma_time_table.local_set_lower(txnMng->get_thd_id(),temp_row->uncommitted_writes[i],txn_commit_ts+1);
						
// 					}
// 				} else {
// 					RdmaTimeTableNode* item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
// 					item = rdma_time_table.remote_get_timeNode(txnMng->get_thd_id(), temp_row->uncommitted_writes[i]);
// 					uint64_t it_lower = item->lower;
// 					if(it_lower <= txn_commit_ts) {
// 						item->lower = txn_commit_ts+1;
// 						rdma_time_table.remote_set_timeNode(txnMng->get_thd_id(), temp_row->uncommitted_writes[i], item);
// 					}
// 					mem_allocator.free(item, sizeof(RdmaTimeTableNode));
// 				}
// 				DEBUG("MAAT forward val set remote lower %ld: %lu\n",temp_row->uncommitted_writes[i],txn_commit_ts+1);
// 			} 
// 		}
// 		temp_row->_tid_word = 0;
// 		memcpy(tmp_buf2, (char *)temp_row, operate_size);
// 		auto res_s3 = rc_qp[loc][thd_id]->send_normal(
// 			{.op = IBV_WR_RDMA_WRITE,
// 			.flags = IBV_SEND_SIGNALED,
// 			.len = operate_size,
// 			.wr_id = 0},
// 			{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(tmp_buf2),
// 			.remote_addr = off,
// 			.imm_data = 0});
// 		RDMA_ASSERT(res_s3 == rdmaio::IOCode::Ok);
// 		auto res_p3 = rc_qp[loc][thd_id]->wait_one_comp();
// 		RDMA_ASSERT(res_p3 == rdmaio::IOCode::Ok);
// 	}

// 	if(data->type == WR) {
// 		if (txn_commit_ts > temp_row->timestamp_last_write) temp_row->timestamp_last_write = txn_commit_ts;
// 		for(uint64_t i = 0; i < row_set_length; i++) {
// 			uint64_t last = temp_row->uncommitted_writes[i];
// 			assert(i < row_set_length - 1);
// 			if(last == 0) break;
// 			if(last == txnMng->get_txn_id()) {
// 				if(temp_row->uncommitted_writes[i+1] == 0) {
// 					temp_row->uncommitted_writes[i] = 0;
// 					break;
// 				}
// 				for(uint64_t j = i; j < row_set_length; j++) {
// 					temp_row->uncommitted_writes[j] = temp_row->uncommitted_writes[j+1];
// 					if(temp_row->uncommitted_writes[j+1] == 0) break;
// 				}
// 			}
// 		}
// 		//temp_row->copy(data->data);
// 		uint64_t lower = rdma_time_table.local_get_lower(txnMng->get_thd_id(),txnMng->get_txn_id());
// 		for(uint64_t i = 0; i < row_set_length; i++) {
// 			if (temp_row->uncommitted_writes[i] == 0) {
// 				break;
// 			}
// 			if(txnMng->uncommitted_writes_y.count(temp_row->uncommitted_writes[i]) == 0) {
// 				if(temp_row->uncommitted_writes[i] % g_node_cnt == g_node_id) {
// 					uint64_t it_upper = rdma_time_table.local_get_upper(txnMng->get_thd_id(),temp_row->uncommitted_writes[i]);
// 					if(it_upper >= txn_commit_ts) {
// 						rdma_time_table.local_set_upper(txnMng->get_thd_id(),temp_row->uncommitted_writes[i],txn_commit_ts-1);
// 					}
// 				} else {
// 					RdmaTimeTableNode* item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
// 					item = rdma_time_table.remote_get_timeNode(txnMng->get_thd_id(), temp_row->uncommitted_writes[i]);
// 					uint64_t it_upper = item->upper;
// 					if(it_upper >= txn_commit_ts) {
// 						item->upper = txn_commit_ts-1;
// 						rdma_time_table.remote_set_timeNode(txnMng->get_thd_id(), temp_row->uncommitted_writes[i], item);
// 					}
// 					mem_allocator.free(item, sizeof(RdmaTimeTableNode));
// 				}
// 				DEBUG("MAAT forward val set upper %ld: %lu\n",temp_row->uncommitted_writes[i],txn_commit_ts -1);
// 			} 
// 		}
// 		for(uint64_t i = 0; i < row_set_length; i++) {
// 			if (temp_row->uncommitted_reads[i] == 0) {
// 				break;
// 			}
// 			if(txnMng->uncommitted_reads.count(temp_row->uncommitted_reads[i]) == 0) {
// 				if(temp_row->uncommitted_reads[i] % g_node_cnt == g_node_id) {
// 					uint64_t it_upper = rdma_time_table.local_get_upper(txnMng->get_thd_id(),temp_row->uncommitted_reads[i]);
// 					if(it_upper >= lower) {
// 						rdma_time_table.local_set_upper(txnMng->get_thd_id(),temp_row->uncommitted_reads[i],lower-1);
// 					}
// 				} else {
// 					RdmaTimeTableNode* item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
// 					item = rdma_time_table.remote_get_timeNode(txnMng->get_thd_id(), temp_row->uncommitted_reads[i]);
// 					uint64_t it_upper = item->upper;
// 					if(it_upper >= lower) {
// 						item->upper = lower-1;
// 						rdma_time_table.remote_set_timeNode(txnMng->get_thd_id(), temp_row->uncommitted_reads[i], item);
// 					}
// 					mem_allocator.free(item, sizeof(RdmaTimeTableNode));
// 				}
// 				DEBUG("MAAT forward val set upper %ld: %lu\n",temp_row->uncommitted_reads[i],lower -1);
// 			} 
// 		}
// 		temp_row->_tid_word = 0;
// 		memcpy(tmp_buf2, (char *)temp_row, operate_size);
// 		auto res_s3 = rc_qp[loc][thd_id]->send_normal(
// 			{.op = IBV_WR_RDMA_WRITE,
// 			.flags = IBV_SEND_SIGNALED,
// 			.len = operate_size,
// 			.wr_id = 0},
// 			{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(tmp_buf2),
// 			.remote_addr = off,
// 			.imm_data = 0});
// 		RDMA_ASSERT(res_s3 == rdmaio::IOCode::Ok);
// 		auto res_p3 = rc_qp[loc][thd_id]->wait_one_comp();
// 		RDMA_ASSERT(res_p3 == rdmaio::IOCode::Ok);
// 		//uncommitted_writes->erase(txn->get_txn_id());
// 	}
// 	mem_allocator.free(temp_row, sizeof(row_t));
// 	return Abort;
// }

#endif

