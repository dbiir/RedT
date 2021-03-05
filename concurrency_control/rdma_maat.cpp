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
#include "rdma_maat.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_rdma_maat.h"
#include "rdma.h"
//#include "src/allocator_master.hh"
#include "qps/op.hh"
#include "string.h"

void RDMA_Maat::init() { sem_init(&_semaphore, 0, 1); }

RC RDMA_Maat::validate(TxnManager * txn) {
	uint64_t start_time = get_sys_clock();
	uint64_t timespan;
	sem_wait(&_semaphore);

	timespan = get_sys_clock() - start_time;
	txn->txn_stats.cc_block_time += timespan;
	txn->txn_stats.cc_block_time_short += timespan;
	INC_STATS(txn->get_thd_id(),maat_cs_wait_time,timespan);
	start_time = get_sys_clock();
	RC rc = RCOK;
	//本地time_table
	printf("txn_id: %d,node_id: %d\n", txn->get_txn_id(), g_node_id);
	uint64_t lower = rdma_time_table.local_get_lower(txn->get_thd_id(),txn->get_txn_id());
	uint64_t upper = rdma_time_table.local_get_upper(txn->get_thd_id(),txn->get_txn_id());
	DEBUG("MAAT Validate Start %ld: [%lu,%lu]\n",txn->get_txn_id(),lower,upper);
	std::set<uint64_t> after;
	std::set<uint64_t> before;
	// lower bound of txn greater than write timestamp
	if(lower <= txn->greatest_write_timestamp) {
		lower = txn->greatest_write_timestamp + 1;
		INC_STATS(txn->get_thd_id(),maat_case1_cnt,1);
	}
	// lower bound of uncommitted writes greater than upper bound of txn
	for(auto it = txn->uncommitted_writes.begin(); it != txn->uncommitted_writes.end();it++) {
		if (*it % g_node_cnt == g_node_id) {
			uint64_t it_lower = rdma_time_table.local_get_lower(txn->get_thd_id(), *it);
			if(upper >= it_lower) {
				MAATState state = rdma_time_table.local_get_state(txn->get_thd_id(), *it);
				if(state == MAAT_VALIDATED || state == MAAT_COMMITTED) {
					INC_STATS(txn->get_thd_id(),maat_case2_cnt,1);
					if(it_lower > 0) {
						upper = it_lower - 1;
					} else {
						upper = it_lower;   
					}
				}
				if(state == MAAT_RUNNING) {
					after.insert(*it);
				}
			}
		} else {
			RdmaTimeTableNode* item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
			item = rdma_time_table.remote_get_timeNode(txn->get_thd_id(), *it);
			auto it_lower = item->lower;
			if(upper >= it_lower) {
				MAATState state = item->state;
				if(state == MAAT_VALIDATED || state == MAAT_COMMITTED) {
					INC_STATS(txn->get_thd_id(),maat_case2_cnt,1);
					if(it_lower > 0) {
						upper = it_lower - 1;
					} else {
						upper = it_lower;   
					}
				}
				if(state == MAAT_RUNNING) {
					after.insert(*it);
				}
			}
		}        
	}
	// lower bound of txn greater than read timestamp
	if(lower <= txn->greatest_read_timestamp) {
		lower = txn->greatest_read_timestamp + 1;
		INC_STATS(txn->get_thd_id(),maat_case3_cnt,1);
	}
	// upper bound of uncommitted reads less than lower bound of txn
	for(auto it = txn->uncommitted_reads.begin(); it != txn->uncommitted_reads.end();it++) {
		if ( *it % g_node_cnt == g_node_id) {
			uint64_t it_upper = rdma_time_table.local_get_upper(txn->get_thd_id(),*it);
			if(lower <= it_upper) {
				MAATState state = rdma_time_table.local_get_state(txn->get_thd_id(),*it);
				if(state == MAAT_VALIDATED || state == MAAT_COMMITTED) {
					INC_STATS(txn->get_thd_id(),maat_case4_cnt,1);
					if(it_upper < UINT64_MAX) {
						lower = it_upper + 1;
					} else {
						lower = it_upper;   
					}
				}
				if(state == MAAT_RUNNING) {
					before.insert(*it);
				}
			}
		} else {
			RdmaTimeTableNode* item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
			item = rdma_time_table.remote_get_timeNode(txn->get_thd_id(), *it);
			auto it_upper = item->upper;
			if(upper >= it_upper) {
				MAATState state = item->state;
				if(state == MAAT_VALIDATED || state == MAAT_COMMITTED) {
					INC_STATS(txn->get_thd_id(),maat_case4_cnt,1);
					if(it_upper < UINT64_MAX) {
						lower = it_upper - 1;
					} else {
						lower = it_upper;   
					}
				}
				if(state == MAAT_RUNNING) {
					before.insert(*it);
				}
			}
		}
	}
	// upper bound of uncommitted write writes less than lower bound of txn
	for(auto it = txn->uncommitted_writes_y.begin(); it != txn->uncommitted_writes_y.end();it++) {
		if ( *it % g_node_cnt == g_node_id) {
			MAATState state = rdma_time_table.local_get_state(txn->get_thd_id(),*it);
			uint64_t it_upper = rdma_time_table.local_get_upper(txn->get_thd_id(),*it);
			if(state == MAAT_ABORTED) {
				continue;
			}
			if(state == MAAT_VALIDATED || state == MAAT_COMMITTED) {
				if(lower <= it_upper) {
					INC_STATS(txn->get_thd_id(),maat_case5_cnt,1);
					if(it_upper < UINT64_MAX) {
						lower = it_upper + 1;
					} else {
						lower = it_upper;
					}
				}
			}
			if(state == MAAT_RUNNING) {
				after.insert(*it);
			}
		} else {
			RdmaTimeTableNode* item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
			item = rdma_time_table.remote_get_timeNode(txn->get_thd_id(), *it);
			auto it_upper = item->upper;
			MAATState state = item->state;
			if(state == MAAT_ABORTED) {
				continue;
			}
			if(state == MAAT_VALIDATED || state == MAAT_COMMITTED) {
				if(lower <= it_upper) {
					INC_STATS(txn->get_thd_id(),maat_case5_cnt,1);
					if(it_upper < UINT64_MAX) {
						lower = it_upper + 1;
					} else {
						lower = it_upper;
					}
				}
			}
			if(state == MAAT_RUNNING) {
				after.insert(*it);
			}
		}
	}
	if(lower >= upper) {
		// Abort
		rdma_time_table.local_set_state(txn->get_thd_id(),txn->get_txn_id(),MAAT_ABORTED);
		rc = Abort;
	} else {
		// Validated
		rdma_time_table.local_set_state(txn->get_thd_id(),txn->get_txn_id(),MAAT_VALIDATED);
		rc = RCOK;

		for(auto it = before.begin(); it != before.end();it++) {
			if( *it % g_node_cnt == g_node_id) {
				uint64_t it_upper = rdma_time_table.local_get_upper(txn->get_thd_id(), *it);
				if(it_upper > lower && it_upper < upper-1) {
					lower = it_upper + 1;
				}
			} else {
				RdmaTimeTableNode* item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
				item = rdma_time_table.remote_get_timeNode(txn->get_thd_id(), *it);
				uint64_t it_upper = item->upper;
				if(it_upper > lower && it_upper < upper-1) {
					lower = it_upper + 1;
				}
			}
		}
		for(auto it = before.begin(); it != before.end();it++) {
			if( *it % g_node_cnt == g_node_id) {
				uint64_t it_upper = rdma_time_table.local_get_upper(txn->get_thd_id(), *it);
				if(it_upper >= lower) {
					if(lower > 0) {
						rdma_time_table.local_set_upper(txn->get_thd_id(), *it, lower-1);
					} else {
						rdma_time_table.local_set_upper(txn->get_thd_id(), *it, lower);
					}
				}
			} else {
				RdmaTimeTableNode* item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
				item = rdma_time_table.remote_get_timeNode(txn->get_thd_id(), *it);
				uint64_t it_upper = item->upper;
				if(it_upper >= lower) {
					if(lower > 0) {
						item->upper = lower - 1;
					} else {
						item->upper = lower;
					}
					rdma_time_table.remote_set_timeNode(txn->get_thd_id(), *it, item);
				}
			}
		}
		for(auto it = after.begin(); it != after.end();it++) {
			if(*it % g_node_cnt == g_node_id) {
				uint64_t it_upper = rdma_time_table.local_get_upper(txn->get_thd_id(), *it);
				uint64_t it_lower = rdma_time_table.local_get_lower(txn->get_thd_id(), *it);
				if(it_upper != UINT64_MAX && it_upper > lower + 2 && it_upper < upper ) {
					upper = it_upper - 2;
				}
				if((it_lower < upper && it_lower > lower+1)) {
					upper = it_lower - 1;
				}
			} else {
				RdmaTimeTableNode* item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
				item = rdma_time_table.remote_get_timeNode(txn->get_thd_id(), *it);
				uint64_t it_upper = item->upper;
				uint64_t it_lower = item->lower;
				if(it_upper != UINT64_MAX && it_upper > lower + 2 && it_upper < upper ) {
					upper = it_upper - 2;
				}
				if((it_lower < upper && it_lower > lower+1)) {
					upper = it_lower - 1;
				}
			}
		}
		// set all upper and lower bounds to meet inequality
		for(auto it = after.begin(); it != after.end();it++) {
			if(*it % g_node_cnt == g_node_id) {
				uint64_t it_lower = rdma_time_table.local_get_lower(txn->get_thd_id(), *it);
				if(it_lower <= upper) {
					if(upper < UINT64_MAX) {
						rdma_time_table.local_set_lower(txn->get_thd_id(), *it, upper + 1);
					} else {
						rdma_time_table.local_set_lower(txn->get_thd_id(), *it, upper);
					}
				}
			} else {
				RdmaTimeTableNode* item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
				item = rdma_time_table.remote_get_timeNode(txn->get_thd_id(), *it);
				uint64_t it_lower = item->lower;
				if(it_lower <= upper) {
					if(upper < UINT64_MAX) {
						item->lower = upper + 1;
					} else {
						item->lower = upper;
					}
					rdma_time_table.remote_set_timeNode(txn->get_thd_id(), *it, item);
				}
			}
		}

		assert(lower < upper);
		INC_STATS(txn->get_thd_id(),maat_range,upper-lower);
		INC_STATS(txn->get_thd_id(),maat_commit_cnt,1);
	}
	rdma_time_table.local_set_lower(txn->get_thd_id(),txn->get_txn_id(),lower);
	rdma_time_table.local_set_upper(txn->get_thd_id(),txn->get_txn_id(),upper);
	INC_STATS(txn->get_thd_id(),maat_validate_cnt,1);
	timespan = get_sys_clock() - start_time;
	INC_STATS(txn->get_thd_id(),maat_validate_time,timespan);
	txn->txn_stats.cc_time += timespan;
	txn->txn_stats.cc_time_short += timespan;
	DEBUG("MAAT Validate End %ld: %d [%lu,%lu]\n",txn->get_txn_id(),rc==RCOK,lower,upper);
	sem_post(&_semaphore);
	return rc;

}

RC RDMA_Maat::find_bound(TxnManager * txn) {
	RC rc = RCOK;
	uint64_t lower = rdma_time_table.local_get_lower(txn->get_thd_id(),txn->get_txn_id());
	uint64_t upper = rdma_time_table.local_get_upper(txn->get_thd_id(),txn->get_txn_id());
	if(lower >= upper) {
		rdma_time_table.local_set_state(txn->get_thd_id(),txn->get_txn_id(),MAAT_VALIDATED);
		rc = Abort;
	} else {
		rdma_time_table.local_set_state(txn->get_thd_id(),txn->get_txn_id(),MAAT_COMMITTED);
		// TODO: can commit_time be selected in a smarter way?
		txn->commit_timestamp = lower;
	}
	DEBUG("MAAT Bound %ld: %d [%lu,%lu] %lu\n", txn->get_txn_id(), rc, lower, upper,
			txn->commit_timestamp);
	return rc;
}

void RdmaTimeTable::init() {
//table_size = g_inflight_max * g_node_cnt * 2 + 1;
	table_size = (RDMA_TIMETABLE_MAX)*(sizeof(RdmaTimeTableNode)+1);
	assert(table_size < rdma_timetable_size);
	DEBUG_M("RdmaTimeTable::init table alloc\n");
	table = (RdmaTimeTableNode*)rdma_timetable_buffer;

	printf("%d",table[0].key);
	for(uint64_t i = 0; i < RDMA_TIMETABLE_MAX; i++) {
		table[i].init(-1);
	}
}

uint64_t RdmaTimeTable::hash(uint64_t key) { return key % table_size; }


void RdmaTimeTable::init(uint64_t thd_id, uint64_t key) {
	table[key]._lock = g_node_id;
	table[key].lower = 0;
	table[key].upper = UINT64_MAX;
	table[key].key = key;
	table[key].state = MAAT_RUNNING;
	table[key]._lock = 0;
}

void RdmaTimeTable::release(uint64_t thd_id, uint64_t key) {
	table[key]._lock = g_node_id;
	table[key].lower = 0;
	table[key].upper = UINT64_MAX;
	table[key].key = -1;
	table[key].state = MAAT_RUNNING;
	table[key]._lock = 0;
}

uint64_t RdmaTimeTable::local_get_lower(uint64_t thd_id, uint64_t key) {

	table[key]._lock = g_node_id;
	uint64_t value = table[key].lower;
	table[key]._lock = 0;
	return value;
}
// uint64_t RdmaTimeTable::remote_get_lower(uint64_t thd_id, UC_SET key) {

//     table[key]._lock = g_node_id;
//     uint64_t value = table[key].lower;
//     table[key]._lock = 0;
//     return value;
// }

uint64_t RdmaTimeTable::local_get_upper(uint64_t thd_id, uint64_t key) {
	table[key]._lock = g_node_id;
	uint64_t value = table[key].upper;
	table[key]._lock = 0;
	return value;
}


void RdmaTimeTable::local_set_lower(uint64_t thd_id, uint64_t key, uint64_t value) {
	table[key]._lock = g_node_id;
	table[key].lower = value;
	table[key]._lock = 0;
}

void RdmaTimeTable::local_set_upper(uint64_t thd_id, uint64_t key, uint64_t value) {
	table[key]._lock = g_node_id;
	table[key].upper = value;
	table[key]._lock = 0;
}

MAATState RdmaTimeTable::local_get_state(uint64_t thd_id, uint64_t key) {
	table[key]._lock = g_node_id;
	MAATState state = MAAT_ABORTED;
	state = table[key].state;   
	table[key]._lock = 0;
	return state;
}

void RdmaTimeTable::local_set_state(uint64_t thd_id, uint64_t key, MAATState value) {
	table[key]._lock = g_node_id;
	table[key].state = value;
	table[key]._lock = 0;
}

RdmaTimeTableNode * RdmaTimeTable::remote_get_timeNode(uint64_t thd_id, uint64_t key) {
	assert(key % g_node_cnt != g_node_id);
	uint64_t node_id = key % g_node_cnt;
	// todo: 这里需要获取对应的索引
	// uint64_t index_key = 0;
	uint64_t timenode_addr = (key) * sizeof(RdmaTimeTableNode);
	uint64_t timenode_size = sizeof(RdmaTimeTableNode);
	// 每个线程只用自己的那一块客户端内存地址
	uint64_t *tmp_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto mr = client_rm_handler->get_reg_attr().value();

	rdmaio::qp::Op<> op;
	op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[node_id].buf + timenode_addr), remote_mr_attr[node_id].key).set_cas(0, key);
	assert(op.set_payload(tmp_buf, sizeof(uint64_t), mr.key) == true);
	auto res_s = op.execute(rc_qp[node_id][thd_id], IBV_SEND_SIGNALED);
	RDMA_ASSERT(res_s == IOCode::Ok);
	auto res_p = rc_qp[node_id][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == IOCode::Ok);

	//timetable读取失败
	if (*tmp_buf != 0) {
		
	}
	char *test_buf = Rdma::get_index_client_memory(thd_id);
	::memset(test_buf, 0, timenode_size);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
	auto res_s2 = rc_qp[node_id][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = timenode_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = timenode_addr,
		.imm_data = 0});
	RDMA_ASSERT(res_s2 == rdmaio::IOCode::Ok);
	auto res_p2 = rc_qp[node_id][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p2 == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();

	//INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	//INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	RdmaTimeTableNode * item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
	::memcpy(item, test_buf, timenode_size);
	assert(item->key == key);
	uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto mr2 = client_rm_handler->get_reg_attr().value();

	rdmaio::qp::Op<> op2;
	op2.set_atomic_rbuf((uint64_t*)(remote_mr_attr[node_id].buf + timenode_addr), remote_mr_attr[node_id].key).set_cas(key, 0);
	assert(op2.set_payload(tmp_buf2, sizeof(uint64_t), mr.key) == true);
	auto res_s3 = op2.execute(rc_qp[node_id][thd_id], IBV_SEND_SIGNALED);
	RDMA_ASSERT(res_s3 == IOCode::Ok);
	auto res_p3 = rc_qp[node_id][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p3 == IOCode::Ok);

	//timetable读取失败
	if (*tmp_buf != 0) {
		
	}
	return item;        
}
void RdmaTimeTable::remote_set_timeNode(uint64_t thd_id, uint64_t key, RdmaTimeTableNode * value) {
	uint64_t node_id = key % g_node_cnt;
	assert(node_id != g_node_id);

	// todo: 这里需要获取对应的索引
	// uint64_t index_key = 0;
	uint64_t timenode_addr = (key) * sizeof(RdmaTimeTableNode);
	uint64_t timenode_size = sizeof(RdmaTimeTableNode);
	// 每个线程只用自己的那一块客户端内存地址
	uint64_t *tmp_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto mr = client_rm_handler->get_reg_attr().value();

	rdmaio::qp::Op<> op;
	op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[node_id].buf + timenode_addr), remote_mr_attr[node_id].key).set_cas(0, key);
	assert(op.set_payload(tmp_buf, sizeof(uint64_t), mr.key) == true);
	auto res_s = op.execute(rc_qp[node_id][thd_id], IBV_SEND_SIGNALED);
	RDMA_ASSERT(res_s == IOCode::Ok);
	auto res_p = rc_qp[node_id][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == IOCode::Ok);

	//timetable读取失败
	if (*tmp_buf != 0) {
		
	}
	char *test_buf = Rdma::get_index_client_memory(thd_id);
	::memset(test_buf, 0, timenode_size);
	::memcpy(test_buf, (char *)value, timenode_size);
	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
	auto res_s2 = rc_qp[node_id][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = timenode_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = timenode_addr,
		.imm_data = 0});
	RDMA_ASSERT(res_s2 == rdmaio::IOCode::Ok);
	auto res_p2 = rc_qp[node_id][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p2 == rdmaio::IOCode::Ok);
	endtime = get_sys_clock();

	//INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
	//INC_STATS(get_thd_id(), rdma_read_cnt, 1);
	RdmaTimeTableNode * item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
	::memcpy(item, test_buf, timenode_size);
	assert(item->key == key);
	uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	auto mr2 = client_rm_handler->get_reg_attr().value();

	rdmaio::qp::Op<> op2;
	op2.set_atomic_rbuf((uint64_t*)(remote_mr_attr[node_id].buf + timenode_addr), remote_mr_attr[node_id].key).set_cas(key, 0);
	assert(op2.set_payload(tmp_buf2, sizeof(uint64_t), mr.key) == true);
	auto res_s3 = op2.execute(rc_qp[node_id][thd_id], IBV_SEND_SIGNALED);
	RDMA_ASSERT(res_s3 == IOCode::Ok);
	auto res_p3 = rc_qp[node_id][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p3 == IOCode::Ok);
	//timetable读取失败
	if (*tmp_buf != 0) {
		
	}
}

// RdmaTimeTableNode* RDMA_Maat::read_remote_timetable(TxnManager * txn, uint64_t node_id) {
// 	assert(node_id != g_node_id);
// 	uint64_t thd_id = txn->get_thd_id();

// 	// todo: 这里需要获取对应的索引
// 	// uint64_t index_key = 0;
// 	uint64_t timenode_addr = (txn->get_txn_id()) * sizeof(RdmaTimeTableNode);
// 	uint64_t timenode_size = sizeof(RdmaTimeTableNode);
// 	// 每个线程只用自己的那一块客户端内存地址
// 	uint64_t *tmp_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
// 	auto mr = client_rm_handler->get_reg_attr().value();

// 	rdmaio::qp::Op<> op;
// 	op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[node_id].buf + timenode_addr), remote_mr_attr[node_id].key).set_cas(0, txn->get_txn_id());
// 	assert(op.set_payload(tmp_buf, sizeof(uint64_t), mr.key) == true);
// 	auto res_s = op.execute(rc_qp[node_id][thd_id], IBV_SEND_SIGNALED);
// 	RDMA_ASSERT(res_s == IOCode::Ok);
// 	auto res_p = rc_qp[node_id][thd_id]->wait_one_comp();
// 	RDMA_ASSERT(res_p == IOCode::Ok);

// 	//timetable读取失败
// 	if (*tmp_buf != 0) {
	
// 	}
// 	char *test_buf = Rdma::get_index_client_memory(thd_id);
// 	::memset(test_buf, 0, timenode_size);

// 	uint64_t starttime;
// 	uint64_t endtime;
// 	starttime = get_sys_clock();
// 	auto res_s2 = rc_qp[g_node_id][thd_id]->send_normal(
// 	{.op = IBV_WR_RDMA_READ,
// 	.flags = IBV_SEND_SIGNALED,
// 	.len = timenode_size,
// 	.wr_id = 0},
// 	{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
// 	.remote_addr = timenode_addr,
// 	.imm_data = 0});
// 	RDMA_ASSERT(res_s2 == rdmaio::IOCode::Ok);
// 	auto res_p2 = rc_qp[node_id][thd_id]->wait_one_comp();
// 	RDMA_ASSERT(res_p2 == rdmaio::IOCode::Ok);
// 	endtime = get_sys_clock();

// 	//INC_STATS(get_thd_id(), rdma_read_time, endtime-starttime);
// 	//INC_STATS(get_thd_id(), rdma_read_cnt, 1);
// 	RdmaTimeTableNode* item = (RdmaTimeTableNode *)mem_allocator.alloc(sizeof(RdmaTimeTableNode));
// 	::memcpy(item, test_buf, timenode_size);
// 	assert(item->key == txn->get_txn_id());

// 	uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
// 	auto mr = client_rm_handler->get_reg_attr().value();

// 	rdmaio::qp::Op<> op;
// 	op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[node_id].buf + timenode_addr), remote_mr_attr[node_id].key).set_cas(txn->get_txn_id(), 0);
// 	assert(op.set_payload(tmp_buf2, sizeof(uint64_t), mr.key) == true);
// 	auto res_s3 = op.execute(rc_qp[node_id][thd_id], IBV_SEND_SIGNALED);
// 	RDMA_ASSERT(res_s3 == IOCode::Ok);
// 	auto res_p3 = rc_qp[node_id][thd_id]->wait_one_comp();
// 	RDMA_ASSERT(res_p3 == IOCode::Ok);

// 	//timetable读取失败
// 	if (*tmp_buf != 0) {
	
// 	}
// 	return item;
// }