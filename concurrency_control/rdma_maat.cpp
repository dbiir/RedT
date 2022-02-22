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

#if CC_ALG == RDMA_MAAT

void RDMA_Maat::init() { sem_init(&_semaphore, 0, 1); }

RC RDMA_Maat::validate(yield_func_t &yield, TxnManager * txn, uint64_t cor_id) {
	uint64_t start_time = get_sys_clock();
	uint64_t timespan;
	// sem_wait(&_semaphore);

	timespan = get_sys_clock() - start_time;
	txn->txn_stats.cc_block_time += timespan;
	txn->txn_stats.cc_block_time_short += timespan;
	INC_STATS(txn->get_thd_id(),maat_cs_wait_time,timespan);
	start_time = get_sys_clock();
	RC rc = RCOK;

	uint64_t lower = rdma_txn_table.local_get_lower(txn->get_txn_id());
	uint64_t upper = rdma_txn_table.local_get_upper(txn->get_txn_id());
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
			uint64_t it_lower = rdma_txn_table.local_get_lower( *it);
			if (!rdma_txn_table.local_is_key(*it)) continue;
			if(upper >= it_lower) {
				MAATState state = rdma_txn_table.local_get_state( *it);
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
			RdmaTxnTableNode* item = rdma_txn_table.remote_get_timeNode(yield, txn, *it, cor_id);
			if (!rdma_txn_table.remote_is_key(item, *it))continue;
			auto it_lower = rdma_txn_table.remote_get_lower(item, *it);
			if(upper >= it_lower) {
				MAATState state = rdma_txn_table.remote_get_state(item, *it);
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
			mem_allocator.free(item, sizeof(RdmaTxnTableNode));
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
			if (!rdma_txn_table.local_is_key(*it)) continue;
			uint64_t it_upper = rdma_txn_table.local_get_upper(*it);
			if(lower <= it_upper) {
				MAATState state = rdma_txn_table.local_get_state(*it);
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
			RdmaTxnTableNode* item = rdma_txn_table.remote_get_timeNode(yield, txn, *it, cor_id);
			if (!rdma_txn_table.remote_is_key(item, *it))continue;
			auto it_upper = rdma_txn_table.remote_get_upper(item, *it);
			if(upper >= it_upper) {
				MAATState state = rdma_txn_table.remote_get_state(item, *it);
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
			mem_allocator.free(item, sizeof(RdmaTxnTableNode));
		}
	}
	// upper bound of uncommitted write writes less than lower bound of txn
	for(auto it = txn->uncommitted_writes_y.begin(); it != txn->uncommitted_writes_y.end();it++) {
		if ( *it % g_node_cnt == g_node_id) {
			if (!rdma_txn_table.local_is_key(*it)) continue;
			MAATState state = rdma_txn_table.local_get_state(*it);
			uint64_t it_upper = rdma_txn_table.local_get_upper(*it);
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
			RdmaTxnTableNode* item = rdma_txn_table.remote_get_timeNode(yield, txn, *it, cor_id);
			if (!rdma_txn_table.remote_is_key(item, *it))continue;
			auto it_upper = rdma_txn_table.remote_get_upper(item, *it);
			MAATState state = rdma_txn_table.remote_get_state(item, *it);
			if(state == MAAT_ABORTED) continue;
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
			mem_allocator.free(item, sizeof(RdmaTxnTableNode));
		}
	}
	if(!rdma_txn_table.local_cas_timeNode(yield, txn,  txn->get_txn_id(), cor_id)) {
		rdma_txn_table.local_set_state(txn, txn->get_txn_id(),MAAT_ABORTED);
		// printf("txn %lu abort due to 190\n", txn->get_txn_id());
		rc = Abort;
		return rc;
	}
	// Validated
	uint64_t now_lower = rdma_txn_table.local_get_lower(txn->get_txn_id());
	uint64_t now_upper = rdma_txn_table.local_get_upper(txn->get_txn_id());
	lower = lower > now_lower ? lower : now_lower;
	upper = upper < now_upper ? upper : now_upper;
	rdma_txn_table.local_set_lower(txn, txn->get_txn_id(),lower);
	rdma_txn_table.local_set_upper(txn, txn->get_txn_id(),upper);
	rdma_txn_table.local_release_timeNode(txn,  txn->get_txn_id());
	if(lower > upper || rc == Abort) {
		// Abort
		rdma_txn_table.local_set_state(txn, txn->get_txn_id(),MAAT_ABORTED);
		// if (lower > upper) printf("txn %lu abort due to 205 lower:%lu upper :%lu\n", txn->get_txn_id(), lower, upper);
		rc = Abort;
		goto end;
	} else {
		rdma_txn_table.local_set_state(txn, txn->get_txn_id(),MAAT_VALIDATED);
		rc = RCOK;
		for(auto it = before.begin(); it != before.end();it++) {
			if( *it % g_node_cnt == g_node_id) {
				if (!rdma_txn_table.local_is_key(*it)) continue;
				uint64_t it_upper = rdma_txn_table.local_get_upper( *it);
				if(it_upper > lower && it_upper < upper-1) {
					lower = it_upper + 1;
				}
			} else {
				RdmaTxnTableNode* item = rdma_txn_table.remote_get_timeNode(yield, txn, *it, cor_id);
				if (!rdma_txn_table.remote_is_key(item, *it))continue;
				uint64_t it_upper = rdma_txn_table.remote_get_upper(item, *it);
				if(it_upper > lower && it_upper < upper-1) {
					lower = it_upper + 1;
				}
				mem_allocator.free(item, sizeof(RdmaTxnTableNode));
			}
		}
		for(auto it = before.begin(); it != before.end();it++) {
			if( *it % g_node_cnt == g_node_id) {
				// while(!simulation->is_done() && !rdma_txn_table.local_cas_timeNode(yield, txn,  *it, cor_id)){total_num_atomic_retry++;}
				if(!rdma_txn_table.local_cas_timeNode(yield, txn,  *it, cor_id)) {
					rc = Abort;
					// printf("txn %lu abort due to 233\n", txn->get_txn_id());
					goto end;
				}
				uint64_t it_upper = rdma_txn_table.local_get_upper( *it);
				if (!rdma_txn_table.local_is_key(*it)) {
					rdma_txn_table.local_release_timeNode(txn,  *it);
					continue;
				}
				if(it_upper >= lower) {
					if(lower > 0) {
						rdma_txn_table.local_set_upper(txn,  *it, lower-1);
					} else {
						rdma_txn_table.local_set_upper(txn,  *it, lower);
					}
				}
			
				rdma_txn_table.local_release_timeNode(txn,  *it);
			} else {
				// while(!simulation->is_done() && !rdma_txn_table.remote_cas_timeNode(yield, txn,  *it, cor_id)){total_num_atomic_retry++;}
				if(!rdma_txn_table.remote_cas_timeNode(yield, txn, *it, cor_id)) {
					rc = Abort;
					// printf("txn %lu abort due to 253\n", txn->get_txn_id());
					goto end;
				}
				RdmaTxnTableNode* item = rdma_txn_table.remote_get_timeNode(yield, txn, *it, cor_id);
				if (!rdma_txn_table.remote_is_key(item, *it)) {
					rdma_txn_table.remote_release_timeNode(yield, txn,*it, cor_id);
					mem_allocator.free(item, sizeof(RdmaTxnTableNode));
					continue;
				}
				uint64_t it_upper = rdma_txn_table.remote_get_upper(item, *it);
				if(it_upper >= lower) {
					if(lower > 0) {
						rdma_txn_table.remote_set_upper(item, *it, lower - 1);
					} else {
						rdma_txn_table.remote_set_upper(item, *it, lower);
					}
					if(*it != 0) {
						rdma_txn_table.remote_set_timeNode(yield, txn, *it, item, cor_id);
					}
				}
				mem_allocator.free(item, sizeof(RdmaTxnTableNode));
				rdma_txn_table.remote_release_timeNode(yield, txn,*it, cor_id);
			}
		}
		for(auto it = after.begin(); it != after.end();it++) {
			if(*it % g_node_cnt == g_node_id) {
				if (!rdma_txn_table.local_is_key(*it)) continue;
				uint64_t it_upper = rdma_txn_table.local_get_upper( *it);
				uint64_t it_lower = rdma_txn_table.local_get_lower( *it);
				if(it_upper != UINT64_MAX && it_upper > lower + 2 && it_upper < upper ) {
					upper = it_upper - 2;
				}
				if((it_lower < upper && it_lower > lower+1)) {
					upper = it_lower - 1;
				}
			
			} else {
				RdmaTxnTableNode* item = rdma_txn_table.remote_get_timeNode(yield, txn, *it, cor_id);
				if (!rdma_txn_table.remote_is_key(item, *it)) {
					continue;
				}
				uint64_t it_upper = rdma_txn_table.remote_get_upper(item, *it);
				uint64_t it_lower = rdma_txn_table.remote_get_lower(item, *it);
				if(it_upper != UINT64_MAX && it_upper > lower + 2 && it_upper < upper ) {
					upper = it_upper - 2;
				}
				if((it_lower < upper && it_lower > lower+1)) {
					upper = it_lower - 1;
				}
				mem_allocator.free(item, sizeof(RdmaTxnTableNode));
			}
		}
		// set all upper and lower bounds to meet inequality
		for(auto it = after.begin(); it != after.end();it++) {
			if(*it % g_node_cnt == g_node_id) {
				// while(!simulation->is_done() && !rdma_txn_table.local_cas_timeNode(yield, txn,  *it, cor_id)){total_num_atomic_retry++;}
				if(!rdma_txn_table.local_cas_timeNode(yield, txn,  *it, cor_id)) {
					rc = Abort;
					// printf("txn %lu abort due to 312\n", txn->get_txn_id());
					goto end;
				}
				if (!rdma_txn_table.local_is_key(*it)) {
					rdma_txn_table.local_release_timeNode(txn,  *it);
					continue;
				}
				uint64_t it_lower = rdma_txn_table.local_get_lower( *it);
				if(it_lower <= upper) {
					if(upper < UINT64_MAX) {
						rdma_txn_table.local_set_lower(txn,  *it, upper + 1);
					} else {
						rdma_txn_table.local_set_lower(txn,  *it, upper);
					}
				}
				rdma_txn_table.local_release_timeNode(txn,  *it);
			} else {
				// while(!simulation->is_done() && !rdma_txn_table.remote_cas_timeNode(yield, txn,  *it, cor_id)){total_num_atomic_retry++;}
				if(!rdma_txn_table.remote_cas_timeNode(yield, txn, *it, cor_id)) {
					rc = Abort;
					// printf("txn %lu abort due to 330\n", txn->get_txn_id());
					goto end;
				}
				RdmaTxnTableNode* item = rdma_txn_table.remote_get_timeNode(yield, txn, *it, cor_id);
				if (!rdma_txn_table.remote_is_key(item, *it)) {
					rdma_txn_table.remote_release_timeNode(yield, txn,*it, cor_id);
					mem_allocator.free(item, sizeof(RdmaTxnTableNode));
					continue;
				}
				uint64_t it_lower = rdma_txn_table.remote_get_lower(item, *it);
				if(it_lower <= upper) {
					if(upper < UINT64_MAX) {
						rdma_txn_table.remote_set_lower(item, *it, upper + 1);
						// item->lower = upper + 1;
					} else {
						rdma_txn_table.remote_set_lower(item, *it, upper);
						// item->lower = upper;
					}
					if(*it != 0) {
						rdma_txn_table.remote_set_timeNode(yield, txn, *it, item, cor_id);
					}
				}
			
				mem_allocator.free(item, sizeof(RdmaTxnTableNode));
				rdma_txn_table.remote_release_timeNode(yield, txn,*it, cor_id);
			}
		}
		INC_STATS(txn->get_thd_id(),maat_range,upper-lower);
		INC_STATS(txn->get_thd_id(),maat_commit_cnt,1);
	}
	rdma_txn_table.local_set_lower(txn, txn->get_txn_id(),lower);
	rdma_txn_table.local_set_upper(txn, txn->get_txn_id(),upper);
end:
	INC_STATS(txn->get_thd_id(),maat_validate_cnt,1);
	timespan = get_sys_clock() - start_time;
	INC_STATS(txn->get_thd_id(),maat_validate_time,timespan);
	txn->txn_stats.cc_time += timespan;
	txn->txn_stats.cc_time_short += timespan;
	DEBUG("MAAT Validate End %ld: %d [%lu,%lu]\n",txn->get_txn_id(),rc==RCOK,lower,upper);
	//printf("MAAT Validate End %ld: %d [%lu,%lu]\n",txn->get_txn_id(),rc==RCOK,lower,upper);
	// sem_post(&_semaphore);
	// rc = RCOK;
	return rc;

}

RC RDMA_Maat::find_bound(TxnManager * txn) {
	RC rc = RCOK;
	uint64_t lower = rdma_txn_table.local_get_lower(txn->get_txn_id());
	uint64_t upper = rdma_txn_table.local_get_upper(txn->get_txn_id());
	// printf("lower: %u upper: %u\n", lower, upper);
	if(lower > upper) {
		rdma_txn_table.local_set_state(txn, txn->get_txn_id(),MAAT_VALIDATED);
		rc = Abort;
		// printf("txn %lu abort due to 390\n", txn->get_txn_id());
	} else {
		rdma_txn_table.local_set_state(txn, txn->get_txn_id(),MAAT_COMMITTED);
		// TODO: can commit_time be selected in a smarter way?
		txn->commit_timestamp = lower;
	}
	DEBUG("MAAT Bound %ld: %d [%lu,%lu] %lu\n", txn->get_txn_id(), rc, lower, upper,
			txn->commit_timestamp);
	// rc = RCOK;
	return rc;
}

RC
RDMA_Maat::finish(yield_func_t &yield, RC rc , TxnManager * txnMng, uint64_t cor_id)
{
	Transaction *txn = txnMng->txn;
	
	if (rc == Abort) {
		for (uint64_t i = 0; i < txnMng->get_access_cnt(); i++) {
			//local
			if(txn->accesses[i]->location == g_node_id){
				rc = txn->accesses[i]->orig_row->manager->abort(yield, txn->accesses[i]->type,txnMng,cor_id);
			} else{
			//remote
				rc = remote_abort(yield, txnMng, txn->accesses[i], cor_id);

			}
			// DEBUG("silo %ld abort release row %ld \n", txnMng->get_txn_id(), txn->accesses[ txnMng->write_set[i] ]->orig_row->get_primary_key());
		}
	} else {
	//commit
		ts_t time = get_sys_clock();
		for (uint64_t i = 0; i < txnMng->get_access_cnt(); i++) {
			//local
			if(txn->accesses[i]->location == g_node_id){
				Access * access = txn->accesses[i];
				
				//access->orig_row->manager->write( access->data);
				rc = txn->accesses[i]->orig_row->manager->commit(yield, txn->accesses[i]->type, txnMng, access->data, cor_id);
			}else{
			//remote
				Access * access = txn->accesses[i];
				//DEBUG("commit access txn %ld, off: %ld loc: %ld and key: %ld\n",txnMng->get_txn_id(), access->offset, access->location, access->data->get_primary_key());
				rc =  remote_commit(yield, txnMng, txn->accesses[i], cor_id);
			}
			// DEBUG("silo %ld abort release row %ld \n", txnMng->get_txn_id(), txn->accesses[ txnMng->write_set[i] ]->orig_row->get_primary_key());
		}
	}
	for (uint64_t i = 0; i < txn->row_cnt; i++) {
		//local
		if(txn->accesses[i]->location != g_node_id){
		//remote
		mem_allocator.free(txn->accesses[i]->data,0);
		mem_allocator.free(txn->accesses[i]->orig_row,0);
		// mem_allocator.free(txn->accesses[i]->test_row,0);
		txn->accesses[i]->data = NULL;
		txn->accesses[i]->orig_row = NULL;
		txn->accesses[i]->orig_data = NULL;
		txn->accesses[i]->version = 0;
		txn->accesses[i]->offset = 0;
		}
	}
	//memset(txnMng->write_set, 0, 100);
	return rc;
}

RC RDMA_Maat::remote_abort(yield_func_t &yield, TxnManager * txnMng, Access * data, uint64_t cor_id) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	INC_STATS(txnMng->get_thd_id(),mtx[32],get_sys_clock() - mtx_wait_starttime);
	DEBUG("Maat Abort %ld: %d -- %ld\n",txnMng->get_txn_id(), data->type, data->data->get_primary_key());
	Transaction * txn = txnMng->txn;
	uint64_t off = data->offset;
	uint64_t loc = data->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id() + 1;
	uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
	
#if USE_DBPAOR == true
	uint64_t try_lock;
	row_t * temp_row = txnMng->cas_and_read_remote(yield,try_lock,loc,off,off,0,lock,cor_id);
	while(try_lock != 0 && !simulation->is_done()) {
		// mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
		try_lock = txnMng->cas_remote_content(yield, loc,off,0,lock, cor_id);
		total_num_atomic_retry++;
	}
#else
    uint64_t try_lock = txnMng->cas_remote_content(yield, loc,off,0,lock, cor_id);
	while(try_lock != 0 && !simulation->is_done()) {
		try_lock = txnMng->cas_remote_content(yield, loc,off,0,lock, cor_id);
		total_num_atomic_retry++;
	}
    // assert(try_lock == 0);
    row_t *temp_row = txnMng->read_remote_row(yield,loc,off, cor_id);
#endif
	assert(temp_row->get_primary_key() == data->data->get_primary_key());

    char *tmp_buf2 = Rdma::get_row_client_memory(thd_id);

	if(data->type == RD || WORKLOAD == TPCC) {
		//uncommitted_reads->erase(txn->get_txn_id());
		//ucread_erase(txn->get_txn_id());
		for(uint64_t i = 0; i < row_set_length; i++) {
			uint64_t last = temp_row->uncommitted_reads[i];
			// assert(i < row_set_length - 1);
			
			if(txnMng->get_txn_id() == 0) break;
			if(last == txnMng->get_txn_id()) {
				temp_row->ucreads_len -= 1;
				temp_row->uncommitted_reads[i] = 0;
				break;
			}
		}
        
		temp_row->_tid_word = 0;
        operate_size = row_t::get_row_size(temp_row->tuple_size);
		// memcpy(tmp_buf2, (char *)temp_row, operate_size);
        assert(txnMng->write_remote_row(yield, loc,operate_size,off,(char *)temp_row, cor_id) == true);
	}

	if(data->type == WR || WORKLOAD == TPCC) {
		for(uint64_t i = 0; i < row_set_length; i++) {
			uint64_t last = temp_row->uncommitted_writes[i];
			// assert(i < row_set_length - 1);
			if(txnMng->get_txn_id() == 0) break;
			if(last == txnMng->get_txn_id()) {
				temp_row->ucwrites_len -= 1;
				temp_row->uncommitted_writes[i] = 0;
				break;
			}
		} 
		temp_row->_tid_word = 0;
        operate_size = row_t::get_row_size(temp_row->tuple_size);
		// memcpy(tmp_buf2, (char *)temp_row, operate_size);
        assert(txnMng->write_remote_row(yield,loc,operate_size,off,(char *)temp_row, cor_id) == true);
		//uncommitted_writes->erase(txn->get_txn_id());
	}
	mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	return Abort;
}

RC RDMA_Maat::remote_commit(yield_func_t &yield, TxnManager * txnMng, Access * data, uint64_t cor_id) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	INC_STATS(txnMng->get_thd_id(),mtx[33],get_sys_clock() - mtx_wait_starttime);
	DEBUG("Maat Commit %ld: %d,%lu -- %ld\n", txnMng->get_txn_id(), data->type, txnMng->get_commit_timestamp(),
			data->data->get_primary_key());
	Transaction * txn = txnMng->txn;
	uint64_t off = data->offset;
	uint64_t loc = data->location;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id() + 1;
	uint64_t operate_size = row_t::get_row_size(ROW_DEFAULT_SIZE);
	uint64_t key;
	RC rc = RCOK;
#if USE_DBPAOR == true
	uint64_t try_lock;
	row_t * temp_row = txnMng->cas_and_read_remote(yield,try_lock,loc,off,off,0,lock,cor_id);
	while(try_lock != 0 && !simulation->is_done()) {
		temp_row = txnMng->cas_and_read_remote(yield,try_lock,loc,off,off,0,lock,cor_id);
		total_num_atomic_retry++;
	}
#else
    uint64_t try_lock = txnMng->cas_remote_content(yield, loc,off,0,lock, cor_id);
	while(try_lock != 0 && !simulation->is_done()) {
		try_lock = txnMng->cas_remote_content(yield, loc,off,0,lock, cor_id);
		total_num_atomic_retry++;
	}
    row_t *temp_row = txnMng->read_remote_row(yield, loc,off, cor_id);
#endif
	assert(temp_row->get_primary_key() == data->data->get_primary_key());

    char *tmp_buf2 = Rdma::get_row_client_memory(thd_id);
	uint64_t txn_commit_ts = txnMng->get_commit_timestamp();

	if(data->type == RD || WORKLOAD == TPCC) {
		if (txn_commit_ts > temp_row->timestamp_last_read) temp_row->timestamp_last_read = txn_commit_ts;
		//ucread_erase(txn->get_txn_id());
		for(uint64_t i = 0; i < row_set_length; i++) {
			uint64_t last = temp_row->uncommitted_reads[i];
			// assert(i < row_set_length - 1);
			if(txnMng->get_txn_id() == 0) break;
			if(last == txnMng->get_txn_id()) {
				temp_row->ucreads_len -= 1;
				temp_row->uncommitted_reads[i] = 0;
				break;
			}
		}
        #if COMMIT_ADJUST
			for(uint64_t i = 0; i < row_set_length; i++) {
				if (temp_row->uncommitted_writes[i] == 0) {
					continue;
				}
				//printf("row->uncommitted_writes has txn: %u\n", _row->uncommitted_writes[i]);
				//exit(0);
				if(txnMng->uncommitted_writes.count(temp_row->uncommitted_writes[i]) == 0) {
					if(temp_row->uncommitted_writes[i] % g_node_cnt == g_node_id) {
						if (!rdma_txn_table.local_is_key(temp_row->uncommitted_writes[i])) {
							continue;
						}
						if(!rdma_txn_table.local_cas_timeNode(yield, txnMng, temp_row->uncommitted_writes[i], cor_id) && !simulation->is_done()) {
							rc = Abort;
						}
						if (!rdma_txn_table.local_is_key(temp_row->uncommitted_writes[i])) {
							rdma_txn_table.local_release_timeNode(txnMng,  temp_row->uncommitted_writes[i]);
							continue;
						}
						uint64_t it_lower = rdma_txn_table.local_get_lower(temp_row->uncommitted_writes[i]);
						if(it_lower <= txn_commit_ts) {
							rdma_txn_table.local_set_lower(txnMng,temp_row->uncommitted_writes[i],txn_commit_ts+1);
							
						}
						rdma_txn_table.local_release_timeNode(txnMng, temp_row->uncommitted_writes[i]);
					} else {
						if(!rdma_txn_table.remote_cas_timeNode(yield, txnMng, temp_row->uncommitted_writes[i], cor_id) && !simulation->is_done()) {
							rc = Abort;
						}
						RdmaTxnTableNode* item = rdma_txn_table.remote_get_timeNode(yield, txnMng, temp_row->uncommitted_writes[i], cor_id);
						if (item->key != temp_row->uncommitted_writes[i]) {
							rdma_txn_table.remote_release_timeNode(yield, txnMng, temp_row->uncommitted_writes[i], cor_id);
							mem_allocator.free(item, sizeof(RdmaTxnTableNode));
							continue;
						}
						uint64_t it_lower = item->lower;
						if(it_lower <= txn_commit_ts) {
							item->lower = txn_commit_ts+1;
							if(temp_row->uncommitted_writes[i] != 0) {
								rdma_txn_table.remote_set_timeNode(yield, txnMng, temp_row->uncommitted_writes[i], item, cor_id);
							}
							
						}
						mem_allocator.free(item, sizeof(RdmaTxnTableNode));
						rdma_txn_table.remote_release_timeNode(yield, txnMng,  temp_row->uncommitted_writes[i], cor_id);
					}
					DEBUG("MAAT forward val set remote lower %ld: %lu\n",temp_row->uncommitted_writes[i],txn_commit_ts+1);
				} 
			}
		#endif
		temp_row->_tid_word = 0;
        operate_size = row_t::get_row_size(temp_row->tuple_size);
		// memcpy(tmp_buf2, (char *)temp_row, operate_size);
        assert(txnMng->write_remote_row(yield,loc,operate_size,off, (char *)temp_row, cor_id) == true);
	}

	if(data->type == WR || WORKLOAD == TPCC) {
		if (txn_commit_ts > temp_row->timestamp_last_write) temp_row->timestamp_last_write = txn_commit_ts;
		for(uint64_t i = 0; i < row_set_length; i++) {
			uint64_t last = temp_row->uncommitted_writes[i];
			// assert(i < row_set_length - 1);
			if(txnMng->get_txn_id() == 0) break;
			if(last == txnMng->get_txn_id()) {
				temp_row->ucwrites_len -= 1;
				temp_row->uncommitted_writes[i] = 0;
				break;
			}
		}
        // temp_row->ucwrites_len -= 1;
		//temp_row->copy(data->data);
		#if COMMIT_ADJUST
			uint64_t lower = rdma_txn_table.local_get_lower(txnMng->get_txn_id());
			for(uint64_t i = 0; i < row_set_length; i++) {
				if (temp_row->uncommitted_writes[i] == 0) {
					continue;
				}
				if(txnMng->uncommitted_writes_y.count(temp_row->uncommitted_writes[i]) == 0) {
					if(temp_row->uncommitted_writes[i] % g_node_cnt == g_node_id) {
						if (!rdma_txn_table.local_is_key(temp_row->uncommitted_writes[i])) {
							continue;
						}
						if(!rdma_txn_table.local_cas_timeNode(yield, txnMng, temp_row->uncommitted_writes[i], cor_id) && !simulation->is_done()) {
							// rc = Abort;
						}
						if (!rdma_txn_table.local_is_key(temp_row->uncommitted_writes[i])) {
							rdma_txn_table.local_release_timeNode(txnMng,  temp_row->uncommitted_writes[i]);
							continue;
						}
						uint64_t it_upper = rdma_txn_table.local_get_upper(temp_row->uncommitted_writes[i]);
						if(it_upper >= txn_commit_ts) {
							rdma_txn_table.local_set_upper(txnMng,temp_row->uncommitted_writes[i],txn_commit_ts-1);
						}
						rdma_txn_table.local_release_timeNode(txnMng, temp_row->uncommitted_writes[i]);
					} else {
						if(!rdma_txn_table.remote_cas_timeNode(yield, txnMng, temp_row->uncommitted_writes[i], cor_id) && !simulation->is_done()) {
							rc = Abort;
						}
						RdmaTxnTableNode* item =  rdma_txn_table.remote_get_timeNode(yield, txnMng, temp_row->uncommitted_writes[i], cor_id);
						if (item->key != temp_row->uncommitted_writes[i]) {
							rdma_txn_table.remote_release_timeNode(yield, txnMng, temp_row->uncommitted_writes[i], cor_id);
							mem_allocator.free(item, sizeof(RdmaTxnTableNode));
							continue;
						}
						uint64_t it_upper = item->upper;
						if(it_upper >= txn_commit_ts) {
							item->upper = txn_commit_ts-1;
							if(temp_row->uncommitted_writes[i] != 0) {
								rdma_txn_table.remote_set_timeNode(yield, txnMng, temp_row->uncommitted_writes[i], item, cor_id);
							}
						}
						mem_allocator.free(item, sizeof(RdmaTxnTableNode));
						rdma_txn_table.remote_release_timeNode(yield, txnMng,  temp_row->uncommitted_writes[i], cor_id);
					}
					DEBUG("MAAT forward val set upper %ld: %lu\n",temp_row->uncommitted_writes[i],txn_commit_ts -1);
				} 
			}
			for(uint64_t i = 0; i < row_set_length; i++) {
				if (temp_row->uncommitted_reads[i] == 0) {
					continue;
				}
				if(txnMng->uncommitted_reads.count(temp_row->uncommitted_reads[i]) == 0) {
					if(temp_row->uncommitted_reads[i] % g_node_cnt == g_node_id) {
						if (!rdma_txn_table.local_is_key(temp_row->uncommitted_reads[i])) {
							continue;
						}
						if(!rdma_txn_table.local_cas_timeNode(yield, txnMng, temp_row->uncommitted_reads[i], cor_id) && !simulation->is_done()) {
							rc = Abort;
						}
						if (!rdma_txn_table.local_is_key(temp_row->uncommitted_reads[i])) {
							rdma_txn_table.local_release_timeNode(txnMng,  temp_row->uncommitted_reads[i]);
							continue;
						}
						uint64_t it_upper = rdma_txn_table.local_get_upper(temp_row->uncommitted_reads[i]);
						if(it_upper >= lower) {
							rdma_txn_table.local_set_upper(txnMng,temp_row->uncommitted_reads[i],lower-1);
						}
						rdma_txn_table.local_release_timeNode(txnMng, temp_row->uncommitted_reads[i]);
					} else {
						if(!rdma_txn_table.remote_cas_timeNode(yield, txnMng, temp_row->uncommitted_reads[i], cor_id) && !simulation->is_done()) {
							rc = Abort;
						}
						RdmaTxnTableNode* item = rdma_txn_table.remote_get_timeNode(yield, txnMng, temp_row->uncommitted_reads[i], cor_id);
						if (item->key != temp_row->uncommitted_reads[i]) {
							rdma_txn_table.remote_release_timeNode(yield, txnMng, temp_row->uncommitted_reads[i], cor_id);
							mem_allocator.free(item, sizeof(RdmaTxnTableNode));
							continue;
						}
						uint64_t it_upper = item->upper;
						if(it_upper >= lower) {
							item->upper = lower-1;
							if(temp_row->uncommitted_reads[i] != 0) {
								rdma_txn_table.remote_set_timeNode(yield, txnMng, temp_row->uncommitted_reads[i], item, cor_id);
							}
							// rdma_txn_table.remote_set_timeNode(yield, txnMng, temp_row->uncommitted_reads[i], item, cor_id);
						}
						mem_allocator.free(item, sizeof(RdmaTxnTableNode));
						rdma_txn_table.remote_release_timeNode(yield, txnMng,  temp_row->uncommitted_reads[i], cor_id);
					}
					DEBUG("MAAT forward val set upper %ld: %lu\n",temp_row->uncommitted_reads[i],lower -1);
				} 
			}
		#endif
		temp_row->_tid_word = 0;
        operate_size = row_t::get_row_size(temp_row->tuple_size);
		// memcpy(tmp_buf2, (char *)temp_row, operate_size);
		assert(txnMng->write_remote_row(yield,loc,operate_size,off,(char *)temp_row, cor_id) == true);
		//uncommitted_writes->erase(txn->get_txn_id());
	}
	mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	return rc;
}
#endif

#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_MAAT || CC_ALG == RDMA_WOUND_WAIT || CC_ALG == RDMA_TS || CC_ALG == RDMA_TS1
void RdmaTxnTable::init() {
	table_size = (RDMA_TXNTABLE_MAX)*(sizeof(RdmaTxnTableNode)+1);
	assert(table_size < rdma_txntable_size);
	DEBUG_M("RdmaTxnTable::init table alloc\n");
	table = (RdmaTxnTableNode*)rdma_txntable_buffer; 

	//printf("%d",table[0].key);
	uint64_t i = 0;
	#if CC_ALG == RDMA_MAAT
		for( i = 0; i < RDMA_TXNTABLE_MAX; i++) {
			table[i].init();
		}
	#else
		for( i = 0; i < RDMA_TXNTABLE_MAX; i++) {
			table[i].init(i);
		}		
	#endif
	// printf("%d",table[200].key);
	// printf("init %ld rdmaTxnTable\n", i);

}
uint64_t RdmaTxnTable::hash(uint64_t key) { return key % table_size; }

void RdmaTxnTable::init(uint64_t thd_id, uint64_t key) {
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	table[index].init(key);
}

void RdmaTxnTable::release(uint64_t thd_id, uint64_t key) {
	//table[key]._lock = g_node_id;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	#if CC_ALG == RDMA_MAAT
		table[index].release(key);
		// lower = 0;
		// table[index].upper = UINT64_MAX;
		// table[index].key = key;
		// table[index].state = MAAT_RUNNING;
	#endif
	#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
		table[index].key = key;
		table[index].state = WOUND_RUNNING;
	#endif
	#if CC_ALG == RDMA_TS
		table[index].key = key;
		table[index].state = TS_RUNNING;
	#endif
	//table[key]._lock = 0;
}

#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
WOUNDState RdmaTxnTable::local_get_state(uint64_t thd_id, uint64_t key) {
	WOUNDState state = WOUND_RUNNING;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	state = table[index].state;   
	// printf("index: %ld, key: %ld\n", index, table[index].key);
	//table[key]._lock = 0;
	return state;
}
bool RdmaTxnTable::local_set_state(TxnManager *txnMng, uint64_t thd_id, uint64_t key, WOUNDState value) {
	uint64_t node_id = key % g_node_cnt;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	uint64_t timenode_addr = (char*)(&table[index]) - rdma_global_buffer + sizeof(uint64_t) * 2;
	// table[index].state = value;

	return txnMng->cas_remote_content(node_id,timenode_addr,WOUND_RUNNING,value);
}
char * RdmaTxnTable::remote_get_state(yield_func_t &yield, TxnManager *txnMng, uint64_t key, uint64_t cor_id) {
	uint64_t node_id = key % g_node_cnt;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	
	// todo: here we need to get the corresponding index
	// uint64_t index_key = 0;
	// printf("init table index: %ld\n", key);
	uint64_t timenode_addr = (index) * sizeof(RdmaTxnTableNode) + (rdma_buffer_size - rdma_txntable_size);
	
	uint64_t timenode_size = sizeof(RdmaTxnTableNode);
	// each thread uses only its own piece of client memory address
    uint64_t thd_id = txnMng->get_thd_id();
    char * item = txnMng->read_remote_txntable(yield,node_id,timenode_addr,cor_id);
	// printf("WOUNDState:%ld\n", value);
	return item; 
}
bool RdmaTxnTable::remote_set_state(yield_func_t &yield, TxnManager *txnMng, uint64_t key, WOUNDState value, uint64_t cor_id) {
	uint64_t node_id = key % g_node_cnt;
	// assert(node_id != g_node_id);
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	// todo: here we need to get the corresponding index
	// uint64_t index_key = 0;
	uint64_t timenode_addr = (index) * sizeof(RdmaTxnTableNode) + (rdma_buffer_size - rdma_txntable_size) + sizeof(uint64_t) * 2;
	uint64_t timenode_size = sizeof(RdmaTxnTableNode);
	// each thread uses only its own piece of client memory address
    uint64_t thd_id = txnMng->get_thd_id();
 
    assert(txnMng->write_remote_row(yield,node_id,timenode_size,timenode_addr,(char *)&value,cor_id) == true);
	return true;
}
#endif

#if CC_ALG == RDMA_TS
TSState RdmaTxnTable::local_get_state(uint64_t thd_id, uint64_t key) {
	TSState state = TS_RUNNING;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	state = table[index].state;   
	// printf("index: %ld, key: %ld\n", index, table[index].key);
	//table[key]._lock = 0;
	return state;
}
void RdmaTxnTable::local_set_state(uint64_t thd_id, uint64_t key, TSState value) {
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	table[index].state = value;
}
char * RdmaTxnTable::remote_get_state(yield_func_t &yield, TxnManager *txnMng, uint64_t key, uint64_t cor_id) {
	uint64_t node_id = key % g_node_cnt;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	
	// todo: here we need to get the corresponding index
	// uint64_t index_key = 0;
	// printf("init table index: %ld\n", key);
	uint64_t timenode_addr = (index) * sizeof(RdmaTxnTableNode) + (rdma_buffer_size - rdma_txntable_size);
	uint64_t timenode_size = sizeof(RdmaTxnTableNode);
	// each thread uses only its own piece of client memory address
    uint64_t thd_id = txnMng->get_thd_id();
    char * item = txnMng->read_remote_txntable(yield,node_id,timenode_addr,cor_id);
	// printf("WOUNDState:%ld\n", value);
	return item; 
}
void RdmaTxnTable::remote_set_state(yield_func_t &yield, TxnManager *txnMng, uint64_t key, RdmaTxnTableNode * value, uint64_t cor_id) {
	uint64_t node_id = key % g_node_cnt;
	assert(node_id != g_node_id);
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	// todo: here we need to get the corresponding index
	// uint64_t index_key = 0;
	uint64_t timenode_addr = (index) * sizeof(RdmaTxnTableNode) + (rdma_buffer_size - rdma_txntable_size);
	uint64_t timenode_size = sizeof(RdmaTxnTableNode);
	// each thread uses only its own piece of client memory address
    uint64_t thd_id = txnMng->get_thd_id();
    // uint64_t try_lock = txnMng->cas_remote_content(yield,node_id,timenode_addr,0,key,cor_id);

    assert(txnMng->write_remote_row(yield,node_id,timenode_size,timenode_addr,(char *)value,cor_id) == true);
}
#endif

#if CC_ALG == RDMA_TS1
TSState RdmaTxnTable::local_get_state(uint64_t thd_id, uint64_t key) {
	TSState state = TS_RUNNING;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;/
	int i = 0;
	for (i = 0; i < RDMA_TSSTATE_COUNT; i++) {
		if (table[index].key[i] == key) break;
	}
	if (i == RDMA_TSSTATE_COUNT) return TS_ABORTING;
	state = table[index].state[i];   
	return state;
}

void RdmaTxnTable::local_set_state(uint64_t thd_id, uint64_t key, TSState value) {
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	uint64_t min_key = 0, min_idx = 0;
	int i = 0;
	for (i = 0; i < RDMA_TSSTATE_COUNT; i++) {
		if (table[index].key[i] == key) break;
	}
	if (i == RDMA_TSSTATE_COUNT) return;
	table[index].state[i] = value;
}

TSState RdmaTxnTable::remote_get_state(yield_func_t &yield, TxnManager *txnMng, uint64_t key, uint64_t cor_id) {
	uint64_t node_id = key % g_node_cnt;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	
	// todo: here we need to get the corresponding index
	// uint64_t index_key = 0;
	// printf("init table index: %ld\n", key);
	uint64_t timenode_addr = (index) * sizeof(RdmaTxnTableNode) + (rdma_buffer_size - rdma_txntable_size);
	uint64_t timenode_size = sizeof(RdmaTxnTableNode);
	// each thread uses only its own piece of client memory address
    uint64_t thd_id = txnMng->get_thd_id();
    char * item = txnMng->read_remote_txntable(yield,node_id,timenode_addr,cor_id);
	RdmaTxnTableNode* node = (RdmaTxnTableNode*)item;
	int i = 0;
	TSState state;
	for (i = 0; i < RDMA_TSSTATE_COUNT; i++) {
		if (node->key[i] == key) break;
	}
	if (i == RDMA_TSSTATE_COUNT) state = TS_ABORTING;
	else state = node->state[i];  
	mem_allocator.free(item,0);
	return state; 
}
#endif

#if CC_ALG == RDMA_MAAT
bool RdmaTxnTable::local_is_key(uint64_t key) {
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (table[index].nodes[i].key == key) return true;
	}
	return false;
}
uint64_t RdmaTxnTable::local_get_lower(uint64_t key) {
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (table[index].nodes[i].key == key) return table[index].nodes[i].lower;
		// printf("%lu get lower Start %ld: [%lu,%lu]\n",key,table[index].nodes[i].key,table[index].nodes[i].lower,table[index].nodes[i].upper);
	}
	return UINT64_MAX;
}
uint64_t RdmaTxnTable::local_get_upper(uint64_t key) {
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (table[index].nodes[i].key == key) return table[index].nodes[i].upper;
	}
	return 0;
}
bool RdmaTxnTable::local_set_lower(TxnManager *txnMng, uint64_t key, uint64_t value) {
	//table[key]._lock = g_node_id;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (table[index].nodes[i].key == key) {
			table[index].nodes[i].lower = value;
			return true;
		}
	}
	return false;
}
bool RdmaTxnTable::local_set_upper(TxnManager *txnMng, uint64_t key, uint64_t value) {
	//table[key]._lock = g_node_id;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (table[index].nodes[i].key == key) {
			table[index].nodes[i].upper = value;
			return true;
		}
	}
	return false;
}
MAATState RdmaTxnTable::local_get_state(uint64_t key) {
	//table[key]._lock = g_node_id;
	MAATState state = MAAT_ABORTED;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (table[index].nodes[i].key == key) {
			state = table[index].nodes[i].state;
		}
	}
	return state;
}
bool RdmaTxnTable::local_set_state(TxnManager *txnMng, uint64_t key, MAATState value) {
	//table[key]._lock = g_node_id;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (table[index].nodes[i].key == key) {
			table[index].nodes[i].state = value;
			return true;
		}
	}
	return false;
}
RdmaTxnTableNode * RdmaTxnTable::remote_get_timeNode(yield_func_t &yield, TxnManager *txnMng, uint64_t key, uint64_t cor_id) {
	assert(key % g_node_cnt != g_node_id);
	uint64_t node_id = key % g_node_cnt;
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);

	uint64_t timenode_addr = (index) * sizeof(RdmaTxnTableNode) + (rdma_buffer_size - rdma_txntable_size);
	uint64_t timenode_size = sizeof(RdmaTxnTableNode);
	// each thread uses only its own piece of client memory address
    uint64_t thd_id = txnMng->get_thd_id();

    RdmaTxnTableNode * item = txnMng->read_remote_timetable(yield,node_id,timenode_addr,cor_id);
	return item;        
}
bool RdmaTxnTable::remote_is_key(RdmaTxnTableNode * root, uint64_t key) {
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (root->nodes[i].key == key) return true;
	}
	return false;
}
uint64_t RdmaTxnTable::remote_get_lower(RdmaTxnTableNode * root, uint64_t key) {
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (root->nodes[i].key == key) return root->nodes[i].lower;
	}
	return UINT64_MAX;
}
uint64_t RdmaTxnTable::remote_get_upper(RdmaTxnTableNode * root, uint64_t key) {
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (root->nodes[i].key == key) return root->nodes[i].upper;
	}
	return 0;
}
bool RdmaTxnTable::remote_set_lower(RdmaTxnTableNode * root, uint64_t key, uint64_t value) {
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (root->nodes[i].key == key) {
			root->nodes[i].lower = value;
			return true;
		}
	}
	return false;
}
bool RdmaTxnTable::remote_set_upper(RdmaTxnTableNode * root, uint64_t key, uint64_t value) {
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (root->nodes[i].key == key) {
			root->nodes[i].upper = value;
			return true;
		}
	}
	return false;
}
MAATState RdmaTxnTable::remote_get_state(RdmaTxnTableNode * root, uint64_t key) {
	MAATState state = MAAT_ABORTED;
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (root->nodes[i].key == key) {
			state = root->nodes[i].state;
		}
	}
	return state;
}
bool RdmaTxnTable::remote_set_state(RdmaTxnTableNode * root, uint64_t key, MAATState value) {
	//table[key]._lock = g_node_id;
	for (int i = 0; i < MAAT_NODES_COUNT; i++) {
		if (root->nodes[i].key == key) {
			root->nodes[i].state = value;
			return true;
		}
	}
	return false;
}

bool RdmaTxnTable::local_cas_timeNode(yield_func_t &yield,TxnManager *txnMng, uint64_t key, uint64_t cor_id) {
	#if !MAAT_CAS
		return true;
	#else
		uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
		uint64_t timenode_addr = (index) * sizeof(RdmaTxnTableNode) + (rdma_buffer_size - rdma_txntable_size);
		uint64_t res = txnMng->cas_remote_content(yield,g_node_id,timenode_addr,0,txnMng->get_txn_id(),cor_id);
		
		if (res == 0 || res == txnMng->get_txn_id()) {
			return true;
		}
		return false;
	#endif
}
void RdmaTxnTable::local_release_timeNode(TxnManager *txnMng, uint64_t key) {
	#if !MAAT_CAS
		return;
	#else
		uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
		// uint64_t timenode_addr = (index) * sizeof(RdmaTxnTableNode) + (rdma_buffer_size - rdma_txntable_size);
		table[index]._lock = 0;
		// return txnMng->cas_remote_content(yield,g_node_id,timenode_addr,0,txnMng->get_txn_id(),cor_id);
		// printf("release node %d index %lu, txn %lu res %lu\n", g_node_id, index, txnMng->get_txn_id(), table[index]._lock);
	#endif
}
bool RdmaTxnTable::remote_cas_timeNode(yield_func_t &yield,TxnManager *txnMng, uint64_t key, uint64_t cor_id) {
	#if !MAAT_CAS
		return true;
	#else
		uint64_t node_id = key % g_node_cnt;
		// assert(node_id != g_node_id);
		uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
		uint64_t timenode_addr = (index) * sizeof(RdmaTxnTableNode) + (rdma_buffer_size - rdma_txntable_size);
		uint64_t res = txnMng->cas_remote_content(yield,node_id,timenode_addr,0,txnMng->get_txn_id(),cor_id);
		if (res == 0 || res == txnMng->get_txn_id()) {
			return true;
		}
		return false;
	#endif
}
bool RdmaTxnTable::remote_release_timeNode(yield_func_t &yield,TxnManager *txnMng, uint64_t key, uint64_t cor_id) {
	#if !MAAT_CAS
		return true;
	#else
		uint64_t node_id = key % g_node_cnt;
		// assert(node_id != g_node_id);
		uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
		// todo: here we need to get the corresponding index
		// uint64_t index_key = 0;
		uint64_t timenode_addr = (index) * sizeof(RdmaTxnTableNode) + (rdma_buffer_size - rdma_txntable_size);
		uint64_t timenode_size = sizeof(uint64_t);
		// each thread uses only its own piece of client memory address
		uint64_t thd_id = txnMng->get_thd_id();    
		// ::memcpy(test_buf, (char *)value, timenode_size);
		uint64_t lock = 0;
		// assert(txnMng->write_remote_row(yield,node_id,timenode_size,timenode_addr,(char *)lock,cor_id) == true);
		uint64_t res = txnMng->cas_remote_content(yield,node_id,timenode_addr,txnMng->get_txn_id(),0,cor_id);
		return true;
	#endif
}
bool RdmaTxnTable::remote_set_timeNode(yield_func_t &yield, TxnManager *txnMng, uint64_t key, RdmaTxnTableNode * value, uint64_t cor_id) {
	uint64_t node_id = key % g_node_cnt;
	// assert(node_id != g_node_id);
	uint64_t index = get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);//key;//get_cor_id_from_txn_id(key) * g_thread_cnt + get_thd_id_from_txn_id(key);
	// todo: here we need to get the corresponding index
	// uint64_t index_key = 0;
	uint64_t timenode_addr = (index) * sizeof(RdmaTxnTableNode) + (rdma_buffer_size - rdma_txntable_size);
	uint64_t timenode_size = sizeof(RdmaTxnTableNode);
	// each thread uses only its own piece of client memory address
    uint64_t thd_id = txnMng->get_thd_id();    
    value->_lock = 0;
	// ::memcpy(test_buf, (char *)value, timenode_size);

    assert(txnMng->write_remote_row(yield,node_id,timenode_size,timenode_addr,(char *)value,cor_id) == true);
	return true;
}
#endif
#endif