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

#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_rdma.h"
#include "tpcc_const.h"
#include "transport.h"
#include "msg_queue.h"
#include "row_rdma_2pl.h"
#include "message.h"
#include "rdma.h"
#include "src/rdma/sop.hh"
#include "qps/op.hh"
#include "src/sshed.hh"
#include "transport.h"

void TPCCTxnManager::init(uint64_t thd_id, Workload * h_wl) {
	TxnManager::init(thd_id, h_wl);
	_wl = (TPCCWorkload *) h_wl;
	reset();
	TxnManager::reset();
}

void TPCCTxnManager::reset() {
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	state = TPCC_PAYMENT0;
	if(tpcc_query->txn_type == TPCC_PAYMENT) {
		state = TPCC_PAYMENT0;
	}else if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
		state = TPCC_NEWORDER0;
	}
    // if(((TPCCQuery*) query)->txn_type == TPCC_PAYMENT) {
	// 	state = TPCC_PAYMENT0;
	// }else if (((TPCCQuery*) query)->txn_type == TPCC_NEW_ORDER) {
	// 	state = TPCC_NEWORDER0;
	// }
	next_item_id = 0;
	TxnManager::reset();
}

RC TPCCTxnManager::run_txn_post_wait() {
	uint64_t starttime = get_sys_clock();
	get_row_post_wait(row);
	uint64_t curr_time = get_sys_clock();
	txn_stats.process_time += curr_time - starttime;
	txn_stats.process_time_short += curr_time - starttime;

	next_tpcc_state();
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - curr_time);
	return RCOK;
}

RC TPCCTxnManager::run_txn(yield_func_t &yield, uint64_t cor_id) {
#if MODE == SETUP_MODE
	return RCOK;
#endif
	RC rc = RCOK;
	uint64_t starttime = get_sys_clock();

#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	rc = run_calvin_txn(yield, cor_id);//run_calvin_txn();
	return rc;
#endif

	if(IS_LOCAL(txn->txn_id) && (state == TPCC_PAYMENT0 || state == TPCC_NEWORDER0)) {
		DEBUG("Running txn %ld\n",txn->txn_id);
#if DISTR_DEBUG
		query->print();
#endif
		query->partitions_touched.add_unique(GET_PART_ID(0,g_node_id));
	}


	while(rc == RCOK && !is_done()) {
		rc = run_txn_state(yield, cor_id);
	}

	uint64_t curr_time = get_sys_clock();
	txn_stats.process_time += curr_time - starttime;
	txn_stats.process_time_short += curr_time - starttime;

	if(IS_LOCAL(get_txn_id())) {
		if(is_done() && rc == RCOK)
			rc = start_commit(yield, cor_id);
		else if(rc == Abort)
			rc = start_abort(yield, cor_id);
	}

	return rc;

}

bool TPCCTxnManager::is_done() {
	bool done = false;
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	switch(tpcc_query->txn_type) {
		case TPCC_PAYMENT:
			//done = state == TPCC_PAYMENT5 || state == TPCC_FIN;
			done = state == TPCC_FIN;
			break;
		case TPCC_NEW_ORDER:
			//done = next_item_id == tpcc_query->ol_cnt || state == TPCC_FIN;
			done = next_item_id >= tpcc_query->items.size() || state == TPCC_FIN;
			break;
		default:
			assert(false);
	}

	return done;
}

RC TPCCTxnManager::acquire_locks() {
	uint64_t starttime = get_sys_clock();
	assert(CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN);
	locking_done = false;
	RC rc = RCOK;
	RC rc2;
	INDEX * index;
	itemid_t * item;
	row_t* row;
	uint64_t key;
	incr_lr();
	TPCCQuery* tpcc_query = (TPCCQuery*) query;

	uint64_t w_id = tpcc_query->w_id;
	uint64_t d_id = tpcc_query->d_id;
	uint64_t c_id = tpcc_query->c_id;
	uint64_t d_w_id = tpcc_query->d_w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t c_d_id = tpcc_query->c_d_id;
	char * c_last = tpcc_query->c_last;
	uint64_t part_id_w = wh_to_part(w_id);
	uint64_t part_id_c_w = wh_to_part(c_w_id);
	switch(tpcc_query->txn_type) {
		case TPCC_PAYMENT:
			if(GET_NODE_ID(part_id_w) == g_node_id) {
			// WH
				index = _wl->i_warehouse;
				item = index_read(index, w_id, part_id_w);
				row_t * row = ((row_t *)item->location);
				rc2 = get_lock(row,g_wh_update? WR:RD);
				if (rc2 != RCOK) rc = rc2;

			// Dist
				key = distKey(d_id, d_w_id);
				item = index_read(_wl->i_district, key, part_id_w);
				row = ((row_t *)item->location);
				rc2 = get_lock(row, WR);
				if (rc2 != RCOK) rc = rc2;
			}
			if(GET_NODE_ID(part_id_c_w) == g_node_id) {
			// Cust
				if (tpcc_query->by_last_name) {

					//key = custNPKey(c_last, c_d_id, c_w_id);
                    key = custKey(c_id, c_d_id, c_w_id);
					index = _wl->i_customer_last;
					item = index_read(index, key, part_id_c_w);
					int cnt = 0;
					itemid_t * it = item;
					itemid_t * mid = item;
					while (it != NULL) {
						cnt ++;
						it = it->next;
						if (cnt % 2 == 0) mid = mid->next;
					}
					row = ((row_t *)mid->location);

				} else {
					key = custKey(c_id, c_d_id, c_w_id);
					index = _wl->i_customer_id;
					item = index_read(index, key, part_id_c_w);
					row = (row_t *) item->location;
				}
				rc2  = get_lock(row, WR);
				if (rc2 != RCOK) rc = rc2;
			}
			break;
		case TPCC_NEW_ORDER:
			if(GET_NODE_ID(part_id_w) == g_node_id) {
			// WH
				index = _wl->i_warehouse;
				item = index_read(index, w_id, part_id_w);
				row_t * row = ((row_t *)item->location);
				rc2 = get_lock(row,RD);
				if (rc2 != RCOK) rc = rc2;
			// Cust
				index = _wl->i_customer_id;
				key = custKey(c_id, d_id, w_id);
				item = index_read(index, key, wh_to_part(w_id));
				row = (row_t *) item->location;
				rc2 = get_lock(row, RD);
				if (rc2 != RCOK) rc = rc2;
			// Dist
				key = distKey(d_id, w_id);
				item = index_read(_wl->i_district, key, wh_to_part(w_id));
				row = ((row_t *)item->location);
				rc2 = get_lock(row, WR);
				if (rc2 != RCOK) rc = rc2;
			}
			// Items
				for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
				if (GET_NODE_ID(wh_to_part(tpcc_query->items[i]->ol_supply_w_id)) != g_node_id) continue;

					key = tpcc_query->items[i]->ol_i_id;
					item = index_read(_wl->i_item, key, 0);
					row = ((row_t *)item->location);
					rc2 = get_lock(row, RD);
				if (rc2 != RCOK) rc = rc2;
					key = stockKey(tpcc_query->items[i]->ol_i_id, tpcc_query->items[i]->ol_supply_w_id);
					index = _wl->i_stock;
					item = index_read(index, key, wh_to_part(tpcc_query->items[i]->ol_supply_w_id));
					row = ((row_t *)item->location);
					rc2 = get_lock(row, WR);
				if (rc2 != RCOK) rc = rc2;
				}
			break;
		default:
			assert(false);
	}
	if(decr_lr() == 0) {
		if (ATOM_CAS(lock_ready, false, true)) rc = RCOK;
	}
	txn_stats.wait_starttime = get_sys_clock();
	locking_done = true;
	INC_STATS(get_thd_id(),calvin_sched_time,get_sys_clock() - starttime);
	return rc;
}


void TPCCTxnManager::next_tpcc_state() {
	//TPCCQuery* tpcc_query = (TPCCQuery*) query;

	switch(state) {
		case TPCC_PAYMENT_S:
			state = TPCC_PAYMENT0;
			break;
		case TPCC_PAYMENT0:
			state = TPCC_PAYMENT1;
			break;
		case TPCC_PAYMENT1:
			state = TPCC_PAYMENT2;
			break;
		case TPCC_PAYMENT2:
			state = TPCC_PAYMENT3;
			break;
		case TPCC_PAYMENT3:
			state = TPCC_PAYMENT4;
			break;
		case TPCC_PAYMENT4:
			state = TPCC_PAYMENT5;
			break;
		case TPCC_PAYMENT5:
			state = TPCC_FIN;
			break;
		case TPCC_NEWORDER_S:
			state = TPCC_NEWORDER0;
			break;
		case TPCC_NEWORDER0:
			state = TPCC_NEWORDER1;
			break;
		case TPCC_NEWORDER1:
			state = TPCC_NEWORDER2;
			break;
		case TPCC_NEWORDER2:
			state = TPCC_NEWORDER3;
			break;
		case TPCC_NEWORDER3:
			state = TPCC_NEWORDER4;
			break;
		case TPCC_NEWORDER4:
			state = TPCC_NEWORDER5;
			break;
		case TPCC_NEWORDER5:
			if(!IS_LOCAL(txn->txn_id) || !is_done()) {
				state = TPCC_NEWORDER6;
			} else {
				state = TPCC_FIN;
			}
			break;
		case TPCC_NEWORDER6: // loop pt 1
			state = TPCC_NEWORDER7;
			break;
		case TPCC_NEWORDER7:
			state = TPCC_NEWORDER8;
			break;
		case TPCC_NEWORDER8: // loop pt 2
			state = TPCC_NEWORDER9;
			break;
		case TPCC_NEWORDER9:
			++next_item_id;
			if(!IS_LOCAL(txn->txn_id) || !is_done()) {
				state = TPCC_NEWORDER6;
			} else {
				state = TPCC_FIN;
			}
			break;
		case TPCC_FIN:
			break;
		default:
			assert(false);
	}

}

bool TPCCTxnManager::is_local_item(uint64_t idx) {
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	uint64_t ol_supply_w_id = tpcc_query->items[idx]->ol_supply_w_id;
	uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
	return GET_NODE_ID(part_id_ol_supply_w) == g_node_id;
}


RC TPCCTxnManager::send_remote_request() {
	assert(IS_LOCAL(get_txn_id()));
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	TPCCRemTxnType next_state = TPCC_FIN;
	uint64_t w_id = tpcc_query->w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t dest_node_id = UINT64_MAX;
	if(state == TPCC_PAYMENT0) {
		dest_node_id = GET_NODE_ID(wh_to_part(w_id));
		next_state = TPCC_PAYMENT2;
	} else if(state == TPCC_PAYMENT4) {
		dest_node_id = GET_NODE_ID(wh_to_part(c_w_id));
		next_state = TPCC_FIN;
	} else if(state == TPCC_NEWORDER0) {
		dest_node_id = GET_NODE_ID(wh_to_part(w_id));
		next_state = TPCC_NEWORDER6;
	} else if(state == TPCC_NEWORDER8) {
		dest_node_id = GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
		/*
		while(GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id)) != dest_node_id)
		{
			msg->items.add(tpcc_query->items[next_item_id++]);
		}
		*/
		if(is_done())
			next_state = TPCC_FIN;
		else
			next_state = TPCC_NEWORDER6;
	} else {
		assert(false);
	}
	TPCCQueryMessage * msg = (TPCCQueryMessage*)Message::create_message(this,RQRY);
	msg->state = state;
	query->partitions_touched.add_unique(GET_PART_ID(0,dest_node_id));
#if USE_RDMA == CHANGE_MSG_QUEUE
    tport_man.rdma_thd_send_msg(get_thd_id(), dest_node_id, msg);
#else
	msg_queue.enqueue(get_thd_id(),msg,dest_node_id);
#endif
	state = next_state;
	return WAIT_REM;
}

void TPCCTxnManager::copy_remote_items(TPCCQueryMessage * msg) {
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	msg->items.init(tpcc_query->items.size());
	if (tpcc_query->txn_type == TPCC_PAYMENT) return;
	uint64_t dest_node_id = GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
	while (next_item_id < tpcc_query->items.size() && !is_local_item(next_item_id) &&
				 GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id)) == dest_node_id) {
		Item_no * req = (Item_no*) mem_allocator.alloc(sizeof(Item_no));
		req->copy(tpcc_query->items[next_item_id++]);
		msg->items.add(req);
	}
}
itemid_t* TPCCTxnManager::tpcc_read_remote_index(TPCCQuery * query) {
    TPCCQuery* tpcc_query = (TPCCQuery*) query;
	uint64_t w_id = tpcc_query->w_id;
	uint64_t d_id = tpcc_query->d_id;
	uint64_t c_id = tpcc_query->c_id;
	uint64_t d_w_id = tpcc_query->d_w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t c_d_id = tpcc_query->c_d_id;
    uint64_t ol_i_id = 0;
	uint64_t ol_supply_w_id = 0;
	uint64_t ol_quantity = 0;
	if(tpcc_query->txn_type == TPCC_NEW_ORDER) {
			ol_i_id = tpcc_query->items[next_item_id]->ol_i_id;
			ol_supply_w_id = tpcc_query->items[next_item_id]->ol_supply_w_id;
			ol_quantity = tpcc_query->items[next_item_id]->ol_quantity;
	}

    uint64_t part_id_w = wh_to_part(w_id);
	uint64_t part_id_c_w = wh_to_part(c_w_id);
	uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
	uint64_t w_loc = GET_NODE_ID(part_id_w);
    uint64_t remote_offset = 0;
    int key = 0;
    uint64_t loc = w_loc; 
    switch(state){
        	case TPCC_PAYMENT0 ://operate table WH
                key = w_id/w_loc;
                loc = GET_NODE_ID(wh_to_part(w_id));
                remote_offset = item_index_size + (w_id/w_loc)*sizeof(IndexInfo);
                break;
            case TPCC_PAYMENT4 ://operate table CUSTOMER
                if(query->by_last_name){
                    key = custKey(c_id, c_d_id, c_w_id);
                                                               // return (distKey(c_d_id, c_w_id) * g_cust_per_dist + c_id );
                    remote_offset = item_index_size + wh_index_size + dis_index_size + cust_index_size 
                                    + (key ) * sizeof(IndexInfo);
                }else{
                    key = custKey(c_id, c_d_id, c_w_id);
                                                               // return (distKey(c_d_id, c_w_id) * g_cust_per_dist + c_id );
                    remote_offset = item_index_size + wh_index_size + dis_index_size 
                                    + (key ) * sizeof(IndexInfo);
                }
                loc = GET_NODE_ID(wh_to_part(c_w_id));
                break;
            case TPCC_NEWORDER0 ://operate table WH
                key = w_id/w_loc;
                loc = GET_NODE_ID(wh_to_part(w_id));
                remote_offset = item_index_size + (w_id/w_loc )*sizeof(IndexInfo);
                break;
            case TPCC_NEWORDER8 ://operate table STOCK
                key = stockKey(ol_i_id, ol_supply_w_id);
                loc = GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
                //loc = GET_NODE_ID(part_id_ol_supply_w);
                remote_offset = item_index_size + wh_index_size + dis_index_size + cust_index_size + cl_index_size
                                + (key ) * sizeof(IndexInfo);
                break;
    }
    
	assert(loc != g_node_id);
   // printf("【tpcc.cpp:451】read index key = %ld \n",key);
    itemid_t *item = read_remote_index(loc,remote_offset,key);

	return item;
}

RC TPCCTxnManager::send_remote_one_side_request(TPCCQuery * query,row_t *& row_local){
    RC rc = RCOK;
	// get the index of row to be operated
	itemid_t * m_item;
	m_item = tpcc_read_remote_index(query);

    TPCCQuery* tpcc_query = (TPCCQuery*) query;
	uint64_t w_id = tpcc_query->w_id;
    uint64_t part_id_w = wh_to_part(w_id);
    uint64_t w_loc = GET_NODE_ID(part_id_w);
    uint64_t c_w_id = tpcc_query->c_w_id;

    uint64_t loc = w_loc;
    if(state == TPCC_PAYMENT0) {
		loc = GET_NODE_ID(wh_to_part(w_id));
	} else if(state == TPCC_PAYMENT4) {
		loc = GET_NODE_ID(wh_to_part(c_w_id));
	} else if(state == TPCC_NEWORDER0) {
		loc = GET_NODE_ID(wh_to_part(w_id));
	} else if(state == TPCC_NEWORDER8) {
		loc = GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
    }
	assert(loc != g_node_id);

    uint64_t remote_offset = m_item->offset;

    access_t type;
    uint64_t version = 0;
    uint64_t lock = get_txn_id() + 1;

    if(g_wh_update)type = WR;
    else type == RD;
#if CC_ALG == RDMA_SILO
    //read request
	if(type == RD || type == WR){
        uint64_t tts = get_timestamp();

	//	get remote row
	    row_t * test_row = read_remote_row(loc,remote_offset);

        if(g_wh_update)this->last_type = WR;
        else this->last_type == RD;

        //preserve the txn->access
        RC rc = RCOK;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
		
        return rc;
    } else {
		assert(false);
	}

	return rc;
#endif

#if CC_ALG == RDMA_MVCC
    //read request
	if(type == RD || type == WR){
        uint64_t tts = get_timestamp();
        uint64_t lock = get_txn_id()+1;
        uint64_t test_loc = -1;
        test_loc = cas_remote_content(loc,remote_offset,0,lock);
        if(test_loc != 0){
        INC_STATS(get_thd_id(), lock_fail, 1);
        rc = Abort;
        return rc;
        }

	//	get remote row
	    row_t * test_row = read_remote_row(loc,remote_offset);

        test_loc = cas_remote_content(loc,remote_offset,lock,0);

        if(g_wh_update)this->last_type = WR;
        else this->last_type == RD;

        //preserve the txn->access
        RC rc = RCOK;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
		
        return rc;
    } else {
		assert(false);
	}

	return rc;
#endif

#if CC_ALG == RDMA_NO_WAIT
//read request
	if(type == RD || type == WR){
        uint64_t tts = get_timestamp();
		uint64_t new_lock_info;
		uint64_t lock_info;
remote_atomic_retry_lock:
		if(type == RD){ //read set
			//first rdma read, get lock info of data
            row_t * test_row = read_remote_row(loc,m_item->offset);

			new_lock_info = 0;
			lock_info = test_row->_tid_word;

			bool conflict = Row_rdma_2pl::conflict_lock(lock_info, DLOCK_SH, new_lock_info);

			if(conflict){
				DEBUG_M("TxnManager::get_row(abort) access free\n");
				row_local = NULL;
				txn->rc = Abort;
				mem_allocator.free(m_item, sizeof(itemid_t));
				return Abort;
			}
			if(new_lock_info == 0){
				printf("---thd：%lu, remote lock fail!!!!!!lock location: %lu; %p, txn: %lu, old lock_info: %lu, new_lock_info: %lu\n", get_thd_id(), loc, remote_mr_attr[loc].buf + m_item->offset, get_txn_id(), lock_info, new_lock_info);
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

		if(try_lock != lock_info && type == RD){ //cas fail,for write set elements means The atomicity is destroyed

			num_atomic_retry++;
			total_num_atomic_retry++;
			if(num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = num_atomic_retry;
			goto remote_atomic_retry_lock;

		}	
		if(try_lock != lock_info && type == WR){ //cas fail,for read set elements means already locked
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			row_local = NULL;
			txn->rc = Abort;
			mem_allocator.free(m_item, sizeof(itemid_t));

			return Abort; //cas fail	
		}	

	//	get remote row
	    row_t * test_row = read_remote_row(loc,remote_offset);

        if(g_wh_update)this->last_type = WR;
        else this->last_type == RD;

        //preserve the txn->access
        RC rc = RCOK;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
		
        return rc;
    } else {
		assert(false);
	}

	return rc;
#endif

#if CC_ALG == RDMA_NO_WAIT2
	//read request
	if(type == RD || type == WR){
        uint64_t tts = get_timestamp();

		//lock by RDMA CAS
        uint64_t try_lock = -1;
        try_lock = cas_remote_content(loc,m_item->offset,0,1);

		if(try_lock != 0){ //CAS fail
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			row_local = NULL;
			txn->rc = Abort;
			mem_allocator.free(m_item, sizeof(itemid_t));

			return Abort;
		}

	//	get remote row
	    row_t * test_row = read_remote_row(loc,remote_offset);

        if(g_wh_update)this->last_type = WR;
        else this->last_type == RD;

        //preserve the txn->access
        RC rc = RCOK;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
		
        return rc;
    } else {
		assert(false);
	}

	return rc;
#endif

#if CC_ALG == RDMA_WAIT_DIE2
	//read request
	if(type == RD || type == WR){
        uint64_t tts = get_timestamp();

		//lock by RDMA CAS
retry_lock:
        uint64_t try_lock = -1;
        try_lock = cas_remote_content(loc,m_item->offset,0,tts);

		if(try_lock != 0){ //CAS fail
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
			//	mem_allocator.free(m_item, sizeof(itemid_t));

				return Abort;
			}
		}

	//	get remote row
	    row_t * test_row = read_remote_row(loc,remote_offset);

        if(g_wh_update)this->last_type = WR;
        else this->last_type == RD;

        //preserve the txn->access
        RC rc = RCOK;
        rc = preserve_access(row_local,m_item,test_row,type,test_row->get_primary_key(),loc);
		
        return rc;
    } else {
		assert(false);
	}

	return rc;
#endif

#if CC_ALG == RDMA_TS1


    ts_t ts = get_timestamp();

    uint64_t try_lock = -1;
    try_lock = cas_remote_content(loc,m_item->offset,0,get_txn_id()+1);
	if(try_lock != 0) {
		rc = Abort;
        return rc;
	}

    row_t * remote_row = read_remote_row(loc,m_item->offset);
	//assert(remote_row->get_primary_key() == req->key);

    if(type == RD) {
		if(ts < remote_row->wts || (ts > remote_row->tid && remote_row->tid != 0))rc = Abort;
		else {
			if (remote_row->rts < ts) remote_row->rts = ts;
			rc = RCOK;
		}
	} 
	else if(type == WR) {	
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

        try_lock = cas_remote_content(loc,m_item->offset,get_txn_id()+1 , 0);

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

    rc = preserve_access(row_local,m_item,remote_row,type,remote_row->get_primary_key(),loc);
	
	return rc;
#endif

#if CC_ALG == RDMA_MAAT
    ts_t ts = get_timestamp();

    uint64_t try_lock = -1;
    try_lock = cas_remote_content(loc,m_item->offset,0,get_txn_id()+1);
	if(try_lock != 0) {
		rc = Abort;
        return rc;
	}

    row_t * remote_row = read_remote_row(loc,m_item->offset);
	//assert(remote_row->get_primary_key() == req->key);
    // if(type == RD) {
        if(remote_row->ucreads_len >= row_set_length - 1 || remote_row->ucwrites_len >= row_set_length - 1) {
            try_lock = cas_remote_content(loc,m_item->offset,get_txn_id()+1,0);
            mem_allocator.free(m_item, sizeof(itemid_t));
            return Abort;

        }
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
                remote_row->ucreads_len += 1;
				remote_row->uncommitted_reads[i] = get_txn_id();
				break;
			}
		}
       
	// }else if(type == WR) {
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
				if(in_set == false) {
                    remote_row->ucwrites_len += 1;
                    remote_row->uncommitted_writes[i] = get_txn_id();
                }
				break;
			}
			uncommitted_writes_y.insert(remote_row->uncommitted_writes[i]);
		}
	// }
    remote_row->_tid_word = 0;

    uint64_t operate_size = row_t::get_row_size(remote_row->tuple_size);
    // char *write_buf = Rdma::get_row_client_memory(get_thd_id());
    // memcpy(write_buf, (char*)remote_row, operate_size);

    assert(write_remote_row(loc,operate_size,m_item->offset,(char*)remote_row) == true);
   
    if(rc == Abort) {
        mem_allocator.free(m_item, sizeof(itemid_t));
		return rc;
	}

    rc = preserve_access(row_local,m_item,remote_row,type,remote_row->get_primary_key(),loc);
	
	return rc;
#endif

#if CC_ALG == RDMA_CICADA
    ts_t ts = get_timestamp();

    uint64_t try_lock = -1;
    try_lock = cas_remote_content(loc,m_item->offset,0,get_txn_id()+1);
	if(try_lock != 0) {
		rc = Abort;
        return rc;
	}

    row_t * remote_row = read_remote_row(loc,m_item->offset);
	//assert(remote_row->get_primary_key() == req->key);

    if(type == RD) {
		assert(remote_row->version_cnt >= 0);
		for(int cnt = remote_row->version_cnt; cnt >= remote_row->version_cnt - 4 && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if(remote_row->cicada_version[i].Wts > this->start_ts || remote_row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(remote_row->cicada_version[i].state == Cicada_PENDING) {
				rc = WAIT;
				while(rc == WAIT && !simulation->is_done()) {
                    //release lock
                    try_lock = cas_remote_content(loc,m_item->offset,lock,0);
                    //add lock
                    try_lock = cas_remote_content(loc,m_item->offset,0,lock);
				
					if (try_lock != 0) {
						row_local = NULL;
						txn->rc = Abort;
						mem_allocator.free(m_item, sizeof(itemid_t));
                        mem_allocator.free(remote_row, sizeof(row_t::get_row_size(ROW_DEFAULT_SIZE)));
						printf("RDMA_MAAT cas lock fault\n");
						return Abort; //cas fail
					}

                    remote_row = read_remote_row(loc,m_item->offset);
					//assert(remote_row->get_primary_key() == req->key);

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
    if(type == WR) {
		assert(remote_row->version_cnt >= 0);
		for(int cnt = remote_row->version_cnt ; cnt >= remote_row->version_cnt - 4 && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if(remote_row->cicada_version[i].Wts > this->start_ts || remote_row->cicada_version[i].Rts > this->start_ts || remote_row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(remote_row->cicada_version[i].state == Cicada_PENDING) {
				// --todo !---pendind need wait //
				rc = WAIT;
				while(rc == WAIT && !simulation->is_done()) {
					//release lock
                    try_lock = cas_remote_content(loc,m_item->offset,lock,0);
                    //add lock
                    try_lock = cas_remote_content(loc,m_item->offset,0,lock);

					if (try_lock != 0) {
						row_local = NULL;
						txn->rc = Abort;
						mem_allocator.free(m_item, sizeof(itemid_t));
                        mem_allocator.free(remote_row, sizeof(row_t::get_row_size(ROW_DEFAULT_SIZE)));
						printf("RDMA_MAAT cas lock fault\n");

						return Abort; //cas fail
					}
				    remote_row = read_remote_row(loc,m_item->offset);
					//assert(remote_row->get_primary_key() == req->key);

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

    if(rc == Abort) {
        mem_allocator.free(m_item, sizeof(itemid_t));
        mem_allocator.free(remote_row, sizeof(row_t::get_row_size(ROW_DEFAULT_SIZE)));
		return rc;
	}

    rc = preserve_access(row_local,m_item,remote_row,type,remote_row->get_primary_key(),loc);
	
	return rc;

#endif
}


RC TPCCTxnManager::run_txn_state(yield_func_t &yield, uint64_t cor_id) {
	uint64_t starttime = get_sys_clock();
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	uint64_t w_id = tpcc_query->w_id;
	uint64_t d_id = tpcc_query->d_id;
	uint64_t c_id = tpcc_query->c_id;
	uint64_t d_w_id = tpcc_query->d_w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t c_d_id = tpcc_query->c_d_id;
	char * c_last = tpcc_query->c_last;
	double h_amount = tpcc_query->h_amount;
	bool by_last_name = tpcc_query->by_last_name;
	bool remote = tpcc_query->remote;
	uint64_t ol_cnt = tpcc_query->ol_cnt;
	uint64_t o_entry_d = tpcc_query->o_entry_d;
	uint64_t ol_i_id = 0;
	uint64_t ol_supply_w_id = 0;
	uint64_t ol_quantity = 0;
	if(tpcc_query->txn_type == TPCC_NEW_ORDER) {
			ol_i_id = tpcc_query->items[next_item_id]->ol_i_id;
			ol_supply_w_id = tpcc_query->items[next_item_id]->ol_supply_w_id;
			ol_quantity = tpcc_query->items[next_item_id]->ol_quantity;
	}
	uint64_t ol_number = next_item_id;
	uint64_t ol_amount = tpcc_query->ol_amount;
	uint64_t o_id = tpcc_query->o_id;

	uint64_t part_id_w = wh_to_part(w_id);
	uint64_t part_id_c_w = wh_to_part(c_w_id);
	uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
	bool w_loc = GET_NODE_ID(part_id_w) == g_node_id;
	bool c_w_loc = GET_NODE_ID(part_id_c_w) == g_node_id;
	bool ol_supply_w_loc = GET_NODE_ID(part_id_ol_supply_w) == g_node_id;

	RC rc = RCOK;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	switch (state) {
		case TPCC_PAYMENT0 :
						if(w_loc)
										rc = run_payment_0(yield,w_id, d_id, d_w_id, h_amount, row,cor_id);
                        else if(rdma_one_side()){//rdma_silo
                            rc = send_remote_one_side_request(tpcc_query,row);
                        }
						else {
							rc = send_remote_request();
						}
						break;
		case TPCC_PAYMENT1 :
						rc = run_payment_1(w_id, d_id, d_w_id, h_amount, row);
						break;
		case TPCC_PAYMENT2 :
						rc = run_payment_2(yield,w_id, d_id, d_w_id, h_amount, row,cor_id);
						break;
		case TPCC_PAYMENT3 :
						rc = run_payment_3(w_id, d_id, d_w_id, h_amount, row);
						break;
		case TPCC_PAYMENT4 :
						if(c_w_loc)
								rc = run_payment_4(yield, w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row,cor_id);
						else if(rdma_one_side()){//rdma_silo
                            rc = send_remote_one_side_request(tpcc_query,row);
                        }else {
								rc = send_remote_request();
						}
						break;
		case TPCC_PAYMENT5 :
						rc = run_payment_5( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row);
						break;
		case TPCC_NEWORDER0 :
						if(w_loc)
								rc = new_order_0( yield,w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row,cor_id);
						else if(rdma_one_side()){//rdma_silo
                            rc = send_remote_one_side_request(tpcc_query,row);
                        }else {
								rc = send_remote_request();
						}
			break;
		case TPCC_NEWORDER1 :
						rc = new_order_1( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
						break;
		case TPCC_NEWORDER2 :
						rc = new_order_2( yield,w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row,cor_id);
						break;
		case TPCC_NEWORDER3 :
						rc = new_order_3( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
						break;
		case TPCC_NEWORDER4 :
						rc = new_order_4( yield,w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row,cor_id);
						break;
		case TPCC_NEWORDER5 :
						rc = new_order_5( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
						break;
		case TPCC_NEWORDER6 :
			rc = new_order_6(yield,ol_i_id, row,cor_id);
			break;
		case TPCC_NEWORDER7 :
			rc = new_order_7(ol_i_id, row);
			break;
		case TPCC_NEWORDER8 :
					if(ol_supply_w_loc) {
				rc = new_order_8(yield,w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number, o_id,row,cor_id);
			        }else if(rdma_one_side()){//rdma_silo
                            rc = send_remote_one_side_request(tpcc_query,row);
                    } else {
							rc = send_remote_request();
					}
					break;
		case TPCC_NEWORDER9 :
			rc = new_order_9(w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number,
											 ol_amount, o_id, row);
						break;
		case TPCC_FIN :
				state = TPCC_FIN;
			if (tpcc_query->rbk) 
	            INC_STATS(get_thd_id(),tpcc_fin_abort,get_sys_clock() - starttime);
                return Abort;
						//return finish(tpcc_query,false);
				break;
		default:
				assert(false);
	}
	starttime = get_sys_clock();
	if (rc == RCOK) next_tpcc_state();
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_payment_0(yield_func_t &yield,uint64_t w_id, uint64_t d_id, uint64_t d_w_id,
																				double h_amount, row_t *&r_wh_local,
                                                                                uint64_t cor_id) {

	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;
/*====================================================+
			EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
		WHERE w_id=:w_id;
	+====================================================*/
	/*===================================================================+
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/


	RC rc;
	key = w_id;
	INDEX * index = _wl->i_warehouse;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(index, key, wh_to_part(w_id));
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	if (g_wh_update)
		rc = get_row(yield,r_wh, WR, r_wh_local,cor_id);
	else
		rc = get_row(yield,r_wh, RD, r_wh_local,cor_id);

	return rc;
}

inline RC TPCCTxnManager::run_payment_1(uint64_t w_id, uint64_t d_id, uint64_t d_w_id,
																				double h_amount, row_t *r_wh_local) {

	assert(r_wh_local != NULL);
/*====================================================+
			EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
		WHERE w_id=:w_id;
	+====================================================*/
	/*===================================================================+
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/

	uint64_t starttime = get_sys_clock();
	double w_ytd;
	r_wh_local->get_value(W_YTD, w_ytd);
	if (g_wh_update) {
		r_wh_local->set_value(W_YTD, w_ytd + h_amount);
	}
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::run_payment_2(yield_func_t &yield,uint64_t w_id, uint64_t d_id, uint64_t d_w_id,
																				double h_amount, row_t *&r_dist_local,uint64_t cor_id) {
	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;
	key = distKey(d_id, d_w_id);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	RC rc = get_row(yield,r_dist, WR, r_dist_local,cor_id);
	return rc;
}

inline RC TPCCTxnManager::run_payment_3(uint64_t w_id, uint64_t d_id, uint64_t d_w_id,
																				double h_amount, row_t *r_dist_local) {
	assert(r_dist_local != NULL);
	uint64_t starttime = get_sys_clock();
	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	double d_ytd;
	r_dist_local->get_value(D_YTD, d_ytd);
	r_dist_local->set_value(D_YTD, d_ytd + h_amount);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::run_payment_4(yield_func_t &yield,uint64_t w_id, uint64_t d_id, uint64_t c_id,
																				uint64_t c_w_id, uint64_t c_d_id, char *c_last,
																				double h_amount, bool by_last_name, row_t *&r_cust_local,uint64_t cor_id) {
	/*====================================================================+
		EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
		INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
		FROM district
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+====================================================================*/
	uint64_t starttime = get_sys_clock();
	itemid_t * item;
	uint64_t key;
	row_t * r_cust;
	if (by_last_name) {
		/*==========================================================+
			EXEC SQL SELECT count(c_id) INTO :namecnt
			FROM customer
			WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
		+==========================================================*/
		/*==========================================================================+
			EXEC SQL DECLARE c_byname CURSOR FOR
			SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
			c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
			ORDER BY c_first;
			EXEC SQL OPEN c_byname;
		+===========================================================================*/

		//key = custNPKey(c_last, c_d_id, c_w_id);
        key = custKey(c_id, c_d_id, c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted...
		// The performance won't be much different.
		INDEX * index = _wl->i_customer_last;
		INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
		item = index_read(index, key, wh_to_part(c_w_id));
		starttime = get_sys_clock();
		assert(item != NULL);

		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0) mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);

		/*============================================================================+
			for (n=0; n<namecnt/2; n++) {
				EXEC SQL FETCH c_byname
				INTO :c_first, :c_middle, :c_id,
					 :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
					 :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
			}
			EXEC SQL CLOSE c_byname;
		+=============================================================================*/
		// XXX: we don't retrieve all the info, just the tuple we are interested in
	} else {  // search customers by cust_id
		/*=====================================================================+
			EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
			c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
			c_discount, c_balance, c_since
			INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
			:c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
			:c_discount, :c_balance, :c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+======================================================================*/
		key = custKey(c_id, c_d_id, c_w_id);
		INDEX * index = _wl->i_customer_id;
		INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
		item = index_read(index, key, wh_to_part(c_w_id));
		starttime = get_sys_clock();
		assert(item != NULL);
		r_cust = (row_t *) item->location;
	}
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
		/*======================================================================+
		 	EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
	 		WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
	 	+======================================================================*/
	RC rc  = get_row(yield,r_cust, WR, r_cust_local,cor_id);

	return rc;
}

inline RC TPCCTxnManager::run_payment_5(uint64_t w_id, uint64_t d_id, uint64_t c_id,
																				uint64_t c_w_id, uint64_t c_d_id, char *c_last,
																				double h_amount, bool by_last_name, row_t *r_cust_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_cust_local != NULL);
	double c_balance;
	double c_ytd_payment;
	double c_payment_cnt;

	r_cust_local->get_value(C_BALANCE, c_balance);
	r_cust_local->set_value(C_BALANCE, c_balance - h_amount);
	r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
	r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + h_amount);
	r_cust_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
	r_cust_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

	//char * c_credit = r_cust_local->get_value(C_CREDIT);

	/*=============================================================================+
		EXEC SQL INSERT INTO
		history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
		VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
		+=============================================================================*/
	row_t * r_hist;
	uint64_t row_id;
	// Which partition should we be inserting into?
	_wl->t_history->get_general_new_row(r_hist, wh_to_part(c_w_id), row_id);
	r_hist->set_value(H_C_ID, c_id);
	r_hist->set_value(H_C_D_ID, c_d_id);
	r_hist->set_value(H_C_W_ID, c_w_id);
	r_hist->set_value(H_D_ID, d_id);
	r_hist->set_value(H_W_ID, w_id);
	int64_t date = 2013;
	r_hist->set_value(H_DATE, date);
	r_hist->set_value(H_AMOUNT, h_amount);
	insert_row(r_hist, _wl->t_history);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

// new_order 0
inline RC TPCCTxnManager::new_order_0(yield_func_t &yield,uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *&r_wh_local,
                                                                            uint64_t cor_id) {
	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;
	/*=======================================================================+
	EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
		INTO :c_discount, :c_last, :c_credit, :w_tax
		FROM customer, warehouse
		WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
	+========================================================================*/
	key = w_id;
	INDEX * index = _wl->i_warehouse;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
        // key = w_id/w_loc;
        // remote_offset = (90 * 1024 *1024L) + (w_id/w_loc)*sizeof(IndexInfo);
	item = index_read(index, key, wh_to_part(w_id));
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	RC rc = get_row(yield,r_wh, RD, r_wh_local,cor_id);
	return rc;
}

inline RC TPCCTxnManager::new_order_1(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *r_wh_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_wh_local != NULL);
	double w_tax;
	r_wh_local->get_value(W_TAX, w_tax);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

inline RC TPCCTxnManager::new_order_2(yield_func_t &yield,uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *&r_cust_local,uint64_t cor_id) {
	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;
	key = custKey(c_id, d_id, w_id);
	INDEX * index = _wl->i_customer_id;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(index, key, wh_to_part(w_id));
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_cust = (row_t *) item->location;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	RC rc = get_row(yield,r_cust, RD, r_cust_local,cor_id);
	return rc;
}

inline RC TPCCTxnManager::new_order_3(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *r_cust_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_cust_local != NULL);
	uint64_t c_discount;
	//char * c_last;
	//char * c_credit;
	r_cust_local->get_value(C_DISCOUNT, c_discount);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	//c_last = r_cust_local->get_value(C_LAST);
	//c_credit = r_cust_local->get_value(C_CREDIT);
	return RCOK;
}

inline RC TPCCTxnManager::new_order_4(yield_func_t &yield,uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *&r_dist_local,uint64_t cor_id) {
	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;
	/*==================================================+
	EXEC SQL SELECT d_next_o_id, d_tax
		INTO :d_next_o_id, :d_tax
		FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
	EXEC SQL UPDATE district SET d _next_o_id = :d _next_o_id + 1
		WH ERE d _id = :d_id AN D d _w _id = :w _id ;
	+===================================================*/
	key = distKey(d_id, w_id);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	RC rc = get_row(yield,r_dist, WR, r_dist_local,cor_id);
	return rc;
}

inline RC TPCCTxnManager::new_order_5(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote,
																			uint64_t ol_cnt, uint64_t o_entry_d, uint64_t *o_id,
																			row_t *r_dist_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_dist_local != NULL);
	//double d_tax;
	//int64_t o_id;
	//d_tax = *(double *) r_dist_local->get_value(D_TAX);
	*o_id = *(int64_t *) r_dist_local->get_value(D_NEXT_O_ID);
	(*o_id) ++;
	r_dist_local->set_value(D_NEXT_O_ID, *o_id);

	// return o_id
	/*========================================================================================+
	EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
		VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
	+========================================================================================*/
	row_t * r_order;
	uint64_t row_id;
	_wl->t_order->get_general_new_row(r_order, wh_to_part(w_id), row_id);
	r_order->set_value(O_ID, *o_id);
	r_order->set_value(O_C_ID, c_id);
	r_order->set_value(O_D_ID, d_id);
	r_order->set_value(O_W_ID, w_id);
	r_order->set_value(O_ENTRY_D, o_entry_d);
	r_order->set_value(O_OL_CNT, ol_cnt);
	int64_t all_local = (remote? 0 : 1);
	r_order->set_value(O_ALL_LOCAL, all_local);
	insert_row(r_order, _wl->t_order);
	/*=======================================================+
		EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
				VALUES (:o_id, :d_id, :w_id);
		+=======================================================*/
	row_t * r_no;
	_wl->t_neworder->get_general_new_row(r_no, wh_to_part(w_id), row_id);
	r_no->set_value(NO_O_ID, *o_id);
	r_no->set_value(NO_D_ID, d_id);
	r_no->set_value(NO_W_ID, w_id);
	insert_row(r_no, _wl->t_neworder);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}



// new_order 1
// Read from replicated read-only item table
inline RC TPCCTxnManager::new_order_6(yield_func_t &yield,uint64_t ol_i_id, row_t *& r_item_local,uint64_t cor_id) {
	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;
	/*===========================================+
	EXEC SQL SELECT i_price, i_name , i_data
		INTO :i_price, :i_name, :i_data
		FROM item
		WHERE i_id = :ol_i_id;
	+===========================================*/
	key = ol_i_id;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(_wl->i_item, key, 0);
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_item = ((row_t *)item->location);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	RC rc = get_row(yield,r_item, RD, r_item_local,cor_id);
	return rc;
}

inline RC TPCCTxnManager::new_order_7(uint64_t ol_i_id, row_t * r_item_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_item_local != NULL);
	int64_t i_price;
	//char * i_name;
	//char * i_data;

	r_item_local->get_value(I_PRICE, i_price);
	//i_name = r_item_local->get_value(I_NAME);
	//i_data = r_item_local->get_value(I_DATA);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}

// new_order 2
inline RC TPCCTxnManager::new_order_8(yield_func_t &yield,uint64_t w_id, uint64_t d_id, bool remote, uint64_t ol_i_id,
																			uint64_t ol_supply_w_id, uint64_t ol_quantity,
																			uint64_t ol_number, uint64_t o_id, row_t *&r_stock_local,uint64_t cor_id) {
	uint64_t starttime = get_sys_clock();
	uint64_t key;
	itemid_t * item;

	/*===================================================================+
	EXEC SQL SELECT s_quantity, s_data,
			s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
			s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
		INTO :s_quantity, :s_data,
			:s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
			:s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
		FROM stock
		WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
	EXEC SQL UPDATE stock SET s_quantity = :s_quantity
		WHERE s_i_id = :ol_i_id
		AND s_w_id = :ol_supply_w_id;
	+===============================================*/

	key = stockKey(ol_i_id, ol_supply_w_id);
	INDEX * index = _wl->i_stock;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	item = index_read(index, key, wh_to_part(ol_supply_w_id));
	starttime = get_sys_clock();
	assert(item != NULL);
	row_t * r_stock = ((row_t *)item->location);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	RC rc = get_row(yield,r_stock, WR, r_stock_local,cor_id);
	return rc;
}

inline RC TPCCTxnManager::new_order_9(uint64_t w_id, uint64_t d_id, bool remote, uint64_t ol_i_id,
																			uint64_t ol_supply_w_id, uint64_t ol_quantity,
																			uint64_t ol_number, uint64_t ol_amount, uint64_t o_id,
																			row_t *r_stock_local) {
	uint64_t starttime = get_sys_clock();
	assert(r_stock_local != NULL);
		// XXX s_dist_xx are not retrieved.
	UInt64 s_quantity;
	int64_t s_remote_cnt;
	s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
#if !TPCC_SMALL
	int64_t s_ytd;
	int64_t s_order_cnt;
	char * s_data __attribute__ ((unused));
	r_stock_local->get_value(S_YTD, s_ytd);
	r_stock_local->set_value(S_YTD, s_ytd + ol_quantity);
	// In Coordination Avoidance, this record must be protected!
	r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
	r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
	s_data = r_stock_local->get_value(S_DATA);
#endif
	if (remote) {
		s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
		s_remote_cnt ++;
		r_stock_local->set_value(S_REMOTE_CNT, &s_remote_cnt);
	}
	uint64_t quantity;
	if (s_quantity > ol_quantity + 10) {
		quantity = s_quantity - ol_quantity;
	} else {
		quantity = s_quantity - ol_quantity + 91;
	}
	r_stock_local->set_value(S_QUANTITY, &quantity);

	/*====================================================+
	EXEC SQL INSERT
		INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
			ol_i_id, ol_supply_w_id,
			ol_quantity, ol_amount, ol_dist_info)
		VALUES(:o_id, :d_id, :w_id, :ol_number,
			:ol_i_id, :ol_supply_w_id,
			:ol_quantity, :ol_amount, :ol_dist_info);
	+====================================================*/
	row_t * r_ol;
	uint64_t row_id;
	_wl->t_orderline->get_general_new_row(r_ol, wh_to_part(ol_supply_w_id), row_id);
	r_ol->set_value(OL_O_ID, &o_id);
	r_ol->set_value(OL_D_ID, &d_id);
	r_ol->set_value(OL_W_ID, &w_id);
	r_ol->set_value(OL_NUMBER, &ol_number);
	r_ol->set_value(OL_I_ID, &ol_i_id);
#if !TPCC_SMALL
	r_ol->set_value(OL_SUPPLY_W_ID, &ol_supply_w_id);
	r_ol->set_value(OL_QUANTITY, &ol_quantity);
	r_ol->set_value(OL_AMOUNT, &ol_amount);
#endif
	insert_row(r_ol, _wl->t_orderline);
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return RCOK;
}


RC TPCCTxnManager::run_calvin_txn(yield_func_t &yield, uint64_t cor_id) {
	RC rc = RCOK;
	uint64_t starttime = get_sys_clock();
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	DEBUG("(%ld,%ld) Run calvin txn\n",txn->txn_id,txn->batch_id);
	while(!calvin_exec_phase_done() && rc == RCOK) {
		DEBUG("(%ld,%ld) phase %d\n",txn->txn_id,txn->batch_id,this->phase);
		switch(this->phase) {
			case CALVIN_RW_ANALYSIS:
				// Phase 1: Read/write set analysis
				calvin_expected_rsp_cnt = tpcc_query->get_participants(_wl);
				if(query->participant_nodes[g_node_id] == 1) {
					calvin_expected_rsp_cnt--;
				}

				DEBUG("(%ld,%ld) expects %d responses; %ld participants, %ld active\n", txn->txn_id,
							txn->batch_id, calvin_expected_rsp_cnt, query->participant_nodes.size(),
							query->active_nodes.size());

				this->phase = CALVIN_LOC_RD;
				break;
			case CALVIN_LOC_RD:
				// Phase 2: Perform local reads
				DEBUG("(%ld,%ld) local reads\n",txn->txn_id,txn->batch_id);
				rc = run_tpcc_phase2(yield,  cor_id);
				//release_read_locks(tpcc_query);

				this->phase = CALVIN_SERVE_RD;
				break;
			case CALVIN_SERVE_RD:
				// Phase 3: Serve remote reads
				if(query->participant_nodes[g_node_id] == 1) {
					rc = send_remote_reads();
				}
				if(query->active_nodes[g_node_id] == 1) {
					this->phase = CALVIN_COLLECT_RD;
					if(calvin_collect_phase_done()) {
						rc = RCOK;
					} else {
						assert(calvin_expected_rsp_cnt > 0);
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
				rc = run_tpcc_phase5(yield,cor_id);
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


RC TPCCTxnManager::run_tpcc_phase2(yield_func_t &yield, uint64_t cor_id) {
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	RC rc = RCOK;
	assert(CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN);

	uint64_t w_id = tpcc_query->w_id;
	uint64_t d_id = tpcc_query->d_id;
	uint64_t c_id = tpcc_query->c_id;
	//uint64_t d_w_id = tpcc_query->d_w_id;
	//uint64_t c_w_id = tpcc_query->c_w_id;
	//uint64_t c_d_id = tpcc_query->c_d_id;
	//char * c_last = tpcc_query->c_last;
	//double h_amount = tpcc_query->h_amount;
	//bool by_last_name = tpcc_query->by_last_name;
	bool remote = tpcc_query->remote;
	uint64_t ol_cnt = tpcc_query->ol_cnt;
	uint64_t o_entry_d = tpcc_query->o_entry_d;
	//uint64_t o_id = tpcc_query->o_id;

	uint64_t part_id_w = wh_to_part(w_id);
	//uint64_t part_id_c_w = wh_to_part(c_w_id);
	bool w_loc = GET_NODE_ID(part_id_w) == g_node_id;
	//bool c_w_loc = GET_NODE_ID(part_id_c_w) == g_node_id;


	switch (tpcc_query->txn_type) {
		case TPCC_PAYMENT :
			break;
		case TPCC_NEW_ORDER :
			if(w_loc) {
				rc = new_order_0(yield,w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row,cor_id);
				rc = new_order_1( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_2( yield,w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row,cor_id);
				rc = new_order_3( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_4(yield, w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row,cor_id);
				tpcc_query->o_id = *(int64_t *) row->get_value(D_NEXT_O_ID);
				//rc = new_order_5( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
			}
				for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {

					uint64_t ol_number = i;
					uint64_t ol_i_id = tpcc_query->items[ol_number]->ol_i_id;
					uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
					//uint64_t ol_quantity = tpcc_query->items[ol_number].ol_quantity;
					//uint64_t ol_amount = tpcc_query->ol_amount;
					uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
					bool ol_supply_w_loc = GET_NODE_ID(part_id_ol_supply_w) == g_node_id;
					if(ol_supply_w_loc) {
						rc = new_order_6(yield,ol_i_id, row,cor_id);
						rc = new_order_7(ol_i_id, row);
					}
				}
				break;
		default:
			assert(false);
	}
	return rc;
}

RC TPCCTxnManager::run_tpcc_phase5(yield_func_t &yield, uint64_t cor_id) {
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	RC rc = RCOK;
	assert(CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN);

	uint64_t w_id = tpcc_query->w_id;
	uint64_t d_id = tpcc_query->d_id;
	uint64_t c_id = tpcc_query->c_id;
	uint64_t d_w_id = tpcc_query->d_w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t c_d_id = tpcc_query->c_d_id;
	char * c_last = tpcc_query->c_last;
	double h_amount = tpcc_query->h_amount;
	bool by_last_name = tpcc_query->by_last_name;
	bool remote = tpcc_query->remote;
	uint64_t ol_cnt = tpcc_query->ol_cnt;
	uint64_t o_entry_d = tpcc_query->o_entry_d;
	uint64_t o_id = tpcc_query->o_id;

	uint64_t part_id_w = wh_to_part(w_id);
	uint64_t part_id_c_w = wh_to_part(c_w_id);
	bool w_loc = GET_NODE_ID(part_id_w) == g_node_id;
	bool c_w_loc = GET_NODE_ID(part_id_c_w) == g_node_id;


	switch (tpcc_query->txn_type) {
		case TPCC_PAYMENT :
			if(w_loc) {
				rc = run_payment_0(yield,w_id, d_id, d_w_id, h_amount, row,cor_id);
				rc = run_payment_1(w_id, d_id, d_w_id, h_amount, row);
				rc = run_payment_2(yield,w_id, d_id, d_w_id, h_amount, row,cor_id);
				rc = run_payment_3(w_id, d_id, d_w_id, h_amount, row);
			}
			if(c_w_loc) {
				rc = run_payment_4(yield, w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row,cor_id);
				rc = run_payment_5( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row);
			}
			break;
		case TPCC_NEW_ORDER :
			if(w_loc) {
				//rc = new_order_4( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
				rc = new_order_5( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
			}
				for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {

					uint64_t ol_number = i;
					uint64_t ol_i_id = tpcc_query->items[ol_number]->ol_i_id;
					uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
					uint64_t ol_quantity = tpcc_query->items[ol_number]->ol_quantity;
					uint64_t ol_amount = tpcc_query->ol_amount;
					uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
					bool ol_supply_w_loc = GET_NODE_ID(part_id_ol_supply_w) == g_node_id;
					if(ol_supply_w_loc) {
					rc = new_order_8(yield,w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number,
													 o_id, row,cor_id);
					rc = new_order_9(w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number,
													 ol_amount, o_id, row);
					}
				}
				break;
		default:
			assert(false);
	}
	return rc;
}
