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
#include <unordered_set>
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
#include "work_queue.h"
#include "log_rdma.h"

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
	for(int i = 0; i < g_center_cnt; i++) {
		remote_center[i].clear();
	}
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

#if CC_ALG == CALVIN 
	rc = run_calvin_txn(yield, cor_id);//run_calvin_txn();
	return rc;
#endif

	if(IS_LOCAL(txn->txn_id) && (state == TPCC_PAYMENT0 || state == TPCC_NEWORDER0)) {
		DEBUG("Running txn %ld\n",txn->txn_id);
#if DISTR_DEBUG
		query->print();
#endif
		query->partitions_touched.add_unique(GET_PART_ID(0,g_node_id));
		query->centers_touched.add_unique(g_center_id);
#if PARAL_SUBTXN
		rc = send_remote_subtxn();
		#if USE_TCP_INTRA_CENTER
		rc = send_intra_subtxn();
		#endif
#endif
	} else if (is_executor() && (state == TPCC_PAYMENT0 || state == TPCC_NEWORDER0)){
		query->partitions_touched.add_unique(GET_PART_ID(0,g_node_id));
		#if PARAL_SUBTXN && USE_TCP_INTRA_CENTER
		rc = send_intra_subtxn();
		#endif	
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
	assert(CC_ALG == CALVIN);
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
			#if PARAL_SUBTXN == true && CENTER_MASTER == true
				if(!IS_LOCAL(txn->txn_id) || !is_done()) {
					state = TPCC_NEWORDER6;
				} else {
					state = TPCC_FIN;
				}
			#else 
				state = TPCC_NEWORDER6;
			#endif
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

RC TPCCTxnManager::generate_center_master(uint64_t w_id, access_t type) {
	vector<uint64_t> node_id;
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
#if USE_REPLICA
	if (type == WR) {
		node_id.push_back(GET_NODE_ID(wh_to_part(w_id)));
		node_id.push_back(GET_FOLLOWER1_NODE(wh_to_part(w_id)));
		node_id.push_back(GET_FOLLOWER2_NODE(wh_to_part(w_id)));
	} else {
		node_id.push_back(GET_NODE_ID(wh_to_part(w_id)));
	}
#else
	node_id.push_back(GET_NODE_ID(wh_to_part(w_id)));
#endif
	for(int j=0;j<node_id.size();j++){
		uint64_t center_id = GET_CENTER_ID(node_id[j]);
		remote_center[center_id].push_back(w_id);
		tpcc_query->centers_touched.add_unique(center_id);
		tpcc_query->partitions_touched.add_unique(GET_PART_ID(0,node_id[j]));
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

RC TPCCTxnManager::generate_intra_center_master(uint64_t w_id, access_t type) {
	vector<uint64_t> node_id;
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
#if USE_REPLICA
	if (type == WR) {
		if (GET_NODE_ID(wh_to_part(w_id)) == g_center_id)
			node_id.push_back(GET_NODE_ID(wh_to_part(w_id)));
		if (GET_FOLLOWER1_NODE(wh_to_part(w_id)) == g_center_id)
			node_id.push_back(GET_FOLLOWER1_NODE(wh_to_part(w_id)));
		if (GET_FOLLOWER2_NODE(wh_to_part(w_id)) == g_center_id)
			node_id.push_back(GET_FOLLOWER2_NODE(wh_to_part(w_id)));
	} else {
		if (GET_NODE_ID(wh_to_part(w_id)) == g_center_id)
			node_id.push_back(GET_NODE_ID(wh_to_part(w_id)));
	}
#else
	node_id.push_back(GET_NODE_ID(wh_to_part(w_id)));
#endif
	for(int j=0;j<node_id.size();j++){
		tpcc_query->partitions_touched.add_unique(GET_PART_ID(0,node_id[j]));
	}
}

RC TPCCTxnManager::send_remote_subtxn() {
	assert(IS_LOCAL(get_txn_id()));
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	RC rc = RCOK;
	unordered_set<uint64_t> node_id;

	uint64_t w_id = tpcc_query->w_id;
	generate_center_master(w_id, WR);

	// for payment
	uint64_t d_w_id = tpcc_query->d_w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	if (tpcc_query->txn_type == TPCC_PAYMENT) {
		generate_center_master(d_w_id, WR);
		generate_center_master(c_w_id, WR);
	}
	// for neworder
	if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
		for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
			uint64_t ol_number = i;
			uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
			generate_center_master(ol_supply_w_id, WR);
		}
	}
	rsp_cnt = 0;
	for(int i=0;i<query->centers_touched.size();i++){
		if(is_primary[query->centers_touched[i]]) ++rsp_cnt;
	}
	--rsp_cnt; //exclude this center

	uint64_t center_id1 = GET_CENTER_ID(GET_FOLLOWER1_NODE(wh_to_part(w_id)));
	uint64_t center_id2 = GET_CENTER_ID(GET_FOLLOWER2_NODE(wh_to_part(w_id)));
	if(is_primary[center_id1] || is_primary[center_id2]){
		//no extra wait for req i
		wh_extra_wait[0] = -1;
		wh_extra_wait[1] = -1;			
	}else{
		wh_need_wait = true;
		wh_extra_wait[0] = center_id1;
		wh_extra_wait[1] = center_id2;
	}

	if (tpcc_query->txn_type == TPCC_PAYMENT){
		// customer wait
		center_id1 = GET_CENTER_ID(GET_FOLLOWER1_NODE(wh_to_part(c_w_id)));
		center_id2 = GET_CENTER_ID(GET_FOLLOWER2_NODE(wh_to_part(c_w_id)));
		if(is_primary[center_id1] || is_primary[center_id2]){
			//no extra wait for req i
			cus_extra_wait[0] = -1;
			cus_extra_wait[1] = -1;			
		}else{
			cus_need_wait = true;
			cus_extra_wait[0] = center_id1;
			cus_extra_wait[1] = center_id2;
		}
	}

	if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
		for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
			uint64_t ol_number = i;
			uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
			center_id1 = GET_CENTER_ID(GET_FOLLOWER1_NODE(wh_to_part(ol_supply_w_id)));
			center_id2 = GET_CENTER_ID(GET_FOLLOWER2_NODE(wh_to_part(ol_supply_w_id)));

			if(is_primary[center_id1] || is_primary[center_id2]){
				//no extra wait for req i
				extra_wait[i][0] = -1;
				extra_wait[i][1] = -1;			
			}else{
				req_need_wait[i] = true;
				extra_wait[i][0] = center_id1;
				extra_wait[i][1] = center_id2;
			}
		}
	}

	for(int i = 0; i < g_center_cnt; i++) {
		if(remote_center[i].size() > 0 && i != g_center_id) {//send message to all masters
			msg_queue.enqueue(get_thd_id(),Message::create_message(this,RQRY),center_master[i]);
			// printf("txn %lu, send message to %d\n", get_txn_id(), center_master[i]);
		}
	}
	#if RECOVERY_TXN_MECHANISM
	update_send_time();
	if (!is_enqueue && wait_queue_entry == nullptr) {
		work_queue.waittxn_enqueue(get_thd_id(),Message::create_message(this,WAIT_TXN),wait_queue_entry);
		DEBUG_T("Txn %ld enqueue wait queue.\n",get_txn_id());
		is_enqueue = true;
	} else {
		DEBUG_T("Txn %ld has already enqueue wait queue %ld.\n",get_txn_id(), wait_queue_entry->txn_id);
	}
	
	#endif
	return rc;
}


RC TPCCTxnManager::send_intra_subtxn() {
	assert(IS_LOCAL(get_txn_id()));
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	// TPCCRemTxnType next_state = TPCC_FIN;
	RC rc = RCOK;
	if (is_send_intra_msg) return rc;
	unordered_set<uint64_t> node_id;

	uint64_t w_id = tpcc_query->w_id;
	generate_intra_center_master(w_id, WR);

	// for payment
	uint64_t d_w_id = tpcc_query->d_w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	if (tpcc_query->txn_type == TPCC_PAYMENT) {
		generate_intra_center_master(d_w_id, WR);
		generate_intra_center_master(c_w_id, WR);
	}
	// for neworder
	if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
		for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
			uint64_t ol_number = i;
			uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
			generate_intra_center_master(ol_supply_w_id, WR);
		}
	}
	intra_rsp_cnt = tpcc_query->partitions_touched.size() - 1;

	for(int i = 0; i < tpcc_query->partitions_touched.size(); i++) {
		uint64_t dest_part_id = tpcc_query->partitions_touched[i];
		msg_queue.enqueue(get_thd_id(),Message::create_message(this,RQRY),dest_part_id);
	}
	is_send_intra_msg = true;
	return rc;
}

RC TPCCTxnManager::send_remote_request() {
	assert(IS_LOCAL(get_txn_id()));
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	TPCCRemTxnType next_state = TPCC_FIN;
	uint64_t w_id = tpcc_query->w_id;
	uint64_t c_w_id = tpcc_query->c_w_id;
	uint64_t dest_node_id = UINT64_MAX;
	uint64_t dest_center_id = UINT64_MAX;
	if(state == TPCC_PAYMENT0) {
		dest_node_id = GET_NODE_ID(wh_to_part(w_id));
		dest_center_id = GET_CENTER_ID(wh_to_part(w_id));
		next_state = TPCC_PAYMENT2;
	} else if(state == TPCC_PAYMENT4) {
		dest_node_id = GET_NODE_ID(wh_to_part(c_w_id));
		dest_center_id = GET_CENTER_ID(wh_to_part(c_w_id));
		next_state = TPCC_FIN;
	} else if(state == TPCC_NEWORDER0) {
		dest_node_id = GET_NODE_ID(wh_to_part(w_id));
		dest_center_id = GET_CENTER_ID(wh_to_part(w_id));
		next_state = TPCC_NEWORDER6;
	} else if(state == TPCC_NEWORDER8) {
		dest_node_id = GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
		dest_center_id = GET_CENTER_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
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
	query->centers_touched.add_unique(dest_center_id);
	if(this->center_master.find(dest_center_id) == this->center_master.end()) {
		this->center_master.insert(pair<uint64_t, uint64_t>(dest_center_id, dest_node_id));
	}
#if CENTER_MASTER == true
#if USE_RDMA == CHANGE_MSG_QUEUE
    tport_man.rdma_thd_send_msg(get_thd_id(), this->center_master[dest_center_id], msg);
#else
	msg_queue.enqueue(get_thd_id(),msg,this->center_master[dest_center_id]);
#endif
#else
#if USE_RDMA == CHANGE_MSG_QUEUE
    tport_man.rdma_thd_send_msg(get_thd_id(), dest_node_id, msg);
#else
	msg_queue.enqueue(get_thd_id(),msg,dest_node_id);
#endif
#endif
	state = next_state;
	#if RECOVERY_TXN_MECHANISM
	update_send_time();
	if (!is_enqueue && wait_queue_entry == nullptr) {
		work_queue.waittxn_enqueue(get_thd_id(),Message::create_message(this,WAIT_TXN),wait_queue_entry);
		DEBUG_T("Txn %ld enqueue wait queue.\n",get_txn_id());
		is_enqueue = true;
	}else {
		DEBUG_T("Txn %ld has already enqueue wait queue %ld.\n",get_txn_id(), wait_queue_entry->txn_id);
	}
	
	#endif
	return WAIT_REM;
}

void TPCCTxnManager::copy_remote_items(TPCCQueryMessage * msg) {
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	msg->items.init(tpcc_query->items.size());
#if PARAL_SUBTXN == true && CENTER_MASTER == true
	if (tpcc_query->txn_type == TPCC_PAYMENT) return;
	// uint64_t dest_node_id = GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
	// uint64_t dest_center_id = GET_CENTER_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
	uint64_t index = 0;
	while (index < tpcc_query->items.size()) {
		Item_no * req = (Item_no*) mem_allocator.alloc(sizeof(Item_no));
		req->copy(tpcc_query->items[index++]);
		msg->items.add(req);
	}
#else
	if (tpcc_query->txn_type == TPCC_PAYMENT) return;
	uint64_t dest_node_id = GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
	uint64_t dest_center_id = GET_CENTER_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
#if CENTER_MASTER == true
	while (next_item_id < tpcc_query->items.size() && !is_local_item(next_item_id) &&
				 GET_CENTER_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id)) == dest_center_id) {
#else
	while (next_item_id < tpcc_query->items.size() && !is_local_item(next_item_id) &&
				 GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id)) == dest_node_id) {
#endif
		Item_no * req = (Item_no*) mem_allocator.alloc(sizeof(Item_no));
		req->copy(tpcc_query->items[next_item_id++]);
		msg->items.add(req);
	}
#endif
}

RC TPCCTxnManager::tpcc_read_remote_index(yield_func_t &yield, TPCCQuery * query, itemid_t* &item, uint64_t cor_id) {
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
    // itemid_t *item;
	RC rc = read_remote_index(yield, loc,remote_offset,key,item,cor_id);

	return rc;
}

RC TPCCTxnManager::send_remote_one_side_request(yield_func_t &yield, TPCCQuery * query,row_t *& row_local, uint64_t cor_id){
    RC rc = RCOK;
	// get the index of row to be operated
	itemid_t * m_item;
	rc = tpcc_read_remote_index(yield, query, m_item, cor_id);
	if (rc != RCOK) {
		rc = rc == NODE_FAILED ? Abort : rc;
		return rc;
	}
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

    if (g_wh_update) type = WR;
    else type = RD;
	rc = get_remote_row(yield, type, loc, m_item, row_local, cor_id);
	query->partitions_touched.add_unique(GET_PART_ID(0,loc));
	// mem_allocator.free(m_item, sizeof(itemid_t));
	return rc;
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
	uint32_t w_cen = GET_CENTER_ID(part_id_w);
	uint32_t c_w_cen = GET_CENTER_ID(part_id_c_w);
	uint32_t ol_supply_w_cen = GET_CENTER_ID(part_id_ol_supply_w);

	RC rc = RCOK;
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	switch (state) {
		case TPCC_PAYMENT0 :
			if(w_loc)
				rc = run_payment_0(yield,w_id, d_id, d_w_id, h_amount, row,cor_id);
			else {
#if PARAL_SUBTXN == true && CENTER_MASTER == true
				rc = RCOK;
#else
				rc = send_remote_request();
#endif
			}
			break;
		case TPCC_PAYMENT1 :
			if(w_loc){
				rc = run_payment_1(w_id, d_id, d_w_id, h_amount, row);
			}
			break;
		case TPCC_PAYMENT2 :
			if(w_loc){
				rc = run_payment_2(yield,w_id, d_id, d_w_id, h_amount, row,cor_id);
			}
			break;
		case TPCC_PAYMENT3 :
			if(w_loc){
				rc = run_payment_3(w_id, d_id, d_w_id, h_amount, row);
			}
			break;
		case TPCC_PAYMENT4 :
			if(c_w_loc)
				rc = run_payment_4(yield, w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row,cor_id);
#if PARAL_SUBTXN == true
			else if(rdma_one_side() && c_w_cen == g_center_id && g_node_id == center_master[c_w_cen]){//rdma_silo
#else
			else if(rdma_one_side() && w_cen == g_center_id){//rdma_silo
#endif
				#if USE_TCP_INTRA_CENTER
				// if (is_executor())rc = send_remote_request();
				#else
				rc = send_remote_one_side_request(yield,tpcc_query,row,cor_id);
				#endif
			}else {
#if PARAL_SUBTXN == true && CENTER_MASTER == true
				rc = RCOK;
#else
				rc = send_remote_request();
#endif
			}
			break;
		case TPCC_PAYMENT5 :
			#if USE_TCP_INTRA_CENTER
			if(c_w_loc){
			#else
			if(c_w_cen == g_center_id){
			#endif
				rc = run_payment_5( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row);
			}
			break;
		case TPCC_NEWORDER0 :
			if(w_loc)
				rc = new_order_0( yield,w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row,cor_id);
			else {
#if PARAL_SUBTXN == true && CENTER_MASTER == true
				rc = RCOK;
#else
				rc = send_remote_request();
#endif
			}
			break;
		case TPCC_NEWORDER1 :
			if(w_loc) {
				rc = new_order_1( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
			}
			break;
		case TPCC_NEWORDER2 :
			if(w_loc) {
				rc = new_order_2( yield,w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row,cor_id);
			}
			break;
		case TPCC_NEWORDER3 :
			if(w_loc) {
				rc = new_order_3( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
			}
			break;
		case TPCC_NEWORDER4 :
			if(w_loc) {
				rc = new_order_4( yield,w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row,cor_id);
			}
			break;
		case TPCC_NEWORDER5 :
			if(w_loc) {
				rc = new_order_5( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
			}
			break;
		case TPCC_NEWORDER6 :
			if(w_loc) {
				rc = new_order_6(yield,ol_i_id, row,cor_id);
			}
			break;
		case TPCC_NEWORDER7 :
			if(w_loc) {
				rc = new_order_7(ol_i_id, row);
			}
			break;
		case TPCC_NEWORDER8 :
			if(ol_supply_w_loc) {
				rc = new_order_8(yield,w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number, o_id,row,cor_id);
			}
#if PARAL_SUBTXN == true
			else if(rdma_one_side() && ol_supply_w_cen == g_center_id && g_node_id == center_master[ol_supply_w_cen]){//rdma_silo
#else
			else if(rdma_one_side() && w_cen == g_center_id){//rdma_silo
#endif
				#if USE_TCP_INTRA_CENTER
				// if (is_executor())rc = send_remote_request();
				#else
				rc = send_remote_one_side_request(yield,tpcc_query,row,cor_id);
				#endif
			} else {
#if PARAL_SUBTXN == true && CENTER_MASTER == true
				rc = RCOK;
#else
				rc = send_remote_request();
#endif
			}
			break;
		case TPCC_NEWORDER9 :
			if(ol_supply_w_loc) {
				rc = new_order_9(w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity, ol_number, ol_amount, o_id, row);
			}
			break;
		case TPCC_FIN :
			state = TPCC_FIN;
			if (tpcc_query->rbk) {
				INC_STATS(get_thd_id(),tpcc_fin_abort,get_sys_clock() - starttime);
				return Abort;
			}
			break;
		default:
			assert(false);
	}
	starttime = get_sys_clock();
	if (rc == RCOK) next_tpcc_state();
	INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
	return rc;
}

inline RC TPCCTxnManager::run_payment_0(yield_func_t &yield,uint64_t w_id, 
										uint64_t d_id, uint64_t d_w_id,
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
	assert(CC_ALG == CALVIN);

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
	assert(CC_ALG == CALVIN);

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

#if USE_REPLICA
RC construct_log(uint64_t w_id, 
	vector<vector<ChangeInfo>> & change, 
	int* change_cnt, RC status) {
	uint64_t part_id = wh_to_part(w_id);
	vector<uint64_t> node_id;
	if(status == RCOK){ //validate success, log all replicas
		node_id.push_back(GET_FOLLOWER1_NODE(part_id));
		node_id.push_back(GET_FOLLOWER2_NODE(part_id));
		node_id.push_back(GET_NODE_ID(part_id));
	}
	else if(status == Abort){ //validate fail, only log the primary replicas that have been locked	
	#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3
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
		if(center_id != g_center_id) continue;
		++change_cnt[node_id[i]];
		ChangeInfo newChange;
		newChange.set_change_info(0,0,nullptr,true); //local 
		change[node_id[i]].push_back(newChange);
	}

}

RC TPCCTxnManager::redo_log(yield_func_t &yield,RC status, uint64_t cor_id) {
	// return RCOK;
	if(CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3){
		assert(status != Abort);
		status = RCOK;		
	}
	
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	RC rc = RCOK;
	//for every node
	int change_cnt[g_node_cnt];
	for(int i=0;i<g_node_cnt;i++){
		change_cnt[i] = 0;
	}
	vector<vector<ChangeInfo>> change(g_node_cnt);
	
	//construct log 
	if (tpcc_query->txn_type == TPCC_PAYMENT) {
		// update warehouse
		if (g_wh_update) construct_log(tpcc_query->w_id, change, change_cnt, status);
		// update district
		construct_log(tpcc_query->w_id, change, change_cnt, status);
		// update customer
		construct_log(tpcc_query->c_w_id, change, change_cnt, status);
	}
	if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
		// update district
		construct_log(tpcc_query->w_id, change, change_cnt, status);

		for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
			uint64_t ol_number = i;
			uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
			// update stock
			construct_log(ol_supply_w_id, change, change_cnt, status);
		}
	}
	
	//write log
	for(int i=0;i<g_node_cnt&&rc==RCOK;i++){
		if(change_cnt[i]>0){
			int num_of_entry = 1;
			//use RDMA_FAA for local and remote
			uint64_t start_idx = -1;
			rc = faa_remote_content(yield, i, rdma_buffer_size-rdma_log_size+sizeof(uint64_t), num_of_entry, &start_idx, cor_id);
			// todo: I do not know whether I should abort here.

			LogEntry* newEntry = (LogEntry*)mem_allocator.alloc(sizeof(LogEntry));

			newEntry->set_entry(get_txn_id(), change_cnt[i],change[i],get_start_timestamp());


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

				while(start_idx + num_of_entry - log_head[i] >= redo_log_buf.get_size() && !simulation->is_done()){
					//wait for AsyncRedoThread to clean buffer
					rc = read_remote_log_head(yield, i, &log_head[i], cor_id);
					// todo: I do not know whether I should abort here.
				}
				pthread_mutex_unlock( LOG_HEAD_LATCH[i] );
				if(simulation->is_done()) break;
				
				start_idx = start_idx % redo_log_buf.get_size();

				uint64_t start_offset = redo_log_buf.get_entry_offset(start_idx);

				write_remote_log(yield, i, sizeof(LogEntry), start_offset, (char *)newEntry, cor_id);
			}
			log_idx[i] = start_idx;
			mem_allocator.free(newEntry, sizeof(LogEntry));
		}
	}
	return rc;
}


RC TPCCTxnManager::redo_commit_log(yield_func_t &yield,RC status, uint64_t cor_id) {
	// return RCOK;
	if(CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3){
		assert(status != Abort);
		status = RCOK;		
	}
	
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	RC rc = RCOK;
	//for every node
	int change_cnt[g_node_cnt];
	for(int i=0;i<g_node_cnt;i++){
		change_cnt[i] = 0;
	}
	vector<vector<ChangeInfo>> change(g_node_cnt);
	
	//construct log 
	if (tpcc_query->txn_type == TPCC_PAYMENT) {
		// update warehouse
		if (g_wh_update) construct_log(tpcc_query->w_id, change, change_cnt, status);
		// update district
		construct_log(tpcc_query->w_id, change, change_cnt, status);
		// update customer
		construct_log(tpcc_query->c_w_id, change, change_cnt, status);
	}
	if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
		// update district
		construct_log(tpcc_query->w_id, change, change_cnt, status);

		for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
			uint64_t ol_number = i;
			uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
			// update stock
			construct_log(ol_supply_w_id, change, change_cnt, status);
		}
	}
	
	//write log
	for(int i=0;i<g_node_cnt&&rc==RCOK;i++){
		if(change_cnt[i]>0){
			int num_of_entry = 1;
			//use RDMA_FAA for local and remote
			uint64_t start_idx = -1;
			rc = faa_remote_content(yield, i, rdma_buffer_size-rdma_log_size+sizeof(uint64_t), num_of_entry, &start_idx, cor_id);
			// todo: I do not know whether I should abort here.

			LogEntry* newEntry = (LogEntry*)mem_allocator.alloc(sizeof(LogEntry));

			newEntry->set_entry(get_txn_id(),0,change[i],get_start_timestamp());
			if(status == Abort){
				newEntry->state = LE_ABORTED;
			}else{
				newEntry->state = LE_COMMITTED;
			}
			newEntry->c_ts = get_commit_timestamp();

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

				while(start_idx + num_of_entry - log_head[i] >= redo_log_buf.get_size() && !simulation->is_done()){
					//wait for AsyncRedoThread to clean buffer
					rc = read_remote_log_head(yield, i, &log_head[i], cor_id);
					// todo: I do not know whether I should abort here.
				}
				pthread_mutex_unlock( LOG_HEAD_LATCH[i] );
				if(simulation->is_done()) break;
				
				start_idx = start_idx % redo_log_buf.get_size();

				uint64_t start_offset = redo_log_buf.get_entry_offset(start_idx);

				write_remote_log(yield, i, sizeof(LogEntry), start_offset, (char *)newEntry, cor_id);
			}
			log_idx[i] = start_idx;
			mem_allocator.free(newEntry, sizeof(LogEntry));
		}
	}
	return rc;
}

RC TPCCTxnManager::redo_local_log(yield_func_t &yield,RC status, uint64_t cor_id) {
	// return RCOK;
	if(CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3){
		assert(status != Abort);
		status = RCOK;		
	}
	
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	RC rc = RCOK;
	//for every node
	int change_cnt[g_node_cnt];
	for(int i=0;i<g_node_cnt;i++){
		change_cnt[i] = 0;
	}
	vector<vector<ChangeInfo>> change(g_node_cnt);
	
	//construct log 
	if (tpcc_query->txn_type == TPCC_PAYMENT) {
		// update warehouse
		if (g_wh_update) construct_log(tpcc_query->w_id, change, change_cnt, status);
		// update district
		construct_log(tpcc_query->w_id, change, change_cnt, status);
		// update customer
		construct_log(tpcc_query->c_w_id, change, change_cnt, status);
	}
	if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
		// update district
		construct_log(tpcc_query->w_id, change, change_cnt, status);

		for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
			uint64_t ol_number = i;
			uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
			// update stock
			construct_log(ol_supply_w_id, change, change_cnt, status);
		}
	}
	
	//write log
	int i = g_node_id;

	if(change_cnt[i]>0){
		int num_of_entry = 1;
		//use RDMA_FAA for local and remote
		uint64_t start_idx = -1;
		rc = faa_remote_content(yield, i, rdma_buffer_size-rdma_log_size+sizeof(uint64_t), num_of_entry, &start_idx, cor_id);
		LogEntry* newEntry = (LogEntry*)mem_allocator.alloc(sizeof(LogEntry));
		newEntry->set_entry(get_txn_id(), change_cnt[i],change[i],get_start_timestamp());

		//consider possible overwritten:if no space, wait until cleaned 
		//for head: there is local read and write, and remote RDMA read, conflicts are ignored here, which might be problematic
		while((start_idx + num_of_entry - *(redo_log_buf.get_head()) >= redo_log_buf.get_size()) && !simulation->is_done()){
			//wait for AsyncRedoThread to clean buffer
		}
		
		start_idx = start_idx % redo_log_buf.get_size();

		char* start_addr = (char*)redo_log_buf.get_entry(start_idx);

		assert(((LogEntry *)start_addr)->state == EMPTY);					
		assert(((LogEntry *)start_addr)->change_cnt == 0);					

		memcpy(start_addr, (char *)newEntry, sizeof(LogEntry));
		assert(((LogEntry *)start_addr)->state == LOGGED);						

		log_idx[i] = start_idx;
		mem_allocator.free(newEntry, sizeof(LogEntry));
	}

	return rc;
}

RC TPCCTxnManager::redo_commit_local_log(yield_func_t &yield,RC status, uint64_t cor_id) {
	// return RCOK;
	if(CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3){
		assert(status != Abort);
		status = RCOK;		
	}
	
	TPCCQuery* tpcc_query = (TPCCQuery*) query;
	RC rc = RCOK;
	//for every node
	int change_cnt[g_node_cnt];
	for(int i=0;i<g_node_cnt;i++){
		change_cnt[i] = 0;
	}
	vector<vector<ChangeInfo>> change(g_node_cnt);
	
	//construct log 
	if (tpcc_query->txn_type == TPCC_PAYMENT) {
		// update warehouse
		if (g_wh_update) construct_log(tpcc_query->w_id, change, change_cnt, status);
		// update district
		construct_log(tpcc_query->w_id, change, change_cnt, status);
		// update customer
		construct_log(tpcc_query->c_w_id, change, change_cnt, status);
	}
	if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
		// update district
		construct_log(tpcc_query->w_id, change, change_cnt, status);

		for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
			uint64_t ol_number = i;
			uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
			// update stock
			construct_log(ol_supply_w_id, change, change_cnt, status);
		}
	}
	
	//write log
	int i = g_node_id;

	if(change_cnt[i]>0){
		int num_of_entry = 1;
		//use RDMA_FAA for local and remote
		uint64_t start_idx = -1;
		rc = faa_remote_content(yield, i, rdma_buffer_size-rdma_log_size+sizeof(uint64_t), num_of_entry, &start_idx, cor_id);
		LogEntry* newEntry = (LogEntry*)mem_allocator.alloc(sizeof(LogEntry));
		newEntry->set_entry(get_txn_id(), change_cnt[i],change[i],get_start_timestamp());
		newEntry->state = LE_COMMITTED;
		newEntry->c_ts = get_commit_timestamp();
		//consider possible overwritten:if no space, wait until cleaned 
		//for head: there is local read and write, and remote RDMA read, conflicts are ignored here, which might be problematic
		while((start_idx + num_of_entry - *(redo_log_buf.get_head()) >= redo_log_buf.get_size()) && !simulation->is_done()){
			//wait for AsyncRedoThread to clean buffer
		}
		
		start_idx = start_idx % redo_log_buf.get_size();

		char* start_addr = (char*)redo_log_buf.get_entry(start_idx);

		assert(((LogEntry *)start_addr)->state == EMPTY);					
		assert(((LogEntry *)start_addr)->change_cnt == 0);					

		memcpy(start_addr, (char *)newEntry, sizeof(LogEntry));
		assert(((LogEntry *)start_addr)->state == LE_COMMITTED);						

		log_idx[i] = start_idx;
		mem_allocator.free(newEntry, sizeof(LogEntry));
	}

	return rc;
}

#endif