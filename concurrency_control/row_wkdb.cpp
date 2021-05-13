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
#include "table.h"
#include "row.h"
#include "txn.h"
#include "row_wkdb.h"
#include "mem_alloc.h"
#include "manager.h"
#include "helper.h"
#include "wkdb.h"

void Row_wkdb::init(row_t * row) {
	_row = row;

	timestamp_last_read = 0;
	timestamp_last_write = 0;
	wkdb_avail = true;
	uncommitted_reads = new std::set<uint64_t>();
	write_trans = 0;

  	// multi-version part
	readreq_mvcc = NULL;
	prereq_mvcc = NULL;
	writehis = NULL;
	writehistail = NULL;
	latch = (pthread_mutex_t *)
		mem_allocator.alloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(latch, NULL);
	whis_len = 0;
	rreq_len = 0;
	preq_len = 0;
}

RC Row_wkdb::access(TsType type, TxnManager * txn, row_t * row) {
    uint64_t starttime = get_sys_clock();
    RC rc = RCOK;

	rc = read_and_write(type, txn, row);

	uint64_t timespan = get_sys_clock() - starttime;
	txn->txn_stats.cc_time += timespan;
  	txn->txn_stats.cc_time_short += timespan;
  	return rc;
}

RC Row_wkdb::read_and_write(TsType type, TxnManager * txn, row_t * row) {
	assert (CC_ALG == WOOKONG);
	RC rc = RCOK;

	uint64_t mtx_wait_starttime = get_sys_clock();
	while(!ATOM_CAS(wkdb_avail,true,false)) { }
	INC_STATS(txn->get_thd_id(),mtx[30],get_sys_clock() - mtx_wait_starttime);
	INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - mtx_wait_starttime);
	if (type == 0) {
		DEBUG("READ %ld -- %ld: lw %lu, table name: %s \n",txn->get_txn_id(),_row->get_primary_key(),timestamp_last_read,_row->get_table_name());
	} else if (type == 1) {
		DEBUG("COMMIT-WRITE %ld -- %ld: lw %lu\n",txn->get_txn_id(),_row->get_primary_key(),timestamp_last_read);
	} else if (type == 2) {
		DEBUG("WRITE (P_REQ) %ld -- %ld: lw %lu\n",txn->get_txn_id(),_row->get_primary_key(),timestamp_last_read);
	} else if (type == 3) {
		DEBUG("XP-REQ %ld -- %ld: lw %lu\n",txn->get_txn_id(),_row->get_primary_key(),timestamp_last_read);
	}

	// pthread_mutex_lock( latch );

	// Add to uncommitted reads (soft lock)
	DEBUG("uncommitted_reads %ld -- %ld\n", _row->get_primary_key(), uncommitted_reads->size());
	uncommitted_reads->insert(txn->get_txn_id());

 	// =======TODO: Do we need using write_txn to adjust it's upper?

  	// Fetch the previous version
	ts_t ts = txn->get_timestamp();
	// ts_t ts =  wkdb_time_table.get_upper(txn->get_thd_id(),txn->get_txn_id());

	if (type == P_REQ) {
		// Optimization for concurrent update
		if ((write_trans != 0 && write_trans != txn->get_txn_id()) && preq_len < g_max_pre_req) {
			rc = WAIT;
			DEBUG("buf P_REQ %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
			buffer_req(P_REQ, txn);
			txn->ts_ready = false;
		} else if (_row->get_table() && !strcmp(_row->get_table_name(),"WAREHOUSE")) {
			DEBUG("set write tran of row: %lu with txn: %lu \n", _row->get_primary_key(), txn->get_txn_id());
			//DEBUG("current row: table: %s, %lu \n", _row->get_table_name(), _row->get_table()->get_table_size())
			write_trans = txn->get_txn_id();
		}
	}

  	if (type == R_REQ || type == P_REQ) {

		if (timestamp_last_write < ts) {
			txn->cur_row = _row;
		} else {
			WKDBMVHisEntry * whis = writehis;
			while (whis_len && whis != NULL && whis->ts > ts)
				whis = whis->next;
			row_t * ret = (whis == NULL)? _row : whis->row;
			txn->cur_row = ret;
			assert(strstr(_row->get_table_name(), ret->get_table_name()));
		}
		DEBUG("R_REQ return %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());

		// Adjust txn.lower
		// uint64_t lower =  wkdb_time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
		// if (lower < timestamp_last_read)
		// 	wkdb_time_table.set_lower(txn->get_thd_id(),txn->get_txn_id(), timestamp_last_read + 1);
	}

	if (rc == RCOK) {
		if (whis_len > g_his_recycle_len) {
		//if (whis_len > g_his_recycle_len || rhis_len > g_his_recycle_len) {
			ts_t t_th = glob_manager.get_min_ts(txn->get_thd_id());
			// Here is a tricky bug. The oldest transaction might be
			// reading an even older version whose timestamp < t_th.
			// But we cannot recycle that version because it is still being used.
			// So the HACK here is to make sure that the first version older than
			// t_th not be recycled.
			if (whis_len > 1 &&
				writehistail->prev->ts < t_th) {
				row_t * latest_row = clear_history(W_REQ, t_th);
				if (latest_row != NULL) {
					assert(_row != latest_row);
					_row->copy(latest_row);
				}
			}
			DEBUG("GC finish\n");
		}
	}

	// pthread_mutex_unlock( latch );

  	ATOM_CAS(wkdb_avail,false,true);

	return rc;
}

RC Row_wkdb::abort(access_t type, TxnManager * txn) {
  	uint64_t mtx_wait_starttime = get_sys_clock();
  	while(!ATOM_CAS(wkdb_avail,true,false)) { }
  	INC_STATS(txn->get_thd_id(),mtx[32],get_sys_clock() - mtx_wait_starttime);
  	DEBUG("wkdb Abort %ld: %d -- %ld\n",txn->get_txn_id(),type,_row->get_primary_key());

  	uncommitted_reads->erase(txn->get_txn_id());

  	if((type == WR || type == XP) && write_trans == txn->get_txn_id()) {
    	write_trans = 0;
  	}

	if (type == XP) {
		DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
		debuffer_req(P_REQ, txn);
	}

  	ATOM_CAS(wkdb_avail,false,true);
  	return Abort;
}

RC Row_wkdb::commit(access_t type, TxnManager * txn, row_t * data) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	while(!ATOM_CAS(wkdb_avail,true,false)) { }
	INC_STATS(txn->get_thd_id(),mtx[33],get_sys_clock() - mtx_wait_starttime);
	DEBUG("wkdb Commit %ld: %d,%lu -- %ld\n",txn->get_txn_id(),type,txn->get_commit_timestamp(),_row->get_primary_key());

	uint64_t txn_commit_ts = txn->get_commit_timestamp();

	if (txn_commit_ts > timestamp_last_read)
		timestamp_last_read = txn_commit_ts;

	uncommitted_reads->erase(txn->get_txn_id());

	if(type == WR) {
		//ts_t ts = txn->get_timestamp();
		if (txn_commit_ts > timestamp_last_write)
			timestamp_last_write = txn_commit_ts;

		// the corresponding prewrite request is debuffered.
		insert_history(txn_commit_ts, data);
		DEBUG("wkdb insert histroy %ld: %lu -- %ld\n",txn->get_txn_id(),txn->get_commit_timestamp(),data->get_primary_key());

		if(write_trans == txn->get_txn_id()) {
    		write_trans = 0;
  		}

		DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
		debuffer_req(P_REQ, txn);

	}

  	ATOM_CAS(wkdb_avail,false,true);
	return RCOK;
}

row_t * Row_wkdb::clear_history(TsType type, ts_t ts) {
	WKDBMVHisEntry ** queue;
	WKDBMVHisEntry ** tail;
    switch (type) {
    //case R_REQ : queue = &readhis; tail = &readhistail; break;
    case W_REQ : queue = &writehis; tail = &writehistail; break;
	default: assert(false);
    }
	WKDBMVHisEntry * his = *tail;
	WKDBMVHisEntry * prev = NULL;
	row_t * row = NULL;
	while (his && his->prev && his->prev->ts < ts) {
		prev = his->prev;
		assert(prev->ts >= his->ts);
		if (row != NULL) {
			row->free_row();
			mem_allocator.free(row, row_t::get_row_size(ROW_DEFAULT_SIZE));
		}
		row = his->row;
		his->row = NULL;
		return_his_entry(his);
		his = prev;
		//if (type == R_REQ) rhis_len --;
		//else
		whis_len --;
	}
	*tail = his;
	if (*tail)
		(*tail)->next = NULL;
	if (his == NULL)
		*queue = NULL;
	return row;
}

WKDBMVReqEntry * Row_wkdb::get_req_entry() {
	return (WKDBMVReqEntry *) mem_allocator.alloc(sizeof(WKDBMVReqEntry));
}

void Row_wkdb::return_req_entry(WKDBMVReqEntry * entry) {
	mem_allocator.free(entry, sizeof(WKDBMVReqEntry));
}

WKDBMVHisEntry * Row_wkdb::get_his_entry() {
	return (WKDBMVHisEntry *) mem_allocator.alloc(sizeof(WKDBMVHisEntry));
}

void Row_wkdb::return_his_entry(WKDBMVHisEntry * entry) {
	if (entry->row != NULL) {
		entry->row->free_row();
		mem_allocator.free(entry->row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	}
	mem_allocator.free(entry, sizeof(WKDBMVHisEntry));
}

void Row_wkdb::buffer_req(TsType type, TxnManager * txn)
{
	WKDBMVReqEntry * req_entry = get_req_entry();
	assert(req_entry != NULL);
	req_entry->txn = txn;
	req_entry->ts = txn->get_timestamp();
	req_entry->starttime = get_sys_clock();
	if (type == R_REQ) {
		rreq_len ++;
		STACK_PUSH(readreq_mvcc, req_entry);
	} else if (type == P_REQ) {
		preq_len ++;
		STACK_PUSH(prereq_mvcc, req_entry);
	}
}

// for type == R_REQ
//	 debuffer all non-conflicting requests
// for type == P_REQ
//   debuffer the request with matching txn.
WKDBMVReqEntry * Row_wkdb::debuffer_req(TsType type, TxnManager * txn) {
	WKDBMVReqEntry ** queue = &prereq_mvcc;
	// WKDBMVReqEntry * return_queue = NULL;
	// DEBUG("debuffer transaction restart\n");
	WKDBMVReqEntry * req = *queue;
	WKDBMVReqEntry * prev_req = NULL;
	if (txn != NULL && req && preq_len) {
		while (req != NULL && req->txn == txn) {
			prev_req = req;
			req = req->next;
			if (prev_req->txn != txn) {
				break;
			}
		}
		if (req == NULL) {
			return NULL;
		}
		if (prev_req != NULL)
			prev_req->next = req->next;
		else {
			assert( req == *queue );
			*queue = req->next;
		}
		preq_len --;
		req->next = NULL;

		write_trans = req->txn->get_txn_id();
		// req->txn->last_row = _row;
		req->txn->cur_row = _row;
		req->txn->ts_ready = true;
		uint64_t timespan = get_sys_clock() - req->starttime;
		req->txn->txn_stats.cc_block_time += timespan;
		req->txn->txn_stats.cc_block_time_short += timespan;
		if (txn->get_commit_timestamp() != 0)
			req->txn->set_timestamp(txn->get_commit_timestamp());
		wkdb_time_table.set_upper(txn->get_thd_id(),req->txn->get_txn_id(),UINT64_MAX);
    	txn_table.restart_txn(txn->get_thd_id(),req->txn->get_txn_id(),0);
		DEBUG("txn %lu restart after txn %lu has been commit \n", txn->get_txn_id(), req->txn->get_txn_id());
		return_req_entry(req);
	}
	return req;
}

void Row_wkdb::insert_history(ts_t ts, row_t * row)
{
	WKDBMVHisEntry * new_entry = get_his_entry();
	new_entry->ts = ts;
	new_entry->row = row;
	if (row != NULL)
		whis_len ++;

	WKDBMVHisEntry ** queue = &(writehis);
	WKDBMVHisEntry ** tail = &(writehistail);
	WKDBMVHisEntry * his = *queue;
	while (his != NULL && ts < his->ts) {
		his = his->next;
	}

	if (his) {
		LIST_INSERT_BEFORE(his, new_entry, (*queue));
	} else
		LIST_PUT_TAIL((*queue), (*tail), new_entry);
}

bool Row_wkdb::conflict(TsType type, ts_t ts) {
	// find the unique prewrite-read couple (prewrite before read)
	// if no such couple found, no conflict.
	// else
	// 	 if exists writehis between them, NO conflict!!!!
	// 	 else, CONFLICT!!!
	ts_t rts;
	ts_t pts;
	if (type == R_REQ) {
		rts = ts;
		pts = 0;
		WKDBMVReqEntry * req = prereq_mvcc;
		while (req != NULL) {
			if (req->ts < ts && req->ts > pts) {
				pts = req->ts;
			}
			req = req->next;
		}
		if (pts == 0) // no such couple exists
			return false;
	} else if (type == P_REQ) {
		rts = 0;
		pts = ts;
		// WKDBMVHisEntry * his = readhis;
		// while (his != NULL) {
		// 	if (his->ts > ts) {
		// 		rts = his->ts;
		// 	} else
		// 		break;
		// 	his = his->next;
		// }
		if (rts == 0) // no couple exists
			return false;
		assert(rts > pts);
	}
	WKDBMVHisEntry * whis = writehis;
    while (whis != NULL && whis->ts > pts) {
		if (whis->ts < rts)
			return false;
		whis = whis->next;
	}
	return true;
}

void Row_wkdb::update_buffer(TxnManager * txn) {
	WKDBMVReqEntry * ready_read = debuffer_req(R_REQ, NULL);
	WKDBMVReqEntry * req = ready_read;
	WKDBMVReqEntry * tofree = NULL;

	while (req != NULL) {
		// find the version for the request
		WKDBMVHisEntry * whis = writehis;
		while (whis != NULL && whis->ts > req->ts)
			whis = whis->next;
		row_t * row = (whis == NULL)?
			_row : whis->row;
		req->txn->cur_row = row;
		//insert_history(req->ts, NULL);
		assert(row->get_data() != NULL);
		assert(row->get_table() != NULL);
		assert(row->get_schema() == _row->get_schema());

		req->txn->ts_ready = true;
		uint64_t timespan = get_sys_clock() - req->starttime;
		req->txn->txn_stats.cc_block_time += timespan;
		req->txn->txn_stats.cc_block_time_short += timespan;
    	txn_table.restart_txn(txn->get_thd_id(),req->txn->get_txn_id(),0);
		tofree = req;
		req = req->next;
		// free ready_read
		return_req_entry(tofree);
	}
}

