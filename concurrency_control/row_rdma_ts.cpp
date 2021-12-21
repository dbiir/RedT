#include "global.h"
#include "txn.h"
#include "row.h"
#include "mem_alloc.h"
#include "manager.h"
#include "stdint.h"
#include "row_rdma_ts.h"
#include "rdma_ts.h"
#include "rdma_maat.h"
#if CC_ALG == RDMA_TS
void Row_rdma_ts::init(row_t * row) {
	_row = row;
}

RC Row_rdma_ts::access(yield_func_t &yield, TxnManager * txn, Access *access, access_t type, uint64_t cor_id) {
	RC rc;
	uint64_t starttime = get_sys_clock();
	ts_t ts = txn->get_timestamp();

	uint64_t suc = false;
	uint64_t new_lock_info = txn->get_txn_id() + 1;
    uint64_t lock_info = 0;
	uint64_t loc = g_node_id;
	int retry = 0;
retry_read:
	suc = txn->cas_remote_content(yield,loc,(char*)_row - rdma_global_buffer,lock_info,new_lock_info,cor_id);
	if (new_lock_info != _row->mutx) {
		INC_STATS(txn->get_thd_id(),lock_retry_cnt,1);
		// printf("txn %ld lock failed, current lock is %ld, suc = %ld\n", new_lock_info, _row->mutx, suc);
		rc = Abort;
		goto end;
	}

	if (type == RD) {
		if (ts < _row->wts) {
			INC_STATS(txn->get_thd_id(),read_retry_cnt,1);
			rc = Abort;
			_row->mutx = 0;
			goto end;
		}
		bool need_wait = false;
		if (_row->up_size > 0)
			for (int i = 0; i < WAIT_QUEUE_LENGTH; i++) {
				if (_row->up[i].ts_ != 0 && !_row->up[i].commit_ && ts > _row->up[i].ts_) {
					need_wait = true; 
					break;
				}
			}
		
		if (need_wait) {
			rdma_txn_table.local_set_state(txn->get_thd_id(), txn->get_txn_id(), TS_WAITING);
			if (_row->ur_size == WAIT_QUEUE_LENGTH) {
				rc = Abort;
				INC_STATS(txn->get_thd_id(),read_retry_cnt,1);
				goto end;
			}
			else {
				// int i = 0;
				// for (i = 0; i < WAIT_QUEUE_LENGTH; i++) {
				// 	if (_row->ur[i].ts_ == 0 && _row->ur[i].txn_id_ == 0) {
				// 		break;
				// 	}
				// }
				// _row->ur[i].ts_ = ts;
				// _row->ur[i].txn_id_ = txn->get_txn_id();
				// // printf("[write ur]current_txn:%ld,idx:%lu,ur_ts:%lu,ur_txn:%ld\n",txn->get_txn_id(),_row->ur_size,_row->ur[_row->ur_size].ts_,_row->ur[_row->ur_size].txn_id_ );
				// _row->ur_size++;
				_row->mutx = 0;
				// printf("[waiting]current_txn:%ld,retry:%ld\n",txn->get_txn_id(),retry);

				// uint64_t starttime = get_sys_clock();
				// while (!simulation->is_done() && rdma_txn_table.local_get_state(txn->get_thd_id(), txn->get_txn_id()) == TS_WAITING);
				// uint64_t endtime = get_sys_clock();
				// INC_STATS(txn->get_thd_id(), worker_idle_time, endtime - starttime);
				// INC_STATS(txn->get_thd_id(), worker_waitcomp_time, endtime - starttime);
				// printf("[leave waiting]current_txn:%ld, wait_time:%ld\n",txn->get_txn_id(),endtime-starttime);
				if (!simulation->is_done() && retry < 3){
					retry++;
					goto retry_read;
				} else {
					rc = Abort;
					INC_STATS(txn->get_thd_id(),read_retry_cnt,1);
					goto end;
				}
			}
		} else if (_row->rts < ts){
			_row->rts = ts;
		}
		_row->mutx = 0;
		rc = RCOK;	
		txn->cur_row->copy(_row);
	} else if (type == WR) {
		if (ts < _row->rts) {
			INC_STATS(txn->get_thd_id(),write_retry_cnt,1);
			rc = Abort;
			_row->mutx = 0;
			goto end;
		}
		if (ts < _row->wts) {
			rc = RCOK;
			_row->mutx = 0;
			goto end;
		}
		if (_row->up_size == WAIT_QUEUE_LENGTH) {
			_row->mutx = 0;
			INC_STATS(txn->get_thd_id(),write_retry_cnt,1);
			rc = Abort;
			goto end;
		} 
		else {
			int i = 0;
			for (i = 0; i < WAIT_QUEUE_LENGTH; i++) {
				if (_row->up[i].ts_ == 0 && _row->up[i].txn_id_ == 0) {
					break;
				}
			}
			_row->up[i].ts_ = ts;
			_row->up[i].commit_ = false;
			_row->up[i].txn_id_ = txn->get_txn_id();
			_row->up_size++;
			_row->mutx = 0;
			txn->cur_row->copy(_row);
			rc = RCOK;
		}
	} else{
		assert(false);
	}
end:
	uint64_t timespan = get_sys_clock() - starttime;
	txn->txn_stats.cc_time += timespan;
	txn->txn_stats.cc_time_short += timespan;
	// mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
	return rc;
}

RC Row_rdma_ts::local_commit(yield_func_t &yield, TxnManager * txn, Access *access, access_t type, uint64_t cor_id) {
	RC rc = RCOK;

	if (type == WR) {
		//lock until success!
    	assert(txn->loop_cas_remote(access->location,access->offset,0,txn->get_txn_id()+1) == true);//lock in loop
		
		Row_rdma_ts::commit_modify_row(yield, txn, _row, cor_id);
		_row->mutx = 0;
		// _row->up_size = 0;
	}
	access->data->free_row();
	mem_allocator.free(access->data, row_t::get_row_size(access->data->tuple_size));
	return rc;
}

void Row_rdma_ts::commit_modify_row(yield_func_t &yield, TxnManager * txn, row_t* _row, uint64_t cor_id) {
	bool need_wait = false;
	ts_t ts = txn->get_timestamp();

	if (!need_wait && _row->ur_size > 0)
		for (int i = 0; i < WAIT_QUEUE_LENGTH; i++) {
			if (_row->ur[i].ts_ != 0 && ts > _row->ur[i].ts_) {
				need_wait = true; 
				break;
			}
		}
	if (!need_wait && _row->up_size > 0)
		for (int i = 0; i < WAIT_QUEUE_LENGTH; i++) {
			if (_row->up[i].ts_ != 0  && !_row->up[i].commit_ && ts > _row->up[i].ts_) {
				need_wait = true; 
				break;
			}
		}
	if (need_wait) {
		for (int i = 0; i < _row->up_size; i++) {
			if (_row->up[i].ts_ == ts) {
				_row->up[i].commit_ = true;
				break;
			}
		}
		return;
	} else {
		if (_row->rts < ts){
			_row->rts = ts;
		}
		if (_row->wts < ts){
			_row->wts = ts;
		}
		int i = 0;
		for (i = 0; i < WAIT_QUEUE_LENGTH; i++) {
			if (_row->up[i].ts_ == ts) break;
		}
		_row->up[i].ts_ = 0;
		_row->up[i].txn_id_ = 0;
		_row->up[i].commit_ = false;
		_row->up_size--;
	}
	//debuffer pre_queue
	uint64_t min_pts = -1;
	if (_row->up_size > 0)
		for (int i = 0; i < _row->up_size; i++) {
			if (_row->up[i].commit_) continue;
			min_pts = min_pts < _row->up[i].ts_ ? min_pts : _row->up[i].ts_;
		}
	//restart read_queue
	uint64_t min_rts = -1;
	if (_row->ur_size > 0)
		for (int i = 0; i < WAIT_QUEUE_LENGTH; i++) {
			if (_row->ur[i].ts_ < min_pts) {
				if (_row->ur[i].txn_id_ % g_node_cnt == g_node_id) {
					rdma_txn_table.local_set_state(txn->get_thd_id(), _row->ur[i].txn_id_, TS_RUNNING);
					printf("[release local read txn]current_txn:%ld,release_txn:%ld,idx:%d,size:%d\n",txn->get_txn_id(),_row->ur[i].txn_id_,i,_row->ur_size);
				} else {
					RdmaTxnTableNode * value = (RdmaTxnTableNode *)mem_allocator.alloc(sizeof(RdmaTxnTableNode));
					value->init(_row->ur[i].txn_id_);
					rdma_txn_table.remote_set_state(yield, txn, _row->ur[i].txn_id_, value, cor_id);
					printf("[release remote read txn]current_txn:%ld,release_txn:%ld,idx:%d,size:%d\n",txn->get_txn_id(),_row->ur[i].txn_id_,i,_row->ur_size);
				}
				_row->ur[i].ts_ = 0;
				_row->ur[i].txn_id_ = 0;
				_row->ur_size--;
			} else {
				min_rts = min_rts < _row->ur[i].ts_ ? min_rts : _row->ur[i].ts_;
			}
		}

	//todo: redo order
	uint64_t youngest_txn = 0, youngest_ts = 0;
	if (_row->up_size > 0) {
		for (int i = 0; i < _row->up_size; i++) {
			if (_row->up[i].commit_ && _row->up[i].ts_ < min_rts) {
				if (youngest_ts > _row->up[i].ts_) {
					youngest_txn = _row->up[i].txn_id_;
					youngest_ts = _row->up[i].ts_;
				}
				printf("[release prewrite txn]current_txn:%ld,release_txn:%ld,idx:%d,size:%d\n",txn->get_txn_id(),_row->up[i].txn_id_,i,_row->up_size);
				_row->up[i].ts_ = 0;
				_row->up[i].txn_id_ = 0;
				_row->up[i].commit_ = false;
				_row->up_size--;
			}
		}
		if (_row->rts < youngest_ts){
			_row->rts = youngest_ts;
		}
		if (_row->wts < youngest_ts){
			_row->wts = youngest_ts;
		}
	}
}
#endif
