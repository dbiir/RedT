#include "global.h"
#include "txn.h"
#include "row.h"
#include "mem_alloc.h"
#include "manager.h"
#include "stdint.h"
#include "row_rdma_ts1.h"
#include "rdma_ts1.h"
#if CC_ALG ==RDMA_TS1
void Row_rdma_ts1::init(row_t * row) {
	_row = row;
}

RC Row_rdma_ts1::access(yield_func_t &yield, TxnManager * txn, Access *access, access_t type, uint64_t cor_id) {
	RC rc;
	uint64_t starttime = get_sys_clock();
	ts_t ts = txn->get_timestamp();
#if 1 //use double read, by default
	//local read;
	row_t *temp_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
	memcpy(temp_row, _row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	if(type == RD) {
		if(ts < temp_row->wts || (ts > temp_row->tid && temp_row->tid != 0)){
			rc = Abort;
			mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			return rc;
		}
		//CAS(old_rts, new_rts) and read again
		if (temp_row->rts < ts){ //if ">=", no need to CAS
			uint64_t old_rts = temp_row->rts;
			uint64_t new_rts = ts;			
			uint64_t rts_offset = access->offset+sizeof(uint64_t)+sizeof(uint64_t)+sizeof(ts_t);
			uint64_t cas_result = txn->cas_remote_content(access->location,rts_offset,old_rts,new_rts);
			if(cas_result!=old_rts){ //cas fail, atomicity violated
				rc = Abort;
				mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
				return rc;
			}
			//cas success, now do double read check
			temp_row->rts = ts;

			row_t *second_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
			memcpy(second_row, _row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			if(second_row->wts!=temp_row->wts || second_row->tid!=temp_row->tid){ //atomicity violated
					rc = Abort;
					mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
					mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
					return rc;					
			}
			//read success
			mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
		}
		txn->cur_row->copy(temp_row);
		mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
		rc = RCOK;
		return rc;
	}
	else if(type == WR) {
		if (temp_row->tid != 0 || ts < temp_row->rts || ts < temp_row->wts){
			rc = Abort;
			mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			return rc;
		}
		//CAS(old_tid, new_tid) and read again
		uint64_t old_tid = 0;
		uint64_t new_tid = ts;
		uint64_t tid_offset = access->offset+sizeof(uint64_t);
		uint64_t cas_result = txn->cas_remote_content(access->location,tid_offset,old_tid,new_tid);
		if(cas_result!=old_tid){ //cas fail, atomicity violated
			rc = Abort;
			mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			return rc;
		}
		//cas success, now do double read check
		row_t *second_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
		memcpy(second_row, _row, row_t::get_row_size(ROW_DEFAULT_SIZE));
		
		if(ts < temp_row->rts || ts < temp_row->wts){//atomicity violated
			rc = Abort;
			_row->tid = 0;
			mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			return rc;					
		}
		//read success
		temp_row->tid = ts;
		txn->cur_row->copy(temp_row);
		mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
		mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
		rc = RCOK;
		return rc;
	}
#else
	bool suc = false;
	//validate lock suc
	row_t *temp_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
	suc = rdmats_man.remote_try_lock(txn, temp_row, access->location, access->offset);
	if (suc == false) {
		rc = Abort;
#if DEBUG_PRINTF
		printf("[本地读写回滚]事务号：%d，主键：%d，ts:%lu，锁：%lu，tid：%lu,rts：%lu,wts：%lu\n",txn->get_txn_id(),_row->get_primary_key(),ts,_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
		goto end;
	}
	if (type == RD) {
		if (ts < _row->wts || (ts > _row->tid && _row->tid != 0)) {
#if DEBUG_PRINTF
		printf("[本地读回滚]事务号：%d，主键：%d，ts:%lu，锁：%lu，tid：%lu,rts：%lu,wts：%lu\n",txn->get_txn_id(),_row->get_primary_key(),ts,_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
			rc = Abort;
			goto end;
		}
		if (_row->rts < ts){
			_row->rts = ts;
			_row->mutx = 0;
		}
		txn->cur_row->copy(_row);
		rc = RCOK;	
#if DEBUG_PRINTF
	if(rc == RCOK)
		printf("[本地读后]事务号：%d，主键：%d，锁：%lu，tid：%lu,rts：%lu,wts：%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
	} 
	else if (type == WR) {
		if (ts < _row->wts || ts < _row->rts || _row->tid != 0) {
#if DEBUG_PRINTF
			printf("[本地写回滚]事务号：%d，主键：%d，ts:%lu，锁：%lu，tid：%lu,rts：%lu,wts：%lu\n",txn->get_txn_id(),_row->get_primary_key(),ts,_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
			rc = Abort;
			goto end;
		}
		else {
			_row->tid = ts;
			_row->mutx = 0;
			txn->cur_row->copy(_row);
			rc = RCOK;
#if DEBUG_PRINTF
			printf("[本地写后]事务号：%d，主键：%d，锁：%lu，tid：%lu,rts:%lu,wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
		}
	} else{
		assert(false);
	}
end:
	_row->mutx = 0;
	uint64_t timespan = get_sys_clock() - starttime;
	txn->txn_stats.cc_time += timespan;
	txn->txn_stats.cc_time_short += timespan;
	mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
	return rc;
#endif
}

RC Row_rdma_ts1::local_commit(yield_func_t &yield, TxnManager * txn, Access *access, access_t type, uint64_t cor_id) {
#if DEBUG_PRINTF
	printf("[本地提交]事务号：%d，主键：%d，锁：%lu，tid：%lu,rts:%lu,wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
	assert(_row->tid != 0);
	RC rc = RCOK;
	//validate lock suc
	// row_t *temp_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
	// bool suc = false;
	// //lock until success!
	// suc = rdmats_man.remote_try_lock(txn, temp_row, access->location, access->offset);
	// while(suc == false){
	// 	INC_STATS(txn->get_thd_id(),lock_retry_cnt,1);
	// 	suc = rdmats_man.remote_try_lock(txn, temp_row, access->location, access->offset);
	// }
	if (type == XP) {
		_row->tid = 0;
	} 
	else if (type == WR) {
		//lock until success!
    	assert(txn->loop_cas_remote(access->location,access->offset,0,txn->get_txn_id()+1) == true);//lock in loop
#if DEBUG_PRINTF
		printf("[本地写入前]事务号：%d，主键：%d，锁：%lu，tid：%lu,rts:%lu,wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
		ts_t ts = txn->get_timestamp();
		if (_row->wts < ts) _row->wts = ts;
		_row->tid = 0;
		//local unlock
		_row->mutx = 0;
	}
	// assert(_row->mutx != 0);
	access->data->free_row();
	mem_allocator.free(access->data, row_t::get_row_size(access->data->tuple_size));
	// mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
#if DEBUG_PRINTF
	printf("[本地提交后]事务号：%d，主键：%d，锁：%lu，tid：%lu，rts:%lu，wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
	return rc;
}

#endif
