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

	row_t *temp_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
	memcpy(temp_row, _row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	if(type == RD) {
		if(ts < temp_row->wts){
			rc = Abort;
			// #if DEBUG_PRINTF
			// printf("[read wts failed]txn:%ld, key:%ld, lock:%lu, tid:%lu, rts:%lu, wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
			// #endif
			mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			return rc;
		}
		//CAS(old_rts, new_rts) and read again
		if (temp_row->rts < ts){ //if ">=", no need to CAS
			uint64_t old_rts = temp_row->rts;
			uint64_t new_rts = ts;			
			uint64_t rts_offset = access->offset+sizeof(uint64_t)+sizeof(uint64_t)*CASCADING_LENGTH+sizeof(ts_t);
			uint64_t cas_result = txn->cas_remote_content(access->location,rts_offset,old_rts,new_rts);
			if(cas_result!=old_rts){ //cas fail, atomicity violated
				rc = Abort;
				// #if DEBUG_PRINTF
				// printf("[change rts failed]txn:%ld, key:%ld, lock:%lu, tid:%lu, rts:%lu, wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
				// #endif
				mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
				return rc;
			}
			//cas success, now do double read check
			temp_row->rts = ts;

			row_t *second_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
			memcpy(second_row, _row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			bool diff = false;
			for (int i = 0; i < CASCADING_LENGTH; i++) {
				if (second_row->tid[i]!=temp_row->tid[i]) {diff = true; break;}
			}
			if(second_row->wts!=temp_row->wts || diff){ //atomicity violated
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
		if (ts < temp_row->rts){
			#if DEBUG_PRINTF
			printf("[write rts failed]txn:%ld, key:%ld, lock:%lu, tid:%lu, rts:%lu, wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
			#endif
			rc = Abort;
			mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			return rc;
		}
		if (ts < temp_row->wts) {
			rc = RCOK;
			return rc;
		}
		//CAS(old_mutx, new_mutx) and read again
		uint64_t old_mutx = 0;
		uint64_t new_mutx = txn->get_txn_id() + 1;
		// uint64_t mutx_offset = access->offset+sizeof(uint64_t);
		uint64_t mutx_offset = access->offset;
		uint64_t cas_result = txn->cas_remote_content(access->location,mutx_offset,old_mutx,new_mutx);
		if(cas_result!=old_mutx){ //cas fail, atomicity violated
			#if DEBUG_PRINTF
			printf("[lock failed]txn:%ld, key:%ld, lock:%lu, tid:%lu, rts:%lu, wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
			#endif
			rc = Abort;
			mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			return rc;
		}
		//cas success, now do double read check
		row_t *second_row = (row_t *)mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
		memcpy(second_row, _row, row_t::get_row_size(ROW_DEFAULT_SIZE));
		
		if(ts < temp_row->rts){//atomicity violated
			rc = Abort;
			_row->mutx = 0;
			mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
			return rc;					
		}
		if (ts < temp_row->wts) {
			rc = RCOK;
			_row->mutx = 0;
			return rc;
		}
		//read success
		// access->wid = 
		for (int i =0; i < CASCADING_LENGTH; i++){
			access->wid[i] = temp_row->tid[i];
		}
		for (int i = 0; i < CASCADING_LENGTH; i++) {
			if (temp_row->tid[i]==0) {temp_row->tid[i] = txn->get_txn_id(); break;}
		}
		txn->cur_row->copy(temp_row);
		_row->mutx = 0;
		mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
		mem_allocator.free(second_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
		rc = RCOK;
		return rc;
	}
}

RC Row_rdma_ts1::local_commit(yield_func_t &yield, TxnManager * txn, Access *access, access_t type, uint64_t cor_id) {
#if DEBUG_PRINTF
	// printf("[本地提交]事务号：%d，主键：%d，锁：%lu，tid：%lu,rts:%lu,wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
	// assert(_row->tid != 0);
	RC rc = RCOK;

	if (type == XP) {
		//lock until success!
    	assert(txn->loop_cas_remote(access->location,access->offset,0,txn->get_txn_id()+1) == true);//lock in loop
#if DEBUG_PRINTF
		// printf("[本地写入前]事务号：%d，主键：%d，锁：%lu，tid：%lu,rts:%lu,wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
		ts_t ts = txn->get_timestamp();
		bool clean = false;
		for (int i = 0; i < CASCADING_LENGTH; i++) {
			if (_row->tid[i] == txn->get_txn_id()) clean = true;
			if (clean && _row->tid[i]!=0) {
				_row->tid[i] = 0;
			}
		}
		//local unlock
		_row->mutx = 0;
	} 
	else if (type == WR) {
		assert(txn->loop_cas_remote(access->location,access->offset,0,txn->get_txn_id()+1) == true);//lock in loop
		int i = 0;
		for (i = 0; i < CASCADING_LENGTH; i++) {
			if (_row->tid[i] == txn->get_txn_id()) break;
		}
		for (; i < CASCADING_LENGTH; i ++) {
			if (i + 1 < CASCADING_LENGTH && _row->tid[i + 1] != 0) {
				_row->tid[i] = _row->tid[i+1];
			}
			if (i + 1 == CASCADING_LENGTH) {
				_row->tid[i] = 0;
			}
		}
		_row->mutx = 0;
	}
	// assert(_row->mutx != 0);
	access->data->free_row();
	mem_allocator.free(access->data, row_t::get_row_size(access->data->tuple_size));
	// mem_allocator.free(temp_row,row_t::get_row_size(ROW_DEFAULT_SIZE));
#if DEBUG_PRINTF
	// printf("[本地提交后]事务号：%d，主键：%d，锁：%lu，tid：%lu，rts:%lu，wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
	return rc;
}

#endif
