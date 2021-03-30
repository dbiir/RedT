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

RC Row_rdma_ts1::access(TxnManager * txn, Access *access, access_t type) {
	RC rc;
	uint64_t starttime = get_sys_clock();
	ts_t ts = txn->get_timestamp();
	bool suc = false;

	//validate lock suc
	row_t *temp_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
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
	mem_allocator.free(temp_row,sizeof(row_t));
	return rc;
}

RC Row_rdma_ts1::local_commit(TxnManager * txn, Access *access, access_t type) {
#if DEBUG_PRINTF
	printf("[本地提交]事务号：%d，主键：%d，锁：%lu，tid：%lu,rts:%lu,wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
	assert(_row->tid != 0);
	RC rc = RCOK;
	//validate lock suc
	row_t *temp_row = (row_t *)mem_allocator.alloc(sizeof(row_t));
	bool suc = false;
	//lock until success!
	suc = rdmats_man.remote_try_lock(txn, temp_row, access->location, access->offset);
	while(suc == false){
		INC_STATS(txn->get_thd_id(),lock_retry_cnt,1);
#if DEBUG_PRINTF
		printf("本地提交 lock retry！事务号：%ld,主键：%ld\n",txn->get_txn_id(),_row->get_primary_key());
#endif
		suc = rdmats_man.remote_try_lock(txn, temp_row, access->location, access->offset);
	}
	if (type == XP) {
		_row->tid = 0;
	} 
	else if (type == WR) {
#if DEBUG_PRINTF
		printf("[本地写入前]事务号：%d，主键：%d，锁：%lu，tid：%lu,rts:%lu,wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
		ts_t ts = txn->get_timestamp();
		if (_row->wts < ts) _row->wts = ts;
		_row->tid = 0;
	}
	// assert(_row->mutx != 0);
	_row->mutx = 0;
	access->data->free_row();
	mem_allocator.free(access->data, sizeof(row_t));
	mem_allocator.free(temp_row,sizeof(row_t));
#if DEBUG_PRINTF
	printf("[本地提交后]事务号：%d，主键：%d，锁：%lu，tid：%lu，rts:%lu，wts:%lu\n",txn->get_txn_id(),_row->get_primary_key(),_row->mutx,_row->tid,_row->rts,_row->wts);
#endif
	return rc;
}

#endif
