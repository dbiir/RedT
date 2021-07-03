#include "global.h"
#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_cicada.h"
#include "mem_alloc.h"

#if CC_ALG == CICADA
void Row_cicada::init(row_t * row) {
    _row = row;
	blatch = false;
    latch = (pthread_mutex_t *)mem_allocator.alloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(latch, NULL);
	recordlist = NULL;
	record_len = 0;
	buffer_record(COMMITED, 0);
}

CARecordEntry * Row_cicada::get_record_ts(ts_t ts) {
	CARecordEntry * whis = recordlist;
	while(whis != NULL && (whis->wts > ts || whis->status == ABORTED)) whis = whis->next;
	return whis;
}

CARecordEntry * Row_cicada::get_record_id(uint64_t num) {
	CARecordEntry * whis = recordlist;
	while(whis != NULL && whis->id != num) whis = whis->next;
	return whis;
}

uint64_t Row_cicada::buffer_record(RecordStatus type, ts_t ts) {
	CARecordEntry * record_entry = (CARecordEntry *) mem_allocator.alloc(sizeof(CARecordEntry));
	assert(record_entry != NULL);
	if(ts == 0)	{ 	//head_entry
		record_entry->id = 0;
		record_entry->status = COMMITED;
		record_entry->wts = 0;
		record_entry->rts = 0;
	}
	else {
		record_entry->id = record_len;
		record_entry->status = PENDING;
		record_entry->rts = ts;
		record_entry->wts = ts;
	}
	
	record_entry->next = recordlist;
	recordlist = record_entry;
	record_len++;
	return record_entry->id;
}

RC Row_cicada::access(TxnManager * txn, access_t type, Access * access){
	RC rc = RCOK;
	ts_t ts = txn->get_timestamp();
	uint64_t starttime = get_sys_clock();

	if (g_central_man)
		glob_manager.lock_row(_row);
	else
		pthread_mutex_lock( latch );
	INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
	
	CARecordEntry * record;
	if (type == RD || type == WR) {	//读操作
		record = get_record_ts(ts);
		assert(record != NULL);
		if(record->status == PENDING) {
			rc = WAIT;
			DEBUG("CICADA access: txn = %ld, ts = %lu WAIT read -- %ld, version = %ld, status = %d, wts = %lu\n", txn->get_txn_id(),ts, _row->get_primary_key(),record->id,record->status,record->wts);
		} else {
			rc = RCOK;
			access->recordId = record->id;
			DEBUG("CICADA access: txn = %ld, ts = %lu READ key = %ld, version = %ld, wts = %lu\n", txn->get_txn_id(),ts, _row->get_primary_key(),record->id,record->wts);
		}		
	} 
	else 
		assert(false);

    uint64_t timespan = get_sys_clock() - starttime;
	txn->txn_stats.cc_time += timespan;
	txn->txn_stats.cc_time_short += timespan;

	if (g_central_man)
		glob_manager.release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return rc;
}

RC Row_cicada::validate(Access * access, ts_t ts, bool check_consis){
	RC rc = RCOK;
	CARecordEntry * record;
	if(access->type == RD) { //Rset
		if(!check_consis) {	// Update read timestamp
			record = get_record_id(access->recordId);
			if (record->rts < ts) record->rts = ts;
			DEBUG("RD validate ts = %lu,  modify rts\n",ts);
		}
		else {	// Check version consistency
			record = get_record_ts(ts);
			if (record->id != access->recordId) rc = Abort;
			DEBUG("RD validate ts = %lu,  check\n",ts);
		}
	}
	else if(access->type == WR) {	//Wset
		if(!check_consis) {	// insert pending version
			record = recordlist;
			while (record->status == ABORTED) record = record->next;
			
			if (record->id > access->recordId)
				rc = Abort;
			else {
				record = get_record_id(access->recordId);
				if (record->rts > ts)
					rc = Abort;
				else {
					buffer_record(PENDING, ts);
					DEBUG("WR validate ts = %lu, pending new version = %ld\n",ts,recordlist->id);
				}
			}
		}
		else {	// Check version consistency
			record = get_record_ts(ts);
			if (record->rts > ts) rc = Abort;
			DEBUG("WR validate ts = %lu,  check version, rc = %d(Abort:2)\n",ts,rc);
		}
	}
	else
		assert(false);
	
	return rc;
}

uint64_t Row_cicada::commit(TxnManager * txn, access_t type) {
	CARecordEntry * record;
	if (type == WR) {	//commit
		record = recordlist;
		assert(record->status == PENDING);
		assert(record->wts == txn->get_timestamp());
		record->status = COMMITED;
		DEBUG("[COMMIT] txn = %ld, key = %ld, id = %ld\n",txn->get_txn_id(), _row->get_primary_key(),record->id);
	} 
	else if (type == XP) {	//abort
		record = recordlist;
		if (record->status == PENDING && record->wts == txn->get_timestamp()) {
			record->status = ABORTED;
			DEBUG("[ABORT] txn = %ld, key = %ld, id = %ld\n",txn->get_txn_id(), _row->get_primary_key(),record->id);
		}
	}
	return 0;
}

#endif