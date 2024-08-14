/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@tencent.com
 *
 */

#include "txn.h"
#include "row.h"
#include "manager.h"
#include "si.h"
#include "row_si.h"
#include "mem_alloc.h"

void Row_si::init(row_t * row) {
    _row = row;
    si_read_lock = NULL;
    write_lock = NULL;

    prereq_mvcc = NULL;
    readhis = NULL;
    writehis = NULL;
    readhistail = NULL;
    writehistail = NULL;
    blatch = false;
    latch = (pthread_mutex_t *) mem_allocator.alloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(latch, NULL);
    whis_len = 0;
    rhis_len = 0;
    commit_lock = 0;
    preq_len = 0;
}

row_t * Row_si::clear_history(TsType type, ts_t ts) {
    SIHisEntry ** queue;
    SIHisEntry ** tail;
    switch (type) {
    case R_REQ:
        queue = &readhis;
        tail = &readhistail;
        break;
    case W_REQ:
        queue = &writehis;
        tail = &writehistail;
        break;
    default:
        assert(false);
    }
    SIHisEntry * his = *tail;
    SIHisEntry * prev = NULL;
    row_t * row = NULL;
    while (his && his->prev && his->prev->ts < ts) {
        prev = his->prev;
        assert(prev->ts >= his->ts);
        if (row != NULL) {
            row->free_row();
            mem_allocator.free(row, sizeof(row_t));
        }
        row = his->row;
        his->row = NULL;
        return_his_entry(his);
        his = prev;
        if (type == R_REQ) rhis_len --;
        else whis_len --;
    }
    *tail = his;
    if (*tail) (*tail)->next = NULL;
    if (his == NULL) *queue = NULL;
    return row;
}

SIReqEntry * Row_si::get_req_entry() {
    return (SIReqEntry *) mem_allocator.alloc(sizeof(SIReqEntry));
}

void Row_si::return_req_entry(SIReqEntry * entry) {
    mem_allocator.free(entry, sizeof(SIReqEntry));
}

SIHisEntry * Row_si::get_his_entry() {
    return (SIHisEntry *) mem_allocator.alloc(sizeof(SIHisEntry));
}

void Row_si::return_his_entry(SIHisEntry * entry) {
    if (entry->row != NULL) {
        entry->row->free_row();
        mem_allocator.free(entry->row, sizeof(row_t));
    }
    mem_allocator.free(entry, sizeof(SIHisEntry));
}

void Row_si::buffer_req(TsType type, TxnManager * txn)
{
    SIReqEntry * req_entry = get_req_entry();
    assert(req_entry != NULL);
    req_entry->txn = txn;
    req_entry->ts = txn->get_start_timestamp();
    req_entry->starttime = get_sys_clock();
    if (type == P_REQ) {
        preq_len ++;
        STACK_PUSH(prereq_mvcc, req_entry);
    }
}

// for type == R_REQ
//     debuffer all non-conflicting requests
// for type == P_REQ
//   debuffer the request with matching txn.
SIReqEntry * Row_si::debuffer_req( TsType type, TxnManager * txn) {
    SIReqEntry ** queue = &prereq_mvcc;
    SIReqEntry * return_queue = NULL;

    SIReqEntry * req = *queue;
    SIReqEntry * prev_req = NULL;
    if (txn != NULL) {
        assert(type == P_REQ);
        while (req != NULL && req->txn != txn) {
            prev_req = req;
            req = req->next;
        }
        assert(req != NULL);
        if (prev_req != NULL)
            prev_req->next = req->next;
        else {
            assert( req == *queue );
            *queue = req->next;
        }
        preq_len --;
        req->next = return_queue;
        return_queue = req;
    }
    return return_queue;
}

void Row_si::insert_history(ts_t ts, TxnManager * txn, row_t * row)
{
    SIHisEntry * new_entry = get_his_entry();
    new_entry->ts = ts;
    new_entry->txn = txn->get_txn_id();
    new_entry->row = row;
    if (row != NULL) {
        whis_len ++;
    } else {
        rhis_len ++;
    }
    SIHisEntry ** queue = (row == NULL)?
        &(readhis) : &(writehis);
    SIHisEntry ** tail = (row == NULL)?
        &(readhistail) : &(writehistail);
    SIHisEntry * his = *queue;
    while (his != NULL && ts < his->ts) {
        his = his->next;
    }

    if (his) {
        LIST_INSERT_BEFORE(his, new_entry,(*queue));
    } else
        LIST_PUT_TAIL((*queue), (*tail), new_entry);
}

SILockEntry * Row_si::get_entry() {
    SILockEntry * entry = (SILockEntry *)
        mem_allocator.alloc(sizeof(SILockEntry));
    entry->type = LOCK_NONE;
    entry->txn = 0;

    return entry;
}

void Row_si::get_lock(lock_t type, TxnManager * txn) {
    SILockEntry * entry = get_entry();
    entry->type = type;
    entry->start_ts = get_sys_clock();
    entry->txn = txn->get_txn_id();
    if (type == DLOCK_EX)
        commit_lock = txn->get_txn_id();
}

void Row_si::release_lock(lock_t type, TxnManager * txn) {
    if (type == DLOCK_EX) {
        if (commit_lock == txn->get_txn_id()) commit_lock = 0;
    }
}

RC Row_si::access(TxnManager * txn, TsType type, row_t * row) {
    RC rc = RCOK;
    ts_t ts = txn->get_commit_timestamp();
    ts_t start_ts = txn->get_start_timestamp();
    uint64_t starttime = get_sys_clock();
    txnid_t txnid = txn->get_txn_id();
    if (g_central_man) {
        glob_manager.lock_row(_row);
    } else {
        pthread_mutex_lock(latch);
    }
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
    if (type == R_REQ) {        
        // Read the row
        rc = RCOK;
        SIHisEntry * whis = writehis;
        while (whis != NULL && whis->ts > start_ts) {
            whis = whis->next;
        }
        row_t * ret = (whis == NULL) ? _row : whis->row;
        txn->cur_row = ret;
        assert(strstr(_row->get_table_name(), ret->get_table_name()));
        // Iterate over a version that is newer than the one you are currently reading.
        whis = writehis;
        while (whis != NULL && whis->ts > start_ts) {
            whis = whis->next;
        }
    } else if (type == P_REQ) {
        if (EARLY_PREPARE) {
            rc = validate(txn);
            if (rc == Abort) goto end;
        }
        // Add the write lock
        if (preq_len < g_max_pre_req){
            // printf("buf P_REQ %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
            buffer_req(P_REQ, txn);
            rc = RCOK;
        } else  {
            rc = Abort;
        }
    } else if (type == W_REQ) {
        rc = RCOK;
        release_lock(DLOCK_EX, txn);
        //TODO: here need to consider whether need to release the si-read lock.
        // release_lock(LOCK_SH, txn);

        // the corresponding prewrite request is debuffered.
        insert_history(ts, txn, row);
        // printf("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
        SIReqEntry * req = debuffer_req(P_REQ, txn);
        assert(req != NULL);
        return_req_entry(req);
    } else if (type == XP_REQ) {
        release_lock(DLOCK_EX, txn);

        // printf("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
        SIReqEntry * req = debuffer_req(P_REQ, txn);
        assert (req != NULL);
        return_req_entry(req);
    } else {
        assert(false);
    }

    if (rc == RCOK) {
        if (whis_len > g_his_recycle_len || rhis_len > g_his_recycle_len) {
            ts_t t_th = glob_manager.get_min_ts(txn->get_thd_id());
            if (readhistail && readhistail->ts < t_th) {
                clear_history(R_REQ, t_th);
            }
            // Here is a tricky bug. The oldest transaction might be
            // reading an even older version whose timestamp < t_th.
            // But we cannot recycle that version because it is still being used.
            // So the HACK here is to make sure that the first version older than
            // t_th not be recycled.
            if (whis_len > 1 && writehistail->prev->ts < t_th) {
                row_t * latest_row = clear_history(W_REQ, t_th);
                if (latest_row != NULL) {
                    assert(_row != latest_row);
                    _row->copy(latest_row);
                }
            }
        }
    }
end:
    uint64_t timespan = get_sys_clock() - starttime;
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;

    if (g_central_man) {
        glob_manager.release_row(_row);
     } else {
        pthread_mutex_unlock(latch);
     }

    return rc;
}

RC Row_si::validate(TxnManager * txn) {
    SIHisEntry *  whis = writehis;
    ts_t start_ts = txn->get_start_timestamp();
    RC rc = RCOK;
    if (commit_lock != 0 && commit_lock != txn->get_txn_id()) {
        DEBUG("si last commit lock %ld, %ld\n",commit_lock, txn->get_txn_id());
        rc = Abort;
        return rc;
    }
    get_lock(DLOCK_EX, txn);
    // Iterate over a version that is newer than the one you are currently reading.
    while (whis != NULL && whis->ts < start_ts) {
        whis = whis->next;
    }
    if (whis != NULL) {
        DEBUG("si last commit whis %ld, %ld, %ld\n",whis->ts, start_ts, txn->get_txn_id());
        release_lock(DLOCK_EX, txn);
        rc = Abort;
        return rc;
    }    
    return RCOK;
}

RC Row_si::validate_last_commit(TxnManager * txn) {
    RC rc = RCOK;
    if (g_central_man) {
        glob_manager.lock_row(_row);
    } else {
        pthread_mutex_lock(latch);
    }
    // INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
    rc = validate(txn);
    if (g_central_man) {
        glob_manager.release_row(_row);
    } else {
        pthread_mutex_unlock(latch);
    }
    return rc;
}
