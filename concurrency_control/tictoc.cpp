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
#include "config.h"
#include "helper.h"
#include "txn.h"
#include "row.h"
#include "tictoc.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_tictoc.h"
bool Tictoc::_pre_abort = PRE_ABORT;

RC Tictoc::get_rw_set(TxnManager * txn, tictoc_set_ent * &rset, tictoc_set_ent *& wset) {
#if CC_ALG == TICTOC
	wset = (tictoc_set_ent*) mem_allocator.alloc(sizeof(tictoc_set_ent));
	rset = (tictoc_set_ent*) mem_allocator.alloc(sizeof(tictoc_set_ent));
	wset->set_size = txn->get_write_set_size();
	rset->set_size = txn->get_read_set_size();
	wset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * wset->set_size);
	rset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * rset->set_size);
	wset->access = (Access **) mem_allocator.alloc(sizeof(Access *) * wset->set_size);
	rset->access = (Access **) mem_allocator.alloc(sizeof(Access *) * rset->set_size);
	wset->txn = txn;
	rset->txn = txn;

	UInt32 n = 0, m = 0;
	for (uint64_t i = 0; i < wset->set_size + rset->set_size; i++) {
		if (txn->get_access_type(i) == WR){
            wset->access[n] = txn->txn->accesses[i];
			wset->rows[n] = txn->get_access_original_row(i);
            n++;
        }
		else {
            rset->access[m] = txn->txn->accesses[i];
			rset->rows[m] = txn->get_access_original_row(i);
            m++;
        }
	}
	assert(n == wset->set_size);
	assert(m == rset->set_size);
#endif
	return RCOK;
}

void Tictoc::init() {
    return ;
}

RC Tictoc::validate(TxnManager * txn)
{
    if (txn->_is_sub_txn)
        return validate_part(txn);
    else
        return validate_coor(txn);
}

RC Tictoc::validate_coor(TxnManager * txn)
{
    RC rc = RCOK;
#if CC_ALG == TICTOC
    // split _access_set into read_set and write_set
	tictoc_set_ent * wset;
	tictoc_set_ent * rset;
    get_rw_set(txn, rset, wset);
    // 1. lock the local write set
    // 2. compute commit ts
    // 3. validate local txn

#if OCC_WAW_LOCK && 0
    // TODO. we need this for TPCC.
    // In the current TPCC implementation, if row_insert() returns WAIT, the function will
    // not be called again. Therefore, after the lock is acquired, the manager does not have
    // the latest wts and rts.
    for (uint32_t i = 0; i < _index_access_set.size(); i ++) {
        IndexAccessTicToc * access = &_index_access_set[i];
        if (access->type != RD) {
            assert(access->locked);
            assert(access->manager->_lock_owner == _txn);
            access->manager->get_ts(access->wts, access->rts);
        }
    }
#endif

    // [Compute the commit timestamp]
    compute_commit_ts(txn);

#if OCC_WAW_LOCK
    if (ISOLATION_LEVEL == SERIALIZABLE)
        rc = validate_read_set(rset, txn, txn->_min_commit_ts);
#else
    if (rc == RCOK) {
        rc = lock_write_set(wset, txn);
        if (rc == Abort) {
            // INC_INT_STATS(num_aborts_ws, 1);  // local abort
        }
    }
    if (rc != Abort) {
        RC rc2 = RCOK;
        // if any rts has been updated, update _min_commit_ts.
        compute_commit_ts(txn);
        // at this point, _min_commit_ts is the final min_commit_ts for the prepare phase. .
        if (ISOLATION_LEVEL == SERIALIZABLE) {
            rc2 = validate_read_set(rset, txn, txn->_min_commit_ts);
            if (rc2 == Abort) {
                rc = rc2;
                // INC_INT_STATS(num_aborts_rs, 1);  // local abort
            }
        }
    }
#endif
    if (rc == Abort) {
        unlock_write_set(rc, wset, txn);
    }
#endif
    return rc;
}

RC Tictoc::validate_part(TxnManager * txn)
{
    RC rc = RCOK;
#if CC_ALG == TICTOC
    // split _access_set into read_set and write_set
	tictoc_set_ent * wset;
	tictoc_set_ent * rset;
    get_rw_set(txn, rset, wset);
    // 1. lock the local write set
    // 2. compute commit ts
    // 3. validate local txn

#if OCC_WAW_LOCK
    if (ISOLATION_LEVEL == SERIALIZABLE)
        rc = validate_read_set(rset, txn, txn->_min_commit_ts);
#else
    if (rc == RCOK) {
        rc = lock_write_set(wset, txn);
        if (rc == Abort) {
            // INC_INT_STATS(num_aborts_ws, 1);  // local abort
        }
    }
    if (rc != Abort) {
        if (validate_write_set(wset, txn, txn->_min_commit_ts) == Abort)
            rc = Abort;
    }
    if (rc != Abort) {
        if (validate_read_set(rset, txn, txn->_min_commit_ts) == Abort) {
            rc = Abort;
            // INC_INT_STATS(num_Aborts_rs, 1);
        }
    }
#endif
    if (rc == Abort) {
        unlock_write_set(rc, wset, txn);
        return rc;
    } else {
        // if (txn->query->readonly()) {
        //     unlock_write_set(rc, wset, txn);
        //     return Commit;
        // } else
        //     return rc;
    }
#endif
    return rc;
}

void Tictoc::compute_commit_ts(TxnManager * txn)
{
#if CC_ALG == TICTOC
	uint64_t size = txn->get_write_set_size();
	size += txn->get_read_set_size();
	for (uint64_t i = 0; i < size; i++) {
		if (txn->get_access_type(i) == WR){
            txn->_min_commit_ts = txn->txn->accesses[i]->orig_rts + 1 >  txn->_min_commit_ts ? txn->txn->accesses[i]->orig_rts + 1 : txn->_min_commit_ts;
        }
		else {
            txn->_min_commit_ts = txn->txn->accesses[i]->orig_wts >  txn->_min_commit_ts ? txn->txn->accesses[i]->orig_wts : txn->_min_commit_ts;
        }
	}
#endif
#if 0
    for (auto access : _remote_set)
    {
        if (access.type == RD)
            _min_commit_ts = max(access.wts, _min_commit_ts);
        else if (access.type == WR)
            _min_commit_ts = max(access.rts + 1, _min_commit_ts);
    }
    for (auto access : _index_access_set) {
        if (access.type == RD || access.type == WR)
            _min_commit_ts = max(access.wts, _min_commit_ts);
        else if (access.type == INS || access.type == DEL)
            _min_commit_ts = max(access.rts + 1, _min_commit_ts);
    }
#endif
}

RC
Tictoc::validate_read_set(tictoc_set_ent * rset, TxnManager * txn, uint64_t commit_ts)
{
#if CC_ALG == TICTOC
#if 0
    for (auto access : _index_access_set) {
        if (access.type == INS || access.type == DEL)
            continue;
        if (access.rts >= commit_ts) {
            INC_INT_STATS(num_no_need_to_renewal, 1);
            continue;
        }
        INC_INT_STATS(num_renewals, 1);
#if LOCK_ALL_BEFORE_COMMIT
        if (!access.manager->renew(access.wts, commit_ts, access.rts))  // without lock check
#else
        if (!access.manager->try_renew(access.wts, commit_ts, access.rts))
#endif
        {
            return Abort;
        }
    }
#endif
    // validate the read set.
    for (UInt32 i = 0; i < rset->set_size; i++) {
        if (rset->access[i]->orig_rts >= commit_ts) {
            continue;
        }
        row_t * cur_rrow = rset->rows[i];
        if (!cur_rrow->manager->try_renew(rset->access[i]->orig_wts, commit_ts, rset->access[i]->orig_rts))
        {
            return Abort;
        }
    }
#endif
    return RCOK;
}

RC
Tictoc::lock_write_set(tictoc_set_ent * wset, TxnManager * txn)
{
    assert(!OCC_WAW_LOCK);
    RC rc = RCOK;
#if CC_ALG == TICTOC
    txn->_num_lock_waits = 0;
#if 0
    vector<AccessTicToc *>::iterator it;
    for (uint32_t i = 0; i < _index_access_set.size(); i++) {
        IndexAccessTicToc &access = _index_access_set[i];
        if (access.type == RD || access.type == WR) continue;
        rc = access.manager->try_lock(_txn);
        if (rc == Abort) return Abort;
        if (rc == WAIT)
            ATOM_ADD_FETCH(_num_lock_waits, 1);
        if (rc == WAIT || rc == RCOK) {
            access.locked = true;
            access.rts = access.manager->get_rts();
            if (access.wts != access.manager->get_wts()) {
                return Abort;
            }
        }
    }
#endif
    for (UInt32 i = 0; i < wset->set_size; i++) {
        row_t * cur_wrow = wset->rows[i];
        rc = cur_wrow->manager->try_lock(txn);
        if (rc == WAIT || rc == RCOK)
            wset->access[i]->locked = true;
        if (rc == WAIT)
            ATOM_ADD_FETCH(txn->_num_lock_waits, 1);
        wset->access[i]->orig_rts = cur_wrow->manager->get_rts();
        if (wset->access[i]->orig_wts != cur_wrow->manager->get_wts()) {
            rc = Abort;
        }
        if (rc == Abort)
            return Abort;
    }
    if (rc == Abort)
        return Abort;
    else if (txn->_num_lock_waits > 0)
        return WAIT;
#endif
    return rc;
}

void
Tictoc::cleanup(RC rc, TxnManager * txn)
{
#if CC_ALG == TICTOC
	tictoc_set_ent * wset;
	tictoc_set_ent * rset;
  get_rw_set(txn, rset, wset);
  unlock_write_set(rc, wset, txn);
#endif
}

void
Tictoc::unlock_write_set(RC rc, tictoc_set_ent * wset, TxnManager * txn)
{
#if CC_ALG == TICTOC
#if 0
    for (vector<IndexAccessTicToc>::iterator it = _index_access_set.begin();
        it != _index_access_set.end(); it ++ )
    {
        if (it->locked) {
            it->manager->release(_txn, rc);
            it->locked = false;
        }
    }
#endif
    for (UInt32 i = 0; i < wset->set_size; i++) {
        if (wset->access[i]->locked) {
            row_t * cur_wrow = wset->rows[i];
            cur_wrow->manager->release(txn, rc);
            wset->access[i]->locked = false;
        }
    }
#endif
}

RC
Tictoc::validate_write_set(tictoc_set_ent * wset, TxnManager * txn, uint64_t commit_ts)
{
#if CC_ALG == TICTOC
    for (UInt32 i = 0; i < wset->set_size; i++) {
        if (txn->_min_commit_ts <= wset->access[i]->orig_rts){
            // INC_INT_STATS(int_urgentwrite, 1);  // write too urgent
            return Abort;
        }
    }
#if 0
    for (auto access : _index_access_set) {
        if ((access.type == INS || access.type == DEL) && _min_commit_ts <= access.rts) {
            INC_INT_STATS(int_urgentwrite, 1);  // write too urgent
            return Abort;
        }
    }
#endif
#endif
    return RCOK;
}

bool
Tictoc::is_txn_ready(TxnManager * txn)
{
    return txn->_num_lock_waits == 0;
}

void
Tictoc::set_txn_ready(RC rc, TxnManager * txn)
{
    // this function can be called by multiple threads concurrently
    if (rc == RCOK)
        ATOM_SUB_FETCH(txn->_num_lock_waits, 1);
    else {
        txn->_signal_abort = true;
    }
}

