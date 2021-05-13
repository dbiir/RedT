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

#include "row.h"
#include "txn.h"
#include "row_tictoc.h"
#include "mem_alloc.h"
#include "manager.h"
#include "helper.h"
#include "tictoc.h"


#if OCC_LOCK_TYPE == WAIT_DIE || OCC_WAW_LOCK
bool
Row_tictoc::CompareWait::operator() (TxnManager * en1, TxnManager * en2) const
{
    return en1->get_priority() < en2->get_priority();
}
#endif

Row_tictoc::Row_tictoc(row_t * row) {
	_row = row;
	_latch = new pthread_mutex_t;
	_blatch = false;
	pthread_mutex_init( _latch, NULL );
	_wts = 0;
	_rts = 0;
	_ts_lock = false;
	_num_remote_reads = 0;

  #if OCC_LOCK_TYPE == WAIT_DIE || OCC_WAW_LOCK
	  _max_num_waits = g_max_num_waits;
	_lock_owner = NULL;
  #endif
#if TICTOC_MV
	_hist_wts = 0;
#endif

	_deleted = false;
	_delete_timestamp = 0;
}

void Row_tictoc::init(row_t * row) {
	_row = row;
	_latch = new pthread_mutex_t;
	_blatch = false;
	pthread_mutex_init( _latch, NULL );
	_wts = 0;
	_rts = 0;
	_ts_lock = false;
	_num_remote_reads = 0;

  #if OCC_LOCK_TYPE == WAIT_DIE || OCC_WAW_LOCK
	  _max_num_waits = g_max_num_waits;
	_lock_owner = NULL;
  #endif
#if TICTOC_MV
	_hist_wts = 0;
#endif

	_deleted = false;
	_delete_timestamp = 0;
}

void
Row_tictoc::latch()
{
    pthread_mutex_lock( _latch );
}

void
Row_tictoc::unlatch()
{
    pthread_mutex_unlock( _latch );
}

RC
Row_tictoc::access(access_t type, TxnManager * txn, row_t *& row,
					uint64_t &wts, uint64_t &rts) {
	uint64_t starttime = get_sys_clock();
	RC rc = RCOK;
  if(type == RD || !OCC_WAW_LOCK)
	read(txn, NULL, wts, rts, true);
  else if(type == WR)
	write(txn, wts, rts);
//   rc = read_and_write(type, txn, row);

  uint64_t timespan = get_sys_clock() - starttime;
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;
  return rc;
}

RC
Row_tictoc::read(TxnManager * txn, char * data,
				 uint64_t &wts, uint64_t &rts, bool latch, bool remote)
{
	uint64_t starttime = get_sys_clock();
    if (latch)
		this->latch();
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
	wts = _wts;
	rts = _rts;
    // txn->cur_row = _row;
	if (data)
		memcpy(data, _row->get_data(), _row->get_tuple_size());

	// if (txn->is_sub_txn())
	// 	_num_remote_reads ++;

	if (latch)
		unlatch();
	return RCOK;
}

RC
Row_tictoc::write(TxnManager * txn, uint64_t &wts, uint64_t &rts, bool latch)
{
	RC rc = RCOK;
	assert(OCC_WAW_LOCK);
#if OCC_LOCK_TYPE == NO_WAIT
	if (_ts_lock)
		return Abort;
#endif
    uint64_t starttime = get_sys_clock();
	if (latch)
		pthread_mutex_lock( _latch );
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
	if (!_ts_lock) {
		_ts_lock = true;
		_lock_owner = txn;
	} else if (_lock_owner != txn) {
#if OCC_LOCK_TYPE == NO_WAIT
		rc = Abort;
#else
		assert(OCC_LOCK_TYPE == WAIT_DIE);
		// M_ASSERT(txn->get_txn_id() != _lock_owner->get_txn_id(),
		// 			"txn=%ld, _lock_owner=%ld. ID=%ld\n", (uint64_t)txn, (uint64_t)_lock_owner, txn->get_txn_id());
		// txn has higher priority, should wait.
		// assert (MAN(txn)->get_priority() != MAN(_lock_owner)->get_priority());
		if (_waiting_set.size() < _max_num_waits &&
            txn->get_priority() < _lock_owner->get_priority())
		{
			_waiting_set.insert(txn);
			rc = WAIT;
			// txn->_start_wait_time = get_sys_clock();
		} else {
			rc = Abort;
            DEBUG("tictoc abort 158 %ld,%lu -- %ld\n",txn->get_txn_id(),txn->_min_commit_ts,_row->get_primary_key());
        }
#endif
	}
	if (rc == RCOK) {
		wts = _wts;
		rts = _rts;
	}
	if (latch)
		pthread_mutex_unlock( _latch );
	return rc;
}

RC
Row_tictoc::abort(access_t type, TxnManager * txn) {
  	// uint64_t mtx_wait_starttime = get_sys_clock();
  	// while(!ATOM_CAS(wkdb_avail,true,false)) { }
  	// INC_STATS(txn->get_thd_id(),mtx[32],get_sys_clock() - mtx_wait_starttime);
  	// DEBUG("wkdb Abort %ld: %d -- %ld\n",txn->get_txn_id(),type,_row->get_primary_key());

  	return Abort;
}

RC
Row_tictoc::commit(access_t type, TxnManager * txn, row_t * data) {
	uint64_t mtx_wait_starttime = get_sys_clock();
	// while(!ATOM_CAS(wkdb_avail,true,false)) { }
	INC_STATS(txn->get_thd_id(),mtx[33],get_sys_clock() - mtx_wait_starttime);
	DEBUG("tictoc Commit %ld: %d,%lu -- %ld\n",txn->get_txn_id(),type,txn->_min_commit_ts,_row->get_primary_key());


	uint64_t wts = txn->_min_commit_ts;
	latch();
	if (_deleted)
		assert(wts < _delete_timestamp);
    _wts = wts;
    _rts = wts;
	_row->copy(data);
	unlatch();

	return RCOK;
}

RC
Row_tictoc::try_lock(TxnManager * txn)
{
#if LOCK_ALL_DEBUG
    printf("Trying to lock read set for txn_id %ld\n", txn_id);
#endif
    RC rc = RCOK;
    pthread_mutex_lock( _latch );
    assert(!OCC_WAW_LOCK);
#if OCC_LOCK_TYPE == NO_WAIT
      if (!_ts_lock) {
        _ts_lock = true;
        rc = RCOK;
    } else
        rc = Abort;
#elif OCC_LOCK_TYPE == WAIT_DIE
      if (!_ts_lock) {
        _ts_lock = true;
        assert(_lock_owner == NULL);
        _lock_owner = txn;
        rc = RCOK;
    } else {
        if (txn->get_txn_id() == _lock_owner->get_txn_id())
            cout << "Error  " << txn->get_txn_id() << "  " << _lock_owner->get_txn_id() << endl;
        // M_ASSERT(txn->get_txn_id() != _lock_owner->get_txn_id(),
        //             "txn=%ld, _lock_owner=%ld. ID=%ld\n", (uint64_t)txn, (uint64_t)_lock_owner, txn->get_txn_id());
        // txn has higher priority, should wait.
        if (_waiting_set.size() >= _max_num_waits){
            rc = Abort;
            DEBUG("tictoc abort 230 %ld,%lu -- %ld\n",txn->get_txn_id(),txn->_min_commit_ts,_row->get_primary_key());
        } else if (txn->get_priority() < _lock_owner->get_priority()) {
            _waiting_set.insert(txn);
            rc = WAIT;
        } else {
            rc = Abort;
            DEBUG("tictoc abort 236 %ld,%lu -- %ld\n",txn->get_txn_id(),txn->_min_commit_ts,_row->get_primary_key());
        }
    }
#endif
    pthread_mutex_unlock( _latch );
    return rc;
}

bool
Row_tictoc::try_renew(ts_t wts, ts_t rts, ts_t &new_rts)
{
    if (wts != _wts) {
        if(_wts < rts)
        {
            // INC_INT_STATS(int_inevitable, 1);
			// report error
        }

        if(_wts == rts + 1) {
            // INC_INT_STATS(int_aborts_rs2, 1);  // The latest version is right behind our version.
			// report error
        }
        else {
            // INC_INT_STATS(int_possibMVCC, 1);
            // INC_INT_STATS(int_aborts_rs1, 1);  // The latest version is right behind our version.
			// report error
        }
        return false;
    }
    latch();
    if (_deleted) {
        bool success = (wts == _wts && rts < _delete_timestamp);
        unlatch();
        return success;
    }
    assert(!_deleted);
    if (_ts_lock) {
        // INC_INT_STATS(int_aborts_rs3, 1);  // Locked by others
        // TODO. even if _ts_lock == true (meaning someone may write to the tuple soon)
        // should check the lower bound of upcoming wts. Maybe we can still renew the current rts without
        // hurting the upcoming write.
        pthread_mutex_unlock( _latch );
        return false;
    }

    if (wts != _wts) {
        if(_wts < rts)
        {
            // INC_INT_STATS(int_inevitable, 1);
        }

        if(_wts == rts + 1) {
            // INC_INT_STATS(int_aborts_rs2, 1);  // The latest version is right behind our version.
        }
        else {
            // INC_INT_STATS(int_possibMVCC, 1);
            // INC_INT_STATS(int_aborts_rs1, 1);  // The latest version is right behind our version.
        }
        pthread_mutex_unlock( _latch );
        return false;
    }
      new_rts = _rts;
    if (rts > _rts) {
        _rts = rts;
        new_rts = rts;
    }
    pthread_mutex_unlock( _latch );
    return true;
}

void
Row_tictoc::release(TxnManager * txn, RC rc)
{
#if CC_ALG == TICTOC
    pthread_mutex_lock( _latch );
#if !OCC_WAW_LOCK
  #if OCC_LOCK_TYPE == NO_WAIT
    _ts_lock = false;
  #elif OCC_LOCK_TYPE == WAIT_DIE
    if (rc == RCOK)
        assert(_ts_lock && txn == _lock_owner);
    if (txn == _lock_owner) {
        if (_waiting_set.size() > 0) {
            // TODO. should measure how often each case happens
            if (rc == Abort) {
                TxnManager * next = *_waiting_set.rbegin();
                set<TxnManager *>::iterator last = _waiting_set.end();
                last --;
                _waiting_set.erase( last );
                _lock_owner = next;
                next->set_txn_ready(RCOK);
            } else { // rc == COMMIT
                for (std::set<TxnManager *>::iterator it = _waiting_set.begin();
                     it != _waiting_set.end(); it ++) {
                    (*it)->set_txn_ready(Abort);
                }
                _waiting_set.clear();
                _ts_lock = false;
                _lock_owner = NULL;
            }
        } else {
            _ts_lock = false;
            _lock_owner = NULL;
        }
    } else {
        assert(rc == Abort);
        // the txn may not be in _waiting_set.
          _waiting_set.erase(txn);
    }
  #endif
#else // OCC_WAW_LOCK
    assert(OCC_WAW_LOCK);
    if (txn != _lock_owner) {
        _waiting_set.erase( txn );
        pthread_mutex_unlock( _latch );
        return;
    }

    assert(_ts_lock);
  #if OCC_LOCK_TYPE == NO_WAIT
    _ts_lock = false;
  #elif OCC_LOCK_TYPE == WAIT_DIE
    if (_waiting_set.size() > 0) {
        set<TxnManager *>::iterator last = _waiting_set.end();
        last --;

        TxnManager * next = *last;
        _waiting_set.erase( last );
        _lock_owner = next;
        next->_lock_acquire_time = get_sys_clock();
        if (rc == Abort) {
            next->_lock_acquire_time_abort = get_sys_clock();
            next->_lock_acquire_time_commit = 0;
        } else {
            next->_lock_acquire_time_commit = get_sys_clock();
            next->_lock_acquire_time_abort = 0;
        }
        // assert(MAN(next)->get_priority() < MAN(txn)->get_priority());
        // COMPILER_BARRIER
        tictoc_man.set_txn_ready(RCOK, next);
    } else {
        _ts_lock = false;
        _lock_owner = NULL;
    }
  #endif
#endif
    pthread_mutex_unlock( _latch );
#endif
}

void
Row_tictoc::get_ts(uint64_t &wts, uint64_t &rts)
{
#if CC_ALG == TICTOC
    pthread_mutex_lock( _latch );
    wts = _wts;
    rts = _rts;
    pthread_mutex_unlock( _latch );
#endif
}

bool
Row_tictoc::renew(ts_t wts, ts_t rts, ts_t &new_rts) // Renew without lock checking
{

#if LOCK_ALL_BEFORE_COMMIT
#if ATOMIC_WORD
    uint64_t v = _ts_word;
    uint64_t lock_mask = (WRITE_PERMISSION_LOCK)? WRITE_BIT : LOCK_BIT;
    if ((v & WTS_MASK) == wts && ((v & RTS_MASK) >> WTS_LEN) >= rts - wts)
        return true;
    if (v & lock_mask)
        return false;
  #if TICTOC_MV
      COMPILER_BARRIER
      uint64_t hist_wts = _hist_wts;
    if (wts != (v & WTS_MASK)) {
        if (wts == hist_wts && rts < (v & WTS_MASK)) {
            return true;
        } else {
            return false;
        }
    }
  #else
    if (wts != (v & WTS_MASK))
        return false;
  #endif

    ts_t delta_rts = rts - wts;
    if (delta_rts < ((v & RTS_MASK) >> WTS_LEN)) // the rts has already been extended.
        return true;
    bool rebase = false;
    if (delta_rts >= (1 << RTS_LEN)) {
        rebase = true;
        uint64_t delta = (delta_rts & ~((1 << RTS_LEN) - 1));
        delta_rts &= ((1 << RTS_LEN) - 1);
        wts += delta;
    }
    uint64_t v2 = 0;
    v2 |= wts;
    v2 |= (delta_rts << WTS_LEN);
    while (true) {
        uint64_t pre_v = __sync_val_compare_and_swap(&_ts_word, v, v2);
        if (pre_v == v)
            return true;
        v = pre_v;
        if (rebase || (v & lock_mask) || (wts != (v & WTS_MASK)))
            return false;
        else if (rts < ((v & RTS_MASK) >> WTS_LEN))
            return true;
    }
    assert(false);
    return false;
#else
#if TICTOC_MV
    if (wts < _hist_wts)
        return false;
#else
    if (wts != _wts){
        if (_wts > rts + 1)
            INC_INT_STATS(int_possibMVCC, 1);
        return false;
    }
#endif
    pthread_mutex_lock( _latch );

    if (wts != _wts) {
#if TICTOC_MV
        if (wts == _hist_wts && rts < _wts) {
            pthread_mutex_unlock( _latch );
            return true;
        }
#endif
        if (_wts > rts + 1)
            INC_INT_STATS(int_possibMVCC, 1);
        pthread_mutex_unlock( _latch );
        return false;
    }
    new_rts = _rts;
    if (rts > _rts) {
        _rts = rts;
        new_rts = rts;
    }
#if ENABLE_LOCAL_CACHING
    if (_row) {
    #if RO_LEASE
        uint64_t max_rts = _row->get_table()->get_max_rts();
        if (max_rts > _rts)
            new_rts = _rts = max_rts;
    #endif
    }
#endif

    pthread_mutex_unlock( _latch );
    return true;
#endif
#else
    assert(false); // Should not be called if LOCK_ALL_BEFORE_COMMIT is false.
#endif

}



