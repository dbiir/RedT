#pragma once

#include "global.h"
#ifndef ROW_TICTOC_H
#define ROW_TICTOC_H

#if WRITE_PERMISSION_LOCK

#define LOCK_BIT (1UL << 63)
#define WRITE_BIT (1UL << 62)
#define RTS_LEN (15)
#define WTS_LEN (62 - RTS_LEN)
#define WTS_MASK ((1UL << WTS_LEN) - 1)
#define RTS_MASK (((1UL << RTS_LEN) - 1) << WTS_LEN)

#else

#define LOCK_BIT (1UL << 63)

#endif

class TxnManager;

class Row_tictoc {
public:

	Row_tictoc() : Row_tictoc(NULL) {};
	Row_tictoc(row_t * row);
	void    init(row_t * row);
	RC		access(access_t type, TxnManager * txn, row_t *& row,
					uint64_t &wts, uint64_t &rts);
	RC		read(TxnManager * txn, char * data,
				uint64_t &wts, uint64_t &rts, bool latch = true, bool remote = false);
	RC		write(TxnManager * txn, uint64_t &wts, uint64_t &rts, bool latch = true);

	void	latch();
	void	unlatch();

	RC		commit(access_t type, TxnManager * txn, row_t * data);
	RC		abort(access_t type, TxnManager * txn);
/////////////////////////////
	void	lock();
	bool	try_lock();
	RC		try_lock(TxnManager * txn);
	void	release(TxnManager * txn, RC rc);



	void	update_ts(uint64_t cts);
	// void	write_ptr(row_t * data, ts_t wts, char *& data_to_free);
/////////////////////////////

	bool	try_renew(ts_t wts, ts_t rts, ts_t &new_rts);
	bool                 renew(ts_t wts, ts_t rts, ts_t &new_rts);


	void                get_last_ts(ts_t & last_rts, ts_t & last_wts);

	ts_t                get_last_rts();
	ts_t                get_last_wts();

	uint64_t             get_wts() {return _wts;}
	uint64_t             get_rts() {return _rts;}
	void                 get_ts(uint64_t &wts, uint64_t &rts);
	void                set_ts(uint64_t wts, uint64_t rts);

	TxnManager *        _lock_owner;
  	#if OCC_LOCK_TYPE == WAIT_DIE || OCC_WAW_LOCK
	// #define MAN(txn) ((TicTocManager *) (txn)->get_cc_manager())
	struct CompareWait {
		bool operator() (TxnManager * en1, TxnManager * en2) const;
	};
	std::set<TxnManager *, CompareWait> _waiting_set;
	uint32_t _max_num_waits;
  	#endif


	bool                 is_deleted() { return _deleted; }
	void                 delete_row(uint64_t del_ts);
	enum State {
		SHARED,
		EXCLUSIVE
	};
	State                _state;
	uint32_t            _owner; // host node ID

	row_t *             _row;

	uint64_t            _wts; // last write timestamp
	uint64_t            _rts; // end lease timestamp

	pthread_mutex_t *     _latch;        // to guarantee read/write consistency
	bool                 _blatch;        // to guarantee read/write consistency
	bool                _ts_lock;     // wts/rts cannot be changed if _ts_lock is true.

	bool                _deleted;
	uint64_t            _delete_timestamp;

	// for locality predictor
	uint32_t             _num_remote_reads; // should cache a local copy if this number is too large.
};
// __attribute__ ((aligned(64)));

#endif
