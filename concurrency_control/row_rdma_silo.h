#pragma once

class table_t;
class Catalog;
class txn_man;
struct TsReqEntry;

#if CC_ALG==RDMA_SILO
#define LOCK_BIT (1UL << 63)

class Row_rdma_silo {
public:
	void 				init(row_t * row);
	RC 					access(TxnManager * txn, TsType type, row_t * local_row);

	bool				validate(ts_t tid, bool in_write_set);
	void				write(row_t * data, uint64_t tid);

	void 				lock();
	void 				release(TxnManager * txnMng , uint64_t num);
	bool				try_lock(TxnManager * txnMng , uint64_t num);
	uint64_t 			get_tid();
	bool 				assert_lock(uint64_t txn_id){ return _row->_tid_word == txn_id; }

private:

	volatile uint64_t	_tid_word;

 	pthread_mutex_t * 	_latch;
	ts_t 				_tid;
	row_t * 			_row; 
};

#endif
