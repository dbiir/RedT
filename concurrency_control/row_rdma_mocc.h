#pragma once

class table_t;
class Catalog;
class txn_man;
struct TsReqEntry;

#if CC_ALG == RDMA_MOCC
#define LOCK_BIT (1UL << 63)

class Row_rdma_mocc {
public:
	void 				init(row_t * row);
	RC 					access(yield_func_t &yield, TxnManager * txn, TsType type, row_t * local_row, uint64_t cor_id);

	bool				validate(ts_t tid,ts_t ts, bool in_write_set);
	void				write(row_t * data, uint64_t tid , ts_t time);

	void 				lock();
	void 				release(yield_func_t &yield, TxnManager * txnMng , uint64_t num, uint64_t cor_id);
	bool				try_lock(yield_func_t &yield, TxnManager * txnMng , uint64_t num, uint64_t cor_id);
	uint64_t 			get_tid();
	bool 				assert_lock(uint64_t txn_id){ return _row->_tid_word == txn_id; }

private:

	volatile uint64_t	_tid_word;
	ts_t                timestamp;

 	pthread_mutex_t * 	_latch;
	ts_t 				_tid;
	row_t * 			_row; 
};

#endif
