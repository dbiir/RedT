#include "global.h"
#if CC_ALG == RDMA_TS1
class row_t;

class RDMA_ts1 {
public:
	bool remote_try_lock(TxnManager * txn, uint64_t loc, uint64_t off);
	//re-read to validate the lock
	bool remote_try_lock(TxnManager * txn, row_t * temp_row, uint64_t loc, uint64_t off);
	void read_remote_row(TxnManager * txn, row_t * remote_row, uint64_t loc, uint64_t off);
	void write_remote(yield_func_t &yield, RC rc, TxnManager * txn, Access *access, uint64_t cor_id);
	void commit_write(yield_func_t &yield, TxnManager * txn , uint64_t num ,access_t type, uint64_t cor_id);
    void finish(RC rc, TxnManager * txnMng);
};

#endif
