#include "global.h"
#if CC_ALG == RDMA_TS1
class row_t;

class RDMA_ts1 {
public:
	bool remote_try_lock(TxnManager * txn, uint64_t loc, uint64_t off);
	//re-read to validate the lock
	bool remote_try_lock(TxnManager * txn, row_t * temp_row, uint64_t loc, uint64_t off);
	void read_remote_row(TxnManager * txn, row_t * remote_row, uint64_t loc, uint64_t off);
	void write_remote(RC rc, TxnManager * txn, Access *access);
	void commit_write(TxnManager * txn , uint64_t num ,access_t type);

};

#endif
