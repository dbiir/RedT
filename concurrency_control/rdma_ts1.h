#include "global.h"
#if CC_ALG == RDMA_TS1
class row_t;

class RDMA_ts1 {
public:

	RC validate(yield_func_t &yield, TxnManager * txnMng, uint64_t cor_id);
	//re-read to validate the lock
	void write_remote(yield_func_t &yield, RC rc, TxnManager * txn, Access *access, uint64_t cor_id);
	void commit_write(yield_func_t &yield, TxnManager * txn , uint64_t num ,access_t type, uint64_t cor_id);
    void finish(RC rc, TxnManager * txnMng);
};

#endif
