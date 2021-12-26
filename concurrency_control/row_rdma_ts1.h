#include "global.h"
#if CC_ALG == RDMA_TS1

class Row_rdma_ts1 {
public:
	void init(row_t * row);
	RC access(yield_func_t &yield, TxnManager * txn, Access *access, access_t type, uint64_t cor_id);
	RC local_commit(yield_func_t &yield, TxnManager * txn, Access *access, access_t type, uint64_t cor_id);
	row_t * _row;
};

#endif
