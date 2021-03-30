#include "global.h"
#if CC_ALG == RDMA_TS1

class Row_rdma_ts1 {
public:
	void init(row_t * row);
	RC access(TxnManager * txn, Access *access, access_t type);
	RC local_commit(TxnManager * txn, Access *access, access_t type);
	
	row_t * _row;
};

#endif
