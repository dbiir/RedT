
#include "row.h"
#if CC_ALG == RDMA_SI

class Row_rdma_si{
public:
	void init(row_t * row);
    RC read(yield_func_t &yield,access_t type, TxnManager * txn, row_t * row,uint64_t cor_id);
    RC write(yield_func_t &yield,access_t type, TxnManager * txn, row_t * row,uint64_t cor_id);
    RC access(yield_func_t &yield,access_t type, TxnManager * txn, row_t * row,uint64_t cor_id);
private:
	row_t * _row;
};

#endif