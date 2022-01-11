
#if CC_ALG == RDMA_DSLR_NO_WAIT

class Row_rdma_dslr_no_wait{
public:

	void init(row_t * row);
    RC lock_get(yield_func_t &yield,access_t type, TxnManager * txn, row_t * row,uint64_t cor_id);

private:

	row_t * _row;

};

#endif