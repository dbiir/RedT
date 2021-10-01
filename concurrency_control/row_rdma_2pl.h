
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT

class Row_rdma_2pl{
public:

	void init(row_t * row);
	//RC lock_get(lock_t type, TxnManager * txn, row_t * row);
	static bool conflict_lock(uint64_t lock_info, lock_t l2, uint64_t& new_lock_info);
	static void info_decode(uint64_t lock_info,uint64_t& lock_type,uint64_t& lock_num);
	static void info_encode(uint64_t& lock_info,uint64_t lock_type,uint64_t lock_num);
    RC lock_get(yield_func_t &yield,lock_t type, TxnManager * txn, row_t * row,uint64_t cor_id);

private:

	row_t * _row;

};

#endif