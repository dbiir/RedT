
#if CC_ALG == RDMA_NO_WAIT

class Row_rdma_nowait{
public:

	void init(row_t * row);
	RC lock_get(lock_t type, TxnManager * txn, row_t * row);
	static bool conflict_lock(uint64_t lock_info, lock_t l2, uint64_t& new_lock_info);
	static void info_decode(uint64_t lock_info,int& lock_type,int& lock_num);
	static void info_encode(uint64_t& lock_info,int lock_type,int lock_num);

private:

	row_t * _row;

};

#endif