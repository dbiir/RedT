
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3

class Row_rdma_2pl{
public:

	void init(row_t * row);
	//RC lock_get(lock_t type, TxnManager * txn, row_t * row);
	static bool conflict_lock(uint64_t lock_info, lock_t l2, uint64_t& new_lock_info);
	static void info_decode(uint64_t lock_info,uint64_t& lock_type,uint64_t& lock_num);
	static void info_encode(uint64_t& lock_info,uint64_t lock_type,uint64_t lock_num);
    RC lock_get(yield_func_t &yield,lock_t type, TxnManager * txn, row_t * row,uint64_t cor_id);
	static bool has_shared_lock(uint64_t lock_info) {
        uint64_t lock_type;
        uint64_t lock_num;
        Row_rdma_2pl::info_decode(lock_info,lock_type,lock_num);
        if(lock_type!=0 || lock_num <= 0) {
            return false;
        } else {
            return true;
        }
    }
	static bool has_exclusive_lock(uint64_t lock_info) {
		return lock_info == 3;
	}
private:

	row_t * _row;

};

#endif