
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3

class RDMA_2pl{
public:

    RC finish(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id);
    RC commit_log(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id);

private:
    // void write_and_unlock(row_t * row, row_t * data, TxnManager * txnMng); 
	// void remote_write_and_unlock(RC rc, TxnManager * txnMng , uint64_t num);
	// void unlock(row_t * row, TxnManager * txnMng); 
	// void remote_unlock(TxnManager * txnMng , uint64_t num);
    RC write_and_unlock(yield_func_t &yield,row_t * row, row_t * data, TxnManager * txnMng,uint64_t cor_id);
    RC remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id);
    RC unlock(yield_func_t &yield,row_t * row , TxnManager * txnMng,uint64_t cor_id);
    RC remote_unlock(yield_func_t &yield,TxnManager * txnMng , uint64_t num,uint64_t cor_id);

};

#endif