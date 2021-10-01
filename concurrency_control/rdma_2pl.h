
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2 || CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WAIT_DIE || CC_ALG == RDMA_WOUND_WAIT

class RDMA_2pl{
public:

   void finish(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id);

private:
    // void write_and_unlock(row_t * row, row_t * data, TxnManager * txnMng); 
	// void remote_write_and_unlock(RC rc, TxnManager * txnMng , uint64_t num);
	// void unlock(row_t * row, TxnManager * txnMng); 
	// void remote_unlock(TxnManager * txnMng , uint64_t num);
    void write_and_unlock(yield_func_t &yield,row_t * row, row_t * data, TxnManager * txnMng,uint64_t cor_id);
    void remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id);
    void unlock(yield_func_t &yield,row_t * row , TxnManager * txnMng,uint64_t cor_id);
    void remote_unlock(yield_func_t &yield,TxnManager * txnMng , uint64_t num,uint64_t cor_id);
    

};

#endif