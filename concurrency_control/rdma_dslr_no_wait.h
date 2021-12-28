
#if CC_ALG == RDMA_DSLR_NO_WAIT

class RDMA_dslr_no_wait{
public:

   void finish(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id);

private:
    // void write_and_unlock(row_t * row, row_t * data, TxnManager * txnMng); 
	// void remote_write_and_unlock(RC rc, TxnManager * txnMng , uint64_t num);
	// void unlock(row_t * row, TxnManager * txnMng); 
	// void remote_unlock(TxnManager * txnMng , uint64_t num);
    void write_and_unlock(yield_func_t &yield,RC rc, row_t * row, row_t * data, TxnManager * txnMng,uint64_t cor_id);
    void remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id);
    void unlock(yield_func_t &yield,RC rc,row_t * row , TxnManager * txnMng,uint64_t cor_id);
    void remote_unlock(yield_func_t &yield,RC rc,TxnManager * txnMng , uint64_t num, uint64_t cor_id);
    

};

#endif