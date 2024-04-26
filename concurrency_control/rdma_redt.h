
#ifndef RDMA_REDT_H
#define RDMA_REDT_H
#if CC_ALG == RDMA_RED_T

class RDMA_redt{
public:

    RC finish(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id);
    RC commit_log(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id);
    RC commit_recover_log(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id);

private:
    RC write_and_unlock(yield_func_t &yield,row_t * row, row_t * data, TxnManager * txnMng,uint64_t cor_id);
    RC remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id);
    RC unlock(yield_func_t &yield,row_t * row , TxnManager * txnMng,uint64_t cor_id);
    RC remote_unlock(yield_func_t &yield,TxnManager * txnMng , uint64_t num,uint64_t cor_id);
};

#endif
#endif