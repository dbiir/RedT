
#if CC_ALG == RDMA_NO_WAIT

class RDMA_nowait{
public:

    void finish(RC rc, TxnManager * txn);

private:
    void write_and_unlock(row_t * row, row_t * data, TxnManager * txnMng); 
	void remote_write_and_unlock(RC rc, TxnManager * txnMng , uint64_t num);
	void unlock(row_t * row, TxnManager * txnMng); 
	void remote_unlock(TxnManager * txnMng , uint64_t num);
};

#endif