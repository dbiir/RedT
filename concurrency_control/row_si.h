/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@tencent.com
 *
 */

#ifndef ROW_SI_H
#define ROW_SI_H

class table_t;
class Catalog;
class TxnManager;

struct SIReqEntry {
    TxnManager * txn;
    ts_t ts;
    ts_t starttime;
    SIReqEntry * next;
};

struct SIHisEntry {
    // TxnManager * txn;
    txnid_t txn;
    ts_t ts;
    // only for write history. The value needs to be stored.
//    char * data;
    row_t * row;
    SIHisEntry * next;
    SIHisEntry * prev;
};

struct SILockEntry {
    lock_t type;
    ts_t   start_ts;
    // TxnManager * txn;
    txnid_t txn;
    SILockEntry * next;
    SILockEntry * prev;
};

class Row_si {
public:
    void init(row_t * row);
    RC access(TxnManager * txn, TsType type, row_t * row);
    RC validate_last_commit(TxnManager * txn);
    RC validate(TxnManager * txn);
private:
    pthread_mutex_t * latch;

    SILockEntry * si_read_lock;
    SILockEntry * write_lock;

    txnid_t commit_lock;

    bool blatch;

    row_t * _row;
    void get_lock(lock_t type, TxnManager * txn);
    void release_lock(lock_t type, TxnManager * txn);

    void insert_history(ts_t ts, TxnManager * txn, row_t * row);

    SIReqEntry * get_req_entry();
    void return_req_entry(SIReqEntry * entry);
    SIHisEntry * get_his_entry();
    void return_his_entry(SIHisEntry * entry);

    bool conflict(TsType type, ts_t ts);
    void buffer_req(TsType type, TxnManager * txn);
    SIReqEntry * debuffer_req( TsType type, TxnManager * txn = NULL);


    SILockEntry * get_entry();
    row_t * clear_history(TsType type, ts_t ts);

    SIReqEntry * prereq_mvcc;
    SIHisEntry * readhis;
    SIHisEntry * writehis;
    SIHisEntry * readhistail;
    SIHisEntry * writehistail;

    uint64_t whis_len;
    uint64_t rhis_len;
    uint64_t preq_len;
};

#endif
