/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@tencent.com
 *
 */

#ifndef ROW_NCC_H
#define ROW_NCC_H
#include "global.h"
#include <vector>
class table_t;
class Catalog;
class TxnManager;

enum NCCStatus {
    NCC_UNDECIDED = 0,
    NCC_COMMIT = 1,
    NCC_ABORT = 2
};

enum NCCRespType {
    NCC_INIT = 0,
    NCC_DONE = 1
};

struct NCCVersion {
    NCCTimeStamp tw;
    NCCTimeStamp tr;
    // ts_t tw; //写时间戳
    // ts_t twcid; //写时间戳的cid
    // ts_t tr; //读时间戳
    // ts_t trcid; //读时间戳的cid
    NCCStatus status;
    row_t * row;
    txnid_t txn_id;
    NCCVersion() {
        status = NCC_UNDECIDED;
        row = NULL;
        txn_id = 0;
    }
    NCCVersion(NCCTimeStamp tw, NCCTimeStamp tr, NCCStatus status, row_t * row, txnid_t txn_id) {
        this->tw = tw;
        this->tr = tr;
        this->status = status;
        this->row = row;
        this->txn_id = txn_id;
    }
};

class Row_ncc {
public:
    void init(row_t * row);
    // RC access(TxnManager * txn, access_t type, row_t * row);
    RC async_commit_or_abort_on_row(TxnManager * txn,bool is_commit);
    RC non_blocking_execute(NCCTimeStamp ts, access_t type, row_t * row, Access *access);
private:
    
    pthread_mutex_t * latch;
    //
    std::vector<NCCVersion*> versions;

    row_t * _row;
};

#endif
