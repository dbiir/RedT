/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@tencent.com
 *
 */

#ifndef _SI_H_
#define _SI_H_

#include "row.h"
#include "semaphore.h"

class TxnManager;
enum SIState { SI_RUNNING=0,SI_COMMITTED,SI_ABORTED};

class si_set_ent{
public:
    si_set_ent();
    UInt64 tn;
    TxnManager * txn;
    UInt32 set_size;
    row_t ** rows; //[MAX_WRITE_SET];
    si_set_ent * next;
};

class si {
public:
    void init();
    RC validate(TxnManager * txn);
    void gene_finish_ts(TxnManager * txn);
    RC get_rw_set(TxnManager * txn, si_set_ent * &rset, si_set_ent *& wset);
private:
    sem_t _semaphore;
};

#endif
