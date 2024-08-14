/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@tencent.com
 *
 */
#include "global.h"
#include "helper.h"
#include "txn.h"
#include "si.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_si.h"
#if CC_ALG == SI

void si::init() {
    sem_init(&_semaphore, 0, 1);
}

RC si::validate(TxnManager * txn) {
    uint64_t start_time = get_sys_clock();
    uint64_t timespan;
    // sem_wait(&_semaphore);

    timespan = get_sys_clock() - start_time;
    txn->txn_stats.cc_block_time += timespan;
    txn->txn_stats.cc_block_time_short += timespan;

    start_time = get_sys_clock();
    RC rc = RCOK;

    // printf("SI Validate Start %ld\n",txn->get_txn_id());
    
    si_set_ent *rset, *wset;
    get_rw_set(txn, rset, wset);
    // si validate

    for (UInt32 i = 0; i < wset->set_size; i++) {
        if (wset->rows[i]->manager->validate_last_commit(txn) == Abort) {
            // printf("si Validate abort, %ld\n",txn->get_txn_id());
            rc = Abort;
        }
    }

    // if (rc != Abort) printf("si Validate ok, %ld\n",txn->get_txn_id());
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
    // printf("SI Validate End %ld: %d\n",txn->get_txn_id(),rc==RCOK);
    // sem_post(&_semaphore);
    return rc;
}

void si::gene_finish_ts(TxnManager * txn) {
    txn->set_commit_timestamp(glob_manager.get_ts(txn->get_thd_id()));
}

RC si::get_rw_set(TxnManager * txn, si_set_ent * &rset, si_set_ent *& wset) {
    wset = (si_set_ent*) mem_allocator.alloc(sizeof(si_set_ent));
    rset = (si_set_ent*) mem_allocator.alloc(sizeof(si_set_ent));
    wset->set_size = txn->get_write_set_size();
    rset->set_size = txn->get_read_set_size();
    wset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * wset->set_size);
    rset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * rset->set_size);
    wset->txn = txn;
    rset->txn = txn;

    UInt32 n = 0, m = 0;
    for (uint64_t i = 0; i < wset->set_size + rset->set_size; i++) {
        if (txn->get_access_type(i) == WR) {
            wset->rows[n ++] = txn->get_access_original_row(i);
        } else {
            rset->rows[m ++] = txn->get_access_original_row(i);
        }
    }

    assert(n == wset->set_size);
    assert(m == rset->set_size);
    return RCOK;
}
#endif
