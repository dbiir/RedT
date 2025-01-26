/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@tencent.com
 *
 */

#include "txn.h"
#include "row.h"
#include "manager.h"
#include "ncc.h"
#include "row_ncc.h"
#include "mem_alloc.h"
#include "helper.h"

void Row_ncc::init(row_t * row) {
    _row = row;
    latch = (pthread_mutex_t *) mem_allocator.alloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(latch, NULL);
}

RC Row_ncc::non_blocking_execute(NCCTimeStamp ts, access_t type, row_t * row, Access *access) {
    RC rc = RCOK;
    pthread_mutex_lock(latch);
    Response *resp;
    if (type == RD) {        
        NCCVersion* version = versions[versions.size() - 1];
        version->tr = maxNCCTimeStamp(version->tr, ts);
        resp = new Response(version->tw, version->tr, row, NCCRespType::NCC_INIT);
    } else if (type == WR) {
        NCCVersion* version = versions[versions.size() - 1];
        NCCVersion* new_version = new NCCVersion();
        NCCTimeStamp tr_1 = version->tr;
        tr_1.time = tr_1.time + 1;
        new_version->tw = maxNCCTimeStamp(ts,tr_1);
        new_version->tr = new_version->tw;
        new_version->row = row;
        new_version->status = NCC_UNDECIDED;
        versions.push_back(new_version);
        resp = new Response(new_version->tw, new_version->tr, row, NCCRespType::NCC_DONE);
    }
    pthread_mutex_unlock(latch);
    //todo: access需要再改改
    //todo: 这里改成，将读写集塞到resp_qs中，也把resp塞到access里
    NCCQueueEntry* qe = new NCCQueueEntry(resp, access, ts, NCCStatus::NCC_UNDECIDED);
    access->ncc_qe = qe;
    resp_qs.insert(row->get_primary_key(), qe);
    resp_qs.RespTimeingControl(row->get_primary_key(), row);
    
    return rc;
}

RC Row_ncc::async_commit_or_abort_on_row(TxnManager * txn, bool is_commit) {
    pthread_mutex_lock(latch); 
    // 写一段遍历std::vector<NCCVersion*> versions的代码
    std::vector<int> remove_list;
    for (uint64_t i = 0; i < versions.size(); i++) {
        NCCVersion* version = versions[i];
        if (version->status == NCC_UNDECIDED && version->txn_id == txn->get_txn_id()) {
            if (is_commit) {
                version->status = NCC_COMMIT;
            } else {
                remove_list.push_back(i);
                // version->status = NCC_ABORT;
            }
        }
    }
    for (uint64_t i = remove_list.size(); i >= 0; i--) {
        versions.erase(versions.begin() + remove_list[i]);
    }
    pthread_mutex_unlock(latch);
    return RCOK;
}
