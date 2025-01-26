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
#include "ncc.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_ncc.h"
#include "msg_queue.h"

void Ncc::init() {
    // sem_init(&_semaphore, 0, 1);
}

bool ResponseQueues::TxnCanSend(TxnManager* txn) {
    for (uint64_t i = 0; i < txn->get_access_cnt(); i++) {
        Access* access = txn->get_access(i);
        Response* resp = access->ncc_qe->resp;
        if (!resp->can_send) return false;
    }
    return true;
}

void ResponseQueues::RespTimeingControl(uint64_t key, row_t * row) {
    if (qs[key].empty()) return;
    // 清理qs中的无效数据项
    NCCQueueEntry* head = qs[key].front();
    while (head->q_status != NCC_UNDECIDED) {
        // 把qs的第一个元素删掉
        qs[key].pop_front();
        NCCQueueEntry* new_head = qs[key].front();
        while(head->q_status == NCC_ABORT && 
                (head->txn_access->type == WR && new_head->txn_access->type == RD)) {
            qs[key].pop_front();
            #if CC_ALG == NCC
            row->manager->non_blocking_execute(new_head->txn_ts, new_head->txn_access->type, new_head->txn_access->data, new_head->txn_access);
            #endif
            delete new_head;
            new_head = qs[key].front();
        }
        head = qs[key].front();
    }
    // 发送满足依赖关系的响应
    // NCCQueueEntry* cur = head;
    auto it = qs[key].begin();
    while(true) {
        NCCQueueEntry* cur = *it;
        Response* resp = cur->resp;
        //todo: 检查子事务是否可以返回
        if (!resp->can_send) {
            // 发送响应
            resp->can_send = true;
        }
        TxnManager* txn_man = cur->txn_access->txn;
        if (!txn_man->finish_read_write && TxnCanSend(txn_man)) {
            txn_man->finish_read_write = true;
            txn_man->log_replica(RLOG, g_node_id);
            // msg_queue.enqueue(txn_man->get_thd_id(),Message::create_message(txn_man,RACK_PREP),txn_man->return_id);
        }
        //todo: 检查子事务是否可以返回
        // 获取下一个元素
        it++;
        NCCQueueEntry* next = *it;
        if (next->txn_access->type != RD || cur->txn_access->type != RD) {
            break;
        }
        cur = next;
    }
}

RC Ncc::async_commit_or_abort(TxnManager * txn, bool is_commit) {
    // 遍历写集
    for (uint64_t i = 0; i < txn->get_access_cnt(); i++) {
        row_t * row = txn->get_access(i)->orig_row;
        if (txn->get_access(i)->type == WR) {
            #if CC_ALG == NCC
            txn->get_access(i)->data->manager->async_commit_or_abort_on_row(txn, is_commit);
            #endif
        }
    }
    // 
    for (UInt32 i = 0; i < txn->get_access_cnt(); i++) {
        Access * access = txn->get_access(i);
        access->ncc_qe->q_status = is_commit ? NCCStatus::NCC_COMMIT : NCCStatus::NCC_ABORT;
        resp_qs.RespTimeingControl(access->ncc_qe->resp->row->get_primary_key(), access->ncc_qe->resp->row);
    }
    return RCOK;
}
