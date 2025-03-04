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
#include "table.h"

void Ncc::init() {
    // sem_init(&_semaphore, 0, 1);
}

void ResponseQueues::create(uint64_t table_id, uint64_t key, row_t* row) {
    pthread_mutex_lock(mutx);
    // if (qs.find(key) == qs.end()) {
    // qs[key]
    NCCQueue* q = new NCCQueue(row);
    std::pair<uint64_t, uint64_t> tk = std::make_pair(table_id, key);
    qs[tk] = q;
    // qs.insert(std::make_pair(std::make_pair(table_id,key), q));
    assert(qs[tk] != nullptr);
    uint64_t index = next_insert_vector % NCC_THREAD_CNT;
    v_qs[index].push_back(q);
    next_insert_vector++;
    // DEBUG_T("NCC: create resp_qs for row %ld\n", key);
    // }
    pthread_mutex_unlock(mutx);
}

// void ResponseQueues::create(uint64_t table_id, uint64_t key, NCCQueue* q) {
//     pthread_mutex_lock(mutx);
//     std::pair<uint64_t, uint64_t> tk = std::make_pair(table_id, key);
//     qs[tk] = q;
//     assert(qs[tk] != nullptr);
//     pthread_mutex_unlock(mutx);
// }

bool ResponseQueues::TxnCanSend(TxnManager* txn) {
    for (uint64_t i = 0; i < txn->get_access_cnt(); i++) {
        Access* access = txn->get_access(i);
        Response* resp = access->ncc_qe->resp;
        if (!resp->can_send) return false;
    }
    return true;
}


void ResponseQueues::insert(uint64_t table_id, uint64_t key, NCCQueueEntry* qe, row_t* row) {
    // pthread_mutex_lock(mutx);
    std::pair<uint64_t, uint64_t> tk = std::make_pair(table_id, key);
    NCCQueue * q = qs[tk];
    assert(q != nullptr);
    // if (q == nullptr) {
    //     qs[key] = new NCCQueue(row);
    // }
    qs[tk]->insert(qe);
    
    // DEBUG_T("NCC: txn %ld access %ld insert into resp_qs, now has %d entry\n", qe->txn_access->txn->get_txn_id(), row->get_primary_key(), qs[key]->q.size());
    // pthread_mutex_unlock(mutx);
}

void ResponseQueues::RespTimeingControl() {
    uint64_t resp_time_start = get_sys_clock();
    for (auto it = qs.begin(); it != qs.end(); it++) {
        NCCQueue* q = it->second;
        if (q->q.empty()) continue;
        row_t* row = q->row;
        RespTimeingControlInner(q, row);
    }
    INC_STATS(0, ncc_resp_time, get_sys_clock() - resp_time_start);
}

void ResponseQueues::RespTimeingControl(uint64_t thd_id) {
    uint64_t resp_time_start = get_sys_clock();
    uint64_t index = thd_id % NCC_THREAD_CNT;
    for (auto it : v_qs[index]) {
        NCCQueue* q = it;
        if (q->q.empty()) continue;
        row_t* row = q->row;
        RespTimeingControlInner(q, row);
    }
    // INC_STATS(thd_id, ncc_resp_time, get_sys_clock() - resp_time_start);
}

void ResponseQueues::RespTimeingControlInner(NCCQueue *q, row_t * row) {
    if (!OPEN_TIME_CONTROL) return;
    assert(q != nullptr);
    if (q->q.empty()) return;
    // 清理qs中的无效数据项
    q->lock();
    NCCQueueEntry* head = q->q.front();
    while (head->q_status != NCC_UNDECIDED) {
        // 把qs的第一个元素删掉
        q->q.pop_front();
        NCCQueueEntry* new_head = q->q.front();
        while(head->q_status == NCC_ABORT && 
                (head->txn_access->type == WR && new_head->txn_access->type == RD)) {
            q->q.pop_front();
            #if CC_ALG == NCC
            row->manager->non_blocking_execute(new_head->txn_ts, new_head->txn_access->type, new_head->txn_access->data, new_head->txn_access, new_head->txn_access->txn);
            #endif
            // delete new_head;
            new_head = q->q.front();
        }
        head = q->q.front();
    }
    // 发送满足依赖关系的响应
    // NCCQueueEntry* cur = head;
    auto it = q->q.begin();
    while(it != q->q.end()) {
        NCCQueueEntry* cur = *it;
        if (*it == nullptr) {
            break;
        }
        Response* resp = cur->resp;
        //todo: 检查子事务是否可以返回
        if (!resp->can_send) {
            // 发送响应
            resp->can_send = true;
        }
        TxnManager* txn_man = cur->txn_access->txn;
        if (!txn_man->has_send_rlog && 
            TxnCanSend(txn_man)) {
            assert(!txn_man->finish_read_write);
            txn_man->log_replica(RLOG, GET_NODE_ID(txn_man->get_txn_id()));
            txn_man->has_send_rlog = true;
        }
        //todo: 检查子事务是否可以返回
        // 获取下一个元素
        it++;
        if (it == q->q.end() || *it == nullptr) {
            break;
        }
        NCCQueueEntry* next = *it;
        if (next->txn_access->type != RD || cur->txn_access->type != RD) {
            break;
        }
        cur = next;
    }
    q->unlock();
}

void ResponseQueues::RespTimeingControl(uint64_t table_id, uint64_t key, row_t * row) {
    // pthread_mutex_lock(mutx);
    std::pair<uint64_t, uint64_t> tk = std::make_pair(table_id, key);
    NCCQueue* q = qs[tk];
    RespTimeingControlInner(q, row);
}

RC Ncc::async_commit_or_abort(TxnManager * txn, bool is_commit) {
    // 遍历写集
    DEBUG_T("NCC %d enter async %s\n", txn->get_txn_id(), is_commit ? "commit" : "abort");
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
        // resp_qs.RespTimeingControl(access->ncc_qe->resp->row->get_table()->get_table_id(),access->ncc_qe->resp->row->get_primary_key(), access->ncc_qe->resp->row);
    }
    // DEBUG_T("NCC %d enter final time control\n", txn->get_txn_id());
    // resp_qs.RespTimeingControl();
    return RCOK;
}

RC Ncc::validate(TxnManager * txn) {
    NCCTimeStamp commitT;
    bool suc = safe_guard_check(txn, commitT);
    txn->set_ncc_commit_timestamp(commitT);
    if (suc) {
        return RCOK;
    } else {
        return Abort;
    }
}

bool Ncc::safe_guard_check(TxnManager * txn, NCCTimeStamp &commitT) {
    DEBUG_T("NCC %d enter validate\n", txn->get_txn_id());
    std::vector<Response *> resps;
    get_rw_set(txn, resps);
    NCCTimeStamp maxTws(0,0);
    NCCTimeStamp minTrs(UINT64_MAX,UINT64_MAX);

    for (auto &resp : resps) {
        DEBUG_T("NCC traverse %d maxTws %lu-%lu\n", txn->get_txn_id(), maxTws.time, resp->tw.time);
        maxTws = maxNCCTimeStamp(maxTws, resp->tw);
        DEBUG_T("NCC traverse %d minTrs %lu-%lu\n", txn->get_txn_id(), minTrs.time, resp->tr.time);
        minTrs = minNCCTimeStamp(minTrs, resp->tr);
    }

    // for (auto &T : Tws) {
    //     DEBUG_T("NCC traverse %d maxTws %lu-%lu\n", txn->get_txn_id(), maxTws.time, T.time);
    //     maxTws = maxNCCTimeStamp(maxTws, T);
    // }
    DEBUG_T("NCC traverse %d maxTws %lu-%lu\n", txn->get_txn_id(), maxTws.time, txn->get_MaxTw().time);
    maxTws = maxNCCTimeStamp(maxTws, txn->get_MaxTw());
    // for (auto &T : Trs) {
    //     DEBUG_T("NCC traverse %d minTrs %lu-%lu\n", txn->get_txn_id(), minTrs.time, T.time);
    //     minTrs = minNCCTimeStamp(minTrs, T);
    // }
    DEBUG_T("NCC traverse %d minTrs %lu-%lu\n", txn->get_txn_id(), minTrs.time, txn->get_MinTr().time);
    minTrs = minNCCTimeStamp(minTrs, txn->get_MinTr());
    
    DEBUG_T("NCC validate %d maxTws %lu minTrs %lu\n", txn->get_txn_id(), maxTws.time, minTrs.time);
    commitT = maxTws;
    if (maxTws.time < minTrs.time) {
        return true;
    } 
    else if (maxTws.time == minTrs.time) {
        return maxTws.cid <= minTrs.cid;
    }
    else {
        return false;
    }
};

void Ncc::get_rw_set(TxnManager * txn, std::vector<Response *> &resps) {
        UInt32 n = 0, m = 0;
        for (uint64_t i = 0; i < txn->get_access_cnt(); i++) {
            resps.push_back(txn->get_access(i)->ncc_qe->resp);
        }
    }