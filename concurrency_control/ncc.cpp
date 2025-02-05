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

void ResponseQueues::create(uint64_t table_id, uint64_t key, row_t* row) {
    pthread_mutex_lock(mutx);
    // if (qs.find(key) == qs.end()) {
    // qs[key]
    NCCQueue* q = new NCCQueue(row);
    std::pair<uint64_t, uint64_t> tk = std::make_pair(table_id, key);
    qs[tk] = q;
    // qs.insert(std::make_pair(std::make_pair(table_id,key), q));
    assert(qs[tk] != nullptr);
    DEBUG_T("NCC: create resp_qs for row %ld\n", key);
    // }
    pthread_mutex_unlock(mutx);
}

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
    // pthread_mutex_lock(mutx);
    uint64_t resp_time_start = get_sys_clock();
    for (auto it = qs.begin(); it != qs.end(); it++) {
        NCCQueue* q = it->second;
        if (q->q.empty()) continue;
        row_t* row = q->row;
        std::pair<uint64_t, uint64_t> tk = it->first;
        RespTimeingControl(tk.first, tk.second, row);
    }
    INC_STATS(0, ncc_resp_time, get_sys_clock() - resp_time_start);
    // pthread_mutex_unlock(mutx);
}

void ResponseQueues::RespTimeingControl(uint64_t table_id, uint64_t key, row_t * row) {
    // pthread_mutex_lock(mutx);
    std::pair<uint64_t, uint64_t> tk = std::make_pair(table_id, key);
    NCCQueue* q = qs[tk];
    // pthread_mutex_unlock(mutx);
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
            delete new_head;
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
        resp_qs.RespTimeingControl();
        // resp_qs.RespTimeingControl(access->ncc_qe->resp->row->get_primary_key(), access->ncc_qe->resp->row);
    }
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
    std::vector<NCCTimeStamp>Trs,Tws;
    get_rw_set(txn, Trs, Tws);
    NCCTimeStamp maxTws, minTrs;
    for (auto &T : Tws) {
        maxTws = maxNCCTimeStamp(maxTws, T);
    }
    maxTws = maxNCCTimeStamp(maxTws, txn->get_MaxTw());
    for (auto &T : Trs) {
        minTrs = minNCCTimeStamp(minTrs, T);
    }
    minTrs = minNCCTimeStamp(minTrs, txn->get_MinTr());

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

void Ncc::get_rw_set(TxnManager * txn, std::vector<NCCTimeStamp> &Trs, std::vector<NCCTimeStamp> &Tws) {
        UInt32 n = 0, m = 0;
        for (uint64_t i = 0; i < txn->get_access_cnt(); i++) {
            if (txn->get_access_type(i) == WR) {
                Tws.push_back(txn->get_access(i)->ncc_qe->txn_ts);
            } else {
                Trs.push_back(txn->get_access(i)->ncc_qe->txn_ts);
            }
        }
    }