/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@tencent.com
 *
 */

#ifndef _NCC_H_
#define _NCC_H_

#include "row.h"
#include "row_ncc.h"
#include "semaphore.h"
#include <list>
#include <vector>
#include <unordered_map>

class TxnManager;

class Response {
public:
    NCCTimeStamp tw;
    NCCTimeStamp tr;
    row_t * row; //读取的数据项
    NCCRespType status; //写操作的结果
    bool can_send;
    Response(NCCTimeStamp tw, NCCTimeStamp tr, row_t * row, NCCRespType status) {
        this->tw = tw;
        this->tr = tr;
        this->row = row;
        this->status = status;
        this->can_send = false;
    }
};

class NCCQueueEntry {
public:
    Response* resp;
    Access * txn_access;
    NCCTimeStamp txn_ts;
    NCCStatus q_status;
    row_t* row;
    NCCQueueEntry(Response* resp, Access * txn_access, NCCTimeStamp txn_ts, NCCStatus q_status, row_t* row) {
        this->resp = resp;
        this->txn_access = txn_access;
        this->txn_ts = txn_ts;
        this->q_status = q_status;
        this->row = row;
    }
};

class NCCQueue {
public:
    row_t* row;
    std::list<NCCQueueEntry*> q;
    pthread_mutex_t* mutx;
    NCCQueue(row_t* row) {
        mutx = (pthread_mutex_t *) mem_allocator.alloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(mutx, NULL);
        this->row = row;
    }
    void insert(NCCQueueEntry* qe) {
        pthread_mutex_lock(mutx);
        q.push_back(qe);
        pthread_mutex_unlock(mutx);
    }
    void lock() {
        pthread_mutex_lock(mutx);
    }
    void unlock() {
        pthread_mutex_unlock(mutx);
    }
};

// 自定义哈希函数
struct pair_hash {
    template <class T1, class T2>
    struct hash_pair {
        size_t operator()(const std::pair<T1, T2>& p) const {
            auto h1 = std::hash<T1>()(p.first);
            auto h2 = std::hash<T2>()(p.second);

            // Combine hashes of the first and second element
            // Here, we are using a simple XOR combination
            return h1 ^ h2;
        }
    };
};

// 自定义相等比较函数
struct pair_equal {
    template <class T1, class T2>
    struct equal {
    bool operator()(const std::pair<T1, T2>& lhs, const std::pair<T1, T2>& rhs) const {
        return lhs.first == rhs.first && lhs.second == rhs.second;
    }
    };
};

class ResponseQueues {
public:
    std::unordered_map<std::pair<uint64_t,uint64_t>, NCCQueue*, pair_hash::hash_pair<uint64_t, uint64_t>, pair_equal::equal<uint64_t, uint64_t>> qs;

    // 帮忙给每个key加一个锁
    pthread_mutex_t* mutx;
    std::unordered_map<uint64_t, pthread_mutex_t*> locks;
    ResponseQueues() {
        mutx = (pthread_mutex_t *) mem_allocator.alloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(mutx, NULL);
    }
    void create(uint64_t table_id, uint64_t key, row_t* row);
    void RespTimeingControl() ;
    void RespTimeingControl(uint64_t table_id, uint64_t key, row_t * row); 
    bool TxnCanSend(TxnManager* txn);
    void insert(uint64_t table_id, uint64_t key, NCCQueueEntry* qe, row_t* row);
};

class Ncc {
public:
    void init();
    RC async_commit_or_abort(TxnManager * txn,bool is_commit);
    RC validate(TxnManager * txn); 
    bool safe_guard_check(TxnManager * txn, NCCTimeStamp &commitT);
private:
    void get_rw_set(TxnManager * txn, std::vector<NCCTimeStamp> &Trs, std::vector<NCCTimeStamp> &Tws);
};

#endif
