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
    NCCQueueEntry(Response* resp, Access * txn_access, NCCTimeStamp txn_ts, NCCStatus q_status) {
        this->resp = resp;
        this->txn_access = txn_access;
        this->txn_ts = txn_ts;
        this->q_status = q_status;
    }
};
class ResponseQueues {
public:
    std::unordered_map<uint64_t, std::list<NCCQueueEntry*>> qs;
    void RespTimeingControl(uint64_t key, row_t * row); 
    bool TxnCanSend(TxnManager* txn);
    void insert(uint64_t key, NCCQueueEntry* qe) {
        qs[key].push_back(qe);
    }
};

class Ncc {
public:
    void init();
    RC async_commit_or_abort(TxnManager * txn,bool is_commit);
    bool safe_guard_check(TxnManager * txn, NCCTimeStamp commitT) {
        std::vector<NCCTimeStamp>Trs,Tws;
        get_rw_set(txn, Trs, Tws);
        NCCTimeStamp maxTws, minTrs;
        for (auto &T : Tws) {
            maxTws = maxNCCTimeStamp(maxTws, T);
        }
        for (auto &T : Trs) {
            minTrs = minNCCTimeStamp(minTrs, T);
        }

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
    }
private:
    void get_rw_set(TxnManager * txn, std::vector<NCCTimeStamp> &Trs, std::vector<NCCTimeStamp> &Tws) {
        UInt32 n = 0, m = 0;
        for (uint64_t i = 0; i < txn->get_access_cnt(); i++) {
            if (txn->get_access_type(i) == WR) {
                Tws.push_back(txn->get_access(i)->ncc_qe->txn_ts);
            } else {
                Trs.push_back(txn->get_access(i)->ncc_qe->txn_ts);
            }
        }
    }
};

#endif
