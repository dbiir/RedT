#include "row_father.h"

#define ASF assert(false);
// dli_based
void Row_father::init(row_t* row) { assert(false); }
RC Row_father::access(TxnManager* txn, TsType type, uint64_t& version) { assert(false); }
void Row_father::latch() { assert(false); }
uint64_t Row_father::write(row_t* data, TxnManager* txn, const access_t type) { ASF }
void Row_father::release() { ASF }
bool Row_father::has_version(uint64_t version){
    ASF} Row_father::Entry* Row_father::get_version(uint64_t version){ASF}

// dta
RC Row_father::access(TsType type, TxnManager* txn, row_t* row, uint64_t& version){
    ASF} RC Row_father::read_and_write(TsType type, TxnManager* txn, row_t* row, uint64_t& version){
    ASF} RC Row_father::prewrite(TxnManager* txn){ASF} RC Row_father::abort(access_t type,
                                                                            TxnManager* txn){
    ASF} RC Row_father::commit(access_t type, TxnManager* txn, row_t* data, uint64_t& version) {
  ASF
}
void Row_father::write(row_t* data){ASF}

// lock
RC Row_father::lock_get(lock_t type, TxnManager* txn){
    ASF} RC Row_father::lock_get(lock_t type, TxnManager* txn, uint64_t*& txnids,
                                 int& txncnt){ASF} RC Row_father::lock_release(TxnManager* txn){ASF}

// maat
RC Row_father::access(access_t type,
                      TxnManager* txn){ASF} RC Row_father::read_and_prewrite(TxnManager* txn){
    ASF} RC Row_father::read(TxnManager* txn){
    ASF} RC Row_father::commit(access_t type, TxnManager* txn, row_t* data){ASF}

// mvcc
RC Row_father::access(TxnManager* txn, TsType type, row_t* row){ASF}

// occ
RC Row_father::access(TxnManager* txn, TsType type) {
  ASF
}
// ts is the start_ts of the validating txn
bool Row_father::validate(uint64_t ts) { ASF }
void Row_father::write(row_t* data, uint64_t ts) { ASF }

// si

// ts
