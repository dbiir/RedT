#ifndef ROW_FATHER_H
#define ROW_FATHER_H
#include <semaphore.h>

#include "./global.h"

class Row_father {
  // dli_based
 public:
  struct Entry {
    Entry(const ts_t w_ts) : w_ts(w_ts), r_trans_ts() {}
    ts_t w_ts;
    std::set<txnid_t> r_trans_ts;
  };

  // dta
  std::set<uint64_t>* uncommitted_reads;
  // std::set<uint64_t> * uncommitted_writes;

  uint64_t write_trans;
  uint64_t timestamp_last_read;

  // multi-verison part
  virtual void init(row_t* row);
  virtual RC access(TxnManager* txn, TsType type, uint64_t& version);
  virtual void latch();
  virtual uint64_t write(row_t* data, TxnManager* txn, const access_t type);
  virtual void release();
  virtual bool has_version(uint64_t version);
  virtual Entry* get_version(uint64_t version);

  // dta
  virtual RC access(TsType type, TxnManager* txn, row_t* row, uint64_t& version);
  virtual RC read_and_write(TsType type, TxnManager* txn, row_t* row, uint64_t& version);
  virtual RC prewrite(TxnManager* txn);
  virtual RC abort(access_t type, TxnManager* txn);
  virtual RC commit(access_t type, TxnManager* txn, row_t* data, uint64_t& version);
  virtual void write(row_t* data);

  // lock
  virtual RC lock_get(lock_t type, TxnManager* txn);
  virtual RC lock_get(lock_t type, TxnManager* txn, uint64_t*& txnids, int& txncnt);
  virtual RC lock_release(TxnManager* txn);

  // maat
  virtual RC access(access_t type, TxnManager* txn);
  virtual RC read_and_prewrite(TxnManager* txn);
  virtual RC read(TxnManager* txn);

  virtual RC commit(access_t type, TxnManager* txn, row_t* data);

  // mvcc
  virtual RC access(TxnManager* txn, TsType type, row_t* row);

  // occ
  virtual RC access(TxnManager* txn, TsType type);
  // ts is the start_ts of the validating txn
  virtual bool validate(uint64_t ts);
  virtual void write(row_t* data, uint64_t ts);

  // si

  std::atomic<uint64_t> w_trans;
  std::atomic<ts_t> timestamp_last_write;

  // ts
};

#endif
