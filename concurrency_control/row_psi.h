/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef ROW_PSI_H
#define ROW_PSI_H
#if CC_ALG == PSI
struct PSIVersion {
    
    ts_t sid;
    ts_t cid;

    std::set<uint64_t> * visitor_list;

    row_t * row;
    txnid_t txn_id;

    PSIVersion() {
        row = NULL;
        txn_id = 0;
        sid = 0;
        cid = 0;
        visitor_list = new std::set<uint64_t>();
    }
    PSIVersion(ts_t cid, row_t * row, txnid_t txn_id) {
      this->cid = cid;
      this->sid = cid;
      this->row = row;
      this->txn_id = txn_id;
      visitor_list = new std::set<uint64_t>();
    }
};

struct PSILockEntry {
    lock_t type;
    txnid_t txn;
};

class Row_psi {
public:
	void init(row_t * row);
  RC access(access_t type, TxnManager * txn, PSIVersion * &version);
  RC read(TxnManager * txn, PSIVersion * &target_version);
  RC prewrite(TxnManager * txn, PSIVersion * &target_version);
  RC abort(access_t type, TxnManager * txn, PSIVersion * orig_version);
  RC commit(access_t type, TxnManager * txn, row_t * data, PSIVersion * orig_version);
  void write(row_t * data);

private:
  volatile bool psi_avail;

	row_t * _row;
  // write lock
  volatile txnid_t wlock;
  // PSILockEntry * write_lock;

  std::vector<PSIVersion*> *versions;
};

#endif

#endif