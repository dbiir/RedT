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

#ifndef _PSI_H_
#define _PSI_H_

#include "row.h"
#include "semaphore.h"
#include <unordered_map>
#include <vector>

class TxnManager;

enum PSIState {
  PSI_RUNNING = 0,
  PSI_VALIDATED,
  PSI_COMMITTED,
  PSI_ABORTED
};

class psi_set_ent{
public:
    psi_set_ent();
    UInt64 tn;
    TxnManager * txn;
    UInt32 set_size;
    row_t ** rows; //[MAX_WRITE_SET];
    psi_set_ent * next;
};

class psi {
public:
  void init();
  RC validate(TxnManager * txn) {return validate_4a(txn);}
  RC find_bound(TxnManager * txn);
  RC validate_4a(TxnManager * txn);
  RC handle_conflict_txn_4b(TxnManager * txn);
  RC finish(RC rc, TxnManager * txn);
private:
 	sem_t 	_semaphore;
};

class anti_dependency_table {
public:
  // std::unordered_map<uint64_t, std::vector<uint64_t>> table; // T_current -> [other T]
  std::unordered_map<uint64_t, std::vector<uint64_t>> reverse_table; // [other T] -> T_current
  void init() { sem_init(&_semaphore, 0, 1); }
 	sem_t 	_semaphore;
};

#endif
