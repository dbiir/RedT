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
#ifndef _RDMA_CNULL_H_
#define _RDMA_CNULL_H_

#include "../storage/row.h"
class table_t;
class Catalog;
class TxnManager;

class RDMA_Null {
 public:

  RC finish(RC rc,TxnManager * txnMng);

 private:
  row_t* _row;

//    // multi-verison part
//  private:
//   pthread_mutex_t* latch;
//   bool blatch;
};
#endif

