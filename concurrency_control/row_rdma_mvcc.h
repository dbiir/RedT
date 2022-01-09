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


class table_t;
class Catalog;
class TxnManager;

// struct RdmaMVHis;

// struct RdmaMVHis {
//     uint64_t mutex;//lock
//     char data[ROW_DEFAULT_SIZE];
//     uint64_t rts;
//     uint64_t start_ts;
//     uint64_t end_ts;
//     uint64_t txn_id;
//     //RTS、start_ts、end_ts、txn-id：
// };

class Row_rdma_mvcc {
public:
	void init(row_t * row);
	RC access(TxnManager * txn, Access *access, access_t type);
   // RdmaMVHis historyVer[HIS_CHAIN_NUM];
   void local_write_back(TxnManager * txnMng , int num);
   void local_release_lock(TxnManager * txnMng , int num);
private:
 	pthread_mutex_t * latch;
	bool blatch;

	row_t * _row;
	
	uint64_t whis_len;
	uint64_t rhis_len;
	uint64_t rreq_len;
	uint64_t preq_len;

   // Array<RdmaMVHis*> historyVer;
    
};

