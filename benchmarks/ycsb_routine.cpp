
#include "global.h"
#include "config.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_rdma.h"
#include "transport/rdma.h"
#include "catalog.h"
#include "manager.h"
#include "row.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "row_rdma_mvcc.h"
#include "row_rdma_2pl.h"
#include "row_rdma_ts1.h"
#include "row_rdma_cicada.h"
#include "rdma_mvcc.h"
#include "rdma_ts1.h"
#include "mem_alloc.h"
#include "query.h"
#include "msg_queue.h"
#include "message.h"
#include "src/rdma/sop.hh"
#include "qps/op.hh"
#include "src/sshed.hh"
#include "transport.h"
#include "qps/op.hh"
// itemid_t* YCSBTxnManager::ycsb_read_remote_index(yield_func_t &yield, ycsb_request * req, uint64_t cor_id) {
// 	uint64_t part_id = _wl->key_to_part( req->key );
//   	uint64_t loc = GET_NODE_ID(part_id);
// 	// printf("%ld:%ld:%ld:%ld in read index:    %ld:%ld\n",cor_id, get_txn_id(), req->key, next_record_id, loc, g_node_id);
// 	assert(loc != g_node_id);
// 	uint64_t thd_id = get_thd_id();

// 	//get corresponding index
//   // uint64_t index_key = 0;
// 	uint64_t index_key = req->key / g_node_cnt;
// 	uint64_t index_addr = (index_key) * sizeof(IndexInfo);
// 	uint64_t index_size = sizeof(IndexInfo);

//     itemid_t* item = read_remote_index(yield, loc, index_addr, req->key, cor_id);
// 	return item;
// }