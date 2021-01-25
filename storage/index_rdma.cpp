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

#include "global.h"
#include "index_rdma.h"
#include "mem_alloc.h"
#include "row.h"
#include "src/allocator_master.hh"

RC IndexRdma::init(uint64_t bucket_cnt) {
	uint64_t index_size = (g_synth_table_size/g_node_cnt)*(sizeof(IndexInfo)+1);

	index_info = (IndexInfo*)rdma_global_buffer;

	printf("%d",index_info[0].key);

	uint64_t i = 0;
	for (i = 0; i < g_synth_table_size/g_node_cnt; i ++) {
		index_info[i].init();
	}

 	printf("init %ld index\n",i);
	return RCOK;
}

RC IndexRdma::init(int part_cnt, table_t *table, uint64_t bucket_cnt) {
	init(bucket_cnt);
	this->table = table;
	return RCOK;
}

void IndexRdma::index_delete() {
	for (UInt32 n = 0; n < _bucket_cnt_per_part; n ++) {
		_buckets[0][n].delete_bucket();
	}
	mem_allocator.free(_buckets[0],sizeof(BucketHeader) * _bucket_cnt_per_part);
	delete _buckets;
}

void IndexRdma::index_reset() {
	for (UInt32 n = 0; n < _bucket_cnt_per_part; n ++) {
		_buckets[0][n].delete_bucket();
	}
}

bool IndexRdma::index_exist(idx_key_t key) {
	assert(false);
}

void
IndexRdma::get_latch(BucketHeader * bucket) {
	while (!ATOM_CAS(bucket->locked, false, true)) {}
}

void
IndexRdma::release_latch(BucketHeader * bucket) {
	bool ok = ATOM_CAS(bucket->locked, true, false);
	assert(ok);
}


RC IndexRdma::index_insert(idx_key_t key, itemid_t * item, int part_id) {
	RC rc = RCOK;

	uint64_t index_key = key/g_node_cnt;
	index_info[index_key].key = key;
	index_info[index_key].address = (row_t*)(item->location);
	index_info[index_key].table_offset = (char*)table - rdma_global_buffer;
	index_info[index_key].offset = (char*)item->location - rdma_global_buffer;

	return rc;
}

// todo:之后可能要改
RC IndexRdma::index_insert_nonunique(idx_key_t key, itemid_t * item, int part_id) {
	RC rc = RCOK;
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	//BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
	// 1. get the ex latch
	get_latch(cur_bkt);

	// 2. update the latch list
	cur_bkt->insert_item_nonunique(key, item, part_id);

	// 3. release the latch
	release_latch(cur_bkt);
	return rc;
}

RC IndexRdma::index_read(idx_key_t key, itemid_t * &item, int part_id) {
	RC rc = RCOK;

	uint64_t index_key = key/g_node_cnt;

	assert(index_info[index_key].key == key);
	item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
	item->location = index_info[index_key].address;
	item->type = index_info[index_key].type;
	item->valid = index_info[index_key].valid;

	return rc;

}
// todo:之后可能要改
RC IndexRdma::index_read(idx_key_t key, int count, itemid_t * &item, int part_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	//BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
	RC rc = RCOK;
	// 1. get the sh latch
//	get_latch(cur_bkt);

	cur_bkt->read_item(key, count, item);

	// 3. release the latch
//	release_latch(cur_bkt);
	return rc;

}

// todo:之后可能要改
RC IndexRdma::index_read(idx_key_t key, itemid_t * &item,int part_id, int thd_id) {
	RC rc = RCOK;

	uint64_t index_key = key/g_node_cnt;
	item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
	assert(index_info[index_key].key == key);
	item->location = index_info[index_key].address;
	item->type = index_info[index_key].type;
	item->valid = index_info[index_key].valid;

	return rc;
}
