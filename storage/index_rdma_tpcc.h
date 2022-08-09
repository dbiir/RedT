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
/*
#include "global.h"
#include "helper.h"
#include "index_base.h"
#include "index_hash.h"

class IndexInfo {
public:
	void init(){
		key = 0;
		length = sizeof(row_t*);
		offset = 0;
     table_offset = 0;
		valid = true;
		type = DT_row;
	};
	uint64_t key;
	row_t* address;
	uint64_t offset;
    uint64_t table_offset;
	uint64_t length;
	bool valid;
	Data_type type;
};

// TODO Hash index does not support partition yet.
class IndexRdmaTpcc  : public index_base
{
public:
	RC 			init(uint64_t bucket_cnt);
	RC 			init(int part_cnt,
					table_t * table,
					uint64_t bucket_cnt);
	void    	index_delete();
	void 		index_reset();
	bool 		index_exist(idx_key_t key); // check if the key exist.
	RC 			index_insert(idx_key_t key, itemid_t * item, int part_id=-1);
	RC 			index_insert_nonunique(idx_key_t key, itemid_t * item, int part_id=-1);
	// the following call returns a single item
	RC	 		index_read(idx_key_t key, itemid_t * &item, int part_id=-1);
	RC	 		index_read(idx_key_t key, int count, itemid_t * &item, int part_id=-1);
	RC	 		index_read(idx_key_t key, itemid_t * &item,
							int part_id=-1, int thd_id=0);

private:

	void get_latch(BucketHeader * bucket);
	void release_latch(BucketHeader * bucket);
	// TODO implement more complex hash function
	uint64_t hash(idx_key_t key) {
#if WORKLOAD == YCSB
		return (key / g_part_cnt) % _bucket_cnt_per_part;
#else
		return key % _bucket_cnt_per_part;
#endif
	}

	BucketHeader ** 	_buckets;
	uint64_t	 		_bucket_cnt;
	uint64_t 			_bucket_cnt_per_part;
	IndexInfo          *index_info;
};

#endif
*/