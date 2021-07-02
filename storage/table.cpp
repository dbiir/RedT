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
#include "config.h"

#include "helper.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "mem_alloc.h"

#include "src/allocator_master.hh"
#include <time.h>
#include "lib.hh"

void table_t::test_read(){
	// if(g_node_id == 1){
	// 	char *test_buf = (char *)(client_rdma_rm->raw_ptr);
	// 	//char *test_buf = (char *)client_rm_handler->get_reg_attr().value().buf;
	// 	*((char *)test_buf) = 'x';
	// 	//*test_buf = 'x';

	// 	auto res_s = rc_qp[0][2]->send_normal(
	// 		{.op = IBV_WR_RDMA_WRITE,
	// 		.flags = IBV_SEND_SIGNALED,
	// 		.len = 1, // only write one byte
	// 		.wr_id = 0},
	// 		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
	// 		.remote_addr = 3,
	// 		.imm_data = 0});
	// 	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	// 	auto res_p = rc_qp[0][2]->wait_one_comp();
	// 	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

	// 	RDMA_LOG(4) << "client write done";
	// }
}


#if RDMA_ONE_SIDE == true
void table_t::init(Catalog schema) {
	//this->table_name = schema->table_name;
	strcpy(this->table_name,schema.table_name);
	this->table_id = schema.table_id;
	this->schema = schema;
	//test_read();
}

void table_t::init(const char * table_name, int table_id) {
	strcpy(this->table_name, table_name);
	this->table_id = table_id;
}
#else

void table_t::init(Catalog * schema) {
	this->table_name = schema->table_name;
	this->table_id = schema->table_id;
	this->schema = schema;
	cur_tab_size = new uint64_t;
	// isolate cur_tab_size with other parameters.
	// Because cur_tab_size is frequently updated, causing false
	// sharing problems
	char * ptr = new char[CL_SIZE*2 + sizeof(uint64_t)];
	cur_tab_size = (uint64_t *) &ptr[CL_SIZE];
}
#endif


RC table_t::get_new_row(row_t *& row) {
	// this function is obsolete.
	assert(false);
	return RCOK;
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC table_t::get_general_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id) {
	RC rc = RCOK;
    DEBUG_M("table_t::get_new_row alloc\n");
    uint64_t size = row_t::get_row_size(get_schema()->get_tuple_size());
	void * ptr = mem_allocator.alloc(row_t::get_row_size(size));
	assert (ptr != NULL);

	row = (row_t *) ptr;
	rc = row->init(this, part_id, row_id);
	row->init_manager(row);

    return rc;

}
RC table_t::get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id) {

#if RDMA_ONE_SIDE == true
	RC rc = RCOK;
  	DEBUG_M("table_t::get_new_row alloc\n");

    // printf("[table.cpp:114]tuple_size = %ld \n",get_schema()->get_tuple_size());
    // printf("[table.cpp:115]tuple_size = %ld \n",row_t::get_row_size(get_schema()->get_tuple_size()));
    pthread_mutex_lock( RDMA_MEMORY_LATCH );
    uint64_t size = row_t::get_row_size(get_schema()->get_tuple_size());
    row_t *ptr = (row_t*)r2::AllocatorMaster<>::get_thread_allocator()->alloc(size);
	assert (ptr != NULL);

    tuple_count++;
    memory_count += size;
    max_tuple_size = max_tuple_size > size ? max_tuple_size : size;
    pthread_mutex_unlock( RDMA_MEMORY_LATCH );
	row = (row_t *) ptr;
	rc = row->init(this, part_id, row_id);
	row->init_manager(row);

	return rc;

#else
	RC rc = RCOK;
    DEBUG_M("table_t::get_new_row alloc\n");
	void * ptr = mem_allocator.alloc(row_t::get_row_size(ROW_DEFAULT_SIZE));
	assert (ptr != NULL);

	row = (row_t *) ptr;
	rc = row->init(this, part_id, row_id);
	row->init_manager(row);

  return rc;
#endif

}
