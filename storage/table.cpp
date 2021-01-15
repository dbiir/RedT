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
	if(g_node_id == 1){
		char *test_buf = (char *)(client_rdma_rm->raw_ptr);
		//char *test_buf = (char *)client_rm_handler->get_reg_attr().value().buf;
		*((char *)test_buf) = 'x';
		//*test_buf = 'x';

		auto res_s = rc_qp[0][2]->send_normal(
			{.op = IBV_WR_RDMA_WRITE,
			.flags = IBV_SEND_SIGNALED,
			.len = 1, // only write one byte
			.wr_id = 0},
			{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
			.remote_addr = 3,
			.imm_data = 0});
		RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
		auto res_p = rc_qp[0][2]->wait_one_comp();
		RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);

		RDMA_LOG(4) << "client write done";
	}
}

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

	test_read();
}

RC table_t::get_new_row(row_t *& row) {
	// this function is obsolete.
	assert(false);
	return RCOK;
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC table_t::get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id) {
	
	RC rc = RCOK;
  	DEBUG_M("table_t::get_new_row alloc\n");
	  
	//char* head = rdma_global_buffer;
	// r2::AllocatorMaster<>::init(head,rdma_buffer_size);
	// auto allocator = r2::AllocatorMaster<>::get_allocator();

    row_t *ptr = (row_t*)r2::AllocatorMaster<>::get_thread_allocator()->alloc(sizeof(row_t));
	
	// auto allocator = r2::AllocatorMaster<RDMA_ONE_SIDE_MEMORY_ID_1>::get_allocator();
	// char *ptr = (char*)allocator->alloc(sizeof(row_t));
	assert (ptr != NULL);

	// srand(time(NULL));
	// if (rand()%10 == 0) {
	// 	char* rhead = rdma_global_buffer+rdma_index_size;
	// 	double size = ((char*)ptr-rhead) / (1024 * 1024);
	// 	printf("rdma row ptr:%p head_ptr:%p size:%f", ptr, rhead, size);
	// }

	row = (row_t *) ptr;
	rc = row->init(this, part_id, row_id);
	row->init_manager(row);

	return rc;
}
