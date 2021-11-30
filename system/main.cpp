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

#include "abort_thread.h"
#include "calvin_thread.h"
#include "client_query.h"
#include "dli.h"
#include "dta.h"
#include "global.h"
#include "io_thread.h"
#include "key_xid.h"
#include "log_thread.h"
#include "logger.h"
#include "maat.h"
#include "manager.h"
#include "math.h"
#include "msg_queue.h"
#include "occ.h"
#include "pps.h"
#include "query.h"
#include "rts_cache.h"
#include "sequencer.h"
#include "sim_manager.h"
#include "abort_queue.h"
#include "thread.h"
#include "tpcc.h"
#include "transport.h"
#include "work_queue.h"
#include "worker_thread.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "da.h"
#include "maat.h"
#include "rdma_maat.h"
#include "rdma_calvin.h"
#include "rdma_cicada.h"
#include "cicada.h"
#include "ssi.h"
#include "wsi.h"
#include "focc.h"
#include "bocc.h"
#include "client_query.h"
#include "wkdb.h"
#include "tictoc.h"
#include "key_xid.h"
#include "rts_cache.h"
#include "lib.hh"
#include "rdma.h"
// #include "http.h"
//#include "rdma_ctrl.hpp"
#include "qps/rc_recv_manager.hh"
#include "qps/recv_iter.hh"
//#include "src/allocator_master.hh"
void network_test();
void network_test_recv();
void * run_thread(void *);
void * run_co_thread(void *);
void * run_nco_thread(void *);
WorkerThread * worker_thds;
WorkerNumThread * worker_num_thds;
InputThread * input_thds;
OutputThread * output_thds;
AbortThread * abort_thds;
LogThread * log_thds;
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
CalvinLockThread * calvin_lock_thds;
CalvinSequencerThread * calvin_seq_thds;
#endif

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char *argv[]) {
	RDMA_MEMORY_LATCH = (pthread_mutex_t *)mem_allocator.alloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(RDMA_MEMORY_LATCH, NULL);
	RDMA_QP_LATCH = (pthread_mutex_t *)mem_allocator.alloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(RDMA_QP_LATCH, NULL);
    // 0. initialize global data structure
    parser(argc, argv);
#if SEED != 0
	uint64_t seed = SEED + g_node_id;
#else
	uint64_t seed = get_sys_clock();
#endif
	srand(seed);
	printf("Random seed: %ld\n",seed);

	int64_t starttime;
	int64_t endtime;
	starttime = get_server_clock();
	printf("Initializing stats... ");
	fflush(stdout);
	stats.init(g_total_thread_cnt);
	printf("Done\n");

	printf("Initializing global manager... ");
	fflush(stdout);
	glob_manager.init();
	printf("Done\n");
	printf("Initializing transport manager... ");
	fflush(stdout);
	tport_man.init();
	printf("Done\n");
	fflush(stdout);
	printf("Initializing simulation... ");
	fflush(stdout);
	simulation = new SimManager;
	simulation->init();
	printf("Done\n");
	fflush(stdout);

    //item_index_size = item_index_size ;
    
	//register memeory
	//prepare QP connection with rdma_global_buffer(as registry memory)
	#if RDMA_ONE_SIDE == true//== CHANGE_MSG_QUEUE || USE_RDMA == CHANGE_TCP_ONLY
    //#if CC_ALG == RDMA_SILO
        rdma_man.init();
    #endif

   //prepare workload
	// Workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new YCSBWorkload;
			break;
		case TPCC :
			m_wl = new TPCCWorkload;
			wh_index_size = wh_index_size * (g_num_wh/g_node_cnt);
			stock_index_size = stock_index_size * (g_num_wh/g_node_cnt);
			dis_index_size = dis_index_size * (g_num_wh/g_node_cnt);
			cust_index_size = cust_index_size * (g_num_wh/g_node_cnt);
			cl_index_size = cl_index_size * (g_num_wh/g_node_cnt) ;
			order_index_size = order_index_size * (g_num_wh/g_node_cnt);
			ol_index_size = ol_index_size * (g_num_wh/g_node_cnt);

			rdma_index_size = item_index_size + wh_index_size + stock_index_size + dis_index_size +
							+ cust_index_size +  cl_index_size + order_index_size + ol_index_size 
							+ (1024*1024L);
			break;
		case PPS :
			m_wl = new PPSWorkload;
			break;
		case DA:
			m_wl = new DAWorkload;
			is_server=true;
			break;
		default:
			assert(false);
	}
	m_wl->init();
	printf("Workload initialized!\n");
	fflush(stdout);
#if NETWORK_TEST
	tport_man.init(g_node_id,m_wl);
	sleep(3);
	if(g_node_id == 0)
		network_test();
	else if(g_node_id == 1)
		network_test_recv();

	return 0;
#endif


	printf("Initializing work queue... ");
	fflush(stdout);
	work_queue.init();
	printf("Done\n");
	printf("Initializing abort queue... ");
	fflush(stdout);
	abort_queue.init();
	printf("Done\n");
	printf("Initializing message queue... ");
	fflush(stdout);
	msg_queue.init();
	printf("Done\n");
	printf("Initializing transaction manager pool... ");
	fflush(stdout);
	txn_man_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing transaction pool... ");
	fflush(stdout);
	txn_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing row pool... ");
	fflush(stdout);
	row_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing access pool... ");
	fflush(stdout);
	access_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing txn node table pool... ");
	fflush(stdout);
	txn_table_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing query pool... ");
	fflush(stdout);
	qry_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing msg pool... ");
	fflush(stdout);
	msg_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing transaction table... ");
	fflush(stdout);
	txn_table.init();
	printf("Done\n");
    printf("***********num_wh = %d**********\n",NUM_WH);
#if SERVER_GENERATE_QUERIES
	printf("Initializing client query queue... ");
	fflush(stdout);
	client_query_queue.init(m_wl);
	printf("Done\n");
	fflush(stdout);
#endif
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	printf("Initializing sequencer... ");
	fflush(stdout);
	seq_man.init(m_wl);
	printf("Done\n");
#if CC_ALG == RDMA_CALVIN
	printf("Initializing RDMA_CALVIN manager... ");
	calvin_man.init();
	printf("Done\n");
#endif
#endif
#if CC_ALG == MAAT
	printf("Initializing Time Table... ");
	fflush(stdout);
	time_table.init();
	printf("Done\n");
	printf("Initializing MaaT manager... ");
	fflush(stdout);
	maat_man.init();
	printf("Done\n");
#endif
#if CC_ALG == RDMA_MAAT
	printf("Initializing Time Table... ");
	fflush(stdout);
	rdma_txn_table.init();
	printf("Done\n");
	printf("Initializing MaaT manager... ");
	fflush(stdout);
	rmaat_man.init();
	printf("Done\n");
#endif
#if CC_ALG == RDMA_WOUND_WAIT2 || CC_ALG == RDMA_WOUND_WAIT
    printf("Initializing Txn Table... ");
	fflush(stdout);
	rdma_txn_table.init();
	printf("Done\n");
#endif
#if CC_ALG == RDMA_CICADA
	printf("Initializing CICADA manager... ");
	fflush(stdout);
	rcicada_man.init();
	printf("Done\n");
#endif
#if CC_ALG == CICADA
	printf("Initializing CICADA manager... ");
	cicada_man.init();
	printf("Done\n");
#endif
#if CC_ALG == SSI
	printf("Initializing In Out Table... ");
	fflush(stdout);
	inout_table.init();
	printf("Done\n");
	printf("Initializing SSI manager... ");
	fflush(stdout);
	ssi_man.init();
	printf("Done\n");
#endif
#if CC_ALG == WSI
	printf("Initializing WSI manager... ");
	fflush(stdout);
	wsi_man.init();
	printf("Done\n");
#endif
#if CC_ALG == WOOKONG
	printf("Initializing WKDB Time Table... ");
	fflush(stdout);
	wkdb_time_table.init();
	printf("Done\n");
	// printf("Initializing WKDB KeyxidCache and RtsCache... ");
	// fflush(stdout);
	// wkdb_key_xid_cache.init();
	// wkdb_rts_cache.init();
	// printf("Done\n");
	printf("Initializing WKDB manager... ");
	fflush(stdout);
	wkdb_man.init();
	printf("Done\n");
#endif
#if CC_ALG == TICTOC
	printf("Initializing MaaT manager... ");
	fflush(stdout);
	tictoc_man.init();
	printf("Done\n");
#endif
#if CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
	printf("Initializing DTA Time Table... ");
	fflush(stdout);
	dta_time_table.init();
	printf("Done\n");
	// printf("Initializing DTA KeyxidCache and RtsCache... ");
	// fflush(stdout);
	// dta_key_xid_cache.init();
	// dta_rts_cache.init();
	// printf("Done\n");
	printf("Initializing DTA manager... ");
	fflush(stdout);
	dta_man.init();
	printf("Done\n");
#endif
#if LOGGING
	printf("Initializing logger... ");
	fflush(stdout);
	logger.init("logfile.log");
	printf("Done\n");
#endif
#if WORKLOAD==DA
	commit_file.open("commit_histroy.txt",ios::app);
	abort_file.open("abort_histroy.txt",ios::app);
#endif
    printf("max_tuple_size:%ld tuple_count:%ld memory_size:%ld\n", max_tuple_size,tuple_count,memory_count);

	// 2. spawn multiple threads
	uint64_t thd_cnt = g_thread_cnt;
	uint64_t wthd_cnt = thd_cnt;
	uint64_t rthd_cnt = g_rem_thread_cnt;
	uint64_t sthd_cnt = g_send_thread_cnt;
	uint64_t all_thd_cnt = thd_cnt + rthd_cnt + sthd_cnt + g_abort_thread_cnt;
#if USE_WORK_NUM_THREAD
        all_thd_cnt += 1; 
#endif
#if LOGGING
		all_thd_cnt += 1; // logger thread
#endif
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
		all_thd_cnt += 2; // sequencer + scheduler thread
#endif

	if (g_ts_alloc == LTS_TCP_CLOCK) {
		printf("Initializing tcp queue... ");
		fflush(stdout);
		// tcp_ts.init(all_thd_cnt);
		printf("Done\n");
	}

	printf("%ld, %ld, %ld, %d \n", thd_cnt, rthd_cnt, sthd_cnt, g_abort_thread_cnt);
	printf("all_thd_cnt: %ld, g_this_total_thread_cnt: %d \n", all_thd_cnt, g_this_total_thread_cnt);
	fflush(stdout);
	assert(all_thd_cnt == g_this_total_thread_cnt);

	pthread_t *p_thds = (pthread_t *)malloc(sizeof(pthread_t) * (all_thd_cnt));
	pthread_attr_t attr;
	pthread_attr_init(&attr);

	worker_thds = new WorkerThread[wthd_cnt];
#if USE_WORK_NUM_THREAD
    worker_num_thds = new WorkerNumThread[1];
#endif
	input_thds = new InputThread[rthd_cnt];
	output_thds = new OutputThread[sthd_cnt];
	abort_thds = new AbortThread[1];
	log_thds = new LogThread[1];
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
	calvin_lock_thds = new CalvinLockThread[1];
	calvin_seq_thds = new CalvinSequencerThread[1];
#endif
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	//if (WORKLOAD != TEST) {
	//	query_queue.init(m_wl);
	//}
#if CC_ALG == OCC
	printf("Initializing occ lock manager... ");
	occ_man.init();
	printf("Done\n");
#endif

#if CC_ALG == BOCC
	printf("Initializing occ lock manager... ");
	bocc_man.init();
	printf("Done\n");
#endif

#if CC_ALG == FOCC
	printf("Initializing occ lock manager... ");
	focc_man.init();
	printf("Done\n");
#endif

#if CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || \
		CC_ALG == DLI_MVCC
	printf("Initializing si manager... ");
	dli_man.init();
	printf("Done\n");
#endif
	/*
	printf("Initializing threads... ");
	fflush(stdout);
		for (uint32_t i = 0; i < all_thd_cnt; i++)
				m_thds[i].init(i, g_node_id, m_wl);
	printf("Done\n");
	fflush(stdout);
	*/

	endtime = get_server_clock();
	printf("Initialization Time = %ld\n", endtime - starttime);
	fflush(stdout);
	warmup_done = true;
	pthread_barrier_init( &warmup_bar, NULL, all_thd_cnt);



#if SET_AFFINITY
	uint64_t cpu_cnt = 0;
	cpu_set_t cpus;
#endif
	// spawn and run txns again.
	starttime = get_server_clock();
	simulation->run_starttime = starttime;
	simulation->last_da_query_time = starttime;

	uint64_t id = 0;
	for (uint64_t i = 0; i < wthd_cnt; i++) {
#if SET_AFFINITY
		// CPU_ZERO(&cpus);
		// CPU_SET(cpu_cnt, &cpus);
		// pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
		// cpu_cnt++;
#endif
		assert(id >= 0 && id < wthd_cnt);
		worker_thds[i].init(id,g_node_id,m_wl);
#if USE_COROUTINE
		pthread_create(&p_thds[id++], &attr, run_co_thread, (void *)&worker_thds[i]);
#else
		pthread_create(&p_thds[id++], &attr, run_nco_thread, (void *)&worker_thds[i]);
#endif
	}
	for (uint64_t j = 0; j < rthd_cnt ; j++) {
		assert(id >= wthd_cnt && id < wthd_cnt + rthd_cnt);
		input_thds[j].init(id,g_node_id,m_wl);
		pthread_create(&p_thds[id++], NULL, run_thread, (void *)&input_thds[j]);
	}
//#if USE_RDMA != CHANGE_MSG_QUEUE
	for (uint64_t j = 0; j < sthd_cnt; j++) {
		assert(id >= wthd_cnt + rthd_cnt && id < wthd_cnt + rthd_cnt + sthd_cnt);
		output_thds[j].init(id,g_node_id,m_wl);
		pthread_create(&p_thds[id++], NULL, run_thread, (void *)&output_thds[j]);
	}
//#endif
#if LOGGING
	log_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], NULL, run_thread, (void *)&log_thds[0]);
#endif

#if CC_ALG != CALVIN && CC_ALG != RDMA_CALVIN
	abort_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], NULL, run_thread, (void *)&abort_thds[0]);
#endif

#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
#if SET_AFFINITY
	CPU_ZERO(&cpus);
	CPU_SET(cpu_cnt, &cpus);
	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
	cpu_cnt++;
#endif

	calvin_lock_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_lock_thds[0]);
#if SET_AFFINITY
	CPU_ZERO(&cpus);
	CPU_SET(cpu_cnt, &cpus);
	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
	cpu_cnt++;
#endif

	calvin_seq_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_seq_thds[0]);
#endif
#if USE_WORK_NUM_THREAD
	worker_num_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)&worker_num_thds[0]);
#endif
	for (uint64_t i = 0; i < all_thd_cnt; i++) pthread_join(p_thds[i], NULL);

	endtime = get_server_clock();

	fflush(stdout);
	printf("PASS! SimTime = %f\n", (float)(endtime - starttime) / BILLION);
	if (STATS_ENABLE) stats.print(false);
	//malloc_stats_print(NULL, NULL, NULL);
	printf("\n");
	fflush(stdout);
	// Free things
	//tport_man.shutdown();
	m_wl->index_delete_all();

// #if USE_RDMA
// 	if(g_node_id == 0) {
// 		system("killall memcached");  
// 	}
// #endif

	/*
	txn_table.delete_all();
	txn_pool.free_all();
	access_pool.free_all();
	txn_table_pool.free_all();
	msg_pool.free_all();
	qry_pool.free_all();
	*/
	// if (g_ts_alloc == LTS_TCP_CLOCK) {
	// 	for (uint32_t i = 0; i < all_thd_cnt; i++) {
	// 		tcp_ts.CloseToLts(i);
	// 	}
	// }
	return 0;
}

void * run_thread(void * id) {
		Thread * thd = (Thread *) id;
	thd->run();
	return NULL;
}
void * run_nco_thread(void * id) {
		WorkerThread * thd = (WorkerThread *) id;
	thd->no_routines();
	thd->start_routine();
	return NULL;
}
#if USE_COROUTINE
void * run_co_thread(void * id) {
		WorkerThread * thd = (WorkerThread *) id;
	thd->create_routines(COROUTINE_CNT);
	thd->start_routine();
	return NULL;
}
#endif
void network_test() {

			/*
	ts_t start;
	ts_t end;
	ts_t time;
	int bytes;
	float total = 0;
	for (int i = 0; i < 4; ++i) {
		time = 0;
		int num_bytes = (int) pow(10,i);
		printf("Network Bytes: %d\nns: ", num_bytes);
		for(int j = 0;j < 1000; j++) {
			start = get_sys_clock();
			tport_man.simple_send_msg(num_bytes);
			while((bytes = tport_man.simple_recv_msg()) == 0) {}
			end = get_sys_clock();
			assert(bytes == num_bytes);
			time = end-start;
			total += time;
			//printf("%lu\n",time);
		}
		printf("Avg(s): %f\n",total/BILLION/1000);
		fflush(stdout);
		//time = time/1000;
		//printf("Network Bytes: %d, s: %f\n",i,time/BILLION);
		//printf("Network Bytes: %d, ns: %.3f\n",i,time);

	}
			*/

}

void network_test_recv() {
	/*
	int bytes;
	while(1) {
		if( (bytes = tport_man.simple_recv_msg()) > 0)
			tport_man.simple_send_msg(bytes);
	}
	*/
}
