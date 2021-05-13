/*
 * Copyright @ stmatengss
 * email -> stmatengss@163.com
 * */
#ifndef LIBMRDMA_H

#define LIBMRDMA_H

#include <libmemcached/memcached.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <infiniband2/verbs.h>
#include <netdb.h>
#include <time.h>
#include <assert.h>


#define M_RC 0x0
#define M_UC 0x1
#define M_UD 0x2

#define MAX_INLINE_DATA 64
#define MAX_SEND_SGE 1
#define MAX_RECV_SGE 1

#define forceinline		inline __attribute__((always_inline))
#define M_UD_PADDING	sizeof(struct ibv_grh)
#define M_CONN_LEN		(sizeof(struct m_param) + sizeof(m_priv_data))
#define M_CONN_PADDING	(M_CONN_LEN + M_UD_PADDING)

 #define RDMA_DEBUG
// #define M_WR_COUNT

// #define M_USE_SRQ
#ifdef M_USE_SRQ
	#define M_SRQ_MAX_SHARE_NUM 1024
#endif

#define M_USE_MEMCACHE

struct m_param {
	int lid;
	int qpn;
	int psn;
};

struct m_priv_data {
	uint64_t buffer_addr;
	uint32_t buffer_rkey;
	size_t buffer_length;
};

struct m_ibv_res {

	int is_server;
	// int is_inline;
	struct ibv_ah **ah;
	struct ibv_port_attr *port_attr;
	struct ibv_context *ctx;
	struct ibv_pd *pd;
	struct ibv_mr *mr;
	struct ibv_qp **qp;
	struct ibv_cq **send_cq, **recv_cq;
// #ifdef M_USE_SRQ
	struct ibv_srq *srq;
// #endif
	struct m_priv_data **lpriv_data, **rpriv_data;
	struct m_param **lparam, **rparam;
//		char *send_buffer;
//		char *recv_buffer;
	int qp_sum;
	int model;
	int port;
	int ib_port;
	int sock;
	uint32_t qkey;
#ifdef M_USE_SRQ
	int wr_counter;
#else
	int* wr_counter;
#endif
};

#define CPEA(ret) if (!ret) { \
		PRINT_LINE \
		printf("ERROR: %s\n", strerror(errno)); \
		printf("ERROR: NULL\n"); \
		exit(1); \
}

#define CPEN(ret) if (ret == NULL) { \
		PRINT_LINE \
		printf("ERROR: %s\n", strerror(errno)); \
		printf("ERROR: NULL\n"); \
		exit(1); \
}

#define CPE(ret) if (ret) { \
		PRINT_LINE \
		printf("ERROR: %s\n", strerror(errno)); \
		printf("ERROR CODE: %d\n", ret); \
		exit(ret); \
}

#define FILL(st) do { \
		memset(&st, 0, sizeof(st)); \
} while(0);

#define time_sec (time((time_t *)NULL))
#define PRINT_TIME printf("time: %Lf\n", (long double)clock());
#define PRINT_LINE printf("line: %d\n", __LINE__);
#define PRINT_FUNC printf("func: %s\n", __FUNC__);
#define FUCK()     printf("FUCK\n");

// static char REG_IP[] = "192.168.1.61\n";
#define MEMCACHED_PORT 10085
static char REG_IP[] = "10.77.110.148";
//static char *REG_IP = getenv("REG_IP");

static __thread memcached_st *memc = NULL;

static int int_to_char(int num, char *res) {
	int counter = 0;
	while(num) {
		res[counter ++ ] = '0' + (num % 10);
		num /= 10;
	}
	return counter;
}

static memcached_st* m_memc_create()
{


    memcached_server_st *servers = NULL;
    memcached_st *memc = memcached_create(NULL);
    memcached_return rc;

    memc = memcached_create(NULL);

    if (memc == NULL) {
        printf("Couldn't create memcached server.\n");
    }

    char *registry_ip = REG_IP;

    // We run the memcached server on the default memcached port 
    printf("[Add Server List]\n");
    servers = memcached_server_list_append(servers,
        registry_ip, MEMCACHED_PORT, &rc);
    printf("[Add Server List end]\n");
    rc = memcached_server_push(memc, servers);
	//printf("memcached success!\n");
    if (rc != MEMCACHED_SUCCESS) {
        printf("Couldn't add memcached server.\n");
    } 

    return memc;
}


static void m_memc_publish(const char *key, void *value, int len)
{
    assert(key != NULL && value != NULL && len > 0);
    memcached_return rc;


    if(memc == NULL) {
        memc = m_memc_create();
    }

    rc = memcached_set(memc, key, strlen(key), (const char *) value, len, 
        (time_t) 0, (uint32_t) 0);
    if (rc != MEMCACHED_SUCCESS) {
        char *registry_ip = REG_IP;
        fprintf(stderr, "\tHRD: Failed to publish key %s. Error %s. "
            "Reg IP = %s\n", key, memcached_strerror(memc, rc), registry_ip);
        exit(-1);
    }
}


static int m_memc_get_published(const char *key, void **value)
{
    assert(key != NULL);

    if(memc == NULL) {
        memc = m_memc_create();
    }

    memcached_return rc;
    size_t value_length;
    uint32_t flags;

    *value = memcached_get(memc, key, strlen(key), &value_length, &flags, &rc);

    if(rc == MEMCACHED_SUCCESS ) {
        return (int) value_length;
    } else if (rc == MEMCACHED_NOTFOUND) {
        assert(*value == NULL);
        return -1;
    } else {
        char *registry_ip = REG_IP;
        fprintf(stderr, "HRD: Error finding value for key \"%s\": %s. "
            "Reg IP = %s\n", key, memcached_strerror(memc, rc), registry_ip);
        exit(-1);
    }
    
    // Never reached 
    assert(false);
}

static void

m_init_ds(struct m_ibv_res *ibv_res) {

	ibv_res->send_cq = (struct ibv_cq **)malloc(sizeof(struct ibv_cq *) * ibv_res->qp_sum);
	ibv_res->recv_cq = (struct ibv_cq **)malloc(sizeof(struct ibv_cq *) * ibv_res->qp_sum);
	ibv_res->qp = (struct ibv_qp **)malloc(sizeof(struct ibv_qp *) * ibv_res->qp_sum);
	ibv_res->ah = (struct ibv_ah **)malloc(sizeof(struct ibv_ah *) * ibv_res->qp_sum);
	ibv_res->lparam = (struct m_param **)malloc(sizeof(struct m_param *) * ibv_res->qp_sum);
	ibv_res->rparam = (struct m_param **)malloc(sizeof(struct m_param *) * ibv_res->qp_sum);
	ibv_res->lpriv_data = (struct m_priv_data **)malloc(sizeof(struct m_priv_data *) * ibv_res->qp_sum);
	ibv_res->rpriv_data = (struct m_priv_data **)malloc(sizeof(struct m_priv_data *) * ibv_res->qp_sum);
#ifndef M_WR_COUNT
	ibv_res->wr_counter = (int *)malloc(sizeof(int) * ibv_res->qp_sum);
#endif
}

static void
m_init_parameter(struct m_ibv_res *ibv_res, int ib_port, 
                 int port, uint32_t qkey, int model, int qp_sum) {
	ibv_res->ib_port = ib_port;
	ibv_res->port = port;
	ibv_res->qkey = qkey;
	ibv_res->model = model;
	ibv_res->qp_sum = qp_sum;

	m_init_ds(ibv_res);
}

static void
m_nano_sleep(int nsec) {
	struct timespec tim, tim2;
	tim.tv_sec = 0;
	tim.tv_nsec = nsec;
	if (nanosleep(&tim, &tim2) < 0) {
		printf("SLEEP ERROR!\n");
		exit(1);
	}
}

static long long
m_get_usec() {
	struct timespec now;
	clock_gettime(CLOCK_REALTIME, &now);
	return (long long)now.tv_sec * 1000000 + now.tv_nsec / 1000;
}

static void
m_get_is_odp (struct m_ibv_res *ibv_res) {
	struct ibv_exp_device_attr dattr;
	dattr.comp_mask = IBV_EXP_DEVICE_ATTR_ODP | IBV_EXP_DEVICE_ATTR_EXP_CAP_FLAGS;
	ibv_exp_query_device(ibv_res->ctx, &dattr);
	if (dattr.exp_device_cap_flags & IBV_EXP_DEVICE_ODP) {
		printf("ODP Support: Yes\n");
	} else {
		printf("ODP Support: No\n");
	}

	if (dattr.odp_caps.general_odp_caps & IBV_EXP_ODP_SUPPORT_IMPLICIT) {
		printf("IMPLICIT ODP Support: Yes\n");
	} else {
		printf("IMPLICIT ODP Support: No\n");
	}

	if (!(dattr.comp_mask & IBV_EXP_DEVICE_ATTR_ODP)) {
		printf("||On-demand paging not supported by driver.\n");
	} else if (!(dattr.odp_caps.per_transport_caps.rc_odp_caps &
		   IBV_EXP_ODP_SUPPORT_SEND)) {
		printf("||Send is not supported for RC transport.\n");
	} else if (!(dattr.odp_caps.per_transport_caps.rc_odp_caps &
		   IBV_EXP_ODP_SUPPORT_RECV)) {
		printf("||Receive is not supported for RC transport.\n");
	}

	const uint32_t rc_caps_mask = IBV_ODP_SUPPORT_SEND |
					      IBV_ODP_SUPPORT_RECV;
	struct ibv_device_attr_ex attrx;

	if (ibv_query_device_ex(ibv_res->ctx, NULL, &attrx)) {
		fprintf(stderr, "Couldn't query device for its features\n");
	}

	if (!(attrx.odp_caps.general_caps & IBV_ODP_SUPPORT) ||
	    (attrx.odp_caps.per_transport_caps.rc_odp_caps & rc_caps_mask) != rc_caps_mask) {
		fprintf(stderr, "The device isn't ODP capable or does not support RC send and receive with ODP\n");
	}

}

static void
m_connection_cpy(struct m_ibv_res *ibv_res, int qp_index_dst, int qp_index_src) {
	ibv_res->qp[qp_index_dst] = ibv_res->qp[qp_index_src];
	ibv_res->send_cq[qp_index_dst] = ibv_res->send_cq[qp_index_src];
	ibv_res->recv_cq[qp_index_dst] = ibv_res->recv_cq[qp_index_src];
	if (ibv_res->model == M_UD) {
		
		ibv_res->ah[qp_index_dst] = ibv_res->ah[qp_index_src];
	}
}

static void
m_client_exchange_via_ud(struct m_ibv_res *ibv_res, struct m_ibv_res *ibv_res_dst, 
	              struct m_param **lparam, struct m_priv_data **lpriv_data, struct m_param **rparam,
                  struct m_priv_data **rpriv_data, int qp_num) {
	// TODO

}

#ifdef M_USE_MEMCACHE

static void
m_client_exchange(const char *server, uint16_t port, struct m_param **lparam,
                  struct m_priv_data **lpriv_data, struct m_param **rparam,
                  struct m_priv_data **rpriv_data, int qp_num) {

#ifdef RDMA_DEBUG
	printf("[Port]%d\n", (int)port);
#endif

	char port_str[10];
	
	int len = int_to_char(port, port_str);

	port_str[len - 1] = '\0';
	port_str[len - 2] = 'c';
	m_memc_publish(port_str, lparam[0], sizeof(**lparam) * qp_num);

	port_str[len - 2] = 'd';	
	m_memc_publish(port_str, lpriv_data[0], sizeof(**lpriv_data) * qp_num);

	port_str[len - 2] = 'a';
	while(m_memc_get_published(port_str, (void **)&rparam[0]) == -1);

	port_str[len - 2] = 'b';
	while(m_memc_get_published(port_str, (void **)&rpriv_data[0]) == -1);


}

static void
m_server_exchange(uint16_t port, struct m_param **lparam, struct m_priv_data **lpriv_data,
                  struct m_param **rparam, struct m_priv_data **rpriv_data, int qp_num) {

#ifdef RDMA_DEBUG
	printf("[Port]%d\n", (int)port);
#endif

	char port_str[10];
	
	int len = int_to_char(port, port_str);

	port_str[len - 1] = '\0';
	port_str[len - 2] = 'a';
	m_memc_publish(port_str, lparam[0], sizeof(**lparam) * qp_num);

	port_str[len - 2] = 'b';	
	m_memc_publish(port_str, lpriv_data[0], sizeof(**lpriv_data) * qp_num);

	port_str[len - 2] = 'c';
	while(m_memc_get_published(port_str, (void **)&rparam[0]) == -1);

	port_str[len - 2] = 'd';
	while(m_memc_get_published(port_str, (void **)&rpriv_data[0]) == -1);

}

#else // NAIVE SOCKET

static void
m_client_exchange(const char *server, uint16_t port, struct m_param **lparam,
                  struct m_priv_data **lpriv_data, struct m_param **rparam,
                  struct m_priv_data **rpriv_data, int qp_num) {

	printf("[Port]%d\n", (int)port);

	int s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	printf("s = %d\n", s);
	if (s == -1) {

		printf("SOCK ERROR!\n");
		exit(1);
	}

 	struct hostent *hent = gethostbyname(server);
	CPEN(hent);

	struct sockaddr_in sin;
	FILL(sin);
	sin.sin_family = PF_INET;
	sin.sin_port = htons(port);
	sin.sin_addr = *((struct in_addr *)hent->h_addr);
    //sockaddr * sin2;
	//sin2->sa_family = sin.sin_family;
	m_nano_sleep(500000000);
	// CPE((connect(s, sin2, sizeof(sin)) == -1));
	printf("find client first error!\n");
	
	
	CPE((connect(s, (struct sockaddr *)&sin, sizeof(sin)) == -1));
    printf("find client second error!\n");
//		PRINT_LINE
	for (int i = 0; i < qp_num; i ++ ) {

		write(s, lparam[i], sizeof(**lparam));
		read(s, (rparam[i]), sizeof(**lparam));
#ifdef RDMA_DEBUG
		printf("remote lid %d, qpn %d\n", (rparam[i])->lid, (rparam[i])->qpn);
#endif
		// PRINT_LINE

		write(s, lpriv_data[i], sizeof(**lpriv_data));
		read(s, (rpriv_data[i]), sizeof(**lpriv_data));
#ifdef RDMA_DEBUG
		printf("remote addr %ld, rkey %d\n", (rpriv_data[i])->buffer_addr,
		       (rpriv_data[i])->buffer_rkey);
#endif
	}

	close(s);
}

static void
m_server_exchange(uint16_t port, struct m_param **lparam, struct m_priv_data **lpriv_data,
                  struct m_param **rparam, struct m_priv_data **rpriv_data, int qp_num) {

	printf("[Port]%d\n", (int)port);
    
	int s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	
	if (s == -1) {

		printf("SOCK ERROR!\n");
		exit(1);
	}

	int on = 1;
    
	CPE((setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on) == -1));
    
	struct sockaddr_in sin;
	FILL(sin);
	sin.sin_family = PF_INET;
	sin.sin_port = htons(port);
	sin.sin_addr.s_addr = htons(INADDR_ANY);
    printf("the first error!\n");
	CPE((bind(s, (struct sockaddr *)&sin, sizeof(sin)) == -1));
    printf("the second error!\n");
	CPE((listen(s, 1) == -1));

	struct sockaddr_in csin;
	socklen_t csinsize = sizeof(csin);

	int c = accept(s, (struct sockaddr *)&csin, &csinsize) ;
	CPE((c == -1));

	for (int i = 0; i < qp_num; i ++ ) {

		write(c, lparam[i], sizeof(**lparam));
		read(c, (rparam[i]), sizeof(**lparam));

#ifdef RDMA_DEBUG
		printf("remote lid %d, qpn %d\n", lparam[i]->lid, lparam[i]->qpn);
#endif

		write(c, lpriv_data[i], sizeof(**lpriv_data));
		read(c, (rpriv_data[i]), sizeof(**lpriv_data));

#ifdef RDMA_DEBUG
		printf("remote addr %ld, rkey %d\n", (rpriv_data[i])->buffer_addr,
		       ((rpriv_data[i]))->buffer_rkey);
#endif
	}

	close(c);
	close(s);
}

#endif

static struct ibv_device *
m_get_deveice (int index) {
	struct ibv_device **devs;
	int num;
	devs = ibv_get_device_list(&num);
	if (index >= num) {
		printf("Cannot get such device\n");
		return NULL;
	}
	return devs[index];
}

static int
m_get_lid (struct m_ibv_res *ibv_res) {
	struct ibv_port_attr port_attr;
	CPEN(ibv_res->ctx);
	// printf("IB Port %d\n", ibv_res->ib_port);
	CPE(ibv_query_port(ibv_res->ctx, ibv_res->ib_port, &port_attr));
	// printf("Get LID %x\n", port_attr.lid);
	return port_attr.lid;
}

static int
m_open_device_and_alloc_pd(struct m_ibv_res *ibv_res) {
	struct ibv_device *dev = m_get_deveice(0);
	ibv_res->ctx = ibv_open_device(dev);
	CPEN(ibv_res->ctx);
	ibv_res->pd = ibv_alloc_pd(ibv_res->ctx);
	CPEN(ibv_res->pd);
	return 0;
}

static forceinline void
m_reg_buffer(struct m_ibv_res *ibv_res, char *buffer, long size) {
	int flags = IBV_ACCESS_LOCAL_WRITE |
	            IBV_ACCESS_REMOTE_WRITE |
	            IBV_ACCESS_REMOTE_READ |
	            IBV_ACCESS_REMOTE_ATOMIC;
	if (ibv_res->mr == NULL) {
		ibv_res->mr = ibv_reg_mr(ibv_res->pd, buffer, size, flags);
		CPEN(ibv_res->mr);
	} else {
		printf("Already register\n");
		exit(1);
	}
}

static forceinline struct ibv_mr *
m_reg_external_buffer(struct m_ibv_res *ibv_res, char *buffer, long size) {
	int flags = IBV_ACCESS_LOCAL_WRITE |
	            IBV_ACCESS_REMOTE_WRITE |
	            IBV_ACCESS_REMOTE_READ |
	            IBV_ACCESS_REMOTE_ATOMIC;
	struct ibv_mr *mr = ibv_reg_mr(ibv_res->pd, buffer, size, flags);
	CPEN(mr);
	return mr;
}

static void
m_init_qp_by_id (struct m_ibv_res *ibv_res, int qp_index) {

	struct ibv_qp_attr qp_attr;
	FILL(qp_attr);
	qp_attr.qp_state = IBV_QPS_INIT;
	qp_attr.port_num = ibv_res->ib_port;
	qp_attr.pkey_index = 0;
	if (ibv_res->model == M_UD) {
		qp_attr.qkey = ibv_res->qkey;
	} else {
		qp_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
		                          IBV_ACCESS_REMOTE_WRITE |
		                          IBV_ACCESS_REMOTE_READ |
		                          IBV_ACCESS_REMOTE_ATOMIC;
	}
	int flags = IBV_QP_STATE |
	            IBV_QP_PKEY_INDEX |
	            IBV_QP_PORT;
	if (ibv_res->model == M_UD) {
		flags |= IBV_QP_QKEY;
	} else {
		flags |= IBV_QP_ACCESS_FLAGS;
	}
	CPE(ibv_modify_qp(ibv_res->qp[qp_index], &qp_attr, flags));
	// printf("QPNum = %d\n", ibv_res->qp->qp_num);
}

static void
m_init_qp (struct m_ibv_res *ibv_res) {
	for (int i = 0; i < ibv_res->qp_sum; i ++ ) {

		struct ibv_qp_attr qp_attr;
		FILL(qp_attr);
		qp_attr.qp_state = IBV_QPS_INIT;
		qp_attr.port_num = ibv_res->ib_port;
		qp_attr.pkey_index = 0;
		if (ibv_res->model == M_UD) {
			qp_attr.qkey = ibv_res->qkey;
		} else {
			qp_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
			                          IBV_ACCESS_REMOTE_WRITE |
			                          IBV_ACCESS_REMOTE_READ |
			                          IBV_ACCESS_REMOTE_ATOMIC;
		}
		int flags = IBV_QP_STATE |
		            IBV_QP_PKEY_INDEX |
		            IBV_QP_PORT;
		if (ibv_res->model == M_UD) {
			flags |= IBV_QP_QKEY;
		} else {
			flags |= IBV_QP_ACCESS_FLAGS;
		}
		CPE(ibv_modify_qp(ibv_res->qp[i], &qp_attr, flags));
	}
	// printf("QPNum = %d\n", ibv_res->qp->qp_num);
}

static forceinline void 
m_destroy_qp_by_id(struct m_ibv_res *ibv_res, int qp_index) {
	if (ibv_res->qp[qp_index] != NULL) {
		CPE(ibv_destroy_qp(ibv_res->qp[qp_index]));
		ibv_res->qp[qp_index] = NULL;
	}
	if (ibv_res->send_cq[qp_index] != NULL) {
		CPE(ibv_destroy_cq(ibv_res->send_cq[qp_index]));
		ibv_res->send_cq[qp_index] = NULL;
	}
	if (ibv_res->recv_cq[qp_index] != NULL) {
		CPE(ibv_destroy_cq(ibv_res->recv_cq[qp_index]));
		ibv_res->recv_cq[qp_index] = NULL;
	}

	FILL(ibv_res->lparam[qp_index]);
	FILL(ibv_res->lpriv_data[qp_index]);
	FILL(ibv_res->rparam[qp_index]);
	FILL(ibv_res->rpriv_data[qp_index]);
}

static forceinline void
m_destroy_qp(struct m_ibv_res *ibv_res) {
	for (int i = 0; i < ibv_res->qp_sum; i ++ ) {
		m_destroy_qp_by_id(ibv_res, i);
	}
}

static forceinline void
m_destroy_all(struct m_ibv_res *ibv_res, char *buffer) {
	m_destroy_qp(ibv_res);

	CPE(ibv_dereg_mr(ibv_res->mr));

	CPE(ibv_dealloc_pd(ibv_res->pd));

	CPE(ibv_close_device(ibv_res->ctx));
}

static forceinline void
m_create_cq_and_qp_by_id (struct m_ibv_res *ibv_res, int max_dep, enum ibv_qp_type qp_type, int qp_index) {

	ibv_res->send_cq[qp_index] = ibv_create_cq(ibv_res->ctx, max_dep, NULL, NULL, 0);
	CPEN(ibv_res->send_cq[qp_index]);
	ibv_res->recv_cq[qp_index] = ibv_create_cq(ibv_res->ctx, max_dep, NULL, NULL, 0);
	CPEN(ibv_res->recv_cq[qp_index]);

	struct ibv_qp_init_attr qp_init_attr;
	FILL(qp_init_attr);
	qp_init_attr.send_cq = ibv_res->send_cq[qp_index];
	qp_init_attr.recv_cq = ibv_res->recv_cq[qp_index];
	qp_init_attr.cap.max_send_wr = max_dep;
	qp_init_attr.cap.max_recv_wr = max_dep;
	qp_init_attr.cap.max_send_sge = MAX_SEND_SGE;
	qp_init_attr.cap.max_recv_sge = MAX_RECV_SGE;
	qp_init_attr.cap.max_inline_data = MAX_INLINE_DATA; //TODO add support of inline
	qp_init_attr.qp_type = qp_type;
	//make sure sq_sig_all == 0 so that passive ack can be enabled 
	qp_init_attr.sq_sig_all = 0; // TODO to complete the sig function

	ibv_res->qp[qp_index] = ibv_create_qp(ibv_res->pd, &qp_init_attr);
	CPEN(ibv_res->qp[qp_index]);

	m_init_qp_by_id(ibv_res, qp_index);
}

static forceinline void
m_create_cq_and_qp (struct m_ibv_res *ibv_res, int max_dep, enum ibv_qp_type qp_type) {

#ifdef M_USE_SRQ
	struct ibv_srq_init_attr srq_init_attr;
	FILL(srq_init_attr);
	srq_init_attr.attr.max_wr  = max_dep;
	srq_init_attr.attr.max_sge = 2;
	srq_init_attr.attr.srq_limit = M_SRQ_MAX_SHARE_NUM;

	ibv_res->srq = ibv_create_srq(ibv_res->pd, &srq_init_attr);
	CPEN(ibv_res->srq);
#endif

	for (int i = 0; i < ibv_res->qp_sum; i ++ ) {

		ibv_res->send_cq[i] = ibv_create_cq(ibv_res->ctx, max_dep, NULL, NULL, 0);
		CPEN(ibv_res->send_cq[i]);

#ifndef M_USE_SRQ
		ibv_res->recv_cq[i] = ibv_create_cq(ibv_res->ctx, max_dep, NULL, NULL, 0);
		CPEN(ibv_res->recv_cq[i]);
#endif

		struct ibv_qp_init_attr qp_init_attr;
		FILL(qp_init_attr);
		qp_init_attr.send_cq = ibv_res->send_cq[i];
#ifdef M_USE_SRQ
		qp_init_attr.recv_cq = ibv_res->send_cq[i];
		qp_init_attr.srq = ibv_res->srq;
#else
		qp_init_attr.recv_cq = ibv_res->recv_cq[i];
#endif
		qp_init_attr.cap.max_send_wr = max_dep;
		qp_init_attr.cap.max_recv_wr = max_dep;
		qp_init_attr.cap.max_send_sge = MAX_SEND_SGE;
		qp_init_attr.cap.max_recv_sge = MAX_RECV_SGE;
		qp_init_attr.cap.max_inline_data = MAX_INLINE_DATA; //TODO add support of inline
		qp_init_attr.qp_type = qp_type;
		//make sure sq_sig_all == 0 so that passive ack can be enabled 
		qp_init_attr.sq_sig_all = 0; // TODO to complete the sig function

		ibv_res->qp[i] = ibv_create_qp(ibv_res->pd, &qp_init_attr);
		CPEN(ibv_res->qp[i]);

	}
	m_init_qp(ibv_res);
}

static void
m_init_ah(struct m_ibv_res *ibv_res) {

	for (int i = 0; i < ibv_res->qp_sum; i ++ ) {

		struct ibv_ah_attr ah_attr;
		ah_attr.dlid = ibv_res->rparam[i]->lid;
		ah_attr.src_path_bits = 0;
		ah_attr.is_global = 0;
		ah_attr.sl = 0;
		ah_attr.port_num = ibv_res->ib_port;
		ibv_res->ah[i] = ibv_create_ah(ibv_res->pd, &ah_attr);
	}
}

static void
m_modify_qp_to_rts_and_rtr_by_id(struct m_ibv_res *ibv_res, int qp_index) {

	struct ibv_qp_attr qp_attr;
	FILL(qp_attr);
	int flags;
	qp_attr.qp_state = IBV_QPS_RTR;
	if (ibv_res->model == M_UD) {
		flags = IBV_QP_STATE;
	} else {
		flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
		        IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
		qp_attr.path_mtu = IBV_MTU_1024;
		qp_attr.dest_qp_num = ibv_res->rparam[qp_index]->qpn;
		qp_attr.rq_psn = ibv_res->rparam[qp_index]->psn;
		if (ibv_res->model == M_RC) {
			qp_attr.max_dest_rd_atomic = 1;
			qp_attr.min_rnr_timer = 12;
			flags |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
		}
		qp_attr.ah_attr.is_global = 0;
		qp_attr.ah_attr.dlid = ibv_res->rparam[qp_index]->lid;
		qp_attr.ah_attr.sl = 0;
		qp_attr.ah_attr.src_path_bits = 0;
		qp_attr.ah_attr.port_num = ibv_res->ib_port;
	}

	CPE(ibv_modify_qp(ibv_res->qp[qp_index], &qp_attr, flags));

	qp_attr.qp_state = IBV_QPS_RTS;
	flags = IBV_QP_STATE | IBV_QP_SQ_PSN;
	if (ibv_res->model == M_UD) {
		qp_attr.sq_psn = lrand48() & 0xffffff;
	} else {
		qp_attr.sq_psn = ibv_res->lparam[qp_index]->psn;
		if (ibv_res->model == M_RC) {

			qp_attr.timeout = 14;
			qp_attr.retry_cnt = 7;
			qp_attr.rnr_retry = 7;
			qp_attr.max_rd_atomic = 1; //1 before
			flags |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
			         IBV_QP_MAX_QP_RD_ATOMIC;
		}

	}
	CPE(ibv_modify_qp(ibv_res->qp[qp_index], &qp_attr, flags));
}

static void
m_modify_qp_to_rts_and_rtr(struct m_ibv_res *ibv_res) {
	for (int i = 0; i < ibv_res->qp_sum; i ++ ) {

		struct ibv_qp_attr qp_attr;
		FILL(qp_attr);
		int flags;
		qp_attr.qp_state = IBV_QPS_RTR;
		if (ibv_res->model == M_UD) {
			flags = IBV_QP_STATE;
		} else {
			flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
			        IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
			qp_attr.path_mtu = IBV_MTU_1024;
			qp_attr.dest_qp_num = ibv_res->rparam[i]->qpn;
			qp_attr.rq_psn = ibv_res->rparam[i]->psn;
			if (ibv_res->model == M_RC) {
				qp_attr.max_dest_rd_atomic = 1;
				qp_attr.min_rnr_timer = 12;
				flags |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
			}
			qp_attr.ah_attr.is_global = 0;
			qp_attr.ah_attr.dlid = ibv_res->rparam[i]->lid;
			qp_attr.ah_attr.sl = 0;
			qp_attr.ah_attr.src_path_bits = 0;
			qp_attr.ah_attr.port_num = ibv_res->ib_port;
		}

		CPE(ibv_modify_qp(ibv_res->qp[i], &qp_attr, flags));

		qp_attr.qp_state = IBV_QPS_RTS;
		flags = IBV_QP_STATE | IBV_QP_SQ_PSN;
		if (ibv_res->model == M_UD) {
			qp_attr.sq_psn = lrand48() & 0xffffff;
		} else {
			qp_attr.sq_psn = ibv_res->lparam[i]->psn;
			if (ibv_res->model == M_RC) {

				qp_attr.timeout = 14;
				qp_attr.retry_cnt = 7;
				qp_attr.rnr_retry = 7;
				qp_attr.max_rd_atomic = 1; //1 before
				flags |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
				         IBV_QP_MAX_QP_RD_ATOMIC;
			}

		}
		CPE(ibv_modify_qp(ibv_res->qp[i], &qp_attr, flags));
	}
	if (ibv_res->model == M_UD ) {
		m_init_ah(ibv_res);
	}
}

/*
 * TODO
 * */
static void
m_sync(struct m_ibv_res *ibv_res, const char *server, char *buffer) {
        
	for (int i = 0; i < ibv_res->qp_sum; i ++ ) {
		ibv_res->lparam[i] = (struct m_param *)malloc(sizeof(struct m_param));
		ibv_res->lparam[i]->psn = lrand48() & 0xffffff;
		ibv_res->lparam[i]->qpn = ibv_res->qp[i]->qp_num;

		ibv_res->lparam[i]->lid = m_get_lid(ibv_res);

		ibv_res->lpriv_data[i] = (struct m_priv_data *)malloc(sizeof(struct m_priv_data));
		ibv_res->lpriv_data[i]->buffer_addr = (uint64_t)buffer;
		ibv_res->lpriv_data[i]->buffer_rkey = ibv_res->mr->rkey;
		ibv_res->lpriv_data[i]->buffer_length = ibv_res->mr->length;

		ibv_res->rparam[i] = (struct m_param *)malloc(sizeof(struct m_param));
		ibv_res->rpriv_data[i] = (struct m_priv_data *)malloc(sizeof(struct m_priv_data));

#ifdef RDMA_DEBUG
		printf("Local LID = %d, QPN = %d, PSN = %d\n",
		       ibv_res->lparam[i]->lid, ibv_res->lparam[i]->qpn, ibv_res->lparam[i]->psn);
		printf("Local Addr = %ld, RKey = %d, LEN = %zu\n",
		       ibv_res->lpriv_data[i]->buffer_addr, ibv_res->lpriv_data[i]->buffer_rkey,
		       ibv_res->lpriv_data[i]->buffer_length);
#endif
	}

	// struct m_param param;
	if (ibv_res->is_server) {
//				ibv_res->rparam = m_server_exchange(ibv_res->port, ibv_res->lparam[i]);
		m_server_exchange(ibv_res->port, ibv_res->lparam, ibv_res->lpriv_data, ibv_res->rparam, ibv_res->rpriv_data, ibv_res->qp_sum);
#ifdef RDMA_DEBUG
		for (int i = 0; i < ibv_res->qp_sum; i ++ ) {

			printf("[SERVER]Local LID = %d, QPN = %d, PSN = %d\n",
			       ibv_res->rparam[i]->lid, ibv_res->rparam[i]->qpn, ibv_res->rparam[i]->psn);
			printf("[SERVER]Local Addr = %ld, RKey = %d, LEN = %zu\n",
			       ibv_res->rpriv_data[i]->buffer_addr, ibv_res->rpriv_data[i]->buffer_rkey,
			       ibv_res->rpriv_data[i]->buffer_length);
		}
#endif
	} else {
		m_client_exchange(server, ibv_res->port, ibv_res->lparam, ibv_res->lpriv_data, ibv_res->rparam, ibv_res->rpriv_data, ibv_res->qp_sum);
#ifdef RDMA_DEBUG
		for (int i = 0; i < ibv_res->qp_sum; i ++ ) {

			printf("[CLIENT]Local LID = %d, QPN = %d, PSN = %d\n",
			       ibv_res->rparam[i]->lid, ibv_res->rparam[i]->qpn, ibv_res->rparam[i]->psn);
			printf("[CLIENT]Local Addr = %ld, RKey = %d, LEN = %zu\n",
			       ibv_res->rpriv_data[i]->buffer_addr, ibv_res->rpriv_data[i]->buffer_rkey,
			       ibv_res->rpriv_data[i]->buffer_length);
		}
#endif
	}
//				ibv_res->rparam = m_client_exchange(server, ibv_res->port, ibv_res->lparam);


/*
	printf("Remote LID = %d, QPN = %d, PSN = %d\n",
	       ibv_res->rparam->lid, ibv_res->rparam->qpn, ibv_res->rparam->psn);

	printf("Local Addr = %ld, RKey = %d, LEN = %zu\n",
	       ibv_res->rpriv_data->buffer_addr, ibv_res->rpriv_data->buffer_rkey,
	       ibv_res->rpriv_data->buffer_length);

*/
}

static forceinline void
m_post_send (struct m_ibv_res *ibv_res, char *buffer, size_t size, int send_flag, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_SEND;
	send_wr.send_flags = send_flag; // FIXME fuck

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_send_sig (struct m_ibv_res *ibv_res, char *buffer, size_t size, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_SEND;
	send_wr.send_flags = IBV_SEND_SIGNALED; // FIXME fuck

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}


static forceinline void
m_post_send_imm (struct m_ibv_res *ibv_res, char *buffer, size_t size, uint64_t imm_data, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);

	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.imm_data = imm_data;
	send_wr.opcode = IBV_WR_SEND_WITH_IMM;
	send_wr.send_flags = IBV_SEND_SIGNALED; // FIXME fuck

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_send_wrid (struct m_ibv_res *ibv_res, char *buffer, size_t size, int wr_id, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = wr_id;
	printf("[W=ADDR=]%p\n", (void *)send_wr.wr.rdma.remote_addr);
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_SEND;
	send_wr.send_flags = IBV_SEND_SIGNALED; // FIXME fuck

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_send_unsig (struct m_ibv_res *ibv_res, char *buffer, size_t size, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_SEND;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_write_offset_sig (struct m_ibv_res *ibv_res, char *buffer, size_t size, long offset, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_RDMA_WRITE;
	send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr + offset;
	send_wr.wr.rdma.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;
	send_wr.send_flags = IBV_SEND_SIGNALED;
	// printf("[W=ADDR=]%p\n", (void *)send_wr.wr.rdma.remote_addr);

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_write_offset_sig_imm (struct m_ibv_res *ibv_res, char *buffer, size_t size, long offset, uint64_t imm_data, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.imm_data = imm_data;
	send_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr + offset;
	send_wr.wr.rdma.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;
	send_wr.send_flags = IBV_SEND_SIGNALED;
	// printf("[W=ADDR=]%p\n", (void *)send_wr.wr.rdma.remote_addr);

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_write_offset_sig_inline (struct m_ibv_res *ibv_res, char *buffer, size_t size, long offset, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_RDMA_WRITE;
	send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr + offset;
	send_wr.wr.rdma.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;
	send_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;

	// printf("[W=ADDR=]%p\n", (void *)send_wr.wr.rdma.remote_addr);
	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_read_offset_sig (struct m_ibv_res *ibv_res, char *buffer, size_t size, long offset, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	// printf("[OFFSET]%ld\n", offset);
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_RDMA_READ;
	send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr + offset;
	// printf("[R=ADDR=]%p\n", (void *)send_wr.wr.rdma.remote_addr);
	send_wr.wr.rdma.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;
	send_wr.send_flags = IBV_SEND_SIGNALED;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_write_offset (struct m_ibv_res *ibv_res, char *buffer, size_t size, long offset, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_RDMA_WRITE;
	send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr + offset;
	send_wr.wr.rdma.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_write_offset_inline (struct m_ibv_res *ibv_res, char *buffer, size_t size, long offset, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_RDMA_WRITE;
	send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr + offset;
	send_wr.wr.rdma.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;
	send_wr.send_flags = IBV_SEND_INLINE;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_read_offset (struct m_ibv_res *ibv_res, char *buffer, size_t size, long offset, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_RDMA_READ;
	send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr + offset;
	send_wr.wr.rdma.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;
//		send_wr.send_flags = IBV_SEND_SIGNALED; // FIXME fuck

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_write (struct m_ibv_res *ibv_res, char *buffer, size_t size, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_RDMA_WRITE;
	send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr;
	send_wr.wr.rdma.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_batch_write(struct m_ibv_res *ibv_res, char **buffers, size_t *sizes, int batch_size, int qp_index) {
	struct ibv_sge *sges = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * batch_size);
	struct ibv_send_wr *send_wrs = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * batch_size);
	for (int i = 0; i < batch_size; i ++ ) {
		sges[i].addr = (uint64_t)buffers[i];
		sges[i].length = (uint32_t)sizes[i];
		sges[i].lkey = ibv_res->mr->lkey;
		send_wrs[i].wr_id = 2;
		send_wrs[i].next = NULL;
		send_wrs[i].sg_list = &sges[i];
		send_wrs[i].num_sge = 1;
		send_wrs[i].next = (i == batch_size - 1 ? NULL : &send_wrs[i + 1]);
		send_wrs[i].opcode = IBV_WR_RDMA_WRITE;
		send_wrs[i].send_flags = IBV_SEND_SIGNALED;
		send_wrs[i].wr.rdma.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr;
		send_wrs[i].wr.rdma.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;
	}

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wrs[0], &bad_send_wr));
}

static forceinline void
m_post_cas(struct m_ibv_res *ibv_res, char *buffer, uint64_t compare, uint64_t swap, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, 8, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
	send_wr.send_flags = IBV_SEND_SIGNALED;

	send_wr.wr.atomic.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr;
	send_wr.wr.atomic.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;
	send_wr.wr.atomic.compare_add = compare;
	send_wr.wr.atomic.swap = swap;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_cas_offset(struct m_ibv_res *ibv_res, char *buffer, uint64_t compare, uint64_t swap, long offset, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, 8, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
	send_wr.send_flags = IBV_SEND_SIGNALED;

	send_wr.wr.atomic.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr + offset;
	send_wr.wr.atomic.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;
	send_wr.wr.atomic.compare_add = compare;
	send_wr.wr.atomic.swap = swap;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_faa(struct m_ibv_res *ibv_res, char *buffer, uint64_t add, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, 8, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
	send_wr.send_flags = IBV_SEND_SIGNALED;

	send_wr.wr.atomic.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr;
	send_wr.wr.atomic.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;
	send_wr.wr.atomic.compare_add = add;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_faa_offset(struct m_ibv_res *ibv_res, char *buffer, uint64_t add, long offset, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, 8, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
	send_wr.send_flags = IBV_SEND_SIGNALED;

	send_wr.wr.atomic.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr + offset;
	send_wr.wr.atomic.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;
	send_wr.wr.atomic.compare_add = add;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_read (struct m_ibv_res *ibv_res, char *buffer, size_t size, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);
	send_wr.wr_id = 2;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_RDMA_READ;
	send_wr.wr.rdma.remote_addr = ibv_res->rpriv_data[qp_index]->buffer_addr;
	send_wr.wr.rdma.rkey = ibv_res->rpriv_data[qp_index]->buffer_rkey;
//		send_wr.send_flags = IBV_SEND_SIGNALED; // FIXME fuck

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_ud_send_sig (struct m_ibv_res *ibv_res, char *buffer, size_t size, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);

	send_wr.wr.ud.ah = ibv_res->ah[qp_index];
	send_wr.wr.ud.remote_qpn = ibv_res->rparam[qp_index]->qpn;
	send_wr.wr.ud.remote_qkey = ibv_res->qkey;

	send_wr.wr_id = 0;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_SEND;
	send_wr.send_flags = IBV_SEND_SIGNALED;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_ud_send (struct m_ibv_res *ibv_res, char *buffer, size_t size, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);

	send_wr.wr.ud.ah = ibv_res->ah[qp_index];
	send_wr.wr.ud.remote_qpn = ibv_res->rparam[qp_index]->qpn;
	send_wr.wr.ud.remote_qkey = ibv_res->qkey;

	send_wr.wr_id = 0;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_SEND;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_ud_send_sig_inline (struct m_ibv_res *ibv_res, char *buffer, size_t size, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);

	send_wr.wr.ud.ah = ibv_res->ah[qp_index];
	send_wr.wr.ud.remote_qpn = ibv_res->rparam[qp_index]->qpn;
	send_wr.wr.ud.remote_qkey = ibv_res->qkey;

	send_wr.wr_id = 0;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_SEND;
	send_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_ud_send_inline (struct m_ibv_res *ibv_res, char *buffer, size_t size, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_send_wr send_wr;
	FILL(send_wr);

	send_wr.wr.ud.ah = ibv_res->ah[qp_index];
	send_wr.wr.ud.remote_qpn = ibv_res->rparam[qp_index]->qpn;
	send_wr.wr.ud.remote_qkey = ibv_res->qkey;

	send_wr.wr_id = 0;
	send_wr.next = NULL;
	send_wr.sg_list = &sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_SEND;
	send_wr.send_flags = IBV_SEND_INLINE;

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wr, &bad_send_wr));
}

static forceinline void
m_post_recv (struct m_ibv_res *ibv_res, char *buffer, size_t size, int qp_index) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_recv_wr recv_wr;
	FILL(recv_wr);
	recv_wr.wr_id = 0;
	recv_wr.next = NULL;
	recv_wr.sg_list = &sge;
	recv_wr.num_sge = 1;

	struct ibv_recv_wr *bad_recv_wr;
	CPE(ibv_post_recv(ibv_res->qp[qp_index], &recv_wr, &bad_recv_wr));
}

static forceinline void
m_post_share_recv (struct m_ibv_res *ibv_res, char *buffer, size_t size) {
	struct ibv_sge sge = {
		(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
	};
	struct ibv_recv_wr recv_wr;
	FILL(recv_wr);
#ifdef M_WR_COUNT
		recv_wr.wr_id = ibv_res->wr_counter ++;
#else
		recv_wr.wr_id = 0;
#endif
	recv_wr.next = NULL;
	recv_wr.sg_list = &sge;
	recv_wr.num_sge = 1;

	struct ibv_recv_wr *bad_recv_wr;
	CPE(ibv_post_srq_recv(ibv_res->srq, &recv_wr, &bad_recv_wr));
}

static forceinline void
m_post_multi_recv (struct m_ibv_res *ibv_res, char *buffer, size_t size, int times, int qp_index) {
	for (int i = 0; i < times; i ++ ) {
		
		struct ibv_sge sge = {
			(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_recv_wr recv_wr;
		FILL(recv_wr);
		recv_wr.wr_id = 0;
		recv_wr.next = NULL;
		recv_wr.sg_list = &sge;
		recv_wr.num_sge = 1;

		struct ibv_recv_wr *bad_recv_wr;
		CPE(ibv_post_recv(ibv_res->qp[qp_index], &recv_wr, &bad_recv_wr));
	}
}

static forceinline void
m_post_multi_share_recv (struct m_ibv_res *ibv_res, char *buffer, size_t size, int times) {
	for (int i = 0; i < times; i ++ ) {
		
		struct ibv_sge sge = {
			(uint64_t)buffer, (uint32_t)size, ibv_res->mr->lkey
		};
		struct ibv_recv_wr recv_wr;
		FILL(recv_wr);
#ifdef M_WR_COUNT
		recv_wr.wr_id = ibv_res->wr_counter ++;
#else
		recv_wr.wr_id = 0;
#endif
		recv_wr.next = NULL;
		recv_wr.sg_list = &sge;
		recv_wr.num_sge = 1;

		struct ibv_recv_wr *bad_recv_wr;
		CPE(ibv_post_srq_recv(ibv_res->srq, &recv_wr, &bad_recv_wr));
	}
}

static forceinline void
m_post_batch_recv (struct m_ibv_res *ibv_res, char **buffers, size_t *sizes, int batch_size, int qp_index) {
	
	struct ibv_sge *sges = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * batch_size);
	struct ibv_recv_wr *recv_wrs = (struct ibv_recv_wr *)malloc(sizeof(struct ibv_recv_wr) * batch_size);

	for (int i = 0; i < batch_size; i ++ ) {
		sges[i].addr = (uint64_t)buffers[i];
		sges[i].length = (uint32_t)sizes[i];
		sges[i].lkey = ibv_res->mr->lkey;
#if (defined M_WR_COUNT) && !(defined M_USE_SRQ) 
		recv_wrs[i].wr_id = ibv_res->wr_counter[qp_index] ++;
#else
		recv_wrs[i].wr_id = 0;
#endif
		recv_wrs[i].sg_list = &sges[i];
		recv_wrs[i].num_sge = 1;
		recv_wrs[i].next = (i == batch_size - 1 ? NULL : &recv_wrs[i + 1]);
	}

	struct ibv_recv_wr *bad_send_wr;
	CPE(ibv_post_recv(ibv_res->qp[qp_index], &recv_wrs[0], &bad_send_wr));

}

static forceinline void
m_poll_send_cq(struct m_ibv_res *ibv_res, int qp_index) {
	struct ibv_wc wc;
	int result = 0;
	while (result < 1){
		sleep(1);
		result = ibv_poll_cq(ibv_res->send_cq[qp_index], 1, &wc);
		printf("%d\n",result);
		if (result == 1) {
			wc.status = IBV_WC_SUCCESS;
		}
	};//y{printf("%d\n",ibv_poll_cq(ibv_res->send_cq[qp_index], 1, &wc));};
	if (wc.status != IBV_WC_SUCCESS) {
		printf("Status: %d\n", wc.status);
		printf("Ibv_poll_cq error!\n");
		printf("Error: %s\n", strerror(errno));
		// exit(1);
		assert(0);
	}
}

static forceinline void
m_poll_send_cq_once(struct m_ibv_res *ibv_res, int qp_index) {
	struct ibv_wc *wcs = (struct ibv_wc *)malloc(sizeof(10));
	
	ibv_poll_cq(ibv_res->send_cq[qp_index], 10, &wcs[0]);
}

static forceinline void
m_poll_send_cq_multi(struct m_ibv_res *ibv_res, int number, int qp_index) {
	struct ibv_wc *wcs = (struct ibv_wc *)malloc(sizeof(number));
	int counter = 0;

	while (counter < number) {
		int poll_num = ibv_poll_cq(ibv_res->send_cq[qp_index], number - counter	, &wcs[counter]);
		if (poll_num != 0) {
			counter += poll_num;
			if (wcs[counter].status != IBV_WC_SUCCESS) {
				printf("Ibv_poll_cq error!\n");
				printf("Error: %s\n", strerror(errno));
				exit(1);
			}
			// printf("poll num: %d, number: %d\n", poll_num, number);
		}
	}
}

static forceinline void
m_poll_recv_cq(struct m_ibv_res *ibv_res, int qp_index) {
	struct ibv_wc wc;
	while (ibv_poll_cq(ibv_res->recv_cq[qp_index], 1, &wc) < 1);
	if (wc.status != IBV_WC_SUCCESS) {
		printf("Ibv_poll_cq error!\n");
		printf("Error: %s\n", strerror(errno));
		// exit(1);
		assert(0);
	}
}

static forceinline void
m_poll_all_recv_cq_rr(struct m_ibv_res *ibv_res, char *buffer, size_t send_size) {
	int counter = 0;
	for (int i = 0; i < ibv_res->qp_sum; i ++ ) {

		struct ibv_wc wc;
		int ret = ibv_poll_cq(ibv_res->send_cq[i], 1, &wc);
		if (ret == 1) {
			counter ++;		
			if (wc.status != IBV_WC_SUCCESS) {
				printf("Ibv_poll_cq error!\n");
				printf("Error: %s\n", strerror(errno));
				// exit(1);
				assert(0);
			}
			m_post_share_recv(ibv_res, buffer, send_size);
		} 
	}
	// return counter;
}

static forceinline int
m_poll_recv_cq_get_id(struct m_ibv_res *ibv_res, int qp_index) {
	struct ibv_wc wc;
#ifdef M_USE_SRQ
	while (ibv_poll_cq(ibv_res->send_cq[qp_index], 1, &wc) < 1);
#else
	while (ibv_poll_cq(ibv_res->recv_cq[qp_index], 1, &wc) < 1);
#endif
	if (wc.status != IBV_WC_SUCCESS) {
		printf("Ibv_poll_cq error!\n");
		printf("Error: %s\n", strerror(errno));
		// exit(1);
		assert(0);
	}
	return wc.wr_id;
}

static forceinline int
m_poll_recv_cq_and_return(struct m_ibv_res *ibv_res, int number, int qp_index, struct ibv_wc *wcs) {

	int poll_num;
	do {
		poll_num = ibv_poll_cq(ibv_res->recv_cq[qp_index], number, &wcs[0]);
	} while(poll_num == 0);

	if (wcs[0].status != IBV_WC_SUCCESS) {
		printf("Ibv_poll_cq error!\n");
		printf("Error: %s\n", strerror(errno));
		// exit(1);
		assert(0);
	}
	return poll_num;
}

static forceinline void
m_poll_recv_cq_lazy(struct m_ibv_res *ibv_res, int nano_sec, int qp_index) {
	struct ibv_wc wc;
	while (ibv_poll_cq(ibv_res->recv_cq[qp_index], 1, &wc) < 1) {
		m_nano_sleep(nano_sec);
	}
	if (wc.status != IBV_WC_SUCCESS) {
		printf("Ibv_poll_cq error!\n");
		printf("Error: %s\n", strerror(errno));
		// exit(1);
		assert(0);
	}
}

static forceinline void
m_poll_recv_cq_multi(struct m_ibv_res *ibv_res, int number, int qp_index) {
	struct ibv_wc *wcs = (struct ibv_wc *)malloc(sizeof(number));
	int counter = 0;

	while (counter < number) {
		int poll_num = ibv_poll_cq(ibv_res->recv_cq[qp_index], number, &wcs[counter]);
		if (poll_num != 0) {
			counter += poll_num;
			if (wcs[0].status != IBV_WC_SUCCESS) {
				printf("Ibv_poll_cq error!\n");
				printf("Error: %s\n", strerror(errno));
				exit(1);
			}
			// printf("poll num: %d, number: %d\n", poll_num, number);
		}
	}
}

static forceinline uint64_t
m_poll_recv_cq_with_data(struct m_ibv_res *ibv_res, int qp_index) {
	struct ibv_wc wc;
	while (ibv_poll_cq(ibv_res->recv_cq[qp_index], 1, &wc) < 1);
	if (wc.status != IBV_WC_SUCCESS) {
		printf("Ibv_poll_cq error!\n");
		printf("Error: %s\n", strerror(errno));
		exit(1);
	}
	return wc.imm_data;
}

static forceinline uint64_t
m_poll_recv_cq_with_data_once(struct m_ibv_res *ibv_res, int qp_index) {
	struct ibv_wc wc;
	int res = ibv_poll_cq(ibv_res->recv_cq[qp_index], 1, &wc);
	if (res == 1) {
		return wc.imm_data;
	} else {
		return -1;
	}
}

static forceinline void
m_post_ud_batch_send_inline (struct m_ibv_res *ibv_res, char **buffers, size_t *sizes, int batch_size, int unsig_dis, int qp_index) {

	struct ibv_sge *sges = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * batch_size);
	struct ibv_send_wr *send_wrs = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * batch_size);

	for (int i = 0; i < batch_size; i ++ ) {
		sges[i].addr = (uint64_t)buffers[i];
		sges[i].length = (uint32_t)sizes[i];
		sges[i].lkey = ibv_res->mr->lkey;

		send_wrs[i].wr.ud.ah = ibv_res->ah[qp_index];
		send_wrs[i].wr.ud.remote_qpn = ibv_res->rparam[qp_index]->qpn;
		send_wrs[i].wr.ud.remote_qkey = ibv_res->qkey;

		send_wrs[i].wr_id = 2;
		send_wrs[i].next = NULL;
		send_wrs[i].sg_list = &sges[i];
		send_wrs[i].num_sge = 1;
		send_wrs[i].next = (i == batch_size - 1 ? NULL : &send_wrs[i + 1]);
		send_wrs[i].opcode = IBV_WR_SEND;
		send_wrs[i].send_flags = IBV_SEND_INLINE;

		if (i % unsig_dis == 0) {
			send_wrs[i].send_flags |= IBV_SEND_SIGNALED;
		}
	}

	struct ibv_send_wr *bad_send_wr;
	CPE(ibv_post_send(ibv_res->qp[qp_index], &send_wrs[0], &bad_send_wr));
    printf("qp_index: %d\n", qp_index);
	for (int i = 0; i < batch_size / unsig_dis; i ++ ) {
		m_poll_send_cq(ibv_res, qp_index);
	}
}
#endif
