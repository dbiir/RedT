/*
    Copyright (c) 2013 250bpm s.r.o.

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"),
    to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense,
    and/or sell copies of the Software, and to permit persons to whom
    the Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included
    in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
    THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
    IN THE SOFTWARE.
*/

#ifndef NN_HPP_INCLUDED
#define NN_HPP_INCLUDED

#include <nanomsg/nn.h>
#include <nanomsg/tcp.h>

/* RDMA communication */
extern "C"{
    #pragma message ( "==== USE RDMA ====" )
#include <libmrdma.h>
}

#include <cassert>
#include <cstring> 
#include <algorithm>
#include <exception>

#if defined __GNUC__
#define nn_slow(x) __builtin_expect ((x), 0)
#else
#define nn_slow(x) (x)
#endif

#define NN_STATIC_SIZE 512

forceinline void PRINT_BINARY_CHAR(char num) {
  for (int i = 0; i < 8; i ++ ) {
    if(num & (1<<(7-i)))
      printf("1");
    else 
      printf("0");
  }
  printf("\n");
}

#define mem_dump(x, n) do { \
  printf("[MEM_DUMP]%p\n", x); \
  for (int i = 0; i < n; i ++ ) { \
    printf("<%d>\t", i); \
    PRINT_BINARY_CHAR(*(x + i)); \
  } \
} while(0);

namespace nn
{
    class socket
    {
    public:

        inline socket ()
        {
            // First Step, 7658 will be changed soon
            // In this version, the qp number is only 1
            buffer_counter_s = 0;
            buffer_counter_r = 0;
            buffer_counter_post = 0;
            buffer_counter_max = RDMA_BUFFER_SIZE / 2 / NN_STATIC_SIZE;

            FILL(ibv_res);

            m_init_parameter(&ibv_res, 1, 7658, 0xdeadbeaf, M_RC, 1);

            m_open_device_and_alloc_pd(&ibv_res);

            rdma_buffer = new char[RDMA_BUFFER_SIZE];
            rdma_send_buffer = rdma_buffer;
            rdma_recv_buffer = rdma_buffer + RDMA_BUFFER_SIZE / 2;

            m_reg_buffer(&ibv_res, rdma_buffer, RDMA_BUFFER_SIZE);

            m_create_cq_and_qp(&ibv_res, RDMA_CYC_QP_NUM, IBV_QPT_RC);

        }

        inline ~socket ()
        {
            // TODO
            // int rc = nn_close (s);
            // assert (rc == 0);
        }

        inline void print_pair_info_local() 
        {   
            printf("Local LID = %d, QPN = %d, PSN = %d\n",
               ibv_res.lparam[0]->lid, ibv_res.lparam[0]->qpn, ibv_res.lparam[0]->psn);
            printf("Local Addr = %ld, RKey = %d, LEN = %zu\n",
               ibv_res.lpriv_data[0]->buffer_addr, ibv_res.lpriv_data[0]->buffer_rkey,
               ibv_res.lpriv_data[0]->buffer_length);
        }

        inline void print_pair_info_remote() 
        {   
            printf("Remote LID = %d, QPN = %d, PSN = %d\n",
               ibv_res.rparam[0]->lid, ibv_res.rparam[0]->qpn, ibv_res.rparam[0]->psn);
            printf("Remote Addr = %ld, RKey = %d, LEN = %zu\n",
               ibv_res.rpriv_data[0]->buffer_addr, ibv_res.rpriv_data[0]->buffer_rkey,
               ibv_res.rpriv_data[0]->buffer_length);
        }

        inline void *get_send_buffer_addr() 
        {
            return (void *)(rdma_send_buffer + buffer_counter_s * NN_STATIC_SIZE);
        }

        inline void *get_recv_buffer_addr() 
        {
            return (void *)rdma_recv_buffer;
        }

        inline void setsockopt (int level, int option, const void *optval,
            size_t optvallen)
        {
            // @mateng do nothing is OK
        }

        inline int bind (const char *addr, uint64_t port)
        {
            ibv_res.port = port;
            ibv_res.is_server = 1;
            m_sync(&ibv_res, "", rdma_buffer);

            // m_post_multi_recv(&ibv_res, rdma_recv_buffer, NN_STATIC_SIZE, RDMA_CYC_QP_NUM - 1, 0);
            for (int i = 0; i < 1000; i ++ ) {
                m_post_recv(&ibv_res, rdma_recv_buffer + i * NN_STATIC_SIZE, NN_STATIC_SIZE, 0);
                incre_counter_post();
            }

            m_modify_qp_to_rts_and_rtr(&ibv_res);

            return 0;
        }

        inline int connect (const char *addr, uint64_t port)
        {
            ibv_res.port = port;
            ibv_res.is_server = 0;
            m_sync(&ibv_res, addr, rdma_buffer);

            // m_post_multi_recv(&ibv_res, rdma_recv_buffer, NN_STATIC_SIZE, RDMA_CYC_QP_NUM - 1, 0);
            for (int i = 0; i < 1000; i ++ ) {
                m_post_recv(&ibv_res, rdma_recv_buffer + i * NN_STATIC_SIZE, NN_STATIC_SIZE, 0);
                incre_counter_post();
            }

            m_modify_qp_to_rts_and_rtr(&ibv_res);

            return 0;
        }

        inline int write (const void *buf, size_t len, int flags)
        {

            m_post_write_offset_sig_imm(&ibv_res, rdma_send_buffer, len, 0, (uint64_t)len, 0);
            m_poll_send_cq(&ibv_res, 0);
            return 0;
        }
#if 0
        inline int send (const void *buf, size_t len, int flags)
        {
            // m_nano_sleep(100000000);

            m_post_write_offset_sig_imm(&ibv_res, rdma_send_buffer, len, 0, (uint64_t)len, 0);
            m_poll_send_cq(&ibv_res, 0);
            return 0;
        }

        inline int recv (void **buf_ptr, size_t len, int flags)
        {
            // int rc = nn_recv (s, buf, len, flags);
            *(char **)buf_ptr = rdma_recv_buffer;
            return  static_cast<int>(m_poll_recv_cq_with_data(&ibv_res, 0));
        }
#else
        inline int send (const void *buf, size_t len, int flags)
        {
            // m_nano_sleep(100000000);
            printf("[BEGIN SEND]\n");
            m_post_send_imm(&ibv_res, (char *)buf, NN_STATIC_SIZE, (uint64_t)len, 0);
            //m_poll_send_cq_once(&ibv_res, 0);
            m_poll_send_cq(&ibv_res, 0);
            printf("[END SEND]%d:%d\n", len, buffer_counter_s);
            // mem_dump((char *)buf, (int)len);
            incre_counter_s();
            return 0;
        }

        // inline int recv (void **buf_ptr, size_t len, int flags)
        // {
        //     // int rc = nn_recv (s, buf, len, flags);
            
        //     REDLOG("[BEGIN RECV]\n");
        //     int res = static_cast<int>(m_poll_recv_cq_with_data_once(&ibv_res, 0));
        //     REDLOG("[END RECV]%d\n", res);

        //     if (res != -1) m_post_recv(&ibv_res, rdma_recv_buffer, NN_STATIC_SIZE, 0);

        //     *(char **)buf_ptr = rdma_recv_buffer;

        //     return res;
        // }
        inline int recv (void **buf_ptr, size_t len, int flags)
        {
            // int rc = nn_recv (s, buf, len, flags);
            
            printf("[BEGIN RECV]\n");
            int res = static_cast<int>(m_poll_recv_cq_with_data(&ibv_res, 0));
            printf("[END RECV]%d:%d\n", res, buffer_counter_r);
            *(char **)buf_ptr = rdma_recv_buffer + buffer_counter_r * NN_STATIC_SIZE;
            // mem_dump(*(char **)buf_ptr + buffer_counter_s * NN_STATIC_SIZE, (int)len);

            m_post_recv(&ibv_res, rdma_recv_buffer + buffer_counter_post * NN_STATIC_SIZE, NN_STATIC_SIZE, 0);

            incre_counter_r();
            incre_counter_post();
            return res;
        }

#endif
/*        inline int sendmsg (const struct nn_msghdr *msghdr, int flags)
        {
            int rc = nn_sendmsg (s, msghdr, flags);
            if (nn_slow (rc < 0)) {
                if (nn_slow (nn_errno () != EAGAIN))
                    throw nn::exception ();
                return -1;
            }
            return rc;
        }

        inline int recvmsg (struct nn_msghdr *msghdr, int flags)
        {
            int rc = nn_recvmsg (s, msghdr, flags);
            if (nn_slow (rc < 0)) {
                if (nn_slow (nn_errno () != EAGAIN))
                    throw nn::exception ();
                return -1;
            }
            return rc;
        }*/

    private:

        int s;
        int buffer_counter_r;
        int buffer_counter_s;
        int buffer_counter_post;
        int buffer_counter_max;
        m_ibv_res ibv_res; 
        char *rdma_buffer;
        char *rdma_send_buffer;
        char *rdma_recv_buffer;

        /*  Prevent making copies of the socket by accident. */
        inline void incre_counter_r() {
            buffer_counter_r = (buffer_counter_r + 1) % buffer_counter_max;
        }
        inline void incre_counter_s() {
            buffer_counter_s = (buffer_counter_s + 1) % buffer_counter_max;
        }
        inline void incre_counter_post() {
            buffer_counter_post = (buffer_counter_post + 1) % buffer_counter_max;
        }
        socket (const socket&);
        void operator = (const socket&);
    };

}

#undef nn_slow

#endif


