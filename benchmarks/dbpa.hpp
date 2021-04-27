

#if ENABLE_DBPA
/*
*doorbell batch 2 RDMA one-sided primitives and implement passive ack
*/
class RDMA2req {
public:
  explicit RDMA2req() {
    // fill the reqs with initial value
    sr[0].num_sge = 1; sr[0].sg_list = &sge[0];
    sr[1].num_sge = 1; sr[1].sg_list = &sge[1];

    // coroutine id
    sr[0].send_flags = 0;
    sr[1].send_flags = IBV_SEND_SIGNALED;
    sr[0].next = &sr[1];
    sr[1].next = NULL;
  }

  struct ibv_send_wr sr[2];
  struct ibv_sge     sge[2];
  struct ibv_send_wr *bad_sr;
};

/*
*RDMA CAS
*RDMA READ
*/
class RDMACASRead  : public RDMA2req {
public:
  explicit RDMACASRead() : RDMA2req()
  {
    // op code
    sr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr[1].opcode = IBV_WR_RDMA_READ;
  }

  inline void set_cas_meta(uint64_t compare, uint64_t swap,
                            uint64_t* local_addr) {
    sr[0].wr.atomic.compare_add = compare;
    sr[0].wr.atomic.swap = swap;
    sr[0].imm_data = 0;
    sge[0].addr = (uint64_t)local_addr;
    sge[0].length = sizeof(uint64_t);
  }

  inline void set_read_meta(char *local_addr,uint64_t len) {
    sr[1].imm_data = 0;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = len;
  }

  inline auto post_reqs(const Arc<RDMARC> &qp,uint64_t remote_off) {
    // to avoid performance overhead of Arc, we first extract QP's raw pointer out
    RDMARC *qp_ptr = ({  // unsafe code
      RDMARC *temp = qp.get();
      temp;
    });

    sr[0].wr.atomic.remote_addr = remote_off;
    sr[0].wr.atomic.rkey = qp_ptr->remote_mr.value().key;
    sge[0].lkey = qp_ptr->local_mr.value().lkey;

    sr[1].wr.rdma.remote_addr = remote_off;
    sr[1].wr.rdma.rkey = qp_ptr->remote_mr.value().key;
    sge[1].lkey = qp_ptr->local_mr.value().lkey;
    //only signaled request need wr_id
    sr[1].wr_id = qp_ptr->encode_my_wr(0, 1);

    RDMA_ASSERT(qp_ptr->status == IOCode::Ok)
        << "a QP should be Ok to send, current status: " << qp_ptr->status.code.name();
    
    //only one signaled request:passive ack
    qp_ptr->out_signaled += 1;

    auto res = ibv_post_send(qp_ptr->qp, &(sr[0]), &bad_sr);

    if (0 == res) {
      return ::rdmaio::Ok(std::string(""));
    }
    return ::rdmaio::Err(std::string(strerror(errno)));
  }
};
#endif