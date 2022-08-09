#if USE_DBPAOR == true
class DBrequests {
public:
  DBrequests(int n):num(n){
    sr = (ibv_send_wr *)mem_allocator.alloc(num*sizeof(ibv_send_wr));
    sge = (ibv_sge *)mem_allocator.alloc(num*sizeof(ibv_sge));  
  };
  ~DBrequests(){
    mem_allocator.free(sr, num*sizeof(ibv_send_wr));
    mem_allocator.free(sge, num*sizeof(ibv_sge));
  };

  void init(){
    for(int n=0;n<num;n++){
      sr[n].num_sge = 1;
      sr[n].sg_list = &sge[n];
      if(n < num-1){
        sr[n].send_flags = 0;  //Passive Ack
        sr[n].next = &sr[n+1]; //Doorbell Batching
      }
      else{  //last request
        sr[n].send_flags = IBV_SEND_SIGNALED;  
        sr[n].next = NULL;
      }  
    }
  };
  
  void set_atomic_meta(int i, uint64_t compare, uint64_t swap, uint64_t* local_addr, uint64_t remote_off){
    sr[i].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr[i].wr.atomic.compare_add = compare;
    sr[i].wr.atomic.swap = swap;
    sr[i].imm_data = 0;
    sr[i].wr.atomic.remote_addr = remote_off;
    sge[i].addr = (uint64_t)local_addr;
    sge[i].length = sizeof(uint64_t);
  }; //for CAS

  void set_rdma_meta(int i, ibv_wr_opcode opcode, uint64_t len, char* local_addr, uint64_t remote_off){
    sr[i].opcode = opcode;
    sr[i].imm_data = 0;
    sr[i].wr.rdma.remote_addr = remote_off;
    sge[i].addr = (uint64_t)local_addr;
    sge[i].length = len;
  };   //for Read/Write
  auto post_reqs(const Arc<RDMARC> &qp){
    // to avoid performance overhead of Arc, we first extract QP's raw pointer out
    RDMARC *qp_ptr = ({  // unsafe code
      RDMARC *temp = qp.get();
      temp;
    });

    for(int i=0;i<num;i++){
      if(sr[i].opcode == IBV_WR_ATOMIC_CMP_AND_SWP){
        sr[i].wr.atomic.rkey = qp_ptr->remote_mr.value().key;
        sge[i].lkey = qp_ptr->local_mr.value().lkey;
      }
      else{
        sr[i].wr.rdma.rkey = qp_ptr->remote_mr.value().key;
        sge[i].lkey = qp_ptr->local_mr.value().lkey;
      }
    }

    //only signaled request need wr_id
    sr[num-1].wr_id = qp_ptr->encode_my_wr(0, 1);
    RDMA_ASSERT(qp_ptr->status == IOCode::Ok)
        << "a QP should be Ok to send, current status: " << qp_ptr->status.code.name();
    //only one signaled request:passive ack
    qp_ptr->out_signaled += 1;

    auto res = ibv_post_send(qp_ptr->qp, &(sr[0]), &bad_sr);
    if (0 == res) {
      return ::rdmaio::Ok(std::string(""));
    }
    return ::rdmaio::Err(std::string(strerror(errno)));
  };

private:
  int num; //number of batched requests
  struct ibv_send_wr *sr;
  struct ibv_sge     *sge;
  struct ibv_send_wr *bad_sr;
};
#endif 
