RedT
=======
Implementation of the paper "Efficient Distributed Transaction Processing in Heterogeneous Networks".

RedT is a novel distributed transaction processing protocol that works in heterogeneous networks, it extends two-phase commitment (a.b.a. 2PC) by decomposing
transactions into sub-transactions in terms of the data center granularity, and proposing a pre-write-log mechanism that is able to eliminate the log synchronization in the prepare phase.

We implemented RedT and other baselines in this repository, and the concurrency control algorithm we used was No-wait. For the other baseline, we use the ordinary No-wait. For RedT, we design three implementations using RDMA single-side primitives.RDMA_NO_WAIT is the algorithm that distinguishes between read and write locks, RDMA_NO_WAIT2 does not distinguish between read and write locks, and RDMA_NO_WAIT3 adds lock_owner to each data item. We used RDMA_NO_WAIT3 in the paper.

RedT is atop of the opensourced distributed framework Deneva, whose study can be found in the following paper:

    Rachael Harding, Dana Van Aken, Andrew Pavlo, and Michael Stonebraker. 2017.
    An Evaluation of Distributed Concurrency Control. PVLDB 10, 5 (2017), 553–564.

Each branch records a different protocol

- RedT-replicacc: the newest RedT protocol.
- tapir: the tapir protocol
- 2PC-Paxos: the 2PC+Paxos protocol and Early-Prepare+Paxos protocol
- MDCC: the MDCC protocol
- RedT-TCP: RedT without RDMA.


Dependencies
------------
To ensure that the code works properly, the server needs to have the following dependencies:
- g++ >= 6.4.0
- Boost = 1.6.1
- jemalloc >= 5.2.1
- nanomsg >= 1.1.5
- libevent >= 1.2
- libibverbs

Build
--------------
- `git clone https://github.com/rhaaaa123/RedT.git`
- `make clean`
- `make deps`
- `make -j16`

Configuration
-------------
In `scripts\run_config.py`, the `vcloud_uname` and `vcloud_machines` need to be changed for running. and the experiments specific configuration can be found in `scripts\experiments.py`.

Run
-------------
### the impact of inter-DC transaction ratio
To test the impact of inter-DC transaction ratio, run the following command:
- `cd scripts`
- `python run_experiments.py -e -c vcloud ycsb_cross_dc -l 20 0`

`-l 20 0` means that the delay between DCs is 40ms and the jitter is 0.

### the impact of inter-DC networks
To test the impact of inter-DC networks, run the following command:
- `cd scripts`
- `sh exp/test_interdelay.sh`

In 'sh exp/test_interdelay.sh', there are shall commands to set different network delays between data centers.

### the impact of read-write ratio
To test the impact of read-write ratio, run the following command:
- `cd scripts`
- `python run_experiments.py -e -c vcloud ycsb_write -l 20 0`

### the impact of contention level
To test the impact of contention level, run the following command:
- `cd scripts`
- `python run_experiments.py -e -c vcloud ycsb_skew -l 20 0`

How to cite our paper
------------

    @article{RedT,
      author = {Qian Zhang, Jingyao Li, Hongyao Zhao, Quanqing Xu, Wei Lu, Jinliang Xiao, Fushan Han, Chuanhui Yang, and Xiaoyong Du},
      year = {2023},
      pages = {},
      title = {Efficient Distributed Transaction Processing in Heterogeneous Networks},
      volume = {16},
      journal = {Proceedings of the VLDB Endowment},
    }


