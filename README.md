RedT
=======

RedT is an RDMA-enhanced distributed transaction processing protocol with availability guarantees atop of the opensourced distributed framework Deneva, whose study can be found in the following paper:

    Rachael Harding, Dana Van Aken, Andrew Pavlo, and Michael Stonebraker. 2017.
    An Evaluation of Distributed Concurrency Control. PVLDB 10, 5 (2017), 553–564.
    
We evaluate the performance of RedT/Tapir/2PC+Paxos on both YCSB and TPC-C workload, and the currency control algorithm is No-Wait.

Dependencies
------------
For build:
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
- `cd scripts`
- `python run_experiments.py -e -c vcloud ycsb_cross_dc`
