RedT
=======

RedT is a novel distributed transaction processing protocol that works in heterogeneous networks, it extends two-phase commitment (a.b.a. 2PC) by decomposing
transactions into sub-transactions in terms of the data center granularity, and proposing a pre-write-log mechanism that is able to eliminate the log synchronization in the prepare phase.

RedT is atop of the opensourced distributed framework Deneva, whose study can be found in the following paper:

    Rachael Harding, Dana Van Aken, Andrew Pavlo, and Michael Stonebraker. 2017.
    An Evaluation of Distributed Concurrency Control. PVLDB 10, 5 (2017), 553–564.
    

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
- `python run_experiments.py -e -c vcloud ycsb_scaling`
