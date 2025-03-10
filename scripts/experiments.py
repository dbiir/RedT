import itertools
from paper_plots import *
# Experiments to run and analyze
# Go to end of file to fill in experiments
SHORTNAMES = {
    "CLIENT_NODE_CNT" : "CN",
    "CLIENT_THREAD_CNT" : "CT",
    "CLIENT_REM_THREAD_CNT" : "CRT",
    "CLIENT_SEND_THREAD_CNT" : "CST",
    "NODE_CNT" : "N",
    "THREAD_CNT" : "T",
    "COROUTINE_CNT" : "CO",
    "REM_THREAD_CNT" : "RT",
    "SEND_THREAD_CNT" : "ST",
    "CC_ALG" : "",
    "WORKLOAD" : "",
    "MAX_TXN_PER_PART" : "TXNS",
    "MAX_TXN_IN_FLIGHT" : "TIF",
    "PART_PER_TXN" : "PPT",
    "DC_PER_TXN" : "DPT",
    "TUP_READ_PERC" : "TRD",
    "TUP_WRITE_PERC" : "TWR",
    "TXN_READ_PERC" : "RD",
    "TXN_WRITE_PERC" : "WR",
    "ZIPF_THETA" : "SKEW",
    "MSG_TIME_LIMIT" : "BT",
    "MSG_SIZE_MAX" : "BS",
    "DATA_PERC":"D",
    "ACCESS_PERC":"A",
    "PERC_PAYMENT":"PP",
    "MPR":"MPR",
    "REQ_PER_QUERY": "RPQ",
    "MODE":"",
    "PRIORITY":"",
    "ABORT_PENALTY":"PENALTY",
    "STRICT_PPT":"SPPT",
    "NETWORK_DELAY":"NDLY",
    "NETWORK_DELAY_TEST":"NDT",
    "REPLICA_CNT":"RN",
    "SYNTH_TABLE_SIZE":"TBL",
    "ISOLATION_LEVEL":"LVL",
    "YCSB_ABORT_MODE":"ABRTMODE",
    "NUM_WH":"WH",
    "HIS_CHAIN_NUM":"VA",
}

fmt_title=["NODE_CNT","CC_ALG","ACCESS_PERC","TXN_WRITE_PERC","PERC_PAYMENT","MPR","MODE","MAX_TXN_IN_FLIGHT","SEND_THREAD_CNT","REM_THREAD_CNT","THREAD_CNT","COROUTINE_CNT","TXN_WRITE_PERC","TUP_WRITE_PERC","ZIPF_THETA","NUM_WH"]

##############################
# PLOTS
##############################
dta_target_algos=['RDMA_REDT']
# tpcc load
#tpcc_loads = ['50', '100', '200', '500', '1000', '2000', '5000']
tpcc_loads = ['50', '100', '200', '500', '1000', '2000', '5000']
# ycsb load
ycsb_loads = ['50', '100', '200', '500', '1000', '2000', '5000']


def ycsb_cross_dc():
    wl = 'YCSB'
    nnodes = [8]
    dcs = [4]
    # algos=['RDMA_RED_T']
    algos=['RDMA_SI']
    # algos=['RDMA_SI']
    # base_table_size=524288
    base_table_size=1048576
    # txn_write_perc = [1]
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    load = [240]
    tcnt = [30]  #THREAD_CNT
    skew = [0.2]
    # cross_dc_perc = [0.1] 
    cross_dc_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","CENTER_CNT","THREAD_CNT","CROSS_DC_TXN_PERC"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,dc,thr,cro_dc_perc] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,dc in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,dcs)]
    return fmt,exp

def ycsb_cross_dc_no_ro():
    wl = 'YCSB'
    nnodes = [8]
    dcs = [4]
    # algos=['RDMA_RED_T']
    # algos=['RDMA_NO_WAIT3']
    algos=['RDMA_SI']
    # base_table_size=524288
    base_table_size=1048576
    # txn_write_perc = [1]
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    # tup_write_perc = [1]
    load = [240]
    tcnt = [30]  #THREAD_CNT
    skew = [0.2]
    # cross_dc_perc = [1.0] 
    cross_dc_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","ZIPF_THETA","CENTER_CNT","THREAD_CNT","CROSS_DC_TXN_PERC"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,"false",sk,dc,thr,cro_dc_perc] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,dc in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,dcs)]
    return fmt,exp

def ycsb_network_delay():
    wl = 'YCSB'
    nnodes = [8]
    algos=['RDMA_SI']
    # algos=['RDMA_RED_T']
    base_table_size=1048576
    # base_table_size=524288
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    load = [240]
    tcnt = [30]  #THREAD_CNT
    skew = [0.2]
    cross_dc_perc = [1]
    network_delay = ['0UL'] 
    rpq =  10
    # network_delay = ['50000000UL','10000000UL'] 
    # cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 

    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","CENTER_CNT","REQ_PER_QUERY","THREAD_CNT","CROSS_DC_TXN_PERC", "NETWORK_DELAY",]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,4,rpq,thr,cro_dc_perc,net_del] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,net_del in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,network_delay)]
    return fmt,exp

def ycsb_network_delay_no_ro():
    wl = 'YCSB'
    nnodes = [8]
    algos=['RDMA_SI']
    # algos=['RDMA_RED_T']
    base_table_size=1048576
    # base_table_size=524288
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    load = [240]
    tcnt = [30]  #THREAD_CNT
    skew = [0.2]
    cross_dc_perc = [1]
    network_delay = ['0UL'] 
    rpq =  10
    # network_delay = ['50000000UL','10000000UL'] 
    # cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 

    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","ZIPF_THETA","CENTER_CNT","REQ_PER_QUERY","THREAD_CNT","CROSS_DC_TXN_PERC", "NETWORK_DELAY",]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,"false",sk,4,rpq,thr,cro_dc_perc,net_del] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,net_del in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,network_delay)]
    return fmt,exp

def ycsb_thread():
    wl = 'YCSB'
    nnodes = [8]
    algos=['RDMA_SI']
    # algos=['RDMA_RED_T']
    base_table_size=1048576
    # base_table_size=524288
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    load = [240]
    tcnt = [4,8,12,16,20,24,28,32,36]  #THREAD_CNT
    # tcnt = [30, 10, 20]  #THREAD_CNT
    skew = [0.2]
    network_delay = ['0UL'] 
    rpq =  10

    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","ZIPF_THETA","CENTER_CNT","REQ_PER_QUERY","THREAD_CNT", "NETWORK_DELAY"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,"false",sk,1,rpq,thr,net_del] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,net_del in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,network_delay)]
    return fmt,exp

def ycsb_skew():
    wl = 'YCSB'
    nnodes = [8]
    # algos=['RDMA_RED_T']
    algos=['RDMA_SI']
    # base_table_size=1048576*10
    # base_table_size=524288
    base_table_size=1048576
    #base_table_size=2097152*8

    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    load = [240]

    tcnt = [30]  #THREAD_CNT

    # skew = [0.0,0.2,0.4,0.5,0.6,0.65,0.7,0.75,0.8,0.85,0.9]
    # skew = [0.0,0.2,0.4,0.5]
    # skew = [0.6,0.65,0.7,0.75,0.8,0.85,0.9]
    # skew = [0.25,0.55,0.65,0.75]
    # skew = [0.0,0.7,0.75,0.8,0.85,0.9]
    skew = [0.7]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","CENTER_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,4,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_skew_no_ro():
    wl = 'YCSB'
    nnodes = [8]
    # algos=['RDMA_RED_T']
    algos=['RDMA_SI']
    base_table_size=1048576

    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    load = [240]

    tcnt = [30]  #THREAD_CNT

    skew = [0.0,0.2,0.4,0.5,0.6,0.65,0.7,0.75,0.8,0.85,0.9]
    # skew = [0.0,0.2,0.4,0.5]
    # skew = [0.6,0.65,0.7,0.75,0.8,0.85,0.9]
    # skew = [0.25,0.55,0.65,0.75]
    # skew = [0.8]
    # skew = [0.0,0.1,0.2,0.3,0.4,0.5]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","CENTER_CNT","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,4,ld,"false",sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp


def ycsb_writes():
    wl = 'YCSB'
    nnodes = [8]
    # algos=['WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    # algos=dta_target_algos
    algos=['RDMA_RED_T']
    # algos=['RDMA_SI']
    base_table_size=1048576
    # base_table_size=524288
    txn_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    # txn_write_perc = [1.0]
    # txn_write_perc = [0.0]
    # tup_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    tup_write_perc = [0.5]
    load = [240]
    tcnt = [30]
    skew = [0.2]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_writes_no_ro():
    wl = 'YCSB'
    nnodes = [8]
    # algos=['RDMA_RED_T']
    algos=['RDMA_SI']
    base_table_size=1048576
    # base_table_size=524288
    txn_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    # txn_write_perc = [1.0]
    # txn_write_perc = [0.0]
    # tup_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    tup_write_perc = [0.5]
    load = [240]
    tcnt = [30]
    skew = [0.2]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,"false",sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_version_array():
    wl = 'YCSB'
    nnodes = [8]
    # algos=['RDMA_RED_T']
    algos=['RDMA_SI']
    base_table_size=1048576
    # base_table_size=524288
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    # version_array = [1,2,4,8,16]
    version_array = [1,100]
    load = [240]
    tcnt = [30]
    skew = [0.0,0.2,0.4,0.5,0.6,0.65,0.7,0.75,0.8,0.85,0.9]
    # skew = [0.0,0.4,0.7,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","HIS_CHAIN_NUM","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,va,tup_wr_perc,txn_wr_perc,ld,"true",sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,va in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,version_array)]
    return fmt,exp


def isolation_levels():
    wl = 'YCSB'
    nnodes = [1,2,4,8,16,32,64]
    algos=['NO_WAIT']
    levels=["READ_UNCOMMITTED","READ_COMMITTED","SERIALIZABLE","NOLOCK"]
    base_table_size=2097152*8
    load = [10000]
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    skew = [0.6,0.7]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","ISOLATION_LEVEL","MAX_TXN_IN_FLIGHT","ZIPF_THETA"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,level,ld,sk] for txn_wr_perc,tup_wr_perc,algo,sk,ld,n,level in itertools.product(txn_write_perc,tup_write_perc,algos,skew,load,nnodes,levels)]
    return fmt,exp

def ycsb_partitions():
    wl = 'YCSB'
    # nnodes = [15]
    nnodes = [16]
    # algos=['RDMA_RED_T']
    algos=['RDMA_SI']
    # load = [10000,12000]
    load = [200]
    nparts = [2,3,4,5,6,7,8]
    # nparts = [2]
    ndcs = [2]
    base_table_size= 1048576
    # txn_write_perc = [1]
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    tcnt = [12]
    skew = [0.2]
    rpq =  10
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","REQ_PER_QUERY","PART_PER_TXN","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","CENTER_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,n,algo,rpq,p,base_table_size*n,tup_wr_perc,txn_wr_perc,4,ld,sk,thr,1] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts)]
    return fmt,exp

def ycsb_partitions_no_ro():
    wl = 'YCSB'
    # nnodes = [15]
    nnodes = [16]
    # algos=['RDMA_RED_T']
    algos=['RDMA_SI']
    # load = [10000,12000]
    load = [200]
    nparts = [2,3,4,5,6,7,8]
    # nparts = [2]
    ndcs = [2]
    base_table_size= 1048576
    # txn_write_perc = [1]
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    tcnt = [12]
    skew = [0.2]
    rpq =  10
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","REQ_PER_QUERY","PART_PER_TXN","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","CENTER_CNT","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,n,algo,rpq,p,base_table_size*n,tup_wr_perc,txn_wr_perc,4,ld,"false",sk,thr,1] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts)]
    return fmt,exp

def ycsb_dcs():
    wl = 'YCSB'
    # nnodes = [15]
    nnodes = [16]
    # algos=['RDMA_RED_T']
    algos=['RDMA_SI']
    # algos=['RDMA_NO_WAIT3']
    # load = [10000,12000]
    load = [200]
    nparts = [8]
    # nparts = [4]
    ndcs = [2,3,4,5,6,7,8]
    # ndcs = [3]
    base_table_size= 1048576
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    tcnt = [12]
    skew = [0.2]
    rpq =  12
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","REQ_PER_QUERY","PART_PER_TXN","DC_PER_TXN","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","CENTER_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,n,algo,rpq,p,dc,base_table_size*n,tup_wr_perc,txn_wr_perc,8,ld,sk,thr,0] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p,dc in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts,ndcs)]
    return fmt,exp


def ycsb_dcs_no_ro():
    wl = 'YCSB'
    nnodes = [16]
    # algos=['RDMA_RED_T']
    algos=['RDMA_SI']
    # load = [10000,12000]
    load = [200]
    nparts = [8]
    # nparts = [4]
    ndcs = [2,3,4,5,6,7,8]
    # ndcs = [3]
    base_table_size= 1048576
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    tcnt = [12]
    skew = [0.2]
    rpq =  12
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","REQ_PER_QUERY","PART_PER_TXN","DC_PER_TXN","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","CENTER_CNT","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,n,algo,rpq,p,dc,base_table_size*n,tup_wr_perc,txn_wr_perc,8,ld,"false",sk,thr,0] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p,dc in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts,ndcs)]
    return fmt,exp

def ycsb_scaling():
    wl = 'YCSB'
    # nnodes = [16]
    nnodes = [4,8,12,16]

    # algos = ['RDMA_RED_T']
    algos=['RDMA_SI']

    # base_table_size=524288 
    base_table_size=1048576 
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    tcnt = [12]
    ctcnt = [1]
    scnt = [1]
    rcnt = [1]
    skew = [0.2]
    # skew = [0.0,0.5,0.9]    
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT","CENTER_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,n*thr,sk,thr,cthr,sthr,rthr,sthr,rthr,4] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,nnodes,algos)]
    return fmt,exp

def tpcc_scaling_n():
    wl = 'TPCC'
    nnodes = [4,8,12,16]
    # nnodes = [4]

    # nalgos=['RDMA_RED_T']
    nalgos=['RDMA_SI']
    npercpay=[0.0]
    # npercpay=[1.0]
    wh=16
    # wh=64
    load = [240]
    tcnt = [12]
    ctcnt = [1]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","CLIENT_NODE_CNT","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,1,thr*n,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling_p():
    wl = 'TPCC'
    nnodes = [4,8,12,16]
    # nnodes = [3,6,9]
    # nalgos=['RDMA_RED_T']
    nalgos=['RDMA_SI']
    npercpay=[1.0]
    # npercpay=[1.0]
    wh=16
    # wh=64
    load = [20000]
    tcnt = [12]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","CLIENT_NODE_CNT","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,1,thr*n,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]
    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling():
    wl = 'TPCC'
    nnodes = [4,8,12,16]
    # nnodes = [8]

    nalgos=['RDMA_SI']
    npercpay=[0.489]
    # npercpay=[1.0]
    wh=16
    # wh=64
    load = [240]
    tcnt = [12]
    ctcnt = [1]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","CLIENT_NODE_CNT","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,1,thr*n,"false",thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def ycsb_scaling_no():
    wl = 'YCSB'
    # nnodes = [16]
    nnodes = [4,8,12,16]

    # algos = ['RDMA_RED_T']
    nalgos=['RDMA_SI']

    # base_table_size=524288 
    base_table_size=1048576 
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    tcnt = [12]
    ctcnt = [1]
    scnt = [1]
    rcnt = [1]
    skew = [0.2]
    # skew = [0.0,0.5,0.9]    
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT","CENTER_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,n*thr,"false",sk,thr,cthr,sthr,rthr,sthr,rthr,4] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,nnodes,algos)]
    return fmt,exp

def tpcc_scaling_n_no_ro():
    wl = 'TPCC'
    nnodes = [4,8,12,16]
    # nnodes = [4]

    # nalgos=['RDMA_RED_T']
    nalgos=['RDMA_SI']
    npercpay=[0.0]
    wh=16
    # wh=64
    load = [240]
    tcnt = [12]
    ctcnt = [1]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","CLIENT_NODE_CNT","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,1,thr*n,"false",thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling_p_no_ro():
    wl = 'TPCC'
    nnodes = [4,8,12,16]
    # nalgos=['RDMA_RED_T']
    nalgos=['RDMA_SI']
    npercpay=[1.0]
    # npercpay=[1.0]
    wh=16
    # wh=64
    load = [20000]
    tcnt = [12]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","CLIENT_NODE_CNT","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,1,thr*n,"false",thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]
    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling_no_ro():
    wl = 'TPCC'
    nnodes = [4,8,12,16]
    # nnodes = [8]

    nalgos=['RDMA_SI']
    npercpay=[0.489]
    # npercpay=[1.0]
    wh=16
    # wh=64
    load = [240]
    tcnt = [12]
    ctcnt = [1]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","CLIENT_NODE_CNT","MAX_TXN_IN_FLIGHT","READ_OPTIMIZATION","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,1,thr*n,"false",thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

##############################
# END PLOTS
##############################

experiment_map = {
    'ycsb_scaling': ycsb_scaling,
    'ycsb_writes': ycsb_writes,
    'ycsb_skew': ycsb_skew,
    'ycsb_cross_dc': ycsb_cross_dc,
    'ycsb_network_delay': ycsb_network_delay,
    'ycsb_partitions': ycsb_partitions,
    'ycsb_dcs': ycsb_dcs,
    'tpcc_scaling_n':tpcc_scaling_n,
    'tpcc_scaling_p':tpcc_scaling_p,
    'ycsb_scaling_no': ycsb_scaling,
    'ycsb_writes_no': ycsb_writes,
    'ycsb_skew_no_ro': ycsb_skew_no_ro,
    'ycsb_cross_dc_no_ro': ycsb_cross_dc_no_ro,
    'ycsb_network_delay_no_ro': ycsb_partitions_no_ro,
    'ycsb_partitions_no_ro': ycsb_partitions,
    'ycsb_dcs_no_ro': ycsb_dcs_no_ro,
    'tpcc_scaling_n_no_ro':tpcc_scaling_n_no_ro,
    'tpcc_scaling_p_no_ro':tpcc_scaling_p_no_ro,
    'tpcc_scaling': tpcc_scaling,
    'ycsb_version_array' : ycsb_version_array,
    'ycsb_thread' : ycsb_thread
}


# Default values for variable configurations
configs = {
    "NODE_CNT" : 5,
    "CENTER_CNT": 4,
    "THREAD_CNT": 24,
    "REPLICA_CNT": 0,
    "REPLICA_TYPE": "AP",
    "REM_THREAD_CNT": 1,
    "SEND_THREAD_CNT": 1,
    "CLIENT_NODE_CNT" : 1,
    "CLIENT_THREAD_CNT" : 4,
    "CLIENT_REM_THREAD_CNT" : 1,
    "CLIENT_SEND_THREAD_CNT" : 1,
    "MAX_TXN_PER_PART" : 10000,
    "WORKLOAD" : "YCSB",
    "CC_ALG" : "WAIT_DIE",
    "MPR" : 1.0,
    "TPORT_TYPE":"IPC",
    "TPORT_PORT":"18000",
    "PART_CNT": "NODE_CNT",
    "PART_PER_TXN": 2,
    "DC_PER_TXN": 2,
    "MAX_TXN_IN_FLIGHT": 10000,
    "NETWORK_DELAY": '10000UL',
    "NETWORK_DELAY_TEST": 'false',
    "DONE_TIMER": "1 * 20 * BILLION // ~1 minutes",
    "WARMUP_TIMER": "1 * 10 * BILLION // ~1 minutes",
    "SEQ_BATCH_TIMER": "5 * 1 * MILLION // ~5ms -- same as CALVIN paper",
    "BATCH_TIMER" : "0",
    "PROG_TIMER" : "10 * BILLION // in s",
    "NETWORK_TEST" : "false",
    "ABORT_PENALTY": "10 * 1000000UL   // in ns.",
    "ABORT_PENALTY_MAX": "5 * 100 * 1000000UL   // in ns.",
    "MSG_TIME_LIMIT": "0",
    # "MSG_SIZE_MAX": 16384,
    "MSG_SIZE_MAX": 4096,
    "TXN_WRITE_PERC":0.2,
    "PRIORITY":"PRIORITY_ACTIVE",
    "TWOPL_LITE":"false",
    "RDMA_SIT":"SIT_COROUTINE",
    "READ_OPTIMIZATION":"true",
    "HIS_CHAIN_NUM":50,
#YCSB
    "INIT_PARALLELISM" : 1,
    "TUP_WRITE_PERC":0.2,
    "ZIPF_THETA":0.3,
    "ACCESS_PERC":0.03,
    "DATA_PERC": 100,
    "REQ_PER_QUERY": 10,
    "SYNTH_TABLE_SIZE":"65536",
#TPCC
    "NUM_WH": 32,
    "PERC_PAYMENT":0.0,
    "DEBUG_DISTR":"false",
    "DEBUG_ALLOC":"false",
    "DEBUG_RACE":"false",
    "MODE":"NORMAL_MODE",
    "SHMEM_ENV":"false",
    "STRICT_PPT":0,
    "SET_AFFINITY":"true",
    "LOGGING":"false",
    "SERVER_GENERATE_QUERIES":"false",
    "SKEW_METHOD":"ZIPF",
    "ENVIRONMENT_EC2":"false",
    "YCSB_ABORT_MODE":"false",
    "LOAD_METHOD": "LOAD_MAX",
    "ISOLATION_LEVEL":"SERIALIZABLE"
}

