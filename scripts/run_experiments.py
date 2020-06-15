import os,sys,datetime,re
import shlex
import subprocess
from experiments import *
from helper import *
from run_config import *
import glob

now = datetime.datetime.now()
strnow=now.strftime("%Y%m%d-%H%M%S")

os.chdir('..')

PATH=os.getcwd()
result_dir = PATH + "/results/" + strnow + '/'

cfgs = configs

execute = True
remote = False
cluster = None
skip = False
exps=[]
arg_cluster = False
merge_mode = False

if len(sys.argv) < 2:
     sys.exit("Usage: %s [-exec/-e/-noexec/-ne] [-c cluster] experiments\n \
            -exec/-e: compile and execute locally (default)\n \
            -noexec/-ne: compile first target only \
            -c: run remote on cluster; possible values: istc, vcloud\n \
            " % sys.argv[0])

for arg in sys.argv[1:]:
    if arg == "-help" or arg == "-h":
        sys.exit("Usage: %s [-exec/-e/-noexec/-ne] [-skip] [-c cluster] experiments\n \
                -exec/-e: compile and execute locally (default)\n \
                -noexec/-ne: compile first target only \
                -skip: skip any experiments already in results folder\n \
                -c: run remote on cluster; possible values: istc, vcloud\n \
                " % sys.argv[0])
    if arg == "-exec" or arg == "-e":
        execute = True
    elif arg == "-noexec" or arg == "-ne":
        execute = False
    elif arg == "-skip":
        skip = True
    elif arg == "-c":
        remote = True
        arg_cluster = True
    elif arg == '-m':
        merge_mode = True
    elif arg_cluster:
        cluster = arg
        arg_cluster = False
    else:
        exps.append(arg)

for exp in exps:
    fmt,experiments = experiment_map[exp]()

    for e in experiments:
        cfgs = get_cfgs(fmt,e)
        if remote:
            cfgs["TPORT_TYPE"],cfgs["TPORT_TYPE_IPC"],cfgs["TPORT_PORT"]="tcp","false",7000

        output_f = get_outfile_name(cfgs, fmt)

        # Check whether experiment has been already been run in this batch
        if skip:
            if len(glob.glob('{}*{}*.out'.format(result_dir,output_f))) > 0:
                print "Experiment exists in results folder... skipping"
                continue

        output_dir = output_f + "/"
        output_f = output_f + strnow 

        f = open("config.h",'r')
        lines = f.readlines()
        f.close()
        with open("config.h",'w') as f_cfg:
            for line in lines:
                found_cfg = False
                for c in cfgs:
                    found_cfg = re.search("#define "+c + "\t",line) or re.search("#define "+c + " ",line)
                    if found_cfg:
                        f_cfg.write("#define " + c + " " + str(cfgs[c]) + "\n")
                        break
                if not found_cfg: f_cfg.write(line)

        cmd = "make clean; make -j16"
        os.system(cmd)
        if not execute:
            exit()

        if execute:
            cmd = "mkdir -p {}".format(result_dir)
            os.system(cmd)
            cmd = "cp config.h {}{}.cfg".format(result_dir,output_f)
            os.system(cmd)

            if remote:
                if cluster == 'istc':
                    machines_ = istc_machines
                    uname = istc_uname
                    cfg_fname = "istc_ifconfig.txt"
                elif cluster == 'vcloud':
                    machines_ = vcloud_machines
                    uname = vcloud_uname
                    cfg_fname = "vcloud_ifconfig.txt"
                else:
                    assert(False)
                if merge_mode:
                    machines = sorted(machines_[:(cfgs["NODE_CNT"])])
                    for m in machines[:]:
                        machines.append(m)
                else:
                    machines = sorted(machines_[:(cfgs["NODE_CNT"]*2)])  
                with open("ifconfig.txt",'w') as f_ifcfg:
                    for m in machines:
                        f_ifcfg.write("10.77.110." + m + "\n")

                if cfgs["WORKLOAD"] == "TPCC":
                    files = ["rundb","runcl","ifconfig.txt","./benchmarks/TPCC_short_schema.txt","./benchmarks/TPCC_full_schema.txt"]
                elif cfgs["WORKLOAD"] == "YCSB":
                    files = ["rundb","runcl","ifconfig.txt","benchmarks/YCSB_schema.txt"]
                for m,f in itertools.product(machines,files):
                    if cluster == 'istc':
                        cmd = 'scp {}/{} {}.csail.mit.edu:/home/{}/'.format(PATH,f,m,uname)
                    elif cluster == 'vcloud':
                        cmd = 'scp {}/{} centos@10.77.70.{}:/home/{}'.format(PATH,f,m,uname)
                    os.system(cmd)

                print("Deploying: {}".format(output_f))
                if cluster == 'istc':
                    cmd = './scripts/deploy.sh \'{}\' /home/{}/ {}'.format(' '.join(machines),uname,cfgs["NODE_CNT"])
                elif cluster == 'vcloud':
                    if merge_mode:
                        cmd = './scripts/vcloud_merge_deploy.sh \'{}\' /home/{}/ {}'.format(' '.join(machines),uname,cfgs["NODE_CNT"])
                    else:
                        cmd = './scripts/vcloud_deploy.sh \'{}\' /home/{}/ {}'.format(' '.join(machines),uname,cfgs["NODE_CNT"])
                os.system(cmd)

                for m,n in zip(machines,range(len(machines))):
                    if cluster == 'istc':
                        cmd = 'scp {}.csail.mit.edu:/home/{}/results.out {}{}_{}.out'.format(m,uname,result_dir,n,output_f)
                        print(cmd)
                        os.system(cmd)
                    elif cluster == 'vcloud':
                        cmd = 'scp centos@10.77.70.{}:/home/{}/dbresults.out results/{}/{}_{}.out'.format(m,uname,strnow,n,output_f)
                        os.system(cmd)
            else:
                nnodes = cfgs["NODE_CNT"]
                nclnodes = cfgs["CLIENT_NODE_CNT"]
                pids = []
                print("Deploying: {}".format(output_f))
                for n in range(nnodes+nclnodes):
                    if n < nnodes:
                        cmd = "./rundb -nid{}".format(n)
                    else:
                        cmd = "./runcl -nid{}".format(n)
                    print(cmd)
                    cmd = shlex.split(cmd)
                    ofile_n = "{}{}_{}.out".format(result_dir,n,output_f)
                    ofile = open(ofile_n,'w')
                    p = subprocess.Popen(cmd,stdout=ofile,stderr=ofile)
                    pids.insert(0,p)
                for n in range(nnodes + nclnodes):
                    pids[n].wait()

    al = []
    for e in experiments:
        al.append(str(e[2]))
    al=sorted(list(set(al)))

    sk = []
    for e in experiments:
        sk.append(str(e[-2]))
    sk=sorted(list(set(sk)))

    wr = []
    for e in experiments:
        wr.append(str(e[-4]))
    wr=sorted(list(set(wr)))

    cn = []
    for e in experiments:
        cn.append(str(e[1]))
    cn=sorted(list(set(cn)))

    cmd=''
    os.chdir('./scripts')
    if exp == 'ycsb_skew':
        cmd='./result.sh -a ycsb_skew -n {} -c {} -s {} -t {}'.format(cn[0], ','.join(al), ','.join(sk), strnow)
    elif exp == 'ycsb_writes':
        cmd='./result.sh -a ycsb_writes -n {} -c {} --wr {} -t {}'.format(cn[0], ','.join(al), ','.join(wr), strnow)
    elif 'ycsb_scaling' in exp:
        cmd='./result.sh -a ycsb_scaling -n {} -c {} -t {}'.format(','.join(cn), ','.join(al), strnow)
    elif 'tpcc_scaling' in exp:
        cmd='./result.sh -a tpcc_scaling2 -n {} -c {} -t {}'.format(','.join(cn), ','.join(al), strnow)
    os.system(cmd)

    cmd=''
    os.chdir('./draw')
    if exp == 'ycsb_skew':
        cmd='./deneva-plot.sh -a ycsb_skew -c {} -t {}'.format(','.join(al), strnow)
    elif exp == 'ycsb_writes':
        cmd='./deneva-plot.sh -a ycsb_writes -c {} -t {}'.format(','.join(al), strnow)
    elif 'ycsb_scaling' in exp:
        cmd='./deneva-plot.sh -a ycsb_scaling -c {} -t {}'.format(','.join(al), strnow)
    elif 'tpcc_scaling' in exp:
        cmd='./deneva-plot.sh -a tpcc_scaling2 -c {} -t {}'.format(','.join(al), strnow)
    os.system(cmd)
