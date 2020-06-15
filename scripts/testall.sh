
#python run_experiments.py -e -c vcloud ycsb_writes
python run_experiments.py -e -c vcloud ycsb_skew
sleep 30
python run_experiments.py -e -c vcloud ycsb_scaling
sleep 30
python run_experiments.py -e -c vcloud tpcc_scaling2
