sh clean_group_delay.sh
sh set_group_delay.sh 0 4

python run_experiments.py -e -c vcloud ycsb_skew -l 20 0
sleep 10
python run_experiments.py -e -c vcloud tpcc_scaling_n -l 20 0
sleep 10
python run_experiments.py -e -c vcloud tpcc_scaling_p -l 20 0
sleep 10