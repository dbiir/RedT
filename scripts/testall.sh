# python run_experiments.py -e -c vcloud ycsb_thread
# sleep 30
# python run_experiments.py -e -c vcloud ycsb_skew
# sleep 30
python run_experiments.py -e -c vcloud ycsb_cross_dc
sleep 30
python run_experiments.py -e -c vcloud ycsb_writes
sleep 30
python run_experiments.py -e -c vcloud ycsb_network_delay
sleep 30
