
sh reset_group_delay.sh 0
python run_experiments.py -e -c vcloud ycsb_network_delay
sleep 10
sh reset_group_delay.sh 2
python run_experiments.py -e -c vcloud ycsb_network_delay
sleep 10
sh reset_group_delay.sh 4
python run_experiments.py -e -c vcloud ycsb_network_delay
sleep 10
sh reset_group_delay.sh 6
python run_experiments.py -e -c vcloud ycsb_network_delay
sleep 10
sh reset_group_delay.sh 8
python run_experiments.py -e -c vcloud ycsb_network_delay
sleep 10
sh reset_group_delay.sh 10
python run_experiments.py -e -c vcloud ycsb_network_delay
sleep 10
sh reset_group_delay.sh 12
python run_experiments.py -e -c vcloud ycsb_network_delay
sleep 10
sh reset_group_delay.sh 14
python run_experiments.py -e -c vcloud ycsb_network_delay
sleep 10
sh reset_group_delay.sh 16
python run_experiments.py -e -c vcloud ycsb_network_delay
sleep 10
sh reset_group_delay.sh 18
python run_experiments.py -e -c vcloud ycsb_network_delay
sleep 10
sh reset_group_delay.sh 20
python run_experiments.py -e -c vcloud ycsb_network_delay
sleep 10
