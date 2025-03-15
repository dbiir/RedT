# sh reset_group_delay.sh 5
python run_experiments.py -e -c vcloud ycsb_network_delay -l 20 0
sleep 10
# sh reset_group_delay.sh 10
python run_experiments.py -e -c vcloud ycsb_network_delay -l 20 10
sleep 10
# sh reset_group_delay.sh 15
python run_experiments.py -e -c vcloud ycsb_network_delay -l 20 20
sleep 10
# sh reset_group_delay.sh 20
python run_experiments.py -e -c vcloud ycsb_network_delay -l 20 30
sleep 10
# sh reset_group_delay.sh 25
python run_experiments.py -e -c vcloud ycsb_network_delay -l 20 40
sleep 10
# sh reset_group_delay.sh 30
python run_experiments.py -e -c vcloud ycsb_network_delay -l 20 50
