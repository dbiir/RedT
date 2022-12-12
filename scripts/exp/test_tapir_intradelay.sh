# sh reset_group_delay.sh 5
python run_experiments.py -e -c vcloud ycsb_tapir_network_delay2 -l 0 0
sleep 10
# sh reset_group_delay.sh 10
python run_experiments.py -e -c vcloud ycsb_tapir_network_delay2 -l 2 0
sleep 10
# sh reset_group_delay.sh 15
python run_experiments.py -e -c vcloud ycsb_tapir_network_delay2 -l 4 0
sleep 10
# sh reset_group_delay.sh 20
python run_experiments.py -e -c vcloud ycsb_tapir_network_delay2 -l 6 0
sleep 10
# sh reset_group_delay.sh 25
python run_experiments.py -e -c vcloud ycsb_tapir_network_delay2 -l 8 0
sleep 10
# sh reset_group_delay.sh 30
python run_experiments.py -e -c vcloud ycsb_tapir_network_delay2 -l 10 0
sleep 10
# sh reset_group_delay.sh 35
python run_experiments.py -e -c vcloud ycsb_tapir_network_delay2 -l 12 0
sleep 10
# sh reset_group_delay.sh 40
python run_experiments.py -e -c vcloud ycsb_tapir_network_delay2 -l 14 0
sleep 10
# sh reset_group_delay.sh 45
python run_experiments.py -e -c vcloud ycsb_tapir_network_delay2 -l 16 0
sleep 10
# sh reset_group_delay.sh 50
python run_experiments.py -e -c vcloud ycsb_tapir_network_delay2 -l 18 0
sleep 10
python run_experiments.py -e -c vcloud ycsb_tapir_network_delay2 -l 20 0
