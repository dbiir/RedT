
sh clean_group_delay.sh
sh set_group_delay.sh 0 6
sh reset_group_delay.sh 20 80
# python run_experiments.py -e -c vcloud ycsb_scaling
python run_experiments.py -e -c vcloud ycsb_dcs
sleep 10

cd /home/ibtest/sigmod21-deneva/scripts # Multiple-2pc
# python run_experiments.py -e -c vcloud ycsb_scaling
python run_experiments.py -e -c vcloud ycsb_dcs
sleep 10

cd /home/ibtest/tapir/sigmod21-deneva/scripts # tapir
python run_experiments.py -e -c vcloud ycsb_dcs_tapir
sleep 10

cd /home/ibtest/tcp/sigmod21-deneva/scripts # 2pc / early prepare
python run_experiments.py -e -c vcloud ycsb_dcs_early #
sleep 10
python run_experiments.py -e -c vcloud ycsb_dcs
sleep 10

sh clean_group_delay.sh