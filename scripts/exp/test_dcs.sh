
sh clean_group_delay.sh
sh set_group_delay.sh 0 6
# sh reset_group_delay.sh 20 80
# python run_experiments.py -e -c vcloud ycsb_scaling
cd /home/ibtest/origin/sigmod21-deneva/scripts
python run_experiments.py -e -c vcloud ycsb_dcs -l 20 80
sleep 10

# cd /home/ibtest/sigmod21-deneva/scripts # Multiple-2pc
# # python run_experiments.py -e -c vcloud ycsb_scaling
# python run_experiments.py -e -c vcloud ycsb_dcs -l 20 80
# sleep 10

# cd /home/ibtest/tapir/sigmod21-deneva/scripts # tapir
# python run_experiments.py -e -c vcloud ycsb_tapir_dcs -l 20 80
# sleep 10

# cd /home/ibtest/tcp/sigmod21-deneva/scripts # 2pc / early prepare
# python run_experiments.py -e -c vcloud ycsb_early_dcs -l 20 80
# sleep 10
# python run_experiments.py -e -c vcloud ycsb_dcs -l 20 80
# sleep 10

# sh clean_group_delay.sh