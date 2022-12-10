sh clean_group_delay.sh
sh set_group_delay.sh 0 3
# cd /home/ibtest/sigmod21-deneva/scripts # Multiple-2pc
# # python run_experiments.py -e -c vcloud ycsb_scaling
# python run_experiments.py -e -c vcloud tpcc_scaling_n
# sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_p
# sleep 10

cd /home/ibtest/origin/sigmod21-deneva/scripts # RedT
python run_experiments.py -e -c vcloud ycsb_scaling -l 20 0
sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_n -l 20 0
# sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_p -l 20 0
# sleep 10

# cd /home/ibtest/tapir/sigmod21-deneva/scripts # tapir
# python run_experiments.py -e -c vcloud ycsb_scaling -l 20 0
# sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_n -l 20 0
# sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_p -l 20 0
# sleep 10

# cd /home/ibtest/tcp/sigmod21-deneva/scripts # 2pc / early prepare
# python run_experiments.py -e -c vcloud ycsb_scaling_early -l 20 0
# sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_n_early -l 20 0
# sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_p_early -l 20 0
# sleep 10
# python run_experiments.py -e -c vcloud ycsb_scaling -l 20 0
# sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_n -l 20 0
# sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_p -l 20 0
# sleep 10