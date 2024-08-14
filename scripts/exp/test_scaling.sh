sh clean_group_delay.sh
sh set_group_delay.sh 0 4

# # RedT
cd ../../RedT/scripts 
# python run_experiments.py -e -c vcloud ycsb_scaling -l 20 0
# sleep 10
python run_experiments.py -e -c vcloud tpcc_scaling_n -l 20 0
sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_n -l 20 0
# sleep 10
python run_experiments.py -e -c vcloud tpcc_scaling_p -l 20 0
sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_p -l 20 0
# sleep 10

# Multiple-2pc
# cd ../../M2PC/scripts 
# python run_experiments.py -e -c vcloud ycsb_scaling
# python run_experiments.py -e -c vcloud tpcc_scaling_n
# sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_p
# sleep 10

# 优化后的RedT
# cd ../../RedT-RO/scripts 
# python run_experiments.py -e -c vcloud ycsb_scaling -l 20 0
# sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_n -l 20 0
# sleep 10
# python run_experiments.py -e -c vcloud tpcc_scaling_p -l 20 0
# sleep 10


# 2pc / early prepare
cd ../../2PC/scripts 
python run_experiments.py -e -c vcloud ycsb_scaling_early -l 20 0
sleep 10
python run_experiments.py -e -c vcloud tpcc_scaling_n_early -l 20 0
sleep 10
python run_experiments.py -e -c vcloud tpcc_scaling_p_early -l 20 0
sleep 10
python run_experiments.py -e -c vcloud ycsb_scaling -l 20 0
sleep 10
python run_experiments.py -e -c vcloud tpcc_scaling_n -l 20 0
sleep 10
python run_experiments.py -e -c vcloud tpcc_scaling_p -l 20 0
sleep 10


# TAPIR
cd ../../TAPIR/scripts 
python run_experiments.py -e -c vcloud ycsb_scaling -l 20 0
sleep 10
python run_experiments.py -e -c vcloud tpcc_scaling_n -l 20 0
sleep 10
python run_experiments.py -e -c vcloud tpcc_scaling_p -l 20 0
sleep 10