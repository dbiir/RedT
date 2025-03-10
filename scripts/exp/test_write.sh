sh clean_group_delay.sh
sh set_group_delay.sh 0 4
# # RedT
# cd /../../../RedT/scripts 
# python run_experiments.py -e -c vcloud ycsb_writes -l 20 0 
# sleep 10

# Multiple-2pc
# cd /../../../M2PC/scripts 
# python run_experiments.py -e -c vcloud ycsb_writes -l 20 0 
# sleep 10

# 优化后的RedT
# cd /../../../RedT-RO/scripts 
# python run_experiments.py -e -c vcloud ycsb_writes -l 20 0 
# sleep 10

# 2pc / early prepare
# cd /../../../2PC/scripts 
# python run_experiments.py -e -c vcloud ycsb_early_writes -l 20 0 
# sleep 10
# python run_experiments.py -e -c vcloud ycsb_writes -l 20 0 #
# sleep 10

# # TAPIR
# cd /../../../TAPIR/scripts 
# python run_experiments.py -e -c vcloud ycsb_tapir_writes -l 20 0 
# sleep 10

# MDCC
# cd /../../../MDCC/scripts 
# python run_experiments.py -e -c vcloud ycsb_tapir_writes -l 20 0 
# sleep 10

#ncc
cd ../../NCC/scripts
python run_experiments.py -e -c vcloud ycsb_early_writes -l 20 0

# #calvin
# cd ../../Calvin/scripts
# python run_experiments.py -e -c vcloud ycsb_writes -l 20 0