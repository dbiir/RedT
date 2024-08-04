
sh clean_group_delay.sh
sh set_group_delay.sh 0 8
# sh set_scal_group_delay.sh 0 0
# sh reset_scal_group_delay.sh 20 0
# sh reset_group_delay.sh 20 80

# # RedT
# cd ../../RedT/scripts 
# python run_experiments.py -e -c vcloud ycsb_dcs -l 20 80
# sleep 10

# Multiple-2pc
# cd ../../M2PC/scripts  
# python run_experiments.py -e -c vcloud ycsb_dcs -l 20 80
# sleep 10

# 优化后的RedT
# cd ../../RedT-RO/scripts 
cd ../../RedTR/scripts 
python run_experiments.py -e -c vcloud ycsb_dcs -l 20 80
sleep 10

# # tapir
# cd ../../TAPIR/scripts 
# python run_experiments.py -e -c vcloud ycsb_tapir_dcs -l 20 80
# sleep 10

# 2pc / early prepare
# cd ../../2PC/scripts 
# python run_experiments.py -e -c vcloud ycsb_early_dcs -l 20 80
# sleep 10
# python run_experiments.py -e -c vcloud ycsb_dcs -l 20 80
# sleep 10

sh clean_group_delay.sh