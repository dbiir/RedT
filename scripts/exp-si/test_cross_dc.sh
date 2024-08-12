# RedT
# 优化后的RedT
# cd ../../RedTR/scripts 
# python run_experiments.py -e -c vcloud ycsb_cross_dc_no_ro -l 20 0
# python run_experiments.py -e -c vcloud ycsb_cross_dc -l 20 0
# sleep 10

# # 2pc / early prepare
# cd ../../2PC/scripts 
# python run_experiments.py -e -c vcloud ycsb_early_cross_dc -l 20 0
# sleep 10
# python run_experiments.py -e -c vcloud ycsb_cross_dc -l 20 0
# sleep 10

# TAPIR
cd ../../TAPIR/scripts 
python run_experiments.py -e -c vcloud ycsb_tapir_cross_dc -l 20 0
sleep 10
