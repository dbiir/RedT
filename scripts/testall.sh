#python run_experiments.py -e -c vcloud tpcc_stress1
#sleep 10


python run_experiments.py -e -c vcloud ycsb_scaling
sleep 10
python run_experiments.py -e -c vcloud ycsb_scaling1
sleep 10
python run_experiments.py -e -c vcloud ycsb_scaling2
sleep 10
python run_experiments.py -e -c vcloud tpcc_scaling
sleep 10
# python run_experiments.py -e -c vcloud ycsb_skew
# sleep 10
#cd ../draw
#./deneva-homepage.sh
#exit 0
#python run_experiments.py -e -c vcloud tpcc_stress1
#sleep 30
#cd ../draw
#./deneva-homepage.sh
#exit 0

#python run_experiments.py -e -c vcloud ycsb_stress5
#sleep 30

#python run_experiments.py -e -c vcloud ycsb_skew
#sleep 30
#python run_experiments.py -e -c vcloud ycsb_scaling
#sleep 30
#python run_experiments.py -e -c vcloud ycsb_scaling1
#sleep 30
#python run_experiments.py -e -c vcloud tpcc_scaling1
#sleep 30
#python run_experiments.py -e -c vcloud tpcc_scaling2
#sleep 30
#python run_experiments.py -e -c vcloud tpcc_scaling3
#sleep 30
#python run_experiments.py -e -c vcloud ycsb_writes
#sleep 30

# neworder
#python run_experiments.py -e -c vcloud tpcc_stress1
#sleep 10

# payment
#python run_experiments.py -e -c vcloud tpcc_stress2
#sleep 10

# skew
# 0.0
#python run_experiments.py -e -c vcloud ycsb_stress1
#sleep 10
# 0.25
#python run_experiments.py -e -c vcloud ycsb_stress2
#sleep 10
# 0.55
#python run_experiments.py -e -c vcloud ycsb_stress3
#sleep 10
# 0.65
#python run_experiments.py -e -c vcloud ycsb_stress4
#sleep 10
# 0.75
#python run_experiments.py -e -c vcloud ycsb_stress5
#sleep 10
# 0.9
#python run_experiments.py -e -c vcloud ycsb_stress6
#sleep 10

# update ratio
# 0.0
#python run_experiments.py -e -c vcloud ycsb_stress7
#sleep 10
# 0.2
#python run_experiments.py -e -c vcloud ycsb_stress8
#sleep 10
# 0.4
#python run_experiments.py -e -c vcloud ycsb_stress9
#sleep 10
## 0.6
#python run_experiments.py -e -c vcloud ycsb_stress10
#sleep 10
# 0.8
#python run_experiments.py -e -c vcloud ycsb_stress11
#sleep 10
# 1.0
#python run_experiments.py -e -c vcloud ycsb_stress12
#sleep 10


#cd ../draw
#./deneva-homepage.sh
