
#python run_experiments.py -e -c vcloud ycsb_skew
#sleep 30
#python run_experiments.py -e -c vcloud ycsb_writes
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

#cd ../draw/
#./tdsql3-homepage.sh

cd /root/mogg-deneva/scripts
./mogg-test.sh

