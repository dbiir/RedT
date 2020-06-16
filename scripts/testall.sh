
python run_experiments.py -m -e -c vcloud ycsb_writes
sleep 30
python run_experiments.py -m -e -c vcloud ycsb_skew
sleep 30
python run_experiments.py -m -e -c vcloud ycsb_scaling
sleep 30
python run_experiments.py -m -e -c vcloud tpcc_scaling2
sleep 30

cd ../draw
./tdsql3-homepage.sh
