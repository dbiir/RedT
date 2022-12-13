# sh reset_group_delay.sh 20

cd /home/ibtest/sigmod21-deneva/scripts # Multiple-2pc
python run_experiments.py -e -c vcloud ycsb_cross_dc --dc 4 -l 20 0
sleep 10
cd /home/ibtest/tapir/sigmod21-deneva/scripts # tapir
python run_experiments.py -e -c vcloud ycsb_tapir_cross_dc --dc 4 -l 20 0
sleep 10
cd /home/ibtest/origin/sigmod21-deneva/scripts # RedT
python run_experiments.py -e -c vcloud ycsb_cross_dc --dc 4 -l 20 0
sleep 10
cd /home/ibtest/tcp/sigmod21-deneva/scripts # 2pc / early prepare
# python run_experiments.py -e -c vcloud ycsb_early_cross_dc --dc 4 -l 20 0
sleep 10
python run_experiments.py -e -c vcloud ycsb_cross_dc --dc 4 -l 20 0
# sleep 10