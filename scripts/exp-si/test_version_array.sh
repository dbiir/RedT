sh clean_group_delay.sh
sh set_group_delay.sh 0 4

# # RedT
cd ../../RedTR/scripts 
python run_experiments.py -e -c vcloud ycsb_version_array -l 20 0
sleep 10
