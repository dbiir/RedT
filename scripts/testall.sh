
# sh exp/test_inter.sh
# sh exp/test_cross_dc.sh
sh exp/test_write.sh
sh exp/test_skew.sh

sh exp/test_scaling.sh
# sh exp/test_part.sh
# sh exp/test_dcs.sh


# sh exp-si/test_inter.sh
# sh exp-si/test_cross_dc.sh
# sh exp-si/test_write.sh
# sh exp-si/test_skew.sh

# sh exp-si/test_scaling.sh
# sh exp-si/test_part.sh
# sh exp-si/test_dcs.sh

# python run_experiments.py -e -c vcloud ycsb_skew -l 20 0