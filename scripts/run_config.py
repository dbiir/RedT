#!/usr/bin/python

#Configuration file for run_experiments.py

istc_uname = 'rhardin'

# ISTC Machines ranked by clock skew
istc_machines=[

#GOOD
"istc1", 
"istc3", 
#"istc4", 
"istc6",
"istc8",
#OK
"istc7",
"istc9",
"istc10", 
"istc13", 
#BAD
"istc11",
"istc12", 
"istc2",
"istc5",
]

vcloud_uname = 'centos'
#identity = "/usr0/home/dvanaken/.ssh/id_rsa_vcloud"
vcloud_machines = [
"202",
"203",
"204",
"205"
]


