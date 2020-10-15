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

vcloud_uname = 'deneva'
#identity = "/usr0/data1/dvanaken/.ssh/id_rsa_vcloud"
vcloud_machines = [
"9.39.242.189",
"9.39.242.175",
"9.39.242.136",
"9.39.242.133",
#"9.39.242.175",
#"9.39.242.189",
]
