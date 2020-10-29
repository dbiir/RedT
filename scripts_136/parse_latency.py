import re, sys

summary = {}


def get_summary(sfile):
    with open(sfile, 'r') as f:
        for line in f:
            if 'summary' in line:
                results = re.split(',', line.rstrip('\n')[10:])
                for r in results:
                    (name, val) = re.split('=', r)
                    val = float(val)
                    if name not in summary.keys():
                        summary[name] = [val]
                    else:
                        summary[name].append(val)


for arg in sys.argv[1:]:
    get_summary(arg)
names = summary.keys()

a = sum(summary['trans_total_run_time'])
b = sum(summary['trans_process_time'])
c = sum(summary['trans_2pc_time'])
d = sum(summary['trans_prepare_time'])
e = sum(summary['trans_validate_time'])
f = sum(summary['trans_finish_time'])
g = sum(summary['trans_commit_time'])
h = sum(summary['trans_abort_time'])
i = sum(summary['lat_l_loc_cc_block_time'])
j = sum(summary['lat_l_rem_cc_block_time'])
k = sum(summary['txn_index_time'])
l = sum(summary['txn_manager_time'])
m = sum(summary['lat_l_loc_cc_time'])
#trans_total_run_time trans_process_time trans_process_time% trans_2pc_time trans_2pc_time% trans_prepare_time trans_prepare_time% trans_validate_time trans_validate_time% trans_finish_time trans_finish_time% trans_commit_time trans_commit_time% trans_abort_time trans_abort_time% trans_block_time trans_block_time% txn_index_time txn_index_time% txn_manager_time txn_manager_time% lat_l_loc_cc_time lat_l_loc_cc_time%
print a,b,b/a,c,c/a,d,d/a,e,e/a,f,f/a,g,g/a,h,h/a,i+j,(i+j)/a,k,k/a,l,l/a,m,m/a

