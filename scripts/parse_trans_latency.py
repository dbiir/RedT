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
k = sum(summary['trans_init_time'])
print b-i-j,i+j,d,g,h
