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

a = sum(summary['abort_time'])/len(summary['abort_time']) if 'abort_time' in summary else 0
b = sum(summary['txn_manager_time'])/len(summary['txn_manager_time']) if 'txn_manager_time' in summary else 0
c = sum(summary['txn_validate_time'])/len(summary['txn_validate_time']) if 'txn_validate_time' in summary else 0
d = sum(summary['txn_cleanup_time'])/len(summary['txn_cleanup_time']) if 'txn_cleanup_time' in summary else 0
e = sum(summary['txn_total_process_time'])/len(summary['txn_total_process_time']) if 'txn_total_process_time' in summary else 0

print a, b, c, d, e
