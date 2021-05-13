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

local_txn_abort_cnt = sum(summary['local_txn_abort_cnt'])
local_txn_commit_cnt = sum(summary['local_txn_commit_cnt'])
if local_txn_abort_cnt + local_txn_abort_cnt == 0:
    f = 0
else:
    f = local_txn_abort_cnt / (local_txn_abort_cnt + local_txn_commit_cnt)

remote_txn_abort_cnt = sum(summary['remote_txn_abort_cnt'])
remote_txn_commit_cnt = sum(summary['remote_txn_commit_cnt'])
if remote_txn_abort_cnt + remote_txn_abort_cnt == 0:
    g = 0
else:
    g = remote_txn_abort_cnt / (remote_txn_abort_cnt + remote_txn_commit_cnt)

t = a+b+c+d+e
a = a/t
b = b/t
c = c/t
d = d/t
e = e/t

print a, b, c, d, e, f, g
