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

a, b, c = 0, 0, 0
if 'tput' in summary:
    a = sum(summary['tput'])
if 'total_txn_abort_cnt' in summary and 'total_txn_commit_cnt' in summary:
    b = summary['total_txn_abort_cnt'][0] / (summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0])
if 'remote_txn_commit_cnt' in summary and 'remote_txn_abort_cnt' in summary and 'total_txn_commit_cnt' in summary and 'total_txn_abort_cnt' in summary:
    c = (summary['remote_txn_commit_cnt'][0] + summary['remote_txn_abort_cnt'][0]) / (
            summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0])

print a, b, c
