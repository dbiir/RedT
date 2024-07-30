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

# a, b, c = 0, 0, 0
# if 'tput' in summary:
#     a = sum(summary['tput'])
# if 'total_txn_abort_cnt' in summary and 'total_txn_commit_cnt' in summary and summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0] != 0:
#     b = summary['total_txn_abort_cnt'][0] / (summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0])
# if 'remote_txn_commit_cnt' in summary and 'remote_txn_abort_cnt' in summary and 'total_txn_commit_cnt' in summary and 'total_txn_abort_cnt' in summary and summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0] != 0:
#     c = (summary['remote_txn_commit_cnt'][0] + summary['remote_txn_abort_cnt'][0]) / (
#             summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0])

# print a, b, c

a, b, c, d, e, f, g, h = 0, 0, 0, 0, 0, 0, 0, 0,
m, n, p, q, i =  0, 0, 0, 0, 0
if 'tput' in summary:
    a = sum(summary['tput'])
if 'total_txn_abort_cnt' in summary and 'total_txn_commit_cnt' in summary and summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0] != 0:
    b = sum(summary['total_txn_abort_cnt']) / (sum(summary['total_txn_commit_cnt']) + sum(summary['total_txn_abort_cnt']))
    #b = summary['total_txn_abort_cnt'][0] / (summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0])
if 'lock_retry_cnt' in summary:
    c = sum(summary['lock_retry_cnt'])
if 'read_retry_cnt' in summary:
    d = sum(summary['read_retry_cnt'])
if 'avg_trans_total_run_time' in summary:
    e = sum(summary['avg_trans_total_run_time']) 
if 'remote_txn_cnt' in summary:
    g = sum(summary['remote_txn_cnt']) 
if 'txn_cnt' in summary:
    h = sum(summary['txn_cnt']) 
if 'worker_oneside_cnt' in summary and 'total_txn_commit_cnt' in summary and summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0] != 0:
    f = sum(summary['worker_oneside_cnt']) / (sum(summary['total_txn_commit_cnt']) + sum(summary['total_txn_abort_cnt']))
if 'max_num_msgs_rw_prep' in summary:
    m = max(summary['max_num_msgs_rw_prep']) 
if 'max_num_msgs_commit' in summary:
    n = max(summary['max_num_msgs_commit']) 
if 'avg_num_msgs_rw_prep' in summary:
    p = sum(summary['avg_num_msgs_rw_prep']) 
if 'avg_num_msgs_commit' in summary:
    q = sum(summary['avg_num_msgs_commit']) 
if 'avg_trans_commit_total_run_time' in summary:
    i = sum(summary['avg_trans_commit_total_run_time']) 

# print a, b, e, f, g / h
print a, b, e, i, m, n, p, q  