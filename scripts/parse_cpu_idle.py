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

wa = sum(summary['worker_idle_time'])
wb = sum(summary['worker_activate_txn_time'])
wc = sum(summary['worker_deactivate_txn_time'])
wd = sum(summary['worker_release_msg_time'])
we = sum(summary['worker_process_time'])
wf = sum(summary['worker_yield_time'])
wg = sum(summary['worker_msg_time'])
wh = sum(summary['worker_waitcomp_time'])
# th = sum(summary['g_thread_cnt'])
ti = sum(summary['total_runtime'])
#trans_total_run_time trans_process_time trans_process_time% trans_2pc_time trans_2pc_time% trans_prepare_time trans_prepare_time% trans_validate_time trans_validate_time% trans_finish_time trans_finish_time% trans_commit_time trans_commit_time% trans_abort_time trans_abort_time% trans_block_time trans_block_time% txn_index_time txn_index_time% txn_manager_time txn_manager_time% lat_l_loc_cc_time lat_l_loc_cc_time%
print (wa*100)/(wa+wb+wc+wd+we),(wf*100)/(wa+wb+wc+wd+we),(wg*100)/(wa+wb+wc+wd+we),(wh*100)/(wa+wb+wc+wd+we),(we*100)/(wa+wb+wc+wd+we)

