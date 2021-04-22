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
n = sum(summary['trans_init_time'])

o = sum(summary['trans_get_access_time'])
p = sum(summary['trans_store_access_time'])
q = sum(summary['trans_get_row_time'])

r = sum(summary['trans_cur_row_copy_time'])
s = sum(summary['trans_cur_row_init_time'])

t = sum(summary['txn_process_time'])
u = sum(summary['trans_benchmark_compute_time'])

v = sum(summary['trans_process_network'])
w = sum(summary['trans_validation_network'])
x = sum(summary['trans_commit_network'])
y = sum(summary['trans_abort_network'])

#trans_total_run_time trans_process_time trans_process_time% trans_2pc_time trans_2pc_time% trans_prepare_time trans_prepare_time% trans_validate_time trans_validate_time% trans_finish_time trans_finish_time% trans_commit_time trans_commit_time% trans_abort_time trans_abort_time% trans_block_time trans_block_time% txn_index_time txn_index_time% txn_manager_time txn_manager_time% lat_l_loc_cc_time lat_l_loc_cc_time%
print "%.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f" % (a,b,(b*100/a),c,(c*100/a),d,(d*100/a),e,(e*100/a),f,(f*100/a),g,(g*100/a),h,(h*100/a),(i+j),((i+j)*100/a),k,(k*100/a),l,(l*100/a),m,(m*100/a),n,(n*100/a),o,(o*100/a),p,(p*100/a),q,(q*100/a),r,(r*100/a),s,(s*100/a),t,(t*100/a),u,(u*100/a),v,(v*100/a),w,(w*100/a),x,(x*100/a),y,(y*100/a))