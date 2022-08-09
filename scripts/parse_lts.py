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

a = sum(summary['lat_short_network_time'])/len(summary[' lat_short_network_time']) if ' lat_short_network_time' in summary else 0
b = sum(summary['lat_short_msg_queue_time'])/len(summary['lat_short_msg_queue_time']) if 'lat_short_msg_queue_time' in summary else 0
c = sum(summary['lat_short_work_queue_time'])/len(summary['lat_short_work_queue_time']) if 'lat_short_work_queue_time' in summary else 0
d = sum(summary['lat_short_cc_time'])/len(summary['lat_short_cc_time']) if 'lat_short_cc_time' in summary else 0
e = sum(summary['lat_short_cc_block_time'])/len(summary['lat_short_cc_block_time']) if 'lat_short_cc_block_time' in summary else 0
f = sum(summary['lat_short_process_time'])/len(summary['lat_short_process_time']) if 'lat_short_process_time' in summary else 0

print a, b, c, d, e, f

