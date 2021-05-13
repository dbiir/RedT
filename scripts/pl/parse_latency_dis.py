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

b = sum(summary['trans_local_process'])
c = sum(summary['trans_remote_process'])
d = sum(summary['trans_work_local_wait'])
e = sum(summary['trans_work_remote_wait'])
f = sum(summary['trans_msg_local_wait'])
g = sum(summary['trans_msg_remote_wait'])
h = sum(summary['trans_network_wait'])

print "%.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f" % (a, b,b*100/a,c,c*100/a,d,d*100/a,e,e*100/a,f,f*100/a,g,g*100/a,h,h/a,a-b-c-d-e-f-g,(a-b-c-d-e-f-g)*100/a)
