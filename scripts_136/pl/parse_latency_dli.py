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
b = sum(summary['dli_init_time'])
c = sum(summary['dli_lock_time'])
d = sum(summary['dli_check_conflict_time'])
e = sum(summary['dli_final_validate'])

# print b,b/a,c,c/a,d,d/a,e,e/a

print "%.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f" % (b,b*100/a,c,c*100/a,d,d*100/a,e,e*100/a)
