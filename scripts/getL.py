from matplotlib.pyplot import *
from numpy import *
import sys

from pylab import *
#mpl.rcParams['font.sans-serif'] = ['KaiTi']

f = open(sys.argv[1])
x = f.readline().split()
z = f.readline().split()
y = []
for i in range(int(sys.argv[2])):
    y.append([float(x) for x in f.readline().split()])

xindex = arange(len(x)+1)


def merge(a, b):
    c = []
    for i in range(len(a)):
        c.append(a[i]+b[i])
    print(c)
    return c


patterns = ['|', 'x', '+', '\\', '*', 'o', 'O', '.'] * 100

bars = []
tmp = [0] * len(x)
for i in range(len(y)):
    if i == 0:
        bars.append(bar(x, y[i]))
        tmp = merge(tmp, y[i])
    else:
        bars.append(bar(x, y[i], bottom=tmp[:], hatch=patterns[i]))
        tmp = merge(tmp, y[i])

ylabel('latency among each phase(ms)')
xticks(xindex, x, rotation=20)

legend(z, loc="upper right")

savefig('2.svg')


