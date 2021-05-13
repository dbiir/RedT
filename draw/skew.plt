set size ratio 0.50
set key left bottom
set key samplen 2
set key reverse
set key Left
set xlabel "skew ratio"
set xrange [0:1.0]
#set logscale x
#set xtics ("0" 0,"0.25" 1,"0.5" 2,"0.3" 3,"0.4" 4, "0.5" 5, "0.6" 6)
#set terminal postscript eps color enhanced linewidth 3 "Helvetica" 25
set terminal svg linewidth 1


set ylabel "Throughput"
#set yrange [0:28]
set output "1tpmc.svg"
plot "tmp-WOOKONG" using 1:2 title "WOOKONG" w lp lw 2 ps 2 pt 1 lc rgb "light-red" dt 1,"tmp-MAAT" using 1:2 title "MAAT" w lp lw 2 ps 2 pt 2 lc rgb "brown4" dt 2,"tmp-TIMESTAMP" using 1:2 title "T/O" w lp lw 2 ps 2 pt 3 lc rgb "black" dt 1, "tmp-MVCC" using 1:2 title "MVCC" w lp lw 2 ps 2 pt 4 lc rgb "blue" dt 2

set ylabel "Rollback Ratio(%)"
#set yrange [0:100]
set output "2rollback.svg"
plot "tmp-WOOKONG" using 1:3 title "WOOKONG" w lp lw 2 ps 2 pt 1 lc rgb "light-red" dt 1,"tmp-MAAT" using 1:3 title "MAAT" w lp lw 2 ps 2 pt 2 lc rgb "brown4" dt 2,"tmp-TIMESTAMP" using 1:3 title "T/O" w lp lw 2 ps 2 pt 3 lc rgb "black" dt 1, "tmp-MVCC" using 1:3 title "MVCC" w lp lw 2 ps 2 pt 4 lc rgb "blue" dt 2

set ylabel "Distributed Txn Ratio(%)"
#set yrange [0:100]
set output "3distributed.svg"
plot "tmp-WOOKONG" using 1:4 title "WOOKONG" w lp lw 2 ps 2 pt 1 lc rgb "light-red" dt 1,"tmp-MAAT" using 1:4 title "MAAT" w lp lw 2 ps 2 pt 2 lc rgb "brown4" dt 2,"tmp-TIMESTAMP" using 1:4 title "T/O" w lp lw 2 ps 2 pt 3 lc rgb "black" dt 1, "tmp-MVCC" using 1:4 title "MVCC" w lp lw 2 ps 2 pt 4 lc rgb "blue" dt 2


