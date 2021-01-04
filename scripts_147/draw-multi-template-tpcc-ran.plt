set size ratio 0.50
set key left bottom
set key samplen 2
set key reverse
set key Left
set xlabel "# of Threads"
set xrange [16:512]
set logscale x
set xtics ("16" 16,"32" 32,"64" 64,"128" 128,"256" 256, "512" 512)
#set terminal postscript eps color enhanced linewidth 3 "Helvetica" 25
set terminal svg linewidth 1


set ylabel "Throughput"
#set yrange [0:28]
set output "OUTPUT/1tpmc.svg"
plot "RES1/TYPE/result_set.txt" using 1:2 title "LEVEL1" w lp lw 2 ps 2 pt 1 lc rgb "light-red" dt 1,"RES2/TYPE/result_set.txt" using 1:2 title "LEVEL2" w lp lw 2 ps 2 pt 2 lc rgb "brown4" dt 2,"RES3/TYPE/result_set.txt" using 1:2 title "LEVEL3" w lp lw 2 ps 2 pt 3 lc rgb "black" dt 1, "RES4/TYPE/result_set.txt" using 1:2 title "LEVEL4" w lp lw 2 ps 2 pt 4 lc rgb "blue" dt 2

set ylabel "Rollback Ratio(%)"
#set yrange [0:100]
set output "OUTPUT/4rollback.svg"
plot "RES1/TYPE/analyse.txt" using 1:2 title "LEVEL1" w lp lw 2 ps 2 pt 1 lc rgb "light-red" dt 1,"RES2/TYPE/analyse.txt" using 1:2 title "LEVEL2" w lp lw 2 ps 2 pt 2 lc rgb "brown4" dt 2,"RES3/TYPE/analyse.txt" using 1:2 title "LEVEL3" w lp lw 2 ps 2 pt 3 lc rgb "black" dt 1, "RES4/TYPE/analyse.txt" using 1:2 title "LEVEL4" w lp lw 2 ps 2 pt 4 lc rgb "blue" dt 2



set ylabel "Distributed Txn Ratio(%)"
#set yrange [0:100]
set output "OUTPUT/5distributed.svg"
plot "RES1/TYPE/analyse.txt" using 1:3 title "LEVEL1" w lp lw 2 ps 2 pt 1 lc rgb "light-red" dt 1,"RES2/TYPE/analyse.txt" using 1:3 title "LEVEL2" w lp lw 2 ps 2 pt 2 lc rgb "brown4" dt 2,"RES3/TYPE/analyse.txt" using 1:3 title "LEVEL3" w lp lw 2 ps 2 pt 3 lc rgb "black" dt 1, "RES4/TYPE/analyse.txt" using 1:3 title "LEVEL4" w lp lw 2 ps 2 pt 4 lc rgb "blue" dt 2


