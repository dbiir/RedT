#!/bin/bash
set -x

BASE_PATH="/root/deneva-code"

homepage="${BASE_PATH}/results/"
files=$(ls -t $homepage | grep r | grep -v patch)

cd $homepage
main=index.html
echo '<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><title>DBT</title></head><body><h1>deneva实验结果汇总</h1><ul>' > $main
echo '<hr />' >> $main
for i in $files; do
  if [ $i != 'index.html' ] && [ $i != 'getindex.sh' ] && [ $i != 'main.html' ]; then
  # todo add the test desc
  echo '<h2>'$i'</h2>' >> $main
      if [[ -f "$i/1tpmc.svg" ]]; then
        # firstline=$(grep -n "测试情况概述" $i/index.html | cut -d ":" -f 1)
        # endline=$(grep -n "测试结果对比" $i/index.html | cut -d ":" -f 1)
        # timeline=$(grep -n "测试时间" $i/index.html)
        # RAW_TIME=(${timeline//'/'/ })
        # RAW_TIME2=(${RAW_TIME[1]//'<'/ })
        # firstline=`expr $firstline + 1`
        # endline=`expr $endline - 1`
        # sed -n "${firstline},${endline}p" $i/index.html >> $main
        # echo "测试时间:"${RAW_TIME2:0:4}-${RAW_TIME2:4:2}-${RAW_TIME2:6:2}-${RAW_TIME2:8:2}:${RAW_TIME2:10:2}:${RAW_TIME2:12} >> $main

	files=$(ls $i | grep .svg)
	for j in $files; do
    		echo '<img width="33%" src="./'$i'/'$j'">' >> $main
	done
        #echo '<li><a href="'$i'">'$i'</a></li>' >> $main
        echo '<hr />' >> $main
      fi
  fi
done
echo '</ul></body></html>' >> $main
echo "homepage has been generated."
