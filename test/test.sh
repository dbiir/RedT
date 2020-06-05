
for concurrency_control in MVCC MAAT WOOKONG
do  
    #修改CC_ALG
    sed -e '/CC_ALG/d' -e 'a #define CC_ALG ${concurrency_control}' config.h
for theta_value in 0.0 0.25 0.5 0.55 0.6 0.65 0.7 0.75 0.8 0.9
do
    ./rundb -zipf${theta_value} &
    #连接146
    ssh -p22 root@10.77.110.146 cd /root/deneva-master sed -e '/CC_ALG/d' -e 'a #define CC_ALG ${concurrency_control}' config.h ./runcl -zipf${theta_value} &
    #输出到
    > output_${concurrency_control}_${theta_value}.txt
    
done
done

