
for concurrency_control in MVCC MAAT
do
for theta_value in 0.0 0.25 0.5 0.55 0.6 0.65 0.7 0.75 0.8 0.9
do
  grep tput output_${concurrency_control}_${theta_value}.txt < output.txt
done
done
