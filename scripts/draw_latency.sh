LatencyName="trans_total_run_time \
          trans_process_time trans_process_time_percent \
          trans_2pc_time trans_2pc_time_percent \
          trans_prepare_time trans_prepare_time_percent \
          trans_validate_time trans_validate_time_percent \
          trans_finish_time trans_finish_time_percent \
          trans_commit_time trans_commit_time_percent \
          trans_abort_time trans_abort_time_percent \
          trans_block_time trans_block_time_percent \
          txn_index_time txn_index_time_percent \
          txn_manager_time txn_manager_time_percent \
          lat_l_loc_cc_time lat_l_loc_cc_time_percent \
          trans_init_time trans_init_time_percent \
          trans_get_access_time trans_get_access_time_percent \
          trans_store_access_time trans_store_access_time_percent \
          trans_get_row_time trans_get_row_time_percent \
          trans_cur_row_copy_time trans_cur_row_copy_time_percent \
          trans_cur_row_init_time trans_cur_row_init_time_percent \
          txn_process_time txn_process_time_percent \
          trans_benchmark_compute_time trans_benchmark_compute_time_percent"

LatencyName+=" trans_process_network trans_process_network_percent \
              trans_validation_network trans_validation_network_percent \
              trans_commit_network trans_commit_network_percent \
              trans_abort_network trans_abort_network_percent"

cc=$1
LatencyNum=$2

#Replace some algorithm-specific monitoring
if [[ "${cc}" == 'MVCC' ]]
then
  LatencyName+=" trans_mvcc_clear_history trans_mvcc_clear_history_percent"
  LatencyNum+=" "$3
  cp draw/draw_latency_mvcc.dot draw_latency_tmp.dot
elif [[ "${cc}" == 'DLI_OCC' ]] || [[ "${cc}" == 'DLI_DTA3' ]] || [[ "${cc}" == 'DLI_DTA' ]] || [[ "${cc}" == 'DLI_DTA2' ]]
then
  LatencyName+=" dli_init_time dli_init_time_percent \
          dli_lock_time dli_lock_time_percent \
          dli_check_conflict_time dli_check_conflict_time_percent \
          dli_final_validate dli_final_validate_percent"
  LatencyNum+=" "$3
  cp draw/draw_latency_dli.dot draw_latency_tmp.dot
else
  cp draw/draw_latency.dot draw_latency_tmp.dot
fi

#Split the LatencyNum and LatencyName
OLD_IFS="$IFS"
IFS=" "
LName=($LatencyName)
LNum=($LatencyNum)
IFS="$OLD_IFS"
#Replace monitoring common to all algorithms

count=${#LNum[@]}
count=`expr $count - 1`
for i in $(seq 0 $count)
do
  echo "s/${LName[$i]} /${LNum[$i]}/g"
  sed -i "s/${LName[$i]} /${LNum[$i]}/g" draw_latency_tmp.dot
done
