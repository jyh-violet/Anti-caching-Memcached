#!/bin/bash

#KEY_TYPE=monoint
#for WORKLOAD_TYPE in e c a; do
#  echo workload${WORKLOAD_TYPE} > workload_config.inp
#  echo ${KEY_TYPE} >> workload_config.inp
#  python gen_workload.py workload_config.inp
#  mv workloads/load_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/mono_inc_load${WORKLOAD_TYPE}_zipf_int_100M.dat
#  mv workloads/txn_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/mono_inc_txns${WORKLOAD_TYPE}_zipf_int_100M.dat
#done

KEY_TYPE=randint
for WORKLOAD_TYPE in a b c d e g; do
  echo workload${WORKLOAD_TYPE} > workload_config.inp
  echo ${KEY_TYPE} >> workload_config.inp
  python2.7 gen_workload.py workload_config.inp
  mv data/load_${KEY_TYPE}_workload${WORKLOAD_TYPE} data/load${WORKLOAD_TYPE}_zipf_int_200M.dat
  mv data/txn_${KEY_TYPE}_workload${WORKLOAD_TYPE} data/txns${WORKLOAD_TYPE}_zipf_int_200M.dat
done

