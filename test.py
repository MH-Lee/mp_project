from bin_class import BinPacking
import numpy as np

bin = BinPacking('11/05/2018', portfolio_effect=True)

oper_allocation, oper_time, oper_real_time, oper_std, planned_stack = bin.allocate_with_condition()
oper_time_sum = {key:sum([sum(value['morning']) ,sum(value['afternoon'])])  for key, value in oper_time.items()}
oper_rtime_sum = {key:sum([sum(value['morning']) ,sum(value['afternoon'])])  for key, value in oper_real_time.items()}
oper_time_count = {key:sum([len(value['morning']) ,len(value['afternoon'])])  for key, value in oper_time.items()}
sum(oper_time_count.values())

# std_gmean = dict((key, round(gmean(list(value['morning']+value['afternoon'])),2) if np.isnan(round(gmean(list(value['morning']+value['afternoon'])),2)) == False else 0) for key, value in oper_std.items())
used_bin = len([key for key, val in oper_time_count.items() if val != 0])
used_bin
oper_time_sum
oper_rtime_sum
planned_stack

oper_allocation[65]['afternoon']
oper_time

oper_allocation
oper_time
oper_real_time
