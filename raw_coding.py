import pandas as pd
import numpy as np
import time
from datetime import datetime
import time
import cx_Oracle
import math
import warnings
from itertools import compress
from scipy.stats import gmean

warnings.filterwarnings(action='ignore')


conn = cx_Oracle.connect('system', 'lmh104636!', 'localhost:1521/xe')
cur = conn.cursor()

surgery =  pd.read_sql('select * from surgery, sur_stat where surgery.CATEGORY_ID = sur_stat.CATEGORY_ID', conn)
Index =['PID', '수술방ID', '수술일자', '수술부서', '협진수술여부', 'CATEGORY',\
       '첫방', '오전수술', '오후수술', '예상시간', '수술방_지정', '수술총시간', '수술시간표준편차']
sur_df = surgery[Index]
sur_df = sur_df.loc[:,~sur_df.columns.duplicated()]

sur_df[['수술일자']] = sur_df[['수술일자']].apply(lambda x: x.astype('category'))
sur_df[['수술방ID']] = sur_df[['수술방ID']].apply(lambda x: x.astype('category'))
sur_df[['수술방_지정']] = sur_df[['수술방_지정']].apply(lambda x: x.astype('category'))
date_list = sur_df['수술일자'].cat.categories.tolist()
oproom_list = sur_df['수술방ID'].cat.categories.tolist()
first_day = sur_df.iloc[np.where(sur_df.loc[::,'수술일자'] == '11/01/2018')]
first_day.sort_values('수술시간표준편차', ascending=False, inplace=True)

ma_time = ['morning','afternoon']
oper_allocation = dict((key ,dict((key2, list()) for key2 in ma_time)) for key in oproom_list)
oper_time = dict((key, dict((key2, list()) for key2 in ma_time)) for key in oproom_list)
oper_real_time = dict((key, dict((key2, list()) for key2 in ma_time)) for key in oproom_list)
oper_std = dict((key, dict((key2, list()) for key2 in ma_time)) for key in oproom_list)

fo_df = first_day.iloc[np.where(first_day['첫방'] == 1)]
fo_df_nominate = fo_df.iloc[np.where(fo_df['수술방_지정'] != 0)].index.tolist()
fo_list = fo_df.index.tolist()

df_prefer = first_day.iloc[np.where(first_day['수술방_지정'] != 0)]
df_prefer_index = df_prefer.index.tolist()
df_prefer_room  = [df_prefer.xs(r).to_dict()['수술방_지정'] for r in df_prefer_index]

def allocation_time(room_list, dict_instance, i, time='morning'):
    oper_allocation[room_list[i]][time].append(dict_instance)
    oper_time[room_list[i]][time].append(dict_instance['예상시간'])
    oper_real_time[room_list[i]][time].append(dict_instance['수술총시간'])
    oper_std[room_list[i]][time].append(dict_instance['수술시간표준편차'])

# 7:55 ~ 18:00 == 11h == 605min
# consider prefer_instance
for i in range(len(df_prefer)):
    print(i)
    prefer_inst = df_prefer.xs(df_prefer_index[i]).to_dict()
    if prefer_inst['오후수술'] == 1:
        allocation_time(df_prefer_room, prefer_inst, i,'afternoon')
    else:
        if sum(oper_time[df_prefer_room[i]]['morning']) >= 185:
            allocation_time(df_prefer_room, prefer_inst, i,'afternoon')
        else:
            allocation_time(df_prefer_room, prefer_inst, i,'morning')

first_day.drop(df_prefer_index, inplace=True)
first_day.shape
oper_time_sum = {key:sum([sum(value['morning']) ,sum(value['afternoon'])])  for key, value in oper_time.items()}
oper_rtime_sum = {key:sum([sum(value['morning']) ,sum(value['afternoon'])]) for key, value in oper_real_time.items()}

oper_mor_time_sum = {key:sum(value['morning']) for key, value in oper_time.items()}
oper_an_time_sum = {key:sum(value['afternoon']) for key, value in oper_time.items()}
first_available_room = [key for key, value in oper_time_sum.items() if value == 0]
morning_available_room = [key for key, value in oper_mor_time_sum.items() if value <= 185]
afternoon_available_room = [key for key, value in oper_an_time_sum.items() if value <= 300]
available_room = [key for key, value in oper_time_sum.items() if value <= 605]

# afternoon 12:00 ~ 18:00
for i in range(len(fo_df)):
    oper_mor_time_sum = {key:sum(value['morning']) for key, value in oper_time.items()}
    first_available_room = [key for key, value in oper_mor_time_sum.items() if value == 0]
    print(first_available_room)
    fo_inst = fo_df.xs(fo_list[i]).to_dict()
    allocation_time(first_available_room, fo_inst, 0)
first_day.drop(fo_list, inplace=True)
    # oper_allocation[first_available_room[0]].append(fo_inst)
    # oper_time[first_available_room[0]].append(fo_inst['예상시간'])
    # oper_real_tiem[first_available_room[0]].append(fo_inst['예상시간'])
    # oper_std[oproom_list[0]].append(fo_inst['수술시간표준편차'])
residual_operation_index = first_day.index
len(residual_operation_index)
first_day['예상시간'].mean()
for i in range(len(first_day)):
    resid_inst = first_day.xs(residual_operation_index[i]).to_dict()
    oper_mor_time_sum = {key:sum(value['morning']) for key, value in oper_time.items()}
    oper_time_sum = {key:sum([sum(value['morning']) ,sum(value['afternoon'])]) for key, value in oper_time.items()}
    oper_an_time_sum = {key:sum(value['afternoon']) for key, value in oper_time.items()}
    morning_available_room = [key for key, value in oper_mor_time_sum.items() if value <= 185]
    afternoon_available_room = [key for key, value in oper_an_time_sum.items() if value <= 200]
    print("오후가능 수술실:", afternoon_available_room)
    available_room = [key for key, value in oper_time_sum.items() if (value <= 605 and key in afternoon_available_room)]
    print(available_room)
    if available_room[0] not in morning_available_room or resid_inst['오후수술'] == 1:
        allocation_time(available_room, resid_inst, 0, 'afternoon')
    else:
        allocation_time(available_room, resid_inst, 0,'morning')
    first_day.drop(residual_operation_index[i], inplace=True)

oper_mor_time_sum = {key:sum(value['morning']) for key, value in oper_time.items()}
oper_an_time_sum = {key:sum(value['afternoon']) for key, value in oper_time.items()}
first_available_room = [key for key, value in oper_time_sum.items() if value == 0]
morning_available_room = [key for key, value in oper_mor_time_sum.items() if value <= 185]
afternoon_available_room = [key for key, value in oper_an_time_sum.items() if value <= 300]
oper_rtime_sum = {key:sum([sum(value['morning']) ,sum(value['afternoon'])]) for key, value in oper_real_time.items()}

oper_time
oper_real_time
oper_time_sum
oper_rtime_sum


'''
# First operation
fo_df = first_day.iloc[np.where(first_day['첫방'] == 1)]
fo_index = fo_df.index
fo_index
fo_room_no = len(fo_df)
oper_allocation = list()
or_time = list()
or_real_time = list()
or_time_std = list()
for i in range(fo_room_no):
    fo_inst = [fo_df.xs(fo_index[i]).to_dict()]
    fo_time = [fo_inst[0]['예상시간']]
    fo_real_time = [fo_inst[0]['수술총시간']]
    fo_std_time = [fo_inst[0]['수술시간표준편차']]
    oper_allocation.append(fo_inst)
    or_time.append(fo_time)
    or_real_time.append(fo_real_time)
    or_time_std.append(fo_std_time)
    if len(oper_allocation) >= max_bin:
        print(len(oper_allocation))
        break
    print(len(oper_allocation))
first_day.drop(fo_index,inplace=True)
first_day.shape
or_time_sum = [sum(ot) for ot in or_time]
or_real_time_sum = [sum(orr) for orr in or_real_time]
or_time
or_time_sum
# planed_stacked = [round(np.linalg.norm(os),2) if len(os) > 1 else 0 for os in or_time_std]
# planed_stacked = [round(np.linalg.norm(os),2) if len(os) > 1 else 0 for os in or_time_std]
# or_time_cons_ps = [round(or_time_sum[i] + planed_stacked[i],1) for i in range(len(oper_allocation))]

# 오전타임 : 7:55 ~ 12:00 == 4h 10min == 245
# 오전타임의 기준 11시 이전 # 185분
'''
'''
list1 = [1,2,3,4,5]
3 in np.where(np.array(list1) > 3)[0]
np.where(np.array(list1) > 3)[0]
bool_ind = list(np.array(list1) >= 3)
opp_ind = [not b for b in bool_ind]
list(compress(list1, bool_ind))
list(compress(list1, opp_ind))
'''
'''
# not_mor_or = np.where(np.array(or_time_cons_ps) >= 185)
not_mor_or = np.where(np.array(or_time_sum) >= 185)
opr_index = list(range(len(oper_allocation)))
available_bool = [not b for b in list(np.array(or_time_sum) >= 185)]
list_available = list(compress(opr_index, available_bool))
mor_or = first_day.iloc[np.where(first_day['오전수술'] == 1)]
mor_or_list = mor_or.index.tolist()
number_add_room = len(mor_or_list) + len(not_mor_or[0]) - len(oper_allocation)

if number_add_room > 0:
    for i in range(len(mor_or_list)):
        # 오전이 넘어가는 수술방이 있을경우 새로운 수술방을 추가
        print(i)
        if i > len(list_available):
            mo_inst = [mor_or.xs(mor_or_list[i]).to_dict()]
            mo_time = [fo_inst[0]['예상시간']]
            mo_real_time = [fo_inst[0]['수술총시간']]
            mo_std_time = [fo_inst[0]['수술시간표준편차']]
            oper_allocation.append(mo_inst)
            or_time.append(mo_time)
            or_real_time.append(mo_real_time)
            or_time_std.append(mo_std_time)
        else:
            mo_inst = mor_or.xs(mor_or_list[i]).to_dict()
            mo_time = mo_inst['예상시간']
            mo_real_time = mo_inst['수술총시간']
            mo_std_time = mo_inst['수술시간표준편차']
            oper_allocation[i].append(mo_inst)
            or_time[i].append(mo_time)
            or_real_time[i].append(mo_real_time)
            or_time_std[i].append(mo_std_time)
else:
    for i in range(len(mor_or_list)):
        print(i)
        mo_inst = mor_or.xs(mor_or_list[i]).to_dict()
        mo_time = mo_inst['예상시간']
        mo_real_time = mo_inst['수술총시간']
        mo_std_time = mo_inst['수술시간표준편차']
        oper_allocation[list_available[i]].append(mo_inst)
        or_time[list_available[i]].append(mo_time)
        or_real_time[list_available[i]].append(mo_real_time)
        or_time_std[list_available[i]].append(mo_std_time)

first_day.drop(mor_or_list,inplace=True)
or_time_sum = [sum(ot) for ot in or_time]
or_real_time_sum = [sum(orr) for orr in or_real_time]
# planed_stacked = [round(gmean(os),2) if len(os) > 1 else 0 for os in or_time_std]
# or_time
# or_time_sum
# or_real_time_sum
# planed_stacked
# planed_stacked = [round(np.linalg.norm(os),2) if len(os) > 1 else 0 for os in or_time_std]
# planed_stacked
# or_time_cons_ps = [round(or_time_sum[i] + planed_stacked[i],1) for i in range(len(oper_allocation))]
# or_time_cons_ps


# 남은 수술 및 오후 수술 배정
# 오후수술 after 13:00 PM, or_time_sum > 425
residual_operation = first_day.index.tolist()
an_or = first_day.iloc[np.where(first_day['오후수술'] == 1)]
ar_or_index = an_or.index.tolist()

for ro in residual_op:
    or_time_sum = [sum(ot) for ot in or_time]
    opr_index = list(range(len(oper_allocation)))
    avail_bool = [not b for b in list(np.array(or_time_sum) >= 605)]
    an_avail_bool = [not b for b in list(np.array(or_time_sum) >= 425)]
    list_avail_room = list(compress(opr_index, avail_bool))
    an_avail_room
'''
