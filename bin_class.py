import pandas as pd
import numpy as np
from datetime import datetime
import cx_Oracle
import math, time, warnings
from itertools import compress
from scipy.stats import gmean

warnings.filterwarnings(action='ignore')

INDEX =['PID', '수술방ID', '수술일자', '수술부서', '협진수술여부', 'CATEGORY',\
       '첫방', '오전수술', '오후수술', '예상시간', '수술방_지정', '수술총시간', '수술시간표준편차']

class BinPacking:

    def __init__(self, date, portfolio_effect=False):
        self.portfolio_effect = portfolio_effect
        self.conn = cx_Oracle.connect('system', 'lmh104636!', 'localhost:1521/xe')
        self.cur = self.conn.cursor()
        self.surgery =  pd.read_sql('select * from surgery, sur_stat where surgery.CATEGORY_ID = sur_stat.CATEGORY_ID', self.conn)
        self.sur_df = self.surgery[INDEX]
        self.sur_df = self.sur_df.loc[:,~self.sur_df.columns.duplicated()]
        self.sur_df[['수술일자']] = self.sur_df[['수술일자']].apply(lambda x: x.astype('category'))
        self.sur_df[['수술방ID']] = self.sur_df[['수술방ID']].apply(lambda x: x.astype('category'))
        self.sur_df[['수술방_지정']] = self.sur_df[['수술방_지정']].apply(lambda x: x.astype('category'))
        self.date_list = self.sur_df['수술일자'].cat.categories.tolist()
        oproom_list = self.sur_df['수술방ID'].cat.categories.tolist()

        ma_time = ['morning','afternoon']
        self.oper_allocation = dict((key ,dict((key2, list()) for key2 in ma_time)) for key in oproom_list)
        self.oper_time = dict((key, dict((key2, list()) for key2 in ma_time)) for key in oproom_list)
        self.oper_real_time = dict((key, dict((key2, list()) for key2 in ma_time)) for key in oproom_list)
        self.oper_std = dict((key, dict((key2, list()) for key2 in ma_time)) for key in oproom_list)
        self.one_day = self.sur_df.iloc[np.where(self.sur_df.loc[::,'수술일자'] == date)]
        self.one_day.sort_values('수술시간표준편차', ascending=False, inplace=True)

        self.fo_df = self.one_day.iloc[np.where(self.one_day['첫방'] == 1)]
        self.df_prefer = self.one_day.iloc[np.where(self.one_day['수술방_지정'] != 0)]
        self.fo_pref_index = self.fo_df.iloc[np.where(self.fo_df['수술방_지정'] != 0)].index.tolist()
        self.fo_index = [li1 for li1 in self.fo_df.index.tolist() if li1 not in self.fo_pref_index]
        self.df_prefer_index = [li2 for li2 in self.df_prefer.index.tolist() if li2 not in self.fo_pref_index]
        self.df_prefer_room  = [self.df_prefer.xs(r).to_dict()['수술방_지정'] for r in self.df_prefer_index]
        print("bin packing ready! date : {}".format(date))

    def allocation_time(self, room_list, dict_instance, i, time='morning'):
        self.oper_allocation[room_list[i]][time].append(dict_instance)
        self.oper_time[room_list[i]][time].append(dict_instance['예상시간'])
        self.oper_real_time[room_list[i]][time].append(dict_instance['수술총시간'])
        self.oper_std[room_list[i]][time].append(dict_instance['수술시간표준편차'])

    def oper_fit(self, index, func):
        if func == 'first':
            for i in range(len(index)):
                oper_mor_time_sum = {key:sum(value['morning']) for key, value in self.oper_time.items()}
                first_available_room = [key for key, value in oper_mor_time_sum.items() if value == 0]
                fo_pref_inst = self.fo_df.xs(index[i]).to_dict()
                print(fo_pref_inst)
                self.allocation_time(first_available_room, fo_pref_inst, 0)
            self.one_day.drop(index, inplace=True)
            print(self.one_day.shape)
            print(self.oper_time)
        elif func == "prefer":
            for i in range(len(index)):
                print(i)
                print("수술 index:",index[i])
                prefer_inst = self.df_prefer.xs(index[i]).to_dict()
                if prefer_inst['오후수술'] == 1:
                    self.allocation_time(self.df_prefer_room, prefer_inst, i,'afternoon')
                else:
                    if sum(self.oper_time[self.df_prefer_room[i]]['morning']) >= 185:
                        if (prefer_inst['오전수술'] == 1) and (sum(self.oper_time[df_prefer_room[i]]['morning']) <= 215):
                            self.allocation_time(self.df_prefer_room, prefer_inst, i,'morning')
                        else:
                            self.allocation_time(self.df_prefer_room, prefer_inst, i,'afternoon')
                    else:
                        self.allocation_time(self.df_prefer_room, prefer_inst, i,'morning')
            self.one_day.drop(index, inplace=True)
            print(self.one_day.shape)
        elif func == "residual":
            residual_operation_index = self.one_day.index
            print(len(residual_operation_index))
            for i in range(len(residual_operation_index)):
                resid_inst = self.one_day.xs(residual_operation_index[i]).to_dict()
                allocate_oper_time = int(resid_inst['예상시간'])
                oper_mor_time_sum = {key:sum(value['morning']) for key, value in self.oper_time.items()}
                oper_time_sum = {key:sum([sum(value['morning']) ,sum(value['afternoon'])]) for key, value in self.oper_time.items()}
                oper_an_time_sum = {key:sum(value['afternoon']) for key, value in self.oper_time.items()}
                morning_available_room = [key for key, value in oper_mor_time_sum.items() if value <= 185]
                afternoon_available_room = [key for key, value in oper_an_time_sum.items() if value <= 200]
                if self.portfolio_effect == True:
                    # std_gmean = dict((key, round(gmean(list(value['morning']+value['afternoon'])),2) if np.isnan(round(gmean(list(value['morning']+value['afternoon'])),2)) == False else 0) for key, value in self.oper_std.items())
                    std_gmean = self.planned_stack = dict((key, np.sqrt(np.dot(np.square(list(value['morning']+value['afternoon'])), [round(i/sum(list(value['morning']+value['afternoon'])),2) for i in list(value['morning']+value['afternoon'])]))) for key, value in self.oper_std.items())
                    available_room = [key for key, value in oper_time_sum.items() if ((value + int(std_gmean[key]) + allocate_oper_time <= 605 and key in afternoon_available_room) or oper_mor_time_sum[key] == 0)]
                else:
                    available_room = [key for key, value in oper_time_sum.items() if ((value + allocate_oper_time <= 605 and key in afternoon_available_room) or oper_mor_time_sum[key] == 0)]
                print("오전가능 수수실:", morning_available_room)
                print("오후가능 수술실:", afternoon_available_room)
                print("전체가능 수술실:", available_room)
                # if available_room[0] not in morning_available_room or resid_inst['오후수술'] == 1:
                #     self.allocation_time(available_room, resid_inst, 0, 'afternoon')
                if resid_inst['오전수술'] == 1:
                    self.allocation_time(morning_available_room, resid_inst, 0, 'morning')
                elif (available_room[0] not in morning_available_room) or resid_inst['오후수술'] == 1:
                    self.allocation_time(available_room, resid_inst, 0, 'afternoon')
                else:
                    self.allocation_time(available_room, resid_inst, 0,'morning')
            # self.planned_stack = dict((key, round(gmean(list(value['morning']+value['afternoon'])),2) if np.isnan(round(gmean(list(value['morning']+value['afternoon'])),2)) == False else 0) for key, value in self.oper_std.items())
            self.planned_stack = dict((key, np.sqrt(np.dot(np.square(list(value['morning']+value['afternoon'])), [round(i/sum(list(value['morning']+value['afternoon'])),2) for i in list(value['morning']+value['afternoon'])]))) for key, value in self.oper_std.items())
            self.one_day.drop(residual_operation_index, inplace=True)
            print(self.one_day.shape)

    def allocate_with_condition(self):
        self.oper_fit(self.fo_pref_index, func="first-pefer")
        self.oper_fit(self.fo_index, func="first")
        self.oper_fit(self.df_prefer_index, func="prefer")
        self.oper_fit(self.fo_index, func="residual")
        return self.oper_allocation, self.oper_time, self.oper_real_time, self.oper_std, self.planned_stack
