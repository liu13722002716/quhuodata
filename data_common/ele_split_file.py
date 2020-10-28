'''
Author: your name
Date: 2020-09-15 21:57:57
LastEditTime: 2020-10-28 14:10:53
LastEditors: Please set LastEditors
Description: In User Settings Edit
FilePath: /work/我的工作/薪资/code/ele_split_file.py
'''
import pandas as pd
import os

path = '.'
file_list = os.listdir(path)
csv_list = [x for x in file_list if 'csv' in x]
# data_set = pd.read_csv('./')
print('待分类文件:{}'.format(csv_list))

col_dict = {'骑手ID': '人员ID', 'dc_id': 'BOSS商圈ID', 'city_code': 'BOSS城市代码',
            'supplier_name': '主体', 'supplier_id': 'BOSS主体ID'}
file_name_dict = {'ele_order_num_allowance_monthly.csv': '饿了么完成单量', 'ele_bad_order_monthly.csv': '饿了么差评单详情', 'ele_good_order_monthly.csv': '饿了么好评单详情',
                  'ele_order_time_monthly.csv': '饿了么时段单量', 'ele_overtime_order_monthly.csv': '饿了么超时单详情', 'ele_refund_order_monthly.csv': '饿了么违规扣款及申诉返款详情'}
# 为违规扣款和申诉返款增加城市信息
# 需要从平台下载ele_day_36_std和std_qplus_dc,两个文件保存为xlsx文件格式
day_36_col = ['骑手ID','站点ID','运单号']
day_36 = pd.read_excel('./ele_day_36_std.xlsx', usecols=day_36_col)
order_info = day_36.drop_duplicates(subset='运单号')

refund_cols = ['运单号','违规扣款_金额','违规扣款_账单类型','违规扣款_账单时间','违规扣款_详情','违规扣款_业务类型','申诉返款_金额','申诉返款_账单类型','申诉返款_账单时间','申诉返款_详情','申诉返款_业务类型']
refund = pd.read_csv('./ele_refund_order_monthly.csv', low_memory=False, usecols=refund_cols)
# refund.drop(columns=['站点ID','骑手ID'], inplace=True)
refund_info = pd.merge(refund, order_info, how='left', on='运单号', validate='m:1')
# refund_info.rename(columns={'站点ID':'商圈ID'}, inplace=True)
# refund_info.to_csv('./test.csv')
col_rename = {'dc_id':'BOSS商圈ID', 'vendor_dc_id':'商圈ID', 'supplier_id':'BOSS主体ID', 'city_code':'BOSS城市代码','dc_name':'商圈','city_name':'城市','supplier_name':'主体'}
dc_info_cols = col_rename.keys()
dc_info = pd.read_excel('./std_qplus_dc.xlsx', usecols=dc_info_cols)
dc_info_rename_cols = {'vendor_dc_id': '站点ID','city_name': '城市','dc_name': '商圈名称'}
dc_info.rename(columns=dc_info_rename_cols, inplace=True)
# dc_info.rename(columns=,inplace=True)
dc_info = dc_info.applymap(lambda x: str(x))
refund_info = refund_info.applymap(lambda x: str(x))
refund_dc_info = pd.merge(refund_info, dc_info, how='left', on='站点ID')
refund_dc_info.to_csv('ele_refund_order_monthly.csv', index=False)



print('文件分类中......')
for file_name in csv_list:
    print('处理{}中......'.format(file_name))
    dataset = pd.read_csv(os.path.join(path, file_name), low_memory=False)
    dataset = dataset.rename(columns=col_dict)
    dataset = dataset.applymap(lambda x: str(x))
    city_list = set(dataset['城市'])
    for c in list(city_list):
        print('所属城市:{}'.format(c))
        dataset_temp = dataset[dataset['城市'] == str(c)]
        dst_path = str(path)+'/'+'拆分'+'/'+str(c)
        os.makedirs(dst_path, exist_ok=True)
        file_path = './拆分/{}/{}.xlsx'.format(c, file_name_dict[file_name])
        dataset_temp.to_excel(file_path, index=False, engine='xlsxwriter')
print('{0} over {0}'.format("*"*20))
