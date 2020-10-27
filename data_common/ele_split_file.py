'''
Author: your name
Date: 2020-09-15 21:57:57
LastEditTime: 2020-10-22 16:02:28
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
