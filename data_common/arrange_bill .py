'''
Author: your name
Date: 2020-09-28 10:49:59
LastEditTime: 2020-09-29 20:14:13
LastEditors: Please set LastEditors
Description: In User Settings Edit
FilePath: /work/我的工作/薪资/code/arrange_bill.py
'''
import pandas as pd
import os 
import shutil

print('{0} arranging file {0}'.format('*'*20))
# path='./代理商代码.xlsx'

city_code = pd.read_excel('./代理商代码.xlsx')
city_dict = dict()
for i, c in zip(city_code['加盟商ID'], city_code['城市']):
    city_dict[i] = c


path = '.'
path2 = './city_folder'

file_list = os.listdir(path)
print(city_dict, '\n', file_list)

bill_list = [x for x in file_list if '.csv' in x]

for i in bill_list:
    print(i)
    city_code = int(i.split('-')[0])
    dst_path = path2+'/{}'.format(city_dict[city_code])
    if os.path.exists(dst_path) != True:
        os.makedirs(dst_path)
    shutil.copyfile(path+'/'+i, dst_path+'/'+i)
print("{0} over {0}".format("*"*20))
